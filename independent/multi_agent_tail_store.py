"""Local fixture tickets, stop, reconciliation and immutable terminal outbox.

No production payload hook or transport is exposed.
"""
from contextlib import contextmanager,closing
import json
import math
from pathlib import Path
import sqlite3
import threading
import time
import uuid
from functools import wraps

from core import Error
from node_config import digest
from multi_agent_lifecycle import encoded,KIND
from multi_agent_tail import TAIL,MultiAgentTail,sealed,unpack,authority
from multi_agent_installation import fact_for,installation_receipt
from multi_agent_execution import execution_receipt

LOCAL='''
CREATE TABLE local_tail(
 id INTEGER PRIMARY KEY CHECK(id=1),authority TEXT NOT NULL,
 installation TEXT,execution TEXT,stop TEXT,stop_receipt TEXT,
 reconciliation TEXT,final TEXT,blocked INTEGER NOT NULL DEFAULT 0);
CREATE TABLE tail_tickets(
 ticket INTEGER PRIMARY KEY AUTOINCREMENT,direction INTEGER NOT NULL CHECK(direction IN (0,1)),
 bytes INTEGER NOT NULL CHECK(bytes>0),successful INTEGER NOT NULL DEFAULT 0 CHECK(successful IN (0,1)));
CREATE TABLE tail_ordinary(
 sequence INTEGER PRIMARY KEY,body TEXT NOT NULL,state TEXT NOT NULL
 CHECK(state IN ('pending','acked','fenced_not_accepted')));
'''

class ExecutionExpired(Error):pass

def serialized(method):
    @wraps(method)
    def run(self,*args,**kwargs):
        with self.activity_lock:
            try:return method(self,*args,**kwargs)
            except (sqlite3.DatabaseError,ExecutionExpired):
                if method.__name__=='freeze':raise  # Already stopped; retry the same durable evidence.
                self.failed=True
                try:self.stop()
                except Exception:pass
                raise
    return run


class MultiTailStore:
    def __init__(self,harness,auth):
        from multi_agent_test_runtime import gate_for
        if gate_for(harness.registry) is not harness:raise Error('tail本地store仅允许隔离harness',403)
        self.h=harness;self.auth=json.loads(encoded(auth));self.value=self.auth['base_binding']
        with harness.registry.connect() as db:
            if authority(db,self.value)!=self.auth:raise Error('tail本地授权非原主控记录',409)
        ident=self.auth['ledger_id']
        self.path=Path(harness.temp.name)/'tail-v1'/ident/'local.db'
        self.path.parent.mkdir(parents=True,exist_ok=True)
        self.recovered=self.path.exists();self.inflight=set();self.io_lock=threading.RLock()
        self.activity_lock=threading.RLock()
        self.deadline=time.monotonic()+max(0,self.auth['execute_until']-time.time())
        self.failed=False
        if not self.recovered:
            with self.db() as db:
                db.executescript(LOCAL)
                db.execute('INSERT INTO local_tail(id,authority) VALUES(1,?)',(encoded(self.auth),))
        with self.db() as db:self._row(db)
        # Additive receipt metadata; never manufacture a final on an E1 reopen.
        with self.db() as db:db.execute('''CREATE TABLE IF NOT EXISTS tail_delivery(
            id INTEGER PRIMARY KEY CHECK(id=1),final_digest TEXT NOT NULL,receipt TEXT NOT NULL,closed TEXT)''')

    @contextmanager
    def db(self):
        with closing(sqlite3.connect(self.path,timeout=0.1)) as db:
            db.row_factory=sqlite3.Row
            db.execute('PRAGMA synchronous=FULL');db.execute('PRAGMA foreign_keys=ON')
            with db:
                db.execute('BEGIN IMMEDIATE')
                yield db

    def _row(self,db):
        row=db.execute('SELECT * FROM local_tail WHERE id=1').fetchone()
        if row is None or row['authority']!=encoded(self.auth):raise Error('本地tail身份缺失/损坏',409)
        return row

    def acknowledge(self,installation,execution):
        fact=fact_for(dict(self.value,tail_authority=self.auth),self.auth['execution_id'])
        if installation!=installation_receipt(self.auth['ledger_id'],fact) or execution!=execution_receipt(self.auth['ledger_id'],fact):
            raise Error('tail本地ACK与原安装执行不匹配',409)
        with self.db() as db:
            row=self._row(db)
            for key,body in (('installation',installation),('execution',execution)):
                if row[key] is not None and row[key]!=encoded(body):raise Error('tail安装执行ACK不可替换',409)
            db.execute('UPDATE local_tail SET installation=?,execution=? WHERE id=1',(encoded(installation),encoded(execution)))

    def _executed(self,row):
        if row['installation'] is None or row['execution'] is None:raise Error('tail缺原持久安装/执行ACK',409)
        fact=fact_for(dict(self.value,tail_authority=self.auth),self.auth['execution_id'])
        if (json.loads(row['installation'])!=installation_receipt(self.auth['ledger_id'],fact)
                or json.loads(row['execution'])!=execution_receipt(self.auth['ledger_id'],fact)):
            raise Error('tail本地执行链损坏',409)
        return fact

    @staticmethod
    def _totals(db):
        used=[0,0];successful=[0,0]
        for row in db.execute('SELECT * FROM tail_tickets'):
            used[row['direction']]+=row['bytes']
            if row['successful']:successful[row['direction']]+=row['bytes']
        return used,successful

    @staticmethod
    def _reservations(db):
        return [dict(r) for r in db.execute('SELECT ticket,direction,bytes FROM tail_tickets ORDER BY ticket')]

    @serialized
    def reserve(self,direction,count):
        if type(direction) is not int or direction not in (0,1) or type(count) is not int or not 0<count<=2**63-1:
            raise Error('tail写入预留字段无效')
        now=time.time()
        if (not math.isfinite(now) or not self.value['issued_at']<=now<self.auth['execute_until']
                or time.monotonic()>=self.deadline):
            self.stop();raise Error('tail执行已截止或时钟异常',409)
        if self.recovered or self.failed:raise Error('重开/失败store不得恢复执行',409)
        with self.db() as db:
            row=self._row(db);self._executed(row)
            now=time.time()
            if (not math.isfinite(now) or not self.value['issued_at']<=now<self.auth['execute_until']
                    or time.monotonic()>=self.deadline):
                raise ExecutionExpired('等待事务期间执行已截止',409)
            if row['stop'] is not None or row['blocked']:raise Error('tail停止栅栏已成立',409)
            used,_=self._totals(db)
            if sum(used)+count>self.value['reserved_bytes']:raise Error('tail写前预留超限',409)
            limits=self.value['client'].get('cycle_budget',{})
            limit=limits.get(('upload_bytes','download_bytes')[direction])
            baseline=self.value['client'].get('cycle_usage',{}).get(('upload_bytes','download_bytes')[direction],0)
            if limit is not None and used[direction]+baseline+count>limit:raise Error('tail方向预留超限',409)
            cursor=db.execute('INSERT INTO tail_tickets(direction,bytes) VALUES(?,?)',(direction,count))
            ticket=cursor.lastrowid
        self.inflight.add(ticket)
        return ticket

    @serialized
    def complete(self,ticket):
        if ticket not in self.inflight:raise Error('未知/重开写入不得补造成功证据',409)
        with self.db() as db:
            row=self._row(db)
            if row['final'] is not None:raise Error('已冻结不得改写计数',409)
            found=db.execute('SELECT * FROM tail_tickets WHERE ticket=?',(ticket,)).fetchone()
            if found is None:raise Error('写入ticket缺失',409)
            if row['stop'] and ticket>unpack(json.loads(row['stop']))['fence_sequence']:
                raise Error('栅栏后ticket非法',409)
            db.execute('UPDATE tail_tickets SET successful=1 WHERE ticket=?',(ticket,))
        self.inflight.discard(ticket)

    @serialized
    def abandon(self,ticket):self.inflight.discard(ticket)

    @serialized
    def ordinary(self):
        with self.db() as db:
            row=self._row(db);self._executed(row)
            if self.recovered or self.failed or row['stop'] or row['blocked']:raise Error('停止/恢复后不得构造ordinary',409)
            pending=db.execute("SELECT body FROM tail_ordinary WHERE state='pending'").fetchone()
            if pending:return json.loads(pending['body'])
            _,successful=self._totals(db)
            seq=db.execute('SELECT coalesce(max(sequence),0)+1 FROM tail_ordinary').fetchone()[0]
            v=self.value
            body=sealed(dict(ledger_kind=KIND,ledger_id=self.auth['ledger_id'],execution_id=self.auth['execution_id'],
                session_id=v['session_id'],policy_id=v['policy_id'],binding_digest=v['digest'],operation='ordinary',
                authority_digest=self.auth['body_digest'],sequence=seq,upload_bytes=successful[0],download_bytes=successful[1]))
            db.execute("INSERT INTO tail_ordinary VALUES(?,?,'pending')",(seq,encoded(body)))
            return body

    def send_ordinary(self,token):
        from multi_agent_meter import MultiAgentMeter
        with self.io_lock:
            body=self.ordinary()
            result=MultiAgentMeter(self.h.registry).report(token,body)
            if (result.get('accepted') is not True or result.get('execution_id')!=self.auth['execution_id']
                    or result.get('body_digest')!=body['body_digest'] or result.get('sequence')!=body['sequence']
                    or result.get('ledger_id')!=self.auth['ledger_id'] or result.get('ready_for_execution') is not False):
                raise Error('ordinary原ACK不匹配',409)
            with self.db() as db:
                row=self._row(db)
                if row['final'] is None:db.execute("UPDATE tail_ordinary SET state='acked' WHERE sequence=?",(body['sequence'],))
            return result

    def stop(self):
        with self.activity_lock:return self._stop()

    def _stop(self):
        """Persist the local fence first; actual stop happens even if storage fails."""
        error=None;intent=None
        try:
            with self.db() as db:
                row=self._row(db);self._executed(row)
                if row['stop']:
                    intent=json.loads(row['stop']);unpack(intent)
                else:
                    now=time.time()
                    if not math.isfinite(now) or now<self.value['issued_at']:
                        self.failed=True;db.execute('UPDATE local_tail SET blocked=1 WHERE id=1')
                        error=Error('停止时钟异常，禁止冻结',409)
                    else:
                        seq=db.execute('SELECT coalesce(max(ticket),0) FROM tail_tickets').fetchone()[0]
                        intent=sealed(dict(execution_id=self.auth['execution_id'],authority_digest=self.auth['body_digest'],
                            freeze_intent_id=str(uuid.uuid4()),stop_at=min(now,self.auth['execute_until']),fence_sequence=seq,
                            ticket_reservation_digest=digest(self._reservations(db))))
                        db.execute('UPDATE local_tail SET stop=? WHERE id=1',(encoded(intent),))
        except Exception as exc:
            self.failed=True;error=exc
        # This path uses the fixture process handle, never current network auth.
        try:
            runtime=self.h.allocations.get(self.auth['ledger_id'])
            if runtime is None or runtime.fixture_execution_id!=self.auth['execution_id']:
                raise Error('缺少原runtime实例，停止不可猜测',409)
            process=runtime.fixture_process
            self.h.stop_sync(self.auth['ledger_id'])
            if process.returncode is None:raise Error('原runtime尚未停止',409)
            receipt=sealed(dict(execution_id=self.auth['execution_id'],authority_digest=self.auth['body_digest'],
                instance_id=runtime.fixture_instance_id,pid=process.pid,stopped=True,
                stop_intent_digest=intent['body_digest'] if intent else None))
            if error is None:
                with self.db() as db:
                    row=self._row(db)
                    if row['stop_receipt'] and row['stop_receipt']!=encoded(receipt):raise Error('原停止回执冲突',409)
                    db.execute('UPDATE local_tail SET stop_receipt=? WHERE id=1',(encoded(receipt),))
        except Exception as exc:
            self.failed=True
            try:
                with self.db() as db:db.execute('UPDATE local_tail SET blocked=1 WHERE id=1')
            except Exception:pass
            if error is None:error=exc
        if error is not None:raise error
        return intent

    def _prepare_request(self,intent):
        return dict(ledger_kind=KIND,ledger_id=self.auth['ledger_id'],execution_id=self.auth['execution_id'],
            session_id=self.value['session_id'],authority_digest=self.auth['body_digest'],
            freeze_intent_id=intent['freeze_intent_id'],stop_intent_digest=intent['body_digest'])

    def reconcile(self,token):
        if not self.io_lock.acquire(blocking=False):raise Error('ordinary发送未排空，保持blocked',409)
        try:
            with self.db() as db:
                row=self._row(db);self._executed(row)
                if row['final']:raise Error('已冻结不可重新对账改正文',409)
                if not row['stop'] or not row['stop_receipt'] or row['blocked'] or self.failed:
                    raise Error('缺持久停止证据，禁止对账',409)
                intent=json.loads(row['stop']);unpack(intent);unpack(json.loads(row['stop_receipt']))
                requests=[json.loads(r['body']) for r in db.execute('SELECT body FROM tail_ordinary ORDER BY sequence')]
            master=MultiAgentTail(self.h.registry);request=self._prepare_request(intent)
            fence=master.prepare(token,request);unpack(fence)
            if fence['request']!=request:raise Error('主控栅栏回执不匹配',409)
            results=[]
            for ordinary in requests:
                unpack(ordinary)
                query={k:ordinary[k] for k in ('ledger_kind','ledger_id','execution_id','session_id','authority_digest','operation','sequence','body_digest')}
                response=master.query(token,query);unpack(response)
                if response['query']!=query or response['fence_digest']!=fence['body_digest']:
                    raise Error('普通对账与栅栏不匹配',409)
                if response['accepted'] and response['ordinary']!=ordinary:raise Error('普通原正文不匹配',409)
                results.append(response)
            accepted=[q['ordinary'] for q in results if q['accepted']]
            last=accepted[-1] if accepted else dict(sequence=0,upload_bytes=0,download_bytes=0)
            if any(last[k]!=fence[k] for k in ('sequence','upload_bytes','download_bytes')):
                raise Error('主控存在本地未知ordinary，禁止冻结',409)
            proof=sealed(dict(fence=fence,results=results,requests=requests))
            with self.db() as db:
                row=self._row(db)
                if row['final']:raise Error('并发已冻结',409)
                if row['reconciliation'] and row['reconciliation']!=encoded(proof):raise Error('原对账结果不可替换',409)
                for ordinary,response in zip(requests,results):
                    db.execute('UPDATE tail_ordinary SET state=? WHERE sequence=?',
                        ('acked' if response['accepted'] else 'fenced_not_accepted',ordinary['sequence']))
                db.execute('UPDATE local_tail SET reconciliation=? WHERE id=1',(encoded(proof),))
            return proof
        finally:self.io_lock.release()

    def _proof(self,db,row):
        intent=json.loads(row['stop']);unpack(intent)
        stop=json.loads(row['stop_receipt']);unpack(stop)
        proof=json.loads(row['reconciliation']);unpack(proof)
        fence=proof['fence'];unpack(fence)
        if (fence['request']!=self._prepare_request(intent) or stop['stop_intent_digest']!=intent['body_digest']
                or stop['execution_id']!=self.auth['execution_id'] or stop['authority_digest']!=self.auth['body_digest']
                or stop['stopped'] is not True):raise Error('冻结原停止/栅栏冲突',409)
        rows=list(db.execute('SELECT * FROM tail_ordinary ORDER BY sequence'))
        if proof['requests']!=[json.loads(r['body']) for r in rows] or len(proof['results'])!=len(rows):
            raise Error('冻结普通对账清单损坏',409)
        accepted=[]
        for row,body,response in zip(rows,proof['requests'],proof['results']):
            unpack(body);unpack(response)
            query={k:body[k] for k in ('ledger_kind','ledger_id','execution_id','session_id','authority_digest','operation','sequence','body_digest')}
            if (response['query']!=query or response['fence_digest']!=fence['body_digest']
                    or type(response['accepted']) is not bool
                    or response['ordinary']!=(body if response['accepted'] else None)
                    or row['state']!=('acked' if response['accepted'] else 'fenced_not_accepted')):
                raise Error('冻结普通回执损坏',409)
            if response['accepted']:accepted.append(body)
        last=accepted[-1] if accepted else dict(sequence=0,upload_bytes=0,download_bytes=0)
        if (any(last[k]!=fence[k] for k in ('sequence','upload_bytes','download_bytes'))
                or fence['ordinary']!=[dict(sequence=b['sequence'],body_digest=b['body_digest']) for b in accepted]):
            raise Error('冻结主控高水位损坏',409)
        reservations=self._reservations(db)
        if (type(intent['fence_sequence']) is not int or intent['fence_sequence']<0
                or any(r['ticket']>intent['fence_sequence'] for r in reservations)
                or len(reservations)!=intent['fence_sequence']
                or digest(reservations)!=intent['ticket_reservation_digest']):
            raise Error('冻结ticket超出原停止栅栏或清单损坏',409)
        self._master_proof(proof)
        return intent,stop,proof,fence

    def _master_proof(self,proof):
        """Fixture-only independent read, never trust a locally rehashed receipt.

        This does not authenticate a network submission or write any master state.
        A future remote implementation needs a protected receipt anchor of its own.
        """
        from multi_agent_test_runtime import gate_for
        from multi_agent_meter import execution_chain
        from multi_agent_tail import validated_fence
        from usage_accounting import require_source,MULTI
        if gate_for(self.h.registry) is not self.h:raise Error('tail独立快照读取gate关闭',403)
        with self.h.registry.connect() as db:
            db.execute('BEGIN')
            allocation=db.execute('SELECT * FROM usage_multi_agent_allocations WHERE allocation_id=?',
                                  (self.auth['ledger_id'],)).fetchone()
            if allocation is None:raise Error('原主控allocation缺失',409)
            value,_,_=execution_chain(db,allocation)
            if value.get('tail_authority')!=self.auth:raise Error('原主控授权改变',409)
            require_source(db,MULTI,allocation)
            fence=validated_fence(db,self.auth['ledger_id'],self.auth)
            if fence!=proof['fence']:raise Error('本地对账非原主控持久快照',409)
            for response in proof['results']:
                query=response['query']
                row=db.execute('SELECT * FROM usage_multi_tail_ordinary WHERE allocation_id=? AND sequence=?',
                               (self.auth['ledger_id'],query['sequence'])).fetchone()
                if row is not None and row['digest']!=query['body_digest']:
                    raise Error('原主控ordinary正文冲突',409)
                expected=sealed(dict(schema=TAIL,query=query,accepted=row is not None,
                    ordinary=json.loads(row['body']) if row else None,fence_digest=fence['body_digest'],
                    ready_for_execution=False))
                if response!=expected:raise Error('本地对账非原主控精确结果',409)

    @serialized
    def freeze(self):
        with self.db() as db:
            row=self._row(db);fact=self._executed(row)
            if self.failed or row['blocked'] or self.inflight or not all(row[k] for k in ('stop','stop_receipt','reconciliation')):
                raise Error('冻结缺持久停止/排空/对账证据',409)
            intent,stop,proof,fence=self._proof(db,row)
            frozen=json.loads(row['final']) if row['final'] else None
            if frozen:unpack(frozen)
            now=frozen['freeze_completed_at'] if frozen else time.time()
            if not intent['stop_at']<=now<=self.auth['settle_until']:raise Error('冻结时间超界或回拨',409)
            used,success=self._totals(db)
            if sum(used)>self.value['reserved_bytes'] or any(success[i]<fence[k] for i,k in enumerate(('upload_bytes','download_bytes'))):
                raise Error('冻结高水位冲突',409)
            maximum=db.execute('SELECT coalesce(max(sequence),0) FROM tail_ordinary').fetchone()[0]
            tickets=[dict(r) for r in db.execute('SELECT * FROM tail_tickets ORDER BY ticket')]
            body=sealed(dict(schema=TAIL,version=1,ledger_kind=KIND,ledger_id=self.auth['ledger_id'],
                execution_id=self.auth['execution_id'],server_id=self.value['server_id'],session_id=self.value['session_id'],
                cycle_id=self.value['cycle_id'],policy_id=self.value['policy_id'],binding_digest=self.value['digest'],
                execution_digest=digest(fact),authority_digest=self.auth['body_digest'],
                final_sequence=max(fence['sequence'],maximum)+1,upload_bytes=success[0],download_bytes=success[1],
                last_ordinary_sequence=fence['sequence'],last_ordinary_upload=fence['upload_bytes'],last_ordinary_download=fence['download_bytes'],
                stop_at=intent['stop_at'],freeze_completed_at=now,stop_fence_sequence=intent['fence_sequence'],
                stop_fence_digest=intent['body_digest'],runtime_stop_receipt_digest=stop['body_digest'],
                ticket_manifest_digest=digest(tickets),master_ordinary_fence_digest=fence['body_digest'],
                reconciliation_digest=proof['body_digest']))
            if frozen:
                if frozen!=body:raise Error('冻结正文与持久证据不匹配',409)
                return frozen
            db.execute('UPDATE local_tail SET final=? WHERE id=1',(encoded(body),))
            return body

    def frozen_bundle(self):
        """Read only an existing frozen outbox, including on recovery."""
        with self.activity_lock:
            with self.db() as db:
                row=self._row(db)
                if row['final'] is None:raise Error('只尾恢复缺原冻结正文',409)
            body=self.freeze()
            with self.db() as db:
                row=self._row(db)
                if row['final']!=encoded(body):raise Error('冻结正文并发改变',409)
                pack=dict(stop=json.loads(row['stop']),stop_receipt=json.loads(row['stop_receipt']),
                    reconciliation=json.loads(row['reconciliation']),
                    tickets=[dict(r) for r in db.execute('SELECT * FROM tail_tickets ORDER BY ticket')])
                from multi_agent_final import evidence,stopped
                with self.h.registry.connect() as master:
                    master.execute('BEGIN');evidence(master,self.auth,body,pack)
                stopped(self.h,body,pack)
                saved=db.execute('SELECT * FROM tail_delivery WHERE id=1').fetchone()
                if saved:
                    self._delivery_receipt(body,json.loads(saved['receipt']))
                    if saved['final_digest']!=body['body_digest']:raise Error('本地final回执错绑',409)
            return dict(final=body,evidence=pack)

    def _delivery_receipt(self,body,receipt):
        from multi_agent_final import final_receipt
        try:
            at=receipt['accepted_at']
            if (type(at) not in (int,float) or not math.isfinite(at)
                    or not body['freeze_completed_at']<=at<=self.auth['settle_until']
                    or receipt!=final_receipt(body,at)):raise ValueError('receipt')
        except (TypeError,KeyError,ValueError) as exc:raise Error('final原ACK不匹配',409) from exc

    def deliver(self,token,endpoint=None):
        from multi_agent_final import MultiAgentFinal
        with self.io_lock:
            bundle=self.frozen_bundle();body=bundle['final']
            result=(endpoint or MultiAgentFinal(self.h.registry)).submit(token,json.loads(encoded(bundle)))
            if (type(result) is not dict or result.get('accepted') is not True
                    or result.get('state') not in ('accepted','closed') or type(result.get('duplicate')) is not bool
                    or result.get('ready_for_execution') is not False):raise Error('final投递ACK字段无效',409)
            receipt=result.get('final_receipt');self._delivery_receipt(body,receipt)
            self._accepted_ack(body,receipt)
            with self.db() as db:
                row=self._row(db)
                if row['final']!=encoded(body):raise Error('原冻结正文已改变',409)
                saved=db.execute('SELECT * FROM tail_delivery WHERE id=1').fetchone()
                if saved and (saved['receipt']!=encoded(receipt) or saved['final_digest']!=body['body_digest']):
                    raise Error('原final ACK不可替换',409)
                if saved is None:db.execute('INSERT INTO tail_delivery VALUES(1,?,?,NULL)',(body['body_digest'],encoded(receipt)))
            return result

    def _accepted_ack(self,body,receipt,closed=None):
        from multi_agent_final import history
        from usage_accounting import require_source
        with self.h.registry.connect() as db:
            db.execute('BEGIN')
            allocation=db.execute('SELECT * FROM usage_multi_agent_allocations WHERE allocation_id=?',(body['ledger_id'],)).fetchone()
            if allocation is None:raise Error('ACK缺原主控allocation',409)
            require_source(db,KIND,allocation)
            actual=history(db,body['ledger_id'],self.auth)
            if actual is None or actual[0]!=body or actual[2]!=receipt:raise Error('非原主控final ACK',409)
            if closed is not None and (actual[3]['state']!='closed' or actual[3]['close_receipt']!=encoded(closed)):
                raise Error('非原主控close ACK',409)

    def finish(self,token,endpoint=None):
        from multi_agent_final import MultiAgentFinal,close_receipt
        with self.io_lock:
            bundle=self.frozen_bundle();body=bundle['final']
            with self.db() as db:
                saved=db.execute('SELECT * FROM tail_delivery WHERE id=1').fetchone()
                if saved is None:raise Error('未持久确认final，不得close',409)
                ack=json.loads(saved['receipt']);self._delivery_receipt(body,ack)
            self._accepted_ack(body,ack)
            request=dict(ledger_kind=KIND,ledger_id=body['ledger_id'],execution_id=body['execution_id'],
                session_id=body['session_id'],authority_digest=body['authority_digest'],final_digest=body['body_digest'])
            result=(endpoint or MultiAgentFinal(self.h.registry)).close(token,request)
            try:
                if (result!=close_receipt(body,result['closed_at']) or type(result['closed_at']) not in (int,float)
                        or not math.isfinite(result['closed_at']) or result['closed_at']<ack['accepted_at']):raise ValueError('close')
            except (KeyError,TypeError,ValueError) as exc:raise Error('close原ACK不匹配',409) from exc
            self._accepted_ack(body,ack,result)
            with self.db() as db:
                saved=db.execute('SELECT * FROM tail_delivery WHERE id=1').fetchone()
                if saved['closed'] is not None and saved['closed']!=encoded(result):raise Error('close ACK不可替换',409)
                db.execute('UPDATE tail_delivery SET closed=? WHERE id=1',(encoded(result),))
            return result
