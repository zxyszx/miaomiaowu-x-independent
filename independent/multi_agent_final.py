"""E2 isolated terminal accounting and explicit close; no production transport."""
import json
import math
import socket
import time

from core import Error
from grant_agent import identifier,checksum
from node_config import digest
from usage_ledger import integer
from multi_agent_lifecycle import gated,encoded,KIND
from multi_agent_tail import TAIL,sealed,unpack,authenticated,validated_fence

SCHEMA='''
CREATE TABLE IF NOT EXISTS usage_multi_finals(
 allocation_id TEXT PRIMARY KEY REFERENCES usage_multi_execution(allocation_id),
 body TEXT NOT NULL,evidence TEXT NOT NULL,receipt TEXT NOT NULL,accepted_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS usage_multi_terminals(
 allocation_id TEXT PRIMARY KEY REFERENCES usage_multi_finals(allocation_id),
 final_digest TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('accepted','closed')),
 close_receipt TEXT);
'''
FIELDS=set('schema version ledger_kind ledger_id execution_id server_id session_id cycle_id policy_id binding_digest execution_digest authority_digest final_sequence upload_bytes download_bytes last_ordinary_sequence last_ordinary_upload last_ordinary_download stop_at freeze_completed_at stop_fence_sequence stop_fence_digest runtime_stop_receipt_digest ticket_manifest_digest master_ordinary_fence_digest reconciliation_digest body_digest'.split())


def shape(body):
    if type(body) is not dict or set(body)!=FIELDS:raise Error('final正文严格字段不匹配')
    try:
        unpack(body)
        if body['schema']!=TAIL or type(body['version']) is not int or body['version']!=1 or body['ledger_kind']!=KIND:
            raise ValueError('version')
        for k in ('ledger_id','execution_id','session_id','cycle_id'):identifier(body[k])
        for k in FIELDS:
            if k.endswith('_digest'):checksum(body[k])
        for k in ('server_id','policy_id','final_sequence'):
            if not integer(body[k],1):raise ValueError(k)
        for k in ('upload_bytes','download_bytes','last_ordinary_sequence','last_ordinary_upload','last_ordinary_download','stop_fence_sequence'):
            if not integer(body[k],0):raise ValueError(k)
        for k in ('stop_at','freeze_completed_at'):
            if type(body[k]) not in (int,float) or not math.isfinite(body[k]):raise ValueError(k)
    except (ValueError,TypeError,KeyError) as exc:raise Error('final字段或摘要无效',409) from exc


def evidence(db,auth,body,pack):
    """Validate complete original proofs, not arbitrary digest placeholders."""
    shape(body)
    try:
        if type(pack) is not dict or set(pack)!={'stop','stop_receipt','reconciliation','tickets'}:raise ValueError('pack')
        v=auth['base_binding']
        from multi_agent_installation import fact_for
        expected=dict(ledger_kind=KIND,ledger_id=auth['ledger_id'],execution_id=auth['execution_id'],
            authority_digest=auth['body_digest'],binding_digest=v['digest'],execution_digest=digest(fact_for(dict(v,tail_authority=auth),auth['execution_id'])))
        expected.update({k:v[k] for k in ('server_id','session_id','cycle_id','policy_id')})
        if any(body[k]!=value for k,value in expected.items()):raise ValueError('identity')
        intent=pack['stop'];stop=pack['stop_receipt'];proof=pack['reconciliation'];tickets=pack['tickets']
        unpack(intent);unpack(stop);unpack(proof)
        if (intent['execution_id']!=auth['execution_id'] or intent['authority_digest']!=auth['body_digest']
                or stop['execution_id']!=auth['execution_id'] or stop['authority_digest']!=auth['body_digest']
                or stop['stopped'] is not True or stop['stop_intent_digest']!=intent['body_digest']
                or body['stop_fence_digest']!=intent['body_digest'] or body['runtime_stop_receipt_digest']!=stop['body_digest']
                or body['reconciliation_digest']!=proof['body_digest'] or body['stop_at']!=intent['stop_at']
                or body['stop_fence_sequence']!=intent['fence_sequence']):raise ValueError('stop')
        identifier(intent['freeze_intent_id']);identifier(stop['instance_id'])
        if not integer(stop['pid'],1):raise ValueError('pid')
        if not v['issued_at']<=body['stop_at']<=auth['execute_until']:raise ValueError('stop time')
        if not body['stop_at']<=body['freeze_completed_at']<=auth['settle_until']:raise ValueError('freeze time')
        fence=validated_fence(db,auth['ledger_id'],auth)
        request=dict(ledger_kind=KIND,ledger_id=auth['ledger_id'],execution_id=auth['execution_id'],
            session_id=v['session_id'],authority_digest=auth['body_digest'],freeze_intent_id=intent['freeze_intent_id'],
            stop_intent_digest=intent['body_digest'])
        if fence is None or proof['fence']!=fence or fence['request']!=request or body['master_ordinary_fence_digest']!=fence['body_digest']:
            raise ValueError('master fence')
        for key,other in (('last_ordinary_sequence','sequence'),('last_ordinary_upload','upload_bytes'),('last_ordinary_download','download_bytes')):
            if body[key]!=fence[other]:raise ValueError('last ordinary')
        if type(tickets) is not list or len(tickets)!=body['stop_fence_sequence']:raise ValueError('ticket count')
        success=[0,0];reservations=[]
        for i,t in enumerate(tickets,1):
            if (type(t) is not dict or set(t)!={'ticket','direction','bytes','successful'}
                    or type(t['ticket']) is not int or t['ticket']!=i or type(t['direction']) is not int or t['direction'] not in (0,1)
                    or not integer(t['bytes'],1) or type(t['successful']) is not int or t['successful'] not in (0,1)):
                raise ValueError('ticket')
            reservations.append({k:t[k] for k in ('ticket','direction','bytes')})
            if t['successful']:success[t['direction']]+=t['bytes']
        if (digest(tickets)!=body['ticket_manifest_digest'] or digest(reservations)!=intent['ticket_reservation_digest']
                or sum(t['bytes'] for t in tickets)>v['reserved_bytes']
                or success!=[body['upload_bytes'],body['download_bytes']]
                or any(success[i]<fence[k] for i,k in enumerate(('upload_bytes','download_bytes')))):
            raise ValueError('ticket counters')
        for direction,key in enumerate(('upload_bytes','download_bytes')):
            limit=v['client'].get('cycle_budget',{}).get(key)
            baseline=v['client'].get('cycle_usage',{}).get(key,0)
            used=sum(t['bytes'] for t in tickets if t['direction']==direction)
            if limit is not None and used+baseline>limit:raise ValueError('original directional constraint')
        requests=proof['requests'];results=proof['results']
        if type(requests) is not list or type(results) is not list or len(requests)!=len(results):raise ValueError('queries')
        previous=0;accepted=[]
        for ordinary,response in zip(requests,results):
            unpack(ordinary);unpack(response)
            if (not integer(ordinary['sequence'],1) or ordinary['sequence']<=previous
                    or ordinary['operation']!='ordinary' or any(ordinary[k]!=expected[k] for k in ('ledger_kind','ledger_id','execution_id','authority_digest','binding_digest','policy_id','session_id'))):
                raise ValueError('ordinary identity')
            previous=ordinary['sequence']
            query={k:ordinary[k] for k in ('ledger_kind','ledger_id','execution_id','session_id','authority_digest','operation','sequence','body_digest')}
            row=db.execute('SELECT * FROM usage_multi_tail_ordinary WHERE allocation_id=? AND sequence=?',(auth['ledger_id'],previous)).fetchone()
            actual=json.loads(row['body']) if row else None
            if row and actual!=ordinary:raise ValueError('ordinary body')
            result=sealed(dict(schema=TAIL,query=query,accepted=row is not None,ordinary=actual,
                               fence_digest=fence['body_digest'],ready_for_execution=False))
            if result!=response:raise ValueError('query result')
            if row:accepted.append(dict(sequence=previous,body_digest=row['digest']))
        if accepted!=fence['ordinary'] or body['final_sequence']<=max(previous,fence['sequence']):raise ValueError('sequence')
    except (TypeError,ValueError,KeyError,AttributeError) as exc:raise Error('final原证据链缺失或冲突',409) from exc


def final_receipt(body,at):
    return sealed(dict(schema=TAIL,operation='final',ledger_kind=KIND,ledger_id=body['ledger_id'],
        execution_id=body['execution_id'],final_sequence=body['final_sequence'],final_digest=body['body_digest'],
        upload_bytes=body['upload_bytes'],download_bytes=body['download_bytes'],accepted_at=at,ready_for_execution=False))


def manual_receipt(body,at,audit_id):
    return sealed(dict(unpack(final_receipt(body,at)),acceptance_mode='manual_recovery',audit_id=audit_id))


def history(db,ident,auth):
    row=db.execute('SELECT * FROM usage_multi_finals WHERE allocation_id=?',(ident,)).fetchone()
    terminal=db.execute('SELECT * FROM usage_multi_terminals WHERE allocation_id=?',(ident,)).fetchone()
    if row is None:
        if terminal:raise Error('final终态缺原正文',409)
        if db.execute('SELECT 1 FROM usage_multi_recovery_audits WHERE allocation_id=?',(ident,)).fetchone():
            raise Error('人工审计缺final终态',409)
        return None
    try:
        body=json.loads(row['body']);pack=json.loads(row['evidence']);receipt=json.loads(row['receipt'])
        evidence(db,auth,body,pack)
        if receipt.get('acceptance_mode')=='manual_recovery':
            from multi_agent_recovery import validate_manual
            validate_manual(db,auth,body,pack,receipt,row['accepted_at'])
        elif (row['receipt']!=encoded(final_receipt(body,row['accepted_at']))
                or not body['freeze_completed_at']<=row['accepted_at']<=auth['settle_until']
                or db.execute('SELECT 1 FROM usage_multi_recovery_audits WHERE allocation_id=?',(ident,)).fetchone()):
            raise ValueError('automatic receipt')
        if (row['body']!=encoded(body) or row['evidence']!=encoded(pack) or row['receipt']!=encoded(receipt)
                or terminal is None or terminal['final_digest']!=body['body_digest']):raise ValueError('terminal')
        life=db.execute('SELECT state FROM usage_multi_lifecycle WHERE allocation_id=?',(ident,)).fetchone()
        if terminal['state']=='closed':
            close=json.loads(terminal['close_receipt']);unpack(close)
            if close!=close_receipt(body,close['closed_at']) or life is None or life['state']!='closed' or close['closed_at']<row['accepted_at']:
                raise ValueError('closed')
        elif terminal['state']!='accepted' or terminal['close_receipt'] is not None or life is None or life['state']=='closed':raise ValueError('state')
    except (TypeError,ValueError,KeyError) as exc:raise Error('final持久终态损坏',409) from exc
    return body,pack,receipt,terminal


def close_receipt(body,at):
    return sealed(dict(ledger_kind=KIND,ledger_id=body['ledger_id'],execution_id=body['execution_id'],
        final_digest=body['body_digest'],state='closed',closed_at=at,ready_for_execution=False))


def stopped(gate,body,pack):
    runtime=gate.allocations.get(body['ledger_id']);stop=pack['stop_receipt']
    if (runtime is None or runtime.fixture_execution_id!=body['execution_id']
            or runtime.fixture_instance_id!=stop['instance_id'] or runtime.fixture_process.pid!=stop['pid']
            or runtime.fixture_process.returncode is None):raise Error('缺原实例实际停止证明',409)
    with socket.socket() as s:
        s.settimeout(0.2)
        if s.connect_ex(('127.0.0.1',runtime.fixture_port))==0:raise Error('原监听仍开放',409)


class MultiAgentFinal:
    def __init__(self,registry):self.registry=registry

    def submit(self,token,data):
        gate=gated(self.registry)
        if type(data) is not dict or set(data)!={'final','evidence'}:raise Error('final操作字段无效')
        body=data['final'];shape(body)
        with gate.transition(),self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            a,_,_,auth=authenticated(self.registry,db,token,body)
            old=history(db,body['ledger_id'],auth)
            if old:
                if old[0]!=body or old[1]!=data['evidence']:raise Error('final原正文不可替换',409)
                return self._result(old[2],old[3]['state'],True)
            now=time.time()
            if not a['issued_at']<=body['freeze_completed_at']<=now<=auth['settle_until']:raise Error('final接收窗口结束或时钟异常',409)
            evidence(db,auth,body,data['evidence']);stopped(gate,body,data['evidence'])
            from usage_accounting import accept_attributed,require_source
            accept_attributed(db,KIND,a,body['final_sequence'],body['upload_bytes'],body['download_bytes'],body['execution_id'],now)
            db.execute('INSERT INTO usage_multi_meter VALUES(?,?,?,?,?)',(body['ledger_id'],body['final_sequence'],body['upload_bytes'],body['download_bytes'],now))
            receipt=final_receipt(body,now)
            db.execute('INSERT INTO usage_multi_finals VALUES(?,?,?,?,?)',(body['ledger_id'],encoded(body),encoded(data['evidence']),encoded(receipt),now))
            db.execute('INSERT INTO usage_multi_terminals VALUES(?,?,?,NULL)',(body['ledger_id'],body['body_digest'],'accepted'))
            require_source(db,KIND,a)
            return self._result(receipt,'accepted',False)

    @staticmethod
    def _result(receipt,state,duplicate):
        return dict(accepted=True,duplicate=duplicate,final_receipt=receipt,state=state,ready_for_execution=False)

    def close(self,token,data):
        gate=gated(self.registry)
        fields={'ledger_kind','ledger_id','execution_id','session_id','authority_digest','final_digest'}
        from multi_agent_tail import MultiAgentTail
        MultiAgentTail._fields(data,fields)
        with gate.transition(),self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            _,_,_,auth=authenticated(self.registry,db,token,data)
            old=history(db,data['ledger_id'],auth)
            if old is None or old[0]['body_digest']!=data['final_digest']:raise Error('close缺原final回执',409)
            body,pack,_,terminal=old
            stopped(gate,body,pack)
            if terminal['state']=='closed':return json.loads(terminal['close_receipt'])
            now=time.time()
            if not math.isfinite(now) or now<old[2]['accepted_at']:raise Error('close时钟异常',409)
            receipt=close_receipt(body,now)
            db.execute("UPDATE usage_multi_terminals SET state='closed',close_receipt=? WHERE allocation_id=?",(encoded(receipt),data['ledger_id']))
            db.execute("UPDATE usage_multi_lifecycle SET state='closed',updated_at=? WHERE allocation_id=?",(now,data['ledger_id']))
            return receipt
