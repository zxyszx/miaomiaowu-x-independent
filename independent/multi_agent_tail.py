"""E1: versioned fixture authority and ordinary ingestion fence, no final API."""
import json
import time
import uuid

from core import Error
from node_config import digest
from multi_agent_lifecycle import gated,encoded,KIND
from grant_agent import identifier,checksum
from usage_settlement import DELIVERY_WINDOW

TAIL='independent.multi-allocation-tail-v1'
SCHEMA='''
CREATE TABLE IF NOT EXISTS usage_multi_tail_authorities(
 allocation_id TEXT PRIMARY KEY REFERENCES usage_multi_agent_allocations(allocation_id),
 body TEXT NOT NULL,digest TEXT NOT NULL,phase TEXT NOT NULL CHECK(phase IN ('active','prepared')));
CREATE TABLE IF NOT EXISTS usage_multi_tail_versions(
 allocation_id TEXT PRIMARY KEY REFERENCES usage_multi_tail_authorities(allocation_id),
 protocol TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS usage_multi_tail_ordinary(
 allocation_id TEXT NOT NULL REFERENCES usage_multi_tail_authorities(allocation_id),
 sequence INTEGER NOT NULL,body TEXT NOT NULL,digest TEXT NOT NULL,
 PRIMARY KEY(allocation_id,sequence));
CREATE TABLE IF NOT EXISTS usage_multi_tail_fences(
 allocation_id TEXT PRIMARY KEY REFERENCES usage_multi_tail_authorities(allocation_id),
 request TEXT NOT NULL,receipt TEXT NOT NULL);
'''


def sealed(body):
    return dict(body,body_digest=digest(body))


def unpack(value):
    if type(value) is not dict or 'body_digest' not in value:
        raise Error('tail正文缺少摘要',409)
    body={k:v for k,v in value.items() if k!='body_digest'}
    if digest(body)!=value['body_digest']: raise Error('tail正文摘要冲突',409)
    return body


def authority(db,value):
    ident=value['grant_id']
    row=db.execute('SELECT * FROM usage_multi_tail_authorities WHERE allocation_id=?',(ident,)).fetchone()
    version=db.execute('SELECT protocol FROM usage_multi_tail_versions WHERE allocation_id=?',(ident,)).fetchone()
    if row is None and version is None:return None
    if row is None or version is None or version['protocol']!=TAIL:
        raise Error('tail版本/授权链缺失',409)
    try:
        body=json.loads(row['body'])
        expected={'schema','ledger_kind','ledger_id','execution_id','base_binding',
                  'execute_until','settle_until'}
        if (type(body) is not dict or set(body)!=expected or body['schema']!=TAIL
                or body['ledger_kind']!=KIND or body['ledger_id']!=ident
                or row['body']!=encoded(body) or digest(body)!=row['digest']
                or {k:v for k,v in body['base_binding'].items() if k not in ('delivery_state','ready_for_execution')}
                   !={k:v for k,v in value.items() if k not in ('tail_authority','delivery_state','ready_for_execution')}
                or body['execute_until']!=value['valid_until']
                or body['settle_until']!=value['valid_until']+DELIVERY_WINDOW):
            raise ValueError('authority')
        identifier(body['execution_id'])
    except (TypeError,ValueError,KeyError) as exc:raise Error('tail授权损坏',409) from exc
    fence=db.execute('SELECT 1 FROM usage_multi_tail_fences WHERE allocation_id=?',(ident,)).fetchone()
    if (row['phase']=='prepared')!=(fence is not None): raise Error('tail主控栅栏损坏',409)
    return dict(body,body_digest=row['digest'])


def attach(db,value):
    a=authority(db,value)
    return dict(value,tail_authority=a) if a else value


def meter_history(db,ident):
    """Pair typed original requests with every E1 meter row, including zeroes."""
    version=db.execute('SELECT 1 FROM usage_multi_tail_versions WHERE allocation_id=?',(ident,)).fetchone()
    row=db.execute('SELECT body FROM usage_multi_tail_authorities WHERE allocation_id=?',(ident,)).fetchone()
    if version is None and row is None:return
    if version is None or row is None:raise Error('tail计量授权缺失',409)
    try:
        a=json.loads(row['body']);auth=authority(db,a['base_binding'])
        meters=db.execute('SELECT * FROM usage_multi_meter WHERE allocation_id=? ORDER BY sequence',(ident,)).fetchall()
        typed=db.execute('SELECT * FROM usage_multi_tail_ordinary WHERE allocation_id=? ORDER BY sequence',(ident,)).fetchall()
        from multi_agent_final import history
        final=history(db,ident,auth)
        ordinary_meters=meters
        if final:
            body=final[0]
            if (not meters or (meters[-1]['sequence'],meters[-1]['upload_bytes'],meters[-1]['download_bytes'])
                    !=(body['final_sequence'],body['upload_bytes'],body['download_bytes'])):raise ValueError('final meter')
            ordinary_meters=meters[:-1]
        if len(ordinary_meters)!=len(typed):raise ValueError('typed meter missing')
        for m,t in zip(ordinary_meters,typed):
            body=json.loads(t['body']);raw=unpack(body)
            if (set(raw)!={'ledger_kind','ledger_id','execution_id','session_id','policy_id','binding_digest',
                          'operation','authority_digest','sequence','upload_bytes','download_bytes'}
                    or t['body']!=encoded(body) or t['digest']!=body['body_digest']
                    or raw.get('operation')!='ordinary' or raw.get('authority_digest')!=digest(a)
                    or any(raw.get(k)!=a['base_binding'][k] for k in ('session_id','policy_id'))
                    or raw.get('binding_digest')!=a['base_binding']['digest']
                    or raw.get('ledger_kind')!=KIND or raw.get('ledger_id')!=ident
                    or raw.get('execution_id')!=a['execution_id']
                    or (raw.get('sequence'),raw.get('upload_bytes'),raw.get('download_bytes'))
                       !=(m['sequence'],m['upload_bytes'],m['download_bytes'])
                    or t['sequence']!=m['sequence']):raise ValueError('typed meter mismatch')
    except (TypeError,ValueError,KeyError) as exc:raise Error('tail普通正文与计量损坏',409) from exc


def authenticated(registry,db,token,data):
    from multi_agent_meter import execution_chain
    from usage_accounting import MULTI,require_source
    sid=registry._agent_id(db,token);registry._check_session(db,sid,data['session_id'])
    a=db.execute('SELECT * FROM usage_multi_agent_allocations WHERE allocation_id=?',(data['ledger_id'],)).fetchone()
    fingerprint=db.execute('SELECT token_hash FROM agents WHERE server_id=?',(sid,)).fetchone()[0]
    if a is None or (a['server_id'],a['session_id'],a['token_hash'])!=(sid,data['session_id'],fingerprint):
        raise Error('tail不属于原Agent身份',403)
    if db.execute('SELECT 1 FROM usage_first_total_grants WHERE grant_id=?',(data['ledger_id'],)).fetchone():
        raise Error('tail双账本冲突',409)
    value,fact,_=execution_chain(db,a)
    auth=value.get('tail_authority')
    if auth is None or data['authority_digest']!=auth['body_digest'] or data['execution_id']!=auth['execution_id']:
        raise Error('tail原执行授权不匹配',409)
    require_source(db,MULTI,a)
    return a,value,fact,auth


def validated_fence(db,ident,auth):
    row=db.execute('SELECT * FROM usage_multi_tail_fences WHERE allocation_id=?',(ident,)).fetchone()
    if row is None:return None
    # The fence snapshots ordinary only; terminal accounting advances the source.
    last=db.execute('SELECT body FROM usage_multi_tail_ordinary WHERE allocation_id=? ORDER BY sequence DESC LIMIT 1',(ident,)).fetchone()
    ordinary=json.loads(last['body']) if last else dict(sequence=0,upload_bytes=0,download_bytes=0)
    try:
        request=json.loads(row['request']);receipt=json.loads(row['receipt']);raw=unpack(receipt)
        rows=[dict(sequence=r['sequence'],body_digest=r['digest']) for r in db.execute(
            'SELECT * FROM usage_multi_tail_ordinary WHERE allocation_id=? ORDER BY sequence',(ident,))]
        if (set(raw)!={'schema','request','sequence','upload_bytes','download_bytes','ordinary','prepared_at','ready_for_execution'}
                or receipt['schema']!=TAIL or receipt['request']!=request
                or row['request']!=encoded(request) or row['receipt']!=encoded(receipt)
                or any(request[k]!=auth[k] for k in ('ledger_kind','ledger_id','execution_id'))
                or request['authority_digest']!=auth['body_digest'] or request['session_id']!=auth['base_binding']['session_id']
                or receipt['ordinary']!=rows or receipt['ready_for_execution'] is not False
                or not auth['base_binding']['issued_at']<=receipt['prepared_at']<=auth['settle_until']
                or (receipt['sequence'],receipt['upload_bytes'],receipt['download_bytes'])
                   !=(ordinary['sequence'],ordinary['upload_bytes'],ordinary['download_bytes'])):
            raise ValueError('fence snapshot')
    except (TypeError,ValueError,KeyError) as exc:raise Error('tail主控冻结快照损坏',409) from exc
    return receipt


class MultiAgentTail:
    def __init__(self,registry):self.registry=registry

    def enroll(self,token,ident,session):
        gate=gated(self.registry);identifier(ident);identifier(session)
        with gate.transition(),self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            r=self.registry;sid=r._agent_id(db,token);r._check_session(db,sid,session)
            a=db.execute('SELECT * FROM usage_multi_agent_allocations WHERE allocation_id=?',(ident,)).fetchone()
            agent=db.execute('SELECT * FROM agents WHERE server_id=?',(sid,)).fetchone()
            if a is None or (a['server_id'],a['session_id'],a['token_hash'])!=(sid,session,agent['token_hash']):
                raise Error('tail授权不属于原身份',403)
            old=db.execute('SELECT body FROM usage_multi_tail_authorities WHERE allocation_id=?',(ident,)).fetchone()
            if old:
                raw=json.loads(old['body']);return authority(db,raw['base_binding'])
            if (db.execute('SELECT 1 FROM usage_multi_agent_configurations WHERE allocation_id=?',(ident,)).fetchone()
                    or db.execute('SELECT 1 FROM usage_multi_tail_versions WHERE allocation_id=?',(ident,)).fetchone()
                    or db.execute('SELECT 1 FROM usage_multi_lifecycle WHERE allocation_id=?',(ident,)).fetchone()):
                raise Error('旧绑定/执行不得补发tail授权',409)
            from grant_capabilities import protocols
            cap=db.execute('SELECT * FROM grant_capabilities WHERE server_id=?',(sid,)).fetchone()
            if (cap is None or (cap['session_id'],cap['token_hash'])!=(session,agent['token_hash'])
                    or TAIL not in protocols(json.loads(cap['offer']))):raise Error('未协商隔离tail版本',409)
            from multi_agent_configuration import MultiAgentConfiguration
            value=MultiAgentConfiguration(r)._bind(db,token,ident,session_id=session)
            body=dict(schema=TAIL,ledger_kind=KIND,ledger_id=ident,execution_id=str(uuid.uuid4()),
                      base_binding=value,execute_until=value['valid_until'],
                      settle_until=value['valid_until']+DELIVERY_WINDOW)
            db.execute('INSERT INTO usage_multi_tail_authorities VALUES(?,?,?,?)',(ident,encoded(body),digest(body),'active'))
            db.execute('INSERT INTO usage_multi_tail_versions VALUES(?,?)',(ident,TAIL))
            return authority(db,value)

    def prepare(self,token,data):
        gate=gated(self.registry)
        fields={'ledger_kind','ledger_id','execution_id','session_id','authority_digest',
                'freeze_intent_id','stop_intent_digest'}
        self._fields(data,fields)
        identifier(data['freeze_intent_id']);checksum(data['stop_intent_digest'])
        with gate.transition(),self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            a,value,_,auth=authenticated(self.registry,db,token,data)
            old=db.execute('SELECT * FROM usage_multi_tail_fences WHERE allocation_id=?',(a['allocation_id'],)).fetchone()
            if old:
                if old['request']!=encoded(data):raise Error('tail准备intent不可更换',409)
                return validated_fence(db,a['allocation_id'],auth)
            now=time.time()
            if not a['issued_at']<=now<=auth['settle_until']:raise Error('tail准备窗口已结束或时钟异常',409)
            rows=[dict(sequence=row['sequence'],body_digest=row['digest']) for row in db.execute(
                'SELECT * FROM usage_multi_tail_ordinary WHERE allocation_id=? ORDER BY sequence',(a['allocation_id'],))]
            from usage_accounting import require_source,MULTI
            source=require_source(db,MULTI,a)
            receipt=sealed(dict(schema=TAIL,request=data,sequence=source['accepted_sequence'],
                upload_bytes=int(source['upload_bytes']),download_bytes=int(source['download_bytes']),
                ordinary=rows,prepared_at=now,ready_for_execution=False))
            db.execute('INSERT INTO usage_multi_tail_fences VALUES(?,?,?)',(a['allocation_id'],encoded(data),encoded(receipt)))
            db.execute("UPDATE usage_multi_tail_authorities SET phase='prepared' WHERE allocation_id=?",(a['allocation_id'],))
            return receipt

    @staticmethod
    def _fields(data,fields):
        if type(data) is not dict or set(data)!=fields or type(data['ledger_kind']) is not str or data['ledger_kind']!=KIND:
            raise Error('tail请求字段无效')
        try:
            for k in ('ledger_id','execution_id','session_id'):identifier(data[k])
            checksum(data['authority_digest'])
        except ValueError as exc:raise Error('tail身份字段无效') from exc

    def query(self,token,data):
        gated(self.registry)
        fields={'ledger_kind','ledger_id','execution_id','session_id','authority_digest',
                'operation','sequence','body_digest'}
        self._fields(data,fields)
        from usage_ledger import integer
        if data['operation']!='ordinary' or not integer(data['sequence'],1):raise Error('tail查询操作无效')
        checksum(data['body_digest'])
        with self.registry.connect() as db:
            db.execute('BEGIN')
            a,_,_,auth=authenticated(self.registry,db,token,data)
            receipt=validated_fence(db,a['allocation_id'],auth)
            if receipt is None:raise Error('未建立主控ordinary栅栏',409)
            row=db.execute('SELECT * FROM usage_multi_tail_ordinary WHERE allocation_id=? AND sequence=?',
                           (a['allocation_id'],data['sequence'])).fetchone()
            if row is not None and row['digest']!=data['body_digest']:raise Error('tail查询正文冲突',409)
            return sealed(dict(schema=TAIL,query=data,accepted=row is not None,
                ordinary=json.loads(row['body']) if row else None,fence_digest=receipt['body_digest'],
                ready_for_execution=False))
