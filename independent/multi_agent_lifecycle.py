"""Isolated multi execution ledger. Production registries have no gate."""
import json
import time
from core import Error
from grant_agent import checksum
from grant_capabilities import STAGING,STAGE_IDENTITY,protocols

MULTI_EXECUTION='independent.multi-allocation-execution-v1'
KIND='multi_allocation'
SCHEMA='''
CREATE TABLE IF NOT EXISTS usage_multi_configuration_refs(
 allocation_id TEXT PRIMARY KEY REFERENCES usage_multi_agent_allocations(allocation_id),
 digest TEXT NOT NULL,token_hash TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS usage_multi_lifecycle(
 allocation_id TEXT PRIMARY KEY REFERENCES usage_multi_configuration_refs(allocation_id),
 state TEXT NOT NULL,updated_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS usage_multi_installations(
 allocation_id TEXT PRIMARY KEY REFERENCES usage_multi_lifecycle(allocation_id),
 fact TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS usage_multi_applied(
 allocation_id TEXT PRIMARY KEY REFERENCES usage_multi_installations(allocation_id),
 revision INTEGER NOT NULL,digest TEXT NOT NULL,execute_until REAL NOT NULL);
CREATE TABLE IF NOT EXISTS usage_multi_execution(
 allocation_id TEXT PRIMARY KEY REFERENCES usage_multi_applied(allocation_id),
 fact TEXT NOT NULL,recorded_at REAL NOT NULL);
'''

def encoded(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)

def gated(registry):
    # Import only on the isolated endpoint, never from production scheduler.
    from multi_agent_test_runtime import gate_for
    gate=gate_for(registry)
    if gate is None: raise Error('multi执行仅允许隔离fixture harness',403)
    return gate

def receipt(ident,state):
    return dict(accepted=True,ledger_kind=KIND,ledger_id=ident,state=state,
                ready_for_execution=False)

def identity(registry,db,token,kind,ident,session,binding_digest):
    gated(registry)
    if type(kind) is not str or kind!=KIND: raise Error('multi账本类型无效')
    registry._session_value(ident); registry._session_value(session)
    checksum(binding_digest)
    sid=registry._agent_id(db,token); registry._check_session(db,sid,session)
    if db.execute('SELECT 1 FROM usage_first_total_grants WHERE grant_id=?',(ident,)).fetchone():
        raise Error('multi账本编号冲突',409)
    agent=db.execute('SELECT * FROM agents WHERE server_id=?',(sid,)).fetchone()
    cap=db.execute('SELECT * FROM grant_capabilities WHERE server_id=?',(sid,)).fetchone()
    if (cap is None or cap['session_id']!=session or cap['token_hash']!=agent['token_hash']
            or not {STAGING,STAGE_IDENTITY,MULTI_EXECUTION}<=set(protocols(json.loads(cap['offer'])))):
        raise Error('当前会话未协商隔离multi协议',409)
    old=db.execute('SELECT * FROM usage_multi_agent_configurations WHERE allocation_id=?',
                   (ident,)).fetchone()
    if old is None or old['digest']!=binding_digest:
        raise Error('multi配置尚未绑定或摘要不一致',409)
    from multi_agent_configuration import MultiAgentConfiguration
    if db.execute('SELECT 1 FROM usage_multi_execution WHERE allocation_id=?',(ident,)).fetchone():
        # Metering by any source moves confirmed beyond the original baseline.
        # Keep the original immutable binding usable for stop/retry operations.
        from multi_agent_meter import execution_chain,live_identity
        allocation=db.execute('SELECT * FROM usage_multi_agent_allocations WHERE allocation_id=?',(ident,)).fetchone()
        if allocation is None or (allocation['server_id'],allocation['session_id'],allocation['token_hash'])!=(sid,session,agent['token_hash']):
            raise Error('multi原执行身份不匹配',403)
        value,_,_=execution_chain(db,allocation)
        live_identity(registry,db,allocation,value,time.time())
        if not allocation['issued_at']<=time.time()<allocation['valid_until']:
            raise Error('multi执行窗口结束',409)
    else:
        value=MultiAgentConfiguration(registry)._bind(db,token,ident,session_id=session)
    if value['digest']!=binding_digest: raise Error('multi绑定摘要冲突',409)
    ref=db.execute('SELECT * FROM usage_multi_configuration_refs WHERE allocation_id=?',
                   (ident,)).fetchone()
    lifecycle=db.execute('SELECT 1 FROM usage_multi_lifecycle WHERE allocation_id=?',(ident,)).fetchone()
    if lifecycle is not None and ref is None:
        raise Error('multi配置引用链缺失',409)
    if ref is not None and (ref['digest'],ref['token_hash'])!=(binding_digest,agent['token_hash']):
        raise Error('multi原配置身份冲突',409)
    from multi_agent_tail import attach
    return attach(db,value),agent['token_hash']

def chain(db,ident,value):
    installation=db.execute('SELECT fact FROM usage_multi_installations WHERE allocation_id=?',
                            (ident,)).fetchone()
    applied=db.execute('SELECT * FROM usage_multi_applied WHERE allocation_id=?',(ident,)).fetchone()
    if installation is None or applied is None: raise Error('multi安装链缺失',409)
    from multi_agent_installation import fact_for
    fact=json.loads(installation['fact'])
    expected=fact_for(value,fact.get('execution_id'))
    if (installation['fact']!=encoded(expected) or
        (applied['revision'],applied['digest'],applied['execute_until'])!=(
            value['revision'],value['digest'],value['valid_until'])):
        raise Error('multi安装链不一致',409)
    return installation['fact']

class MultiAgentLifecycle:
    def __init__(self,registry): self.registry=registry

    def advance(self,token,data):
        gate=gated(self.registry)
        if type(data) is dict and data.get('action') in ('close','block'):
            store=gate.tail_stores.get(data.get('ledger_id'))
            if store is not None:store.stop()  # Even if current network identity is invalid.
        with gate.transition():
            # E2 terminal close has its own authenticated, original-final path.
            # Legacy lifecycle actions must never downgrade its durable state.
            if type(data) is dict and set(data)=={'ledger_kind','ledger_id','session_id','binding_digest','action'}:
                if type(data['ledger_kind']) is not str or data['ledger_kind']!=KIND:raise Error('multi账本类型无效')
                with self.registry.connect() as db:
                    db.execute('BEGIN')
                    terminal=db.execute('SELECT f.body,t.state FROM usage_multi_finals f JOIN usage_multi_terminals t USING(allocation_id) WHERE allocation_id=?',
                                        (data['ledger_id'],)).fetchone()
                    if terminal is not None and data['action'] in ('close','block'):
                        body=json.loads(terminal['body'])
                        if data['binding_digest']!=body['binding_digest']:raise Error('multi绑定摘要冲突',409)
                        from multi_agent_tail import authenticated
                        authenticated(self.registry,db,token,dict(body,session_id=data['session_id'],ledger_kind=data['ledger_kind']))
                        if data['action']=='close':
                            from multi_agent_final import MultiAgentFinal
                            request={k:body[k] for k in ('ledger_kind','ledger_id','execution_id','session_id','authority_digest')}
                            request['final_digest']=body['body_digest']
                        elif terminal['state']=='closed':return receipt(data['ledger_id'],'closed')
                        else:request=None
                    else:request=None
                if request is not None:
                    result=MultiAgentFinal(self.registry).close(token,request)
                    return receipt(data['ledger_id'],result['state'])
            return self._advance(token,data,gate)

    def _advance(self,token,data,gate):
        if type(data) is not dict or set(data)!={
                'ledger_kind','ledger_id','session_id','binding_digest','action'}:
            raise Error('multi生命周期字段无效')
        action=data['action']
        if type(action) is not str or action not in ('begin','installed','close','block'):
            raise Error('multi生命周期动作无效')
        ident=data['ledger_id']
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            value,fingerprint=identity(self.registry,db,token,data['ledger_kind'],ident,
                                       data['session_id'],data['binding_digest'])
            old=db.execute('SELECT * FROM usage_multi_lifecycle WHERE allocation_id=?',(ident,)).fetchone()
            if old is None:
                if action!='begin': raise Error('multi尚未begin',409)
                busy=db.execute('''SELECT 1 FROM usage_multi_lifecycle l
                    JOIN usage_multi_agent_allocations a ON a.allocation_id=l.allocation_id
                    WHERE a.server_id=? AND a.node_id=? AND a.user_id=? AND a.credential=?
                    AND l.state IN ('installing','installed','execution_recorded','closing','blocked')''',
                    (value['server_id'],value['node_id'],value['client']['user_id'],
                     value['client']['uuid'])).fetchone()
                if busy: raise Error('fixture身份已被另一multi执行占用',409)
                db.execute('INSERT INTO usage_multi_configuration_refs VALUES(?,?,?)',
                           (ident,value['digest'],fingerprint))
                db.execute('INSERT INTO usage_multi_lifecycle VALUES(?,?,?)',
                           (ident,'installing',time.time()))
                return receipt(ident,'installing')
            state=old['state']
            if action in ('close','block'):
                try:
                    gate.stop_sync(ident)
                except Exception:
                    # Commit the conservative fence while retaining occupancy.
                    db.execute('UPDATE usage_multi_lifecycle SET state=?,updated_at=? WHERE allocation_id=?',
                               ('blocked',time.time(),ident))
                    db.commit()
                    raise Error('隔离runtime停止未确认，保持占用',409) from None
            if state in ('installed','execution_recorded'):
                original=chain(db,ident,value)
                if state=='execution_recorded':
                    execution=db.execute('SELECT fact FROM usage_multi_execution WHERE allocation_id=?',
                                         (ident,)).fetchone()
                    if execution is None or execution['fact']!=original:
                        raise Error('multi执行链不一致',409)
            if action=='begin': return receipt(ident,state)
            if action=='installed':
                chain(db,ident,value)
                if state not in ('installing','installed','execution_recorded'):
                    raise Error('multi生命周期不可安装',409)
                target='installed' if state=='installing' else state
            elif action=='close':
                if state=='blocked': raise Error('multi生命周期已阻断',409)
                target='closing'
            else: target='blocked'
            if target!=state:
                db.execute('UPDATE usage_multi_lifecycle SET state=?,updated_at=? WHERE allocation_id=?',
                           (target,time.time(),ident))
            return receipt(ident,target)
