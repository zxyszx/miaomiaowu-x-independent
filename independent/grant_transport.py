"""Explicit authenticated staging delivery; never runtime installation or ACK."""
import hashlib
import json
import time

from agent_liveness import ONLINE_WINDOW
from core import Error
from grant_configuration import GrantConfiguration
from grant_agent import checksum,identifier,validate
from grant_capabilities import STAGE_IDENTITY,STAGING,protocols

FIRST='first_grant'
MULTI='multi_allocation'
LEDGER_KINDS=frozenset((FIRST,MULTI))


def resolve(registry,token,data):
    """Resolve one exact master ledger identity without mutating master state."""
    if type(data) is not dict or set(data)!={
            'session_id','ledger_id','expected_ledger_kind','binding_digest'}:
        raise Error('授予账本解析字段无效')
    session=registry._session_value(data['session_id'])
    ledger_id=registry._session_value(data['ledger_id'])
    expected=data['expected_ledger_kind']
    if expected is not None and (type(expected) is not str or expected not in LEDGER_KINDS):
        raise Error('授予账本类型无效')
    try: checksum(data['binding_digest'])
    except ValueError as error: raise Error(str(error)) from None
    binding_digest=data['binding_digest']
    fingerprint=hashlib.sha256(token.encode()).hexdigest() if isinstance(token,str) else ''
    with registry.connect() as db:
        db.execute('BEGIN')
        sid=registry._agent_id(db,token)
        registry._check_session(db,sid,session)
        agent=db.execute('SELECT * FROM agents WHERE server_id=?',(sid,)).fetchone()
        now=time.time()
        if (agent['token_hash']!=fingerprint or agent['last_seen'] is None or
                not 0<=now-agent['last_seen']<ONLINE_WINDOW):
            raise Error('Agent已离线，不能解析授予账本',409)
        capability=db.execute(
            'SELECT * FROM grant_capabilities WHERE server_id=?',(sid,)).fetchone()
        if (capability is None or capability['session_id']!=session or
                capability['token_hash']!=fingerprint):
            raise Error('当前会话未协商授予暂存协议',409)
        offered=protocols(json.loads(capability['offer']))
        if STAGING not in offered or (expected is not None and STAGE_IDENTITY not in offered):
            raise Error('当前会话未协商授予账本身份协议',409)

        first=db.execute('SELECT * FROM usage_first_total_grants WHERE grant_id=?',
                         (ledger_id,)).fetchone()
        multi=db.execute('SELECT * FROM usage_multi_agent_allocations WHERE allocation_id=?',
                         (ledger_id,)).fetchone()
        if first is not None and multi is not None:
            raise Error('授予账本编号冲突',409)
        if first is None and multi is None:
            raise Error('授予账本不存在',404)
        kind,row=(FIRST,first) if first is not None else (MULTI,multi)
        if expected is not None and expected!=kind:
            raise Error('授予账本类型不匹配',409)
        if (row['server_id']!=sid or row['session_id']!=session or
                row['token_hash']!=fingerprint):
            raise Error('授予账本身份不匹配',403)
        table,key=('usage_first_total_configurations','grant_id') if kind==FIRST else (
                  'usage_multi_agent_configurations','allocation_id')
        binding=db.execute(
            f'SELECT digest FROM {table} WHERE {key}=?',(ledger_id,)).fetchone()
        if binding is None or binding['digest']!=binding_digest:
            raise Error('授予配置摘要不匹配',409)
        return {'accepted':True,'state':'ledger_resolved','ledger_kind':kind,
                'ledger_id':ledger_id,'binding_digest':binding_digest,
                'ready_for_execution':False}


def pull(registry,token,data):
    if type(data) is not dict or set(data)!={'grant_id','session_id'}:
        raise Error('授予配置请求字段无效')
    registry._session_value(data['grant_id'])
    registry._session_value(data['session_id'])
    # Authentication, exact ID resolution, policy checks and immutable binding
    # share one write TX. Never fall through from one ledger to the other.
    with registry.connect() as db:
        db.execute('BEGIN IMMEDIATE')
        # Authenticate before resolving the identifier. Each exact ledger
        # branch retains its established session/capability error ordering.
        registry._agent_id(db,token)
        first=db.execute(
            'SELECT 1 FROM usage_first_total_grants WHERE grant_id=?',
            (data['grant_id'],)).fetchone()
        later=db.execute(
            'SELECT 1 FROM usage_multi_agent_allocations WHERE allocation_id=?',
            (data['grant_id'],)).fetchone()
        if first is not None and later is not None:
            raise Error('授予配置编号冲突',409)
        if first is not None:
            return GrantConfiguration(registry)._bind(
                db,token,data['grant_id'],session_id=data['session_id'],
                require_staging=True)
        if later is not None:
            from multi_agent_configuration import MultiAgentConfiguration
            return MultiAgentConfiguration(registry)._bind(
                db,token,data['grant_id'],session_id=data['session_id'],
                require_staging=True)
        raise Error('授予配置不存在',404)


def resolve_ledger(base,token,session_id,ledger_id,binding_digest,
                   expected_ledger_kind=None):
    from agent import request
    identifier(session_id); identifier(ledger_id); checksum(binding_digest)
    if (expected_ledger_kind is not None and
            (type(expected_ledger_kind) is not str or expected_ledger_kind not in LEDGER_KINDS)):
        raise ValueError('授予账本类型无效')
    response=request(base,token,'/agent/grants/resolve',{
        'session_id':session_id,'ledger_id':ledger_id,
        'expected_ledger_kind':expected_ledger_kind,
        'binding_digest':binding_digest})
    if (type(response) is not dict or set(response)!={
            'accepted','state','ledger_kind','ledger_id','binding_digest',
            'ready_for_execution'} or response.get('accepted') is not True or
            response.get('ready_for_execution') is not False or
            response.get('state')!='ledger_resolved' or
            type(response.get('ledger_kind')) is not str or
            response.get('ledger_kind') not in LEDGER_KINDS or
            response.get('ledger_id')!=ledger_id or
            response.get('binding_digest')!=binding_digest or
            (expected_ledger_kind is not None and
             response['ledger_kind']!=expected_ledger_kind)):
        raise ValueError('主控授予账本回执不匹配')
    return response


def fetch_and_stage(base,token,store,grant_id,ledger_kind=None):
    from agent import request
    identifier(grant_id)
    if ledger_kind is not None and (type(ledger_kind) is not str or ledger_kind not in LEDGER_KINDS):
        raise ValueError('授予账本类型无效')
    value=request(base,token,'/agent/grants/configuration',
                  dict(grant_id=grant_id,session_id=store.session_id))
    validate(value,store.server_id,store.session_id)
    if value['grant_id']!=grant_id:
        raise ValueError('主控返回了其他授予')
    resolver=lambda ident,digest:resolve_ledger(
        base,token,store.session_id,ident,digest,None)
    store.migrate(resolver)
    identity=resolve_ledger(base,token,store.session_id,grant_id,value['digest'],ledger_kind)
    # stage revalidates after its durable storage lock; network success alone
    # changes neither local runtime nor the master applied ledger.
    return store.stage(value,identity=identity)
