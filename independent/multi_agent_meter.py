"""C7-D internal fixture meter. No production route or settlement authority."""
import json
import time

from core import Error
from usage_ledger import integer
from usage_accounting import MULTI,require_source,accept_attributed
from multi_agent_lifecycle import gated

SCHEMA='''
CREATE TABLE IF NOT EXISTS usage_multi_meter(
 allocation_id TEXT NOT NULL REFERENCES usage_multi_execution(allocation_id),
 sequence INTEGER NOT NULL CHECK(sequence>0),
 upload_bytes INTEGER NOT NULL CHECK(upload_bytes>=0),
 download_bytes INTEGER NOT NULL CHECK(download_bytes>=0),
 received_at REAL NOT NULL,
 PRIMARY KEY(allocation_id,sequence));
'''


def execution_chain(db,allocation):
    """Compatibility delegate; caller retains authentication and require_source."""
    from multi_execution_evidence import _read_multi_execution
    return _read_multi_execution(db,MULTI,allocation['allocation_id'],
                                 expected_allocation=allocation)


def live_identity(registry,db,allocation,value,now):
    from agent_liveness import ONLINE_WINDOW
    agent=db.execute('SELECT last_seen FROM agents WHERE server_id=?',
                     (allocation['server_id'],)).fetchone()
    if agent['last_seen'] is None or not 0<=now-agent['last_seen']<ONLINE_WINDOW:
        raise Error('multi计量Agent离线',409)
    from grant_capabilities import STAGING,STAGE_IDENTITY,protocols
    from multi_agent_lifecycle import MULTI_EXECUTION
    cap=db.execute('SELECT * FROM grant_capabilities WHERE server_id=?',(allocation['server_id'],)).fetchone()
    if (cap is None or (cap['session_id'],cap['token_hash'])!=(allocation['session_id'],allocation['token_hash'])
            or not {STAGING,STAGE_IDENTITY,MULTI_EXECUTION}<=set(protocols(json.loads(cap['offer'])))):
        raise Error('multi计量缺少当前会话能力',409)
    cycle=db.execute('SELECT * FROM usage_cycles WHERE id=?',(allocation['cycle_id'],)).fetchone()
    from usage_cycles import boundary
    if (cycle is None or cycle['expired'] or boundary(db,allocation['user_id'],now)
            !=(allocation['cycle_id'],cycle['ends_at'])):
        raise Error('multi计量周期失效',409)
    from plan_lifecycle import view
    from plan_nodes import check_user
    if view(db,allocation['user_id'])['status']!='active': raise Error('multi用户停用',409)
    check_user(db,allocation['user_id'],[allocation['node_id']])
    binding=db.execute('''SELECT c.payload FROM nodes n
        JOIN node_users u ON u.node_id=n.id JOIN node_configs c ON c.node_id=n.id
        WHERE n.server_id=? AND n.id=? AND u.user_id=? AND u.credential=?''',
        (allocation['server_id'],allocation['node_id'],allocation['user_id'],allocation['credential'])).fetchone()
    plan=db.execute('SELECT plan_id FROM user_plans WHERE user_id=?',(allocation['user_id'],)).fetchone()
    if (binding is None or json.loads(binding['payload'])!=value['node_config']
            or plan is None or plan[0]!=value['plan_id']):
        raise Error('multi节点或用户绑定已变化',409)


class MultiAgentMeter:
    def __init__(self,registry): self.registry=registry

    def report(self,token,data):
        gate=gated(self.registry)
        fields={'ledger_kind','ledger_id','session_id','policy_id','binding_digest',
                'execution_id','sequence','upload_bytes','download_bytes'}
        tail_fields={'operation','authority_digest','body_digest'}
        if type(data) is not dict or set(data) not in (fields,fields|tail_fields) or type(data['ledger_kind']) is not str or data['ledger_kind']!=MULTI:
            raise Error('multi计量字段无效')
        from grant_agent import identifier,checksum
        try:
            for key in ('ledger_id','session_id','execution_id'): identifier(data[key])
            checksum(data['binding_digest'])
        except ValueError as exc: raise Error('multi计量身份无效') from exc
        for key in ('policy_id','sequence','upload_bytes','download_bytes'):
            if not integer(data[key],1 if key in ('policy_id','sequence') else 0):
                raise Error('multi计量需要有效整数字节')
        with gate.transition(),self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            r=self.registry; sid=r._agent_id(db,token); r._check_session(db,sid,data['session_id'])
            ident=data['ledger_id']
            if db.execute('SELECT 1 FROM usage_first_total_grants WHERE grant_id=?',(ident,)).fetchone():
                raise Error('multi账本编号冲突',409)
            a=db.execute('SELECT * FROM usage_multi_agent_allocations WHERE allocation_id=?',(ident,)).fetchone()
            fingerprint=db.execute('SELECT token_hash FROM agents WHERE server_id=?',(sid,)).fetchone()[0]
            if a is None or (a['server_id'],a['session_id'],a['token_hash'])!=(sid,data['session_id'],fingerprint):
                raise Error('multi计量不属于当前Agent会话',403)
            value,fact,state=execution_chain(db,a)
            if (data['policy_id'],data['binding_digest'],data['execution_id'])!=(a['policy_id'],value['digest'],fact['execution_id']):
                raise Error('multi计量执行身份不匹配',409)
            source=require_source(db,MULTI,a)
            auth=value.get('tail_authority')
            from multi_agent_tail import unpack
            if auth:
                if (set(data)!=fields|tail_fields or data['operation']!='ordinary'
                        or data['authority_digest']!=auth['body_digest']):raise Error('tail普通请求版本不匹配',409)
                unpack(data)
            elif set(data)!=fields:raise Error('v1不得冒充tail协议',409)
            old=db.execute('SELECT * FROM usage_multi_meter WHERE allocation_id=? AND sequence=?',
                           (ident,data['sequence'])).fetchone()
            if old is not None:
                if any(old[k]!=data[k] for k in ('upload_bytes','download_bytes')):
                    raise Error('multi计量序列正文冲突',409)
                if auth:
                    typed=db.execute('SELECT body FROM usage_multi_tail_ordinary WHERE allocation_id=? AND sequence=?',(ident,data['sequence'])).fetchone()
                    from multi_agent_lifecycle import encoded
                    if typed is None or typed['body']!=encoded(data):raise Error('tail普通原正文冲突',409)
            else:
                if auth and db.execute('SELECT 1 FROM usage_multi_tail_fences WHERE allocation_id=?',(ident,)).fetchone():
                    raise Error('tail普通接收已被栅栏关闭',409)
                now=time.time()
                if state!='execution_recorded' or not a['issued_at']<=now<fact['execute_until']:
                    raise Error('multi普通计量窗口结束或停止，尾量尚未启用',409)
                live_identity(r,db,a,value,now)
                from total_usage import directions
                if directions(db,a['cycle_id'])[2]: raise Error('multi历史方向未确认',409)
                accept_attributed(db,MULTI,a,data['sequence'],data['upload_bytes'],
                                  data['download_bytes'],fact['execution_id'],now)
                db.execute('INSERT INTO usage_multi_meter VALUES(?,?,?,?,?)',
                           (ident,data['sequence'],data['upload_bytes'],data['download_bytes'],now))
                if auth:
                    from multi_agent_lifecycle import encoded
                    db.execute('INSERT INTO usage_multi_tail_ordinary VALUES(?,?,?,?)',
                               (ident,data['sequence'],encoded(data),data['body_digest']))
                source=require_source(db,MULTI,a)
            up,down=int(source['upload_bytes']),int(source['download_bytes'])
            result=dict(accepted=True,duplicate=old is not None,ledger_kind=MULTI,ledger_id=ident,
                        sequence=data['sequence'],confirmed_bytes=up+down,
                        held_bytes=a['reserved_bytes']-up-down,ready_for_execution=False)
            if auth:result.update(execution_id=fact['execution_id'],body_digest=data['body_digest'])
            return result
