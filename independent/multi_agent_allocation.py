"""Durable subsequent multi-Agent quota reservations, never execution Grants.

Every allocation is written under the same SQLite write transaction that
reads confirmed usage and every conservative hold for the cycle. Identity or
time becoming stale never releases a reservation and never permits takeover.
No HTTP, scheduler, listener or enforcement path consumes these records.
"""
import time
import uuid

from agent_liveness import ONLINE_WINDOW
from core import Error
from drain_seal import inspect as inspect_seal
from total_policy import current as current_policy
from total_usage import current as confirmed_total
from usage_ledger import integer
from usage_settlement import EXECUTION_LEASE


SCHEMA='''
CREATE TABLE IF NOT EXISTS usage_multi_agent_allocations(
 request_id TEXT PRIMARY KEY,allocation_id TEXT NOT NULL UNIQUE,
 first_grant_id TEXT NOT NULL REFERENCES usage_first_total_grants(grant_id),
 cycle_id TEXT NOT NULL REFERENCES usage_cycles(id) ON DELETE CASCADE,
 policy_id INTEGER NOT NULL REFERENCES usage_total_policy_versions(id),
 seal_digest TEXT NOT NULL,server_id INTEGER NOT NULL,session_id TEXT NOT NULL,
 token_hash TEXT NOT NULL,node_id INTEGER NOT NULL,user_id INTEGER NOT NULL,
 credential TEXT NOT NULL,requested_bytes INTEGER NOT NULL CHECK(requested_bytes>0),
 reserved_bytes INTEGER NOT NULL CHECK(reserved_bytes>0 AND reserved_bytes<=requested_bytes),
 baseline_bytes TEXT NOT NULL,issued_at REAL NOT NULL,valid_until REAL NOT NULL);
CREATE INDEX IF NOT EXISTS usage_multi_agent_allocation_cycle
 ON usage_multi_agent_allocations(cycle_id);
'''


FIELDS={'request_id','first_grant_id','cycle_id','policy_id','seal_digest',
        'session_id','node_id','user_id','credential','requested_bytes'}


def receipt(row):
    return {**{key:row[key] for key in (
        'request_id','allocation_id','first_grant_id','cycle_id','policy_id',
        'seal_digest','server_id','session_id','node_id','user_id','credential',
        'requested_bytes','reserved_bytes','issued_at','valid_until')},
        'baseline_bytes':int(row['baseline_bytes']),
        'allocation_state':'held_not_delivered',
        'ready_for_execution':False}


def outstanding(db,cycle_id):
    """Return every later allocation as an irreversible conservative hold."""
    if not db.in_transaction:
        raise RuntimeError('multi-Agent balance requires a transaction')
    from usage_accounting import MULTI,outstanding as source_outstanding
    return source_outstanding(db,cycle_id,MULTI)


class MultiAgentAllocations:
    def __init__(self,registry):
        self.registry=registry

    def allocate(self,token,data):
        if not isinstance(data,dict) or set(data)!=FIELDS:
            raise Error('后续配额字段无效')
        r=self.registry
        for key in ('request_id','first_grant_id','cycle_id','session_id','credential'):
            r._session_value(data[key])
        if (not isinstance(data['seal_digest'],str)
                or len(data['seal_digest'])!=64
                or any(char not in '0123456789abcdef' for char in data['seal_digest'])):
            raise Error('后续配额封存标识无效')
        if any(not integer(data[key],1) for key in
               ('policy_id','node_id','user_id','requested_bytes')):
            raise Error('后续配额需要有效资源编号和正整数字节')

        with r.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            sid=r._agent_id(db,token)
            r._check_session(db,sid,data['session_id'])
            agent=db.execute('SELECT * FROM agents WHERE server_id=?',(sid,)).fetchone()
            token_hash=agent['token_hash']

            now=time.time()
            if (agent['last_seen'] is None or not 0<=now-agent['last_seen']<ONLINE_WINDOW):
                raise Error('Agent已离线，不能分配后续配额',409)
            cycle=db.execute('SELECT * FROM usage_cycles WHERE id=?',
                             (data['cycle_id'],)).fetchone()
            if (cycle is None or cycle['expired']
                    or not cycle['starts_at']<=now<cycle['ends_at']):
                raise Error('后续配额周期未生效或已结束',409)

            first=db.execute('SELECT * FROM usage_first_total_grants WHERE grant_id=?',
                             (data['first_grant_id'],)).fetchone()
            if (first is None or first['cycle_id']!=data['cycle_id']
                    or first['user_id']!=data['user_id']
                    or first['seal_digest']!=data['seal_digest']):
                raise Error('后续配额与首次授予基线不匹配',409)
            sealed,digest=inspect_seal(db,data['cycle_id'])
            if (digest!=data['seal_digest'] or sealed['cycle_id']!=data['cycle_id']
                    or sealed['ready_for_execution'] is not False):
                raise Error('后续配额封存基线已失效',409)

            policy=current_policy(db,data['cycle_id'])
            if policy is None or policy['id']!=data['policy_id']:
                raise Error('后续配额策略版本不是当前版本',409)

            from usage_cycles import boundary
            if boundary(db,data['user_id'])!=(data['cycle_id'],cycle['ends_at']):
                raise Error('后续配额周期不是用户当前周期',409)
            from plan_lifecycle import view as lifecycle_view
            if lifecycle_view(db,data['user_id'])['status']!='active':
                raise Error('后续配额用户已停用',409)
            from plan_nodes import check_user
            check_user(db,data['user_id'],[data['node_id']])
            binding=db.execute('''SELECT 1 FROM node_users u JOIN nodes n ON n.id=u.node_id
                WHERE n.server_id=? AND u.node_id=? AND u.user_id=? AND u.credential=?''',
                (sid,data['node_id'],data['user_id'],data['credential'])).fetchone()
            snapshot=r._snapshot(db,sid)
            node=next((item for item in snapshot['nodes']
                       if item['id']==data['node_id']),None)
            if (not binding or node is None or not node['config']['enabled']
                    or node['config'].get('protocol')!='vless'):
                raise Error('后续配额不属于此服务器当前执行身份',403)
            client={'user_id':data['user_id'],'uuid':data['credential']}
            r._client_policy(db,client,data['node_id'],True)

            until=min(now+EXECUTION_LEASE,cycle['ends_at'],
                      agent['last_seen']+ONLINE_WINDOW,
                      client.get('valid_until',float('inf')))
            if until<=now:
                raise Error('后续配额窗口已结束',409)

            old=db.execute(
                'SELECT * FROM usage_multi_agent_allocations WHERE request_id=?',
                (data['request_id'],)).fetchone()
            if old is not None:
                if (old['server_id']!=sid or old['session_id']!=data['session_id']
                        or old['token_hash']!=token_hash):
                    raise Error('后续配额不属于此Agent会话',403)
                if any(old[key]!=data[key] for key in FIELDS):
                    raise Error('后续配额请求编号冲突',409)
                if not old['issued_at']<=now<old['valid_until']:
                    raise Error('后续配额已过期，不能恢复',409)
                from usage_accounting import MULTI,require_source
                require_source(db,MULTI,old)
                return receipt(old)

            confirmed=confirmed_total(db,data['cycle_id'])
            # Import here to avoid a module cycle: total_allocation.held includes
            # this table and must observe every hold in this same write TX.
            from total_allocation import held
            unresolved=held(db,data['cycle_id'])
            count=data['requested_bytes']
            if policy['total_bytes'] is not None:
                count=min(count,max(0,policy['total_bytes']-confirmed-unresolved))
            if count==0:
                raise Error('周期没有可分配的后续配额余额',409)

            ident=str(uuid.uuid4())
            db.execute('''INSERT INTO usage_multi_agent_allocations
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (data['request_id'],ident,data['first_grant_id'],data['cycle_id'],
                 policy['id'],data['seal_digest'],sid,data['session_id'],token_hash,
                 data['node_id'],data['user_id'],data['credential'],
                 data['requested_bytes'],count,str(confirmed),now,until))
            row=db.execute(
                'SELECT * FROM usage_multi_agent_allocations WHERE allocation_id=?',
                (ident,)).fetchone()
            from usage_accounting import MULTI,create_source
            create_source(db,MULTI,row)
            return receipt(row)
