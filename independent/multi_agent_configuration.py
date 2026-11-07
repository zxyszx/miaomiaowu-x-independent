"""Immutable staging configuration for a later multi-Agent allocation.

This emits the existing binding-v1 envelope so an Agent can durably stage it,
but stores it in a separate ledger that no installation, lifecycle, execution
or usage endpoint consumes. Staging is not delivery or execution evidence.
"""
import json
import time

from agent_liveness import ONLINE_WINDOW
from core import Error
from drain_seal import inspect as inspect_seal
from grant_capabilities import STAGING,protocols
from node_config import digest
from total_policy import current as current_policy
from total_usage import directions


SCHEMA='''
CREATE TABLE IF NOT EXISTS usage_multi_agent_configurations(
 allocation_id TEXT PRIMARY KEY REFERENCES usage_multi_agent_allocations(allocation_id),
 payload TEXT NOT NULL,digest TEXT NOT NULL,created_at REAL NOT NULL);
'''


class MultiAgentConfiguration:
    def __init__(self,registry):
        self.registry=registry

    def bind(self,token,allocation_id,*,session_id=None,require_staging=True):
        self.registry._session_value(allocation_id)
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            return self._bind(db,token,allocation_id,session_id=session_id,
                              require_staging=require_staging)

    def _bind(self,db,token,allocation_id,*,session_id=None,
              require_staging=True):
        r=self.registry
        sid=r._agent_id(db,token)
        allocation=db.execute(
            'SELECT * FROM usage_multi_agent_allocations WHERE allocation_id=?',
            (allocation_id,)).fetchone()
        if allocation is None:
            raise Error('后续配额不存在',404)
        agent=db.execute('SELECT * FROM agents WHERE server_id=?',(sid,)).fetchone()
        fingerprint=agent['token_hash']
        if (allocation['server_id']!=sid
                or allocation['token_hash']!=fingerprint
                or allocation['session_id']!=session_id):
            raise Error('后续配额身份不匹配',403)
        r._check_session(db,sid,session_id)
        now=time.time()
        if (agent['last_seen'] is None
                or not 0<=now-agent['last_seen']<ONLINE_WINDOW):
            raise Error('Agent已离线，不能领取后续配额',409)
        if not allocation['issued_at']<=now<allocation['valid_until']:
            raise Error('后续配额窗口不可用',409)

        if require_staging:
            capability=db.execute(
                'SELECT * FROM grant_capabilities WHERE server_id=?',(sid,)).fetchone()
            if (capability is None or capability['session_id']!=session_id
                    or capability['token_hash']!=fingerprint
                    or STAGING not in protocols(json.loads(capability['offer']))):
                raise Error('当前会话未协商授予暂存协议',409)

        cycle=db.execute('SELECT * FROM usage_cycles WHERE id=?',
                         (allocation['cycle_id'],)).fetchone()
        if (cycle is None or cycle['expired']
                or not cycle['starts_at']<=now<cycle['ends_at']):
            raise Error('后续配额周期已失效',409)
        first=db.execute('SELECT * FROM usage_first_total_grants WHERE grant_id=?',
                         (allocation['first_grant_id'],)).fetchone()
        if (first is None or first['cycle_id']!=allocation['cycle_id']
                or first['user_id']!=allocation['user_id']
                or first['seal_digest']!=allocation['seal_digest']):
            raise Error('后续配额首次授予基线不匹配',409)
        sealed,seal_digest=inspect_seal(db,allocation['cycle_id'])
        if (seal_digest!=allocation['seal_digest']
                or sealed['cycle_id']!=allocation['cycle_id']
                or sealed['ready_for_execution'] is not False):
            raise Error('后续配额封存基线已失效',409)
        policy=current_policy(db,allocation['cycle_id'])
        if policy is None or policy['id']!=allocation['policy_id']:
            raise Error('后续配额策略已变化',409)

        from usage_cycles import boundary
        if boundary(db,allocation['user_id'])!=(allocation['cycle_id'],
                                                    cycle['ends_at']):
            raise Error('后续配额周期已变化',409)
        from plan_lifecycle import view as lifecycle_view
        if lifecycle_view(db,allocation['user_id'])['status']!='active':
            raise Error('后续配额用户已停用',409)
        from plan_nodes import check_user
        check_user(db,allocation['user_id'],[allocation['node_id']])
        binding=db.execute('''SELECT 1 FROM node_users u JOIN nodes n ON n.id=u.node_id
            WHERE n.server_id=? AND u.node_id=? AND u.user_id=? AND u.credential=?''',
            (sid,allocation['node_id'],allocation['user_id'],
             allocation['credential'])).fetchone()
        snapshot=r._snapshot(db,sid)
        node=next((item for item in snapshot['nodes']
                   if item['id']==allocation['node_id']),None)
        if (not binding or node is None or not node['config']['enabled']
                or node['config'].get('protocol')!='vless'):
            raise Error('后续配额节点或身份不可用',409)

        used=directions(db,allocation['cycle_id'])
        if used[2] or sum(used[:2])!=int(allocation['baseline_bytes']):
            raise Error('后续配额方向用量基线已变化',409)
        plan=db.execute('SELECT plan_id FROM user_plans WHERE user_id=?',
                        (allocation['user_id'],)).fetchone()
        if plan is None:
            raise Error('后续配额套餐已解绑',409)
        client={'user_id':allocation['user_id'],
                'uuid':allocation['credential'],
                'cycle_id':allocation['cycle_id'],
                'cycle_usage':{'upload_bytes':used[0],
                               'download_bytes':used[1]}}
        budget=db.execute('''SELECT upload_bytes,download_bytes
            FROM usage_cycle_budgets WHERE cycle_id=?''',
            (allocation['cycle_id'],)).fetchone()
        if budget is not None:
            client['cycle_budget']={key:budget[key]
                                    for key in ('upload_bytes','download_bytes')}
        r._client_policy(db,client,allocation['node_id'],True)
        if client.get('valid_until',allocation['valid_until'])<allocation['valid_until']:
            raise Error('后续配额策略期限已缩短',409)

        content={'schema':'independent.total-grant.binding.v1',
                 'grant_id':allocation['allocation_id'],
                 'server_id':sid,'session_id':allocation['session_id'],
                 'cycle_id':allocation['cycle_id'],
                 'policy_id':allocation['policy_id'],'plan_id':plan[0],
                 'revision':snapshot['revision'],
                 'configuration_digest':snapshot['digest'],
                 'node_id':allocation['node_id'],'node_config':node['config'],
                 'client':client,'reserved_bytes':allocation['reserved_bytes'],
                 'baseline_bytes':int(allocation['baseline_bytes']),
                 'issued_at':allocation['issued_at'],
                 'valid_until':allocation['valid_until']}
        if 'bandwidth' in snapshot:
            content['server_bandwidth']=snapshot['bandwidth']
        encoded=json.dumps(content,sort_keys=True,separators=(',',':'),
                           ensure_ascii=False,allow_nan=False)
        checksum=digest(content)
        old=db.execute('''SELECT * FROM usage_multi_agent_configurations
            WHERE allocation_id=?''',(allocation_id,)).fetchone()
        if old is not None:
            if old['payload']!=encoded or old['digest']!=checksum:
                raise Error('后续配额配置已变化，不能改写',409)
        else:
            db.execute('''INSERT INTO usage_multi_agent_configurations
                VALUES(?,?,?,?)''',(allocation_id,encoded,checksum,now))
        return dict(content,digest=checksum,delivery_state='not_delivered',
                    ready_for_execution=False)
