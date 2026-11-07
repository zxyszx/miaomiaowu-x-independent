"""Internal immutable configuration binding; never an Agent execution ACK.

Explicit staging transport may deliver this envelope. In particular, a normal
runtime ACK cannot turn this prerequisite into an applied aggregate grant.
"""
import json
import time
from core import Error
from node_config import digest

SCHEMA='''
CREATE TABLE IF NOT EXISTS usage_first_total_configurations(
 grant_id TEXT PRIMARY KEY REFERENCES usage_first_total_grants(grant_id),
 payload TEXT NOT NULL,digest TEXT NOT NULL,created_at REAL NOT NULL);
'''

class GrantConfiguration:
    def __init__(self,registry): self.registry=registry

    def bind(self,token,grant_id,*,session_id=None,require_staging=False):
        r=self.registry; r._session_value(grant_id)
        with r.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            return self._bind(db,token,grant_id,session_id=session_id,require_staging=require_staging)

    def _bind(self,db,token,grant_id,*,session_id=None,require_staging=False):
        """Caller owns the write transaction, including subsequent ACK writes."""
        r=self.registry
        sid=r._agent_id(db,token)
        g=db.execute('SELECT * FROM usage_first_total_grants WHERE grant_id=?',(grant_id,)).fetchone()
        if g is None: raise Error('授予不存在',404)
        fingerprint=db.execute('SELECT token_hash FROM agents WHERE server_id=?',(sid,)).fetchone()[0]
        if g['server_id']!=sid or g['token_hash']!=fingerprint: raise Error('授予身份不匹配',403)
        r._check_session(db,sid,g['session_id'])
        if require_staging:
            from grant_capabilities import STAGING,protocols
            capability=db.execute('SELECT * FROM grant_capabilities WHERE server_id=?',(sid,)).fetchone()
            if (session_id!=g['session_id'] or capability is None or
                capability['session_id']!=session_id or capability['token_hash']!=fingerprint or
                STAGING not in protocols(json.loads(capability['offer']))):
                raise Error('当前会话未协商授予暂存协议',409)
        # Refresh the same policies as ordinary configurations in this TX.
        snapshot=r._snapshot(db,sid)
        now=time.time()
        if not g['issued_at']<=now<g['valid_until']: raise Error('授予窗口不可用',409)
        from usage_cycles import boundary
        if boundary(db,g['user_id'])!=(g['cycle_id'],db.execute('SELECT ends_at FROM usage_cycles WHERE id=?',(g['cycle_id'],)).fetchone()[0]):
            raise Error('授予周期已变化',409)
        from plan_lifecycle import view
        if view(db,g['user_id'])['status']!='active': raise Error('用户已停用',409)
        from plan_nodes import check_user
        check_user(db,g['user_id'],[g['node_id']])
        binding=db.execute('SELECT 1 FROM node_users WHERE node_id=? AND user_id=? AND credential=?',
                           (g['node_id'],g['user_id'],g['credential'])).fetchone()
        node=next((n for n in snapshot['nodes'] if n['id']==g['node_id']),None)
        if not binding or node is None or not node['config']['enabled'] or node['config'].get('protocol')!='vless':
            raise Error('授予节点或身份不可用',409)
        from total_policy import current
        policy=current(db,g['cycle_id'])
        if policy is None or policy['id']!=g['policy_id']: raise Error('授予策略已变化',409)
        from total_usage import directions
        used=directions(db,g['cycle_id'])
        if used[2] or sum(used[:2])!=int(g['baseline_bytes']): raise Error('授予基线已变化',409)
        client={'user_id':g['user_id'],'uuid':g['credential'],
                'cycle_usage':{'upload_bytes':used[0],'download_bytes':used[1]}}
        plan=db.execute('SELECT plan_id FROM user_plans WHERE user_id=?',(g['user_id'],)).fetchone()
        if plan is None: raise Error('授予套餐已解绑',409)
        budget=db.execute('SELECT upload_bytes,download_bytes FROM usage_cycle_budgets WHERE cycle_id=?',(g['cycle_id'],)).fetchone()
        if budget is not None:
            client['cycle_budget']={k:budget[k] for k in ('upload_bytes','download_bytes')}
        r._client_policy(db,client,g['node_id'],True)
        until=min(g['valid_until'],client.get('valid_until',g['valid_until']))
        if until<=now: raise Error('授予策略期限已结束',409)
        content={'schema':'independent.total-grant.binding.v1','grant_id':grant_id,
                 'server_id':sid,'session_id':g['session_id'],'cycle_id':g['cycle_id'],
                 'policy_id':g['policy_id'],'plan_id':plan[0],'revision':snapshot['revision'],
                 'configuration_digest':snapshot['digest'],'node_id':g['node_id'],
                 'node_config':node['config'],'client':client,
                 'reserved_bytes':g['reserved_bytes'],'baseline_bytes':int(g['baseline_bytes']),
                 'issued_at':g['issued_at'],'valid_until':until}
        if 'bandwidth' in snapshot: content['server_bandwidth']=snapshot['bandwidth']
        encoded=json.dumps(content,sort_keys=True,separators=(',',':')); checksum=digest(content)
        old=db.execute('SELECT * FROM usage_first_total_configurations WHERE grant_id=?',(grant_id,)).fetchone()
        if old is not None:
            if old['payload']!=encoded or old['digest']!=checksum: raise Error('授予配置已变化，不能改写原绑定',409)
        else:
            db.execute('INSERT INTO usage_first_total_configurations VALUES(?,?,?,?)',(grant_id,encoded,checksum,now))
        return dict(content,digest=checksum,delivery_state='not_delivered',ready_for_execution=False)
