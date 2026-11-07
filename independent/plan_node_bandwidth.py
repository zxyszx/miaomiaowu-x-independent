"""Per-plan node rates: absent inherits; explicit null rates mean unlimited."""
import json
import hashlib
import re
from core import Error
from node_config import bandwidth

SCHEMA='''
CREATE TABLE IF NOT EXISTS plan_node_bandwidth(
 plan_id INTEGER NOT NULL REFERENCES plans(id) ON DELETE CASCADE,
 node_id INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
 payload TEXT NOT NULL, PRIMARY KEY(plan_id,node_id));
'''


def override_for_user(db,user_id,node_id):
    from plan_policy import node_override
    return node_override(db,user_id,node_id,'bandwidth')


class PlanNodeBandwidth:
    def __init__(self,registry): self.registry=registry

    @staticmethod
    def version(db,plan_id,node_id,override,inherited):
        from plan_nodes import PlanNodes
        # Content version, not a sequence or an execution/authorization token.
        node=db.execute('SELECT server_id FROM nodes WHERE id=?',(node_id,)).fetchone()
        config=db.execute('SELECT payload FROM node_configs WHERE node_id=?',(node_id,)).fetchone()
        scope=PlanNodes.result(db,plan_id)
        content=[plan_id,node_id,override,inherited,node[0],
                 json.loads(config[0]) if config else None,scope]
        encoded=json.dumps(content,sort_keys=True,separators=(',',':')).encode()
        return '"bw1-'+hashlib.sha256(encoded).hexdigest()+'"'

    def manage(self,plan_id,node_id,data=None,*,inherit=False,if_match=None,versioned=False,db=None):
        from plan_nodes import PlanNodes,check
        from accounts import Accounts
        if if_match is not None and (not isinstance(if_match,str) or not re.fullmatch(r'"bw1-[0-9a-f]{64}"',if_match)):
            raise Error('限速配置版本无效')
        if data is not None:
            try: data=bandwidth(data)
            except ValueError as error: raise Error(str(error)) from None
        with self.registry._resource_transaction(db,write=data is not None or inherit) as db:
            PlanNodes.exists(db,plan_id); hosts=Accounts.node_hosts(db,[node_id])
            old=db.execute('SELECT payload FROM plan_node_bandwidth WHERE plan_id=? AND node_id=?',(plan_id,node_id)).fetchone()
            previous=json.loads(old['payload']) if old else None
            inherited=json.loads(db.execute('SELECT bandwidth FROM plans WHERE id=?',(plan_id,)).fetchone()[0])
            if data is not None or inherit:
                if not inherit: check(db,plan_id,[node_id])
                if if_match is not None and if_match!=self.version(db,plan_id,node_id,previous,inherited):
                    raise Error('限速配置已变化，请重新读取后再编辑',412)
                if inherit:
                    db.execute('DELETE FROM plan_node_bandwidth WHERE plan_id=? AND node_id=?',(plan_id,node_id))
                else:
                    db.execute('''INSERT INTO plan_node_bandwidth VALUES(?,?,?)
                        ON CONFLICT(plan_id,node_id) DO UPDATE SET payload=excluded.payload''',
                        (plan_id,node_id,json.dumps(data,sort_keys=True)))
                if previous!=data:
                    # Only identities actually executing this plan/node are affected.
                    used=db.execute('''SELECT 1 FROM node_users a JOIN user_plans u ON u.user_id=a.user_id
                        WHERE a.node_id=? AND u.plan_id=? AND NOT EXISTS(
                        SELECT 1 FROM user_bandwidth b WHERE b.user_id=a.user_id) AND NOT EXISTS(
                        SELECT 1 FROM temporary_bandwidth t WHERE t.user_id=a.user_id) LIMIT 1''',(node_id,plan_id)).fetchone()
                    if used: self.registry._bump(db,hosts[node_id])
                previous=data
            result={'plan_id':plan_id,'node_id':node_id,'override':previous,'inherited':inherited,
                    'effective':previous if previous is not None else inherited}
            if versioned: result['version']=self.version(db,plan_id,node_id,previous,inherited)
            return result
