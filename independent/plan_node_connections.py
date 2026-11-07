"""Per-plan node TCP slots: absent inherits; explicit null means unlimited."""
import json
from core import Error
from node_config import connections

SCHEMA='''
CREATE TABLE IF NOT EXISTS plan_node_connections(
 plan_id INTEGER NOT NULL REFERENCES plans(id) ON DELETE CASCADE,
 node_id INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
 payload TEXT NOT NULL, PRIMARY KEY(plan_id,node_id));
'''


def node_connections(value):
    if not isinstance(value,dict) or set(value)!={'max_connections'}:
        raise ValueError('节点连接限制需要 max_connections')
    return connections(value)


def override_for_user(db,user_id,node_id):
    from plan_policy import node_override
    return node_override(db,user_id,node_id,'connections')


class PlanNodeConnections:
    def __init__(self,registry): self.registry=registry

    def manage(self,plan_id,node_id,data=None,*,inherit=False,db=None):
        from plan_nodes import PlanNodes,check
        from accounts import Accounts
        if data is not None:
            try: data=node_connections(data)
            except ValueError as error: raise Error(str(error)) from None
        with self.registry._resource_transaction(db,write=data is not None or inherit) as db:
            PlanNodes.exists(db,plan_id); hosts=Accounts.node_hosts(db,[node_id])
            old=db.execute('SELECT payload FROM plan_node_connections WHERE plan_id=? AND node_id=?',(plan_id,node_id)).fetchone()
            previous=json.loads(old['payload']) if old else None
            if data is not None or inherit:
                if not inherit: check(db,plan_id,[node_id])
                if inherit:
                    db.execute('DELETE FROM plan_node_connections WHERE plan_id=? AND node_id=?',(plan_id,node_id))
                else:
                    db.execute('''INSERT INTO plan_node_connections VALUES(?,?,?)
                        ON CONFLICT(plan_id,node_id) DO UPDATE SET payload=excluded.payload''',
                        (plan_id,node_id,json.dumps(data,sort_keys=True)))
                if previous!=data:
                    used=db.execute('''SELECT 1 FROM node_users a JOIN user_plans u ON u.user_id=a.user_id
                        WHERE a.node_id=? AND u.plan_id=? AND NOT EXISTS(
                        SELECT 1 FROM user_connections c WHERE c.user_id=a.user_id) LIMIT 1''',(node_id,plan_id)).fetchone()
                    if used: self.registry._bump(db,hosts[node_id])
                previous=data
            row=db.execute('SELECT payload FROM plan_connections WHERE plan_id=?',(plan_id,)).fetchone()
            inherited=json.loads(row['payload']) if row else {'max_connections':None}
            effective=dict(inherited)
            if previous is not None: effective.update(previous)
            return {'plan_id':plan_id,'node_id':node_id,'override':previous,'inherited':inherited,'effective':effective}
