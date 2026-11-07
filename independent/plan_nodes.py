"""Opt-in plan node ranges; changing a range never grants new credentials."""
from core import Error

SCHEMA='''
CREATE TABLE IF NOT EXISTS plan_node_scopes(
 plan_id INTEGER PRIMARY KEY REFERENCES plans(id) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS plan_nodes(
 plan_id INTEGER NOT NULL REFERENCES plan_node_scopes(plan_id) ON DELETE CASCADE,
 node_id INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
 PRIMARY KEY(plan_id,node_id));
'''

def check(db,plan_id,nodes):
    if plan_id is None or not db.execute('SELECT 1 FROM plan_node_scopes WHERE plan_id=?',(plan_id,)).fetchone(): return
    from node_groups import allowed as group_allowed
    allowed=group_allowed(db,plan_id)
    if set(nodes)-allowed: raise Error('节点不在套餐允许范围内',403)

def check_user(db,user_id,nodes):
    plan=db.execute('SELECT plan_id FROM user_plans WHERE user_id=?',(user_id,)).fetchone()
    if plan: check(db,plan[0],nodes)

class PlanNodes:
    def __init__(self,registry): self.registry=registry

    @staticmethod
    def exists(db,ident):
        if not db.execute('SELECT 1 FROM plans WHERE id=?',(ident,)).fetchone(): raise Error('套餐不存在',404)

    @staticmethod
    def result(db,ident):
        from node_groups import allowed
        direct=[r[0] for r in db.execute('SELECT node_id FROM plan_nodes WHERE plan_id=? ORDER BY node_id',(ident,))]
        groups=[r[0] for r in db.execute('SELECT group_id FROM plan_node_groups WHERE plan_id=? ORDER BY group_id',(ident,))]
        result={'plan_id':ident,'restricted':bool(db.execute('SELECT 1 FROM plan_node_scopes WHERE plan_id=?',(ident,)).fetchone()),
                'node_ids':sorted(allowed(db,ident))}
        if groups: result.update(group_ids=groups,direct_node_ids=direct)
        return result

    def get(self,ident,*,db=None):
        with self.registry._resource_transaction(db) as db:
            self.exists(db,ident)
            return self.result(db,ident)

    def save(self,ident,data,*,db=None):
        from accounts import Accounts
        if not isinstance(data,dict) or set(data) not in ({'node_ids'},{'node_ids','group_ids'}): raise Error('套餐节点范围需要 node_ids')
        nodes=data['node_ids']; Accounts.validate_nodes(nodes)
        groups=data.get('group_ids',[]); Accounts.validate_nodes(groups)
        with self.registry._resource_transaction(db,write=True) as db:
            self.exists(db,ident)
            Accounts.node_hosts(db,nodes)
            from node_groups import NodeGroups,enforce
            for gid in groups: NodeGroups.exists(db,gid)
            db.execute('INSERT OR IGNORE INTO plan_node_scopes VALUES(?)',(ident,))
            db.execute('DELETE FROM plan_nodes WHERE plan_id=?',(ident,))
            db.executemany('INSERT INTO plan_nodes VALUES(?,?)',[(ident,nid) for nid in nodes])
            db.execute('DELETE FROM plan_node_groups WHERE plan_id=?',(ident,))
            db.executemany('INSERT INTO plan_node_groups VALUES(?,?)',[(ident,gid) for gid in groups])
            enforce(db,self.registry,[ident])
            return self.result(db,ident)

    def clear(self,ident,*,db=None):
        with self.registry._resource_transaction(db,write=True) as db:
            self.exists(db,ident)
            db.execute('DELETE FROM plan_node_scopes WHERE plan_id=?',(ident,))
            return self.result(db,ident)
