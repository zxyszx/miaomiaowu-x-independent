"""Reusable owner-node sets for explicit plan scope composition."""
from core import Error

SCHEMA='''
CREATE TABLE IF NOT EXISTS node_groups(id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS node_group_members(
 group_id INTEGER NOT NULL REFERENCES node_groups(id) ON DELETE CASCADE,
 node_id INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
 PRIMARY KEY(group_id,node_id));
CREATE TABLE IF NOT EXISTS plan_node_groups(
 plan_id INTEGER NOT NULL REFERENCES plan_node_scopes(plan_id) ON DELETE CASCADE,
 group_id INTEGER NOT NULL REFERENCES node_groups(id) ON DELETE CASCADE,
 PRIMARY KEY(plan_id,group_id));
'''


def allowed(db,plan_id):
    return {r[0] for r in db.execute('''SELECT node_id FROM plan_nodes WHERE plan_id=?
        UNION SELECT m.node_id FROM node_group_members m JOIN plan_node_groups p
        ON p.group_id=m.group_id WHERE p.plan_id=?''',(plan_id,plan_id))}


def enforce(db,registry,plans):
    """Revoke outside the union, with each affected host versioned only once."""
    hosts=set()
    for pid in plans:
        nodes=allowed(db,pid)
        rows=db.execute('''SELECT a.node_id,a.user_id,n.server_id FROM node_users a
            JOIN nodes n ON n.id=a.node_id JOIN user_plans u ON u.user_id=a.user_id
            WHERE u.plan_id=?''',(pid,)).fetchall()
        for row in rows:
            if row['node_id'] not in nodes:
                db.execute('DELETE FROM node_users WHERE node_id=? AND user_id=?',(row['node_id'],row['user_id']))
                hosts.add(row['server_id'])
    for sid in hosts: registry._bump(db,sid)


class NodeGroups:
    def __init__(self,registry): self.registry=registry

    @staticmethod
    def exists(db,ident):
        if not db.execute('SELECT 1 FROM node_groups WHERE id=?',(ident,)).fetchone():
            raise Error('节点组不存在',404)

    @staticmethod
    def result(db,ident):
        return {'id':ident,'name':db.execute('SELECT name FROM node_groups WHERE id=?',(ident,)).fetchone()[0],
                'node_ids':[r[0] for r in db.execute('SELECT node_id FROM node_group_members WHERE group_id=? ORDER BY node_id',(ident,))]}

    def get(self,ident=None):
        with self.registry.connect() as db:
            db.execute('BEGIN')
            if ident is not None:
                self.exists(db,ident); return self.result(db,ident)
            return [self.result(db,r[0]) for r in db.execute('SELECT id FROM node_groups ORDER BY id').fetchall()]

    def save(self,data,ident=None):
        from accounts import Accounts
        if not isinstance(data,dict) or set(data)!={'name','node_ids'}: raise Error('节点组需要 name 和 node_ids')
        name=self.registry.fields('users',{'name':data['name']})['name']
        nodes=data['node_ids']; Accounts.validate_nodes(nodes)
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if ident is not None: self.exists(db,ident)
            Accounts.node_hosts(db,nodes)
            if db.execute('SELECT 1 FROM node_groups WHERE name=? AND id!=?',(name,ident or 0)).fetchone():
                raise Error('节点组名称已存在',409)
            if ident is None: ident=db.execute('INSERT INTO node_groups(name) VALUES(?)',(name,)).lastrowid
            else: db.execute('UPDATE node_groups SET name=? WHERE id=?',(name,ident))
            db.execute('DELETE FROM node_group_members WHERE group_id=?',(ident,))
            db.executemany('INSERT INTO node_group_members VALUES(?,?)',[(ident,n) for n in nodes])
            enforce(db,self.registry,[r[0] for r in db.execute('SELECT plan_id FROM plan_node_groups WHERE group_id=?',(ident,))])
            return self.result(db,ident)

    def delete(self,ident):
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE'); self.exists(db,ident)
            plans=[r[0] for r in db.execute('SELECT plan_id FROM plan_node_groups WHERE group_id=?',(ident,))]
            db.execute('DELETE FROM node_groups WHERE id=?',(ident,))
            enforce(db,self.registry,plans)
            return {'deleted':True}
