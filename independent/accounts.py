"""Explicit per-plan resource identities; these are not panel login accounts."""
import json
import uuid
from core import Error

SCHEMA='''
CREATE TABLE IF NOT EXISTS plan_accounts(
 user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
 parent_id INTEGER NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
 plan_id INTEGER NOT NULL REFERENCES plans(id) ON DELETE RESTRICT,
 UNIQUE(parent_id,plan_id), CHECK(user_id!=parent_id));
'''

class Accounts:
    def __init__(self,registry): self.registry=registry

    @staticmethod
    def parent(db,ident):
        if db.execute('SELECT 1 FROM users WHERE id=?',(ident,)).fetchone() is None:
            raise Error('用户不存在',404)
        if db.execute('SELECT 1 FROM plan_accounts WHERE user_id=?',(ident,)).fetchone():
            raise Error('套餐子账号不能再创建子账号',409)
        if db.execute('SELECT 1 FROM shared_users WHERE user_id=?',(ident,)).fetchone():
            raise Error('委托分享身份不能创建套餐子账号',409)

    def result(self,db,row):
        ident=row['user_id']
        from plan_lifecycle import view
        return {'user_id':ident,'parent_id':row['parent_id'],'plan_id':row['plan_id'],
                'lifecycle':view(db,ident),
                'name':db.execute('SELECT name FROM users WHERE id=?',(ident,)).fetchone()[0],
                'bandwidth':self.registry._bandwidth_policy(db,ident),
                'connections':self.registry._connections_policy(db,ident),
                'nodes':[{'node_id':n['node_id'],'uuid':n['credential']} for n in db.execute(
                    'SELECT node_id,credential FROM node_users WHERE user_id=? ORDER BY node_id',(ident,))]}

    def list(self,parent_id):
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE'); self.parent(db,parent_id)
            self.registry._expire_bandwidth(db)
            return [self.result(db,row) for row in db.execute('SELECT * FROM plan_accounts WHERE parent_id=? ORDER BY user_id',(parent_id,)).fetchall()]

    def update(self,parent_id,ident,data):
        if not isinstance(data,dict) or set(data)!={'name','node_ids'}:
            raise Error('编辑子账号需要 name 和 node_ids；套餐不能在此更换')
        name=self.registry.fields('users',{'name':data['name']})['name']
        nodes=data['node_ids']
        self.validate_nodes(nodes)
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE'); self.parent(db,parent_id)
            row=db.execute('SELECT * FROM plan_accounts WHERE parent_id=? AND user_id=?',(parent_id,ident)).fetchone()
            if row is None: raise Error('套餐子账号不存在',404)
            from plan_nodes import check
            check(db,row['plan_id'],nodes)
            self.registry._expire_bandwidth(db)
            hosts=self.node_hosts(db,nodes)
            old={n['node_id']:n['server_id'] for n in db.execute('SELECT a.node_id,n.server_id FROM node_users a JOIN nodes n ON n.id=a.node_id WHERE a.user_id=?',(ident,))}
            removed=set(old)-set(nodes); added=set(nodes)-set(old)
            for nid in removed: db.execute('DELETE FROM node_users WHERE node_id=? AND user_id=?',(nid,ident))
            for nid in added: db.execute('INSERT INTO node_users VALUES(?,?,?)',(nid,ident,str(uuid.uuid4())))
            db.execute('UPDATE users SET name=? WHERE id=?',(name,ident))
            for sid in {old[n] for n in removed}|{hosts[n] for n in added}: self.registry._bump(db,sid)
            return self.result(db,row)

    @staticmethod
    def validate_nodes(nodes):
        if not isinstance(nodes,list) or len(nodes)>128 or any(type(n) is not int or n<1 for n in nodes) or len(set(nodes))!=len(nodes):
            raise Error('节点编号应为不重复的正整数列表，最多128个')

    @staticmethod
    def node_hosts(db,nodes):
        hosts={}
        for nid in nodes:
            node=db.execute('SELECT n.server_id,c.payload,s.share_id FROM nodes n LEFT JOIN node_configs c ON c.node_id=n.id LEFT JOIN shared_nodes s ON s.node_id=n.id WHERE n.id=?',(nid,)).fetchone()
            if node is None: raise Error('节点不存在',404)
            if node['share_id']: raise Error('不能将所有者套餐身份混入委托分享节点',409)
            if not node['payload'] or json.loads(node['payload']).get('protocol')!='vless': raise Error('仅VLESS节点支持套餐子账号',409)
            hosts[nid]=node['server_id']
        return hosts

    def create(self,parent_id,data):
        if not isinstance(data,dict) or set(data)!={'name','plan_id','node_ids'}:
            raise Error('子账号需要 name、plan_id 和 node_ids')
        name=self.registry.fields('users',{'name':data['name']})['name']
        plan_id=data['plan_id']; nodes=data['node_ids']
        if type(plan_id) is not int or plan_id<1: raise Error('套餐编号无效')
        self.validate_nodes(nodes)
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE'); self.parent(db,parent_id)
            self.registry._expire_bandwidth(db)
            if db.execute('SELECT 1 FROM plans WHERE id=?',(plan_id,)).fetchone() is None: raise Error('套餐不存在',404)
            from plan_nodes import check
            check(db,plan_id,nodes)
            if db.execute('SELECT 1 FROM plan_accounts WHERE parent_id=? AND plan_id=?',(parent_id,plan_id)).fetchone() or db.execute('SELECT 1 FROM user_plans WHERE user_id=? AND plan_id=?',(parent_id,plan_id)).fetchone():
                raise Error('该用户已持有此套餐',409)
            quota=db.execute("SELECT value FROM quotas WHERE kind='users'").fetchone()
            if quota and quota[0] is not None and db.execute('SELECT COUNT(*) FROM users').fetchone()[0]>=quota[0]:
                raise Error('已达到管理员设置的用户配额',409)
            affected=set(self.node_hosts(db,nodes).values())
            ident=db.execute('INSERT INTO users(name) VALUES(?)',(name,)).lastrowid
            db.execute('INSERT INTO plan_accounts VALUES(?,?,?)',(ident,parent_id,plan_id))
            db.execute('INSERT INTO user_plans VALUES(?,?)',(ident,plan_id))
            for nid in nodes: db.execute('INSERT INTO node_users VALUES(?,?,?)',(nid,ident,str(uuid.uuid4())))
            for sid in affected: self.registry._bump(db,sid)
            return self.result(db,db.execute('SELECT * FROM plan_accounts WHERE user_id=?',(ident,)).fetchone())

    def delete(self,parent_id,ident):
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE'); self.parent(db,parent_id)
            if not db.execute('SELECT 1 FROM plan_accounts WHERE parent_id=? AND user_id=?',(parent_id,ident)).fetchone():
                raise Error('套餐子账号不存在',404)
            affected={n[0] for n in db.execute('SELECT DISTINCT n.server_id FROM nodes n JOIN node_users a ON a.node_id=n.id WHERE a.user_id=?',(ident,))}
            db.execute('DELETE FROM users WHERE id=?',(ident,))
            for sid in affected: self.registry._bump(db,sid)
            return {'deleted':True}
