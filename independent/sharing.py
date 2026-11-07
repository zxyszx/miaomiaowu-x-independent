"""Owner-scoped server delegation; no vendor licensing or data dependency."""
import hashlib
import json
import math
import re
import secrets
import time
import uuid
from core import Error
from node_config import profile

SCHEMA='''
CREATE TABLE IF NOT EXISTS server_shares(
 id TEXT PRIMARY KEY, server_id INTEGER NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
 name TEXT NOT NULL, max_nodes INTEGER NOT NULL, max_users INTEGER NOT NULL,
 port_start INTEGER NOT NULL, port_end INTEGER NOT NULL, expires_at REAL NOT NULL,
 invite_hash TEXT UNIQUE, access_hash TEXT UNIQUE, status TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS shared_nodes(
 node_id INTEGER PRIMARY KEY REFERENCES nodes(id) ON DELETE CASCADE,
 share_id TEXT NOT NULL REFERENCES server_shares(id) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS shared_users(
 user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
 share_id TEXT NOT NULL REFERENCES server_shares(id) ON DELETE CASCADE,
 reference TEXT NOT NULL, UNIQUE(share_id,reference));
CREATE TABLE IF NOT EXISTS share_usage_periods(
 id TEXT PRIMARY KEY,
 share_id TEXT NOT NULL REFERENCES server_shares(id) ON DELETE CASCADE,
 starts_at REAL NOT NULL, ends_at REAL, status TEXT NOT NULL,
 created_at REAL NOT NULL, closed_at REAL, reason TEXT NOT NULL DEFAULT 'initial');
CREATE UNIQUE INDEX IF NOT EXISTS share_usage_period_active
 ON share_usage_periods(share_id) WHERE status='active';
'''

class Shares:
    def __init__(self, registry): self.registry=registry

    @staticmethod
    def clean(db, row, status):
        # Owner may explicitly reuse a delegated user on another local host.
        # Revocation removes that identity everywhere, so bump every host too.
        affected={row['server_id']} | {item[0] for item in db.execute('SELECT DISTINCT n.server_id FROM nodes n JOIN node_users a ON a.node_id=n.id JOIN shared_users u ON u.user_id=a.user_id WHERE u.share_id=?',(row['id'],))}
        # Delete only delegated resources, never the owner's other resources.
        from total_usage import preserve_nodes
        preserve_nodes(db,[n[0] for n in db.execute('SELECT node_id FROM shared_nodes WHERE share_id=?',(row['id'],)).fetchall()])
        db.execute('DELETE FROM nodes WHERE id IN (SELECT node_id FROM shared_nodes WHERE share_id=?)',(row['id'],))
        db.execute('DELETE FROM users WHERE id IN (SELECT user_id FROM shared_users WHERE share_id=?)',(row['id'],))
        db.execute('UPDATE server_shares SET status=?,invite_hash=NULL,access_hash=NULL WHERE id=?',(status,row['id']))
        now=time.time()
        db.execute("UPDATE share_usage_periods SET status='closed',ends_at=COALESCE(ends_at,?),closed_at=COALESCE(closed_at,?) WHERE share_id=? AND status='active'",(now,now,row['id']))
        for server_id in affected:
            db.execute('UPDATE server_configs SET revision=revision+1 WHERE server_id=?',(server_id,))

    @classmethod
    def expire(cls, db):
        for row in db.execute("SELECT * FROM server_shares WHERE status IN ('invited','active') AND expires_at<=?",(time.time(),)).fetchall():
            cls.clean(db,row,'expired')

    def sweep(self):
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE'); self.expire(db)

    @staticmethod
    def hashed(token):
        if not isinstance(token,str) or not 32<=len(token)<=128: raise Error('分享凭据无效',401)
        return hashlib.sha256(token.encode()).hexdigest()

    @staticmethod
    def summary(db,row):
        result={k:row[k] for k in ('id','server_id','name','max_nodes','max_users','port_start','port_end','expires_at','status')}
        result['nodes_used']=db.execute('SELECT COUNT(*) FROM shared_nodes WHERE share_id=?',(row['id'],)).fetchone()[0]
        result['users_used']=db.execute('SELECT COUNT(*) FROM shared_users WHERE share_id=?',(row['id'],)).fetchone()[0]
        return result

    @staticmethod
    def _period(db, row, create=True):
        period=db.execute("SELECT * FROM share_usage_periods WHERE share_id=? AND status='active' ORDER BY starts_at DESC LIMIT 1",(row['id'],)).fetchone()
        if period is None and create and row['status'] in ('invited','active'):
            now=time.time(); ident=str(uuid.uuid4())
            db.execute("INSERT INTO share_usage_periods(id,share_id,starts_at,ends_at,status,created_at,closed_at,reason) VALUES(?,?,?,NULL,'active',?,NULL,'initial')",(ident,row['id'],now,now))
            period=db.execute('SELECT * FROM share_usage_periods WHERE id=?',(ident,)).fetchone()
        return period

    @staticmethod
    def period_result(period):
        if period is None: return None
        return {k:period[k] for k in ('id','share_id','starts_at','ends_at','status','created_at','closed_at','reason')}

    @staticmethod
    def periods(db, row):
        Shares._period(db, row)
        return [Shares.period_result(period) for period in db.execute(
            'SELECT * FROM share_usage_periods WHERE share_id=? ORDER BY starts_at DESC, rowid DESC',
            (row['id'],)).fetchall()]

    @staticmethod
    def usage(db,row,start=None,end=None):
        """Confirmed usage attributed to this share, excluding unknown writes."""
        period=Shares._period(db,row)
        if start is None and end is None and period is not None:
            start,end=period['starts_at'],period['ends_at']
        values=[]; clauses=['d.server_id=?','sn.share_id=?','su.share_id=?']
        values.extend((row['server_id'],row['id'],row['id']))
        if start is not None:
            clauses.append('d.received_at>=?'); values.append(start)
        if end is not None:
            clauses.append('d.received_at<?'); values.append(end)
        where=' AND '.join(clauses)
        rows=db.execute(f'''SELECT d.node_id,su.reference,
                COALESCE(SUM(d.upload_bytes),0) upload_bytes,
                COALESCE(SUM(d.download_bytes),0) download_bytes
            FROM usage_deltas d
            JOIN shared_nodes sn ON sn.node_id=d.node_id
            JOIN shared_users su ON su.user_id=d.user_id
            WHERE {where} GROUP BY d.node_id,su.reference
            ORDER BY d.node_id,su.reference''',values).fetchall()
        items=[dict(r) for r in rows]
        return {'share_id':row['id'],'server_id':row['server_id'],
                'basis':'confirmed_deltas','unknown_bytes_excluded':True,
                'start':start,'end':end,'period':Shares.period_result(period),
                'upload_bytes':sum(x['upload_bytes'] for x in items),
                'download_bytes':sum(x['download_bytes'] for x in items),
                'items':items}

    def usage_for_owner(self, ident, start=None, end=None):
        self.sweep()
        with self.registry.connect() as db:
            db.execute('BEGIN')
            row=db.execute('SELECT * FROM server_shares WHERE id=?',(ident,)).fetchone()
            if row is None: raise Error('分享不存在',404)
            return self.usage(db,row,start,end)

    def periods_for_owner(self, ident):
        self.sweep()
        with self.registry.connect() as db:
            db.execute('BEGIN')
            row=db.execute('SELECT * FROM server_shares WHERE id=?',(ident,)).fetchone()
            if row is None: raise Error('分享不存在',404)
            return self.periods(db,row)

    def reset_usage(self, ident, reason='manual'):
        if not isinstance(reason,str) or not 1<=len(reason.strip())<=120: raise Error('周期重置原因无效')
        self.sweep()
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT * FROM server_shares WHERE id=?',(ident,)).fetchone()
            if row is None: raise Error('分享不存在',404)
            if row['status'] not in ('invited','active'): raise Error('分享已失效',409)
            now=time.time(); current=self._period(db,row)
            if current is not None:
                db.execute("UPDATE share_usage_periods SET status='closed',ends_at=?,closed_at=? WHERE id=? AND status='active'",(now,now,current['id']))
            ident2=str(uuid.uuid4())
            db.execute("INSERT INTO share_usage_periods(id,share_id,starts_at,ends_at,status,created_at,closed_at,reason) VALUES(?,?,?,NULL,'active',?,NULL,?)",(ident2,row['id'],now,now,reason.strip()))
            return self.period_result(db.execute('SELECT * FROM share_usage_periods WHERE id=?',(ident2,)).fetchone())

    def create(self, server_id, data):
        keys={'name','max_nodes','max_users','port_start','port_end','expires_at'}
        if not isinstance(data,dict) or set(data)!=keys: raise Error('分享字段不完整或包含不支持的字段')
        if not isinstance(data['name'],str) or not 1<=len(data['name'].strip())<=120: raise Error('分享名称无效')
        for key,maximum in (('max_nodes',1000),('max_users',10000),('port_start',65535),('port_end',65535)):
            if type(data[key]) is not int or not 1<=data[key]<=maximum: raise Error('分享数量或端口范围无效')
        if data['port_start']>data['port_end']: raise Error('分享端口范围无效')
        expiry=data['expires_at']; now=time.time()
        if type(expiry) not in (int,float) or not math.isfinite(expiry) or not now+60<=expiry<=now+366*86400:
            raise Error('分享有效期应为未来60秒至366天')
        token=secrets.token_urlsafe(32); ident=str(uuid.uuid4())
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE'); self.expire(db)
            if not db.execute('SELECT 1 FROM servers WHERE id=?',(server_id,)).fetchone(): raise Error('服务器不存在',404)
            db.execute('INSERT INTO server_shares VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                (ident,server_id,data['name'].strip(),data['max_nodes'],data['max_users'],data['port_start'],data['port_end'],expiry,self.hashed(token),None,'invited'))
            db.execute("INSERT INTO share_usage_periods(id,share_id,starts_at,ends_at,status,created_at,closed_at,reason) VALUES(?,?,?,NULL,'active',?,NULL,'initial')",(str(uuid.uuid4()),ident,now,now))
            return {**self.summary(db,db.execute('SELECT * FROM server_shares WHERE id=?',(ident,)).fetchone()),'invite_token':token}

    def owner(self, server_id=None, ident=None, action=None):
        self.sweep()
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if ident is None:
                if not db.execute('SELECT 1 FROM servers WHERE id=?',(server_id,)).fetchone(): raise Error('服务器不存在',404)
                return [self.summary(db,row) for row in db.execute('SELECT * FROM server_shares WHERE server_id=? ORDER BY rowid',(server_id,))]
            row=db.execute('SELECT * FROM server_shares WHERE id=?',(ident,)).fetchone()
            if row is None: raise Error('分享不存在',404)
            if action=='revoke':
                if row['status'] not in ('revoked','expired'): self.clean(db,row,'revoked')
            elif action=='rotate':
                if row['status'] not in ('active','invited'): raise Error('该分享已失效',409)
                token=secrets.token_urlsafe(32)
                db.execute("UPDATE server_shares SET invite_hash=?,access_hash=NULL,status='invited' WHERE id=?",(self.hashed(token),ident))
                row=db.execute('SELECT * FROM server_shares WHERE id=?',(ident,)).fetchone()
                return {**self.summary(db,row),'invite_token':token}
            return self.summary(db,db.execute('SELECT * FROM server_shares WHERE id=?',(ident,)).fetchone())

    def claim(self, token):
        self.sweep(); hashed=self.hashed(token); access=secrets.token_urlsafe(32)
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute("SELECT * FROM server_shares WHERE invite_hash=? AND status='invited' AND expires_at>?",(hashed,time.time())).fetchone()
            if row is None: raise Error('分享邀请无效或已使用',401)
            db.execute("UPDATE server_shares SET invite_hash=NULL,access_hash=?,status='active' WHERE id=?",(self.hashed(access),row['id']))
            current=db.execute('SELECT * FROM server_shares WHERE id=?',(row['id'],)).fetchone()
            return {**self.summary(db,current),'access_token':access}

    def auth(self,db,token):
        row=db.execute("SELECT * FROM server_shares WHERE access_hash=? AND status='active' AND expires_at>?",(self.hashed(token),time.time())).fetchone()
        if row is None: raise Error('分享凭据无效或已到期',401)
        return row

    def node(self,db,row,ident):
        node=db.execute('SELECT n.id,n.name,c.payload FROM shared_nodes s JOIN nodes n ON n.id=s.node_id LEFT JOIN node_configs c ON c.node_id=n.id WHERE s.share_id=? AND n.id=? AND n.server_id=?',(row['id'],ident,row['server_id'])).fetchone()
        if node is None: raise Error('分享节点不存在',404)
        return node

    def result(self,db,node):
        return {'id':node['id'],'name':node['name'],'config':json.loads(node['payload']) if node['payload'] else None,
                'clients':[{'reference':u['reference'],'uuid':u['credential']} for u in db.execute('SELECT s.reference,a.credential FROM node_users a JOIN shared_users s ON s.user_id=a.user_id WHERE a.node_id=? AND s.share_id=(SELECT share_id FROM shared_nodes WHERE node_id=?) ORDER BY s.reference',(node['id'],node['id']))]}

    def quota(self,db,kind):
        row=db.execute('SELECT value FROM quotas WHERE kind=?',(kind,)).fetchone()
        if row and row[0] is not None and db.execute(f'SELECT COUNT(*) FROM {kind}').fetchone()[0]>=row[0]:
            raise Error('已达到管理员设置的资源配额',409)

    def guest(self,token,action,ident=None,data=None,reference=None):
        self.sweep()
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE'); row=self.auth(db,token)
            if action=='status': return self.summary(db,row)
            if action=='periods': return self.periods(db,row)
            if action=='usage': return self.usage(db,row,*(data or (None,None)))
            if action=='list':
                return [self.result(db,self.node(db,row,n[0])) for n in db.execute('SELECT node_id FROM shared_nodes WHERE share_id=? ORDER BY node_id',(row['id'],))]
            if action in ('create','update'):
                if not isinstance(data,dict) or set(data)!={'name','config'}: raise Error('节点需要名称与完整配置')
                fields=self.registry.fields('nodes',{'name':data['name'],'server_id':row['server_id']})
                try: config=profile(data['config'])
                except ValueError as error: raise Error(str(error)) from None
                if config.get('protocol')!='vless': raise Error('共享节点目前仅支持VLESS',409)
                if not row['port_start']<=config['listen_port']<=row['port_end']: raise Error('监听端口不在分享范围内',403)
                if action=='create':
                    if self.summary(db,row)['nodes_used']>=row['max_nodes']: raise Error('已达到分享节点配额',409)
                    self.quota(db,'nodes')
                    ident=db.execute('INSERT INTO nodes(name,server_id) VALUES(?,?)',(fields['name'],row['server_id'])).lastrowid
                    db.execute('INSERT INTO shared_nodes VALUES(?,?)',(ident,row['id']))
                else:
                    old=self.node(db,row,ident)
                    if old['name']!=fields['name']:
                        db.execute('UPDATE nodes SET name=? WHERE id=?',(fields['name'],ident))
                        self.registry._bump(db,row['server_id'])
                self.registry._configure_node(db,ident,config)
                return self.result(db,self.node(db,row,ident))
            node=self.node(db,row,ident)
            if action=='delete':
                from total_usage import preserve_nodes
                preserve_nodes(db,[ident])
                db.execute('DELETE FROM nodes WHERE id=?',(ident,)); self.prune(db,row)
                self.registry._bump(db,row['server_id']); return {'deleted':True}
            if action not in ('authorize','revoke'): raise Error('分享操作不存在',404)
            if not isinstance(reference,str) or not re.fullmatch('[A-Za-z0-9_-]{1,64}',reference): raise Error('接入方用户标识无效')
            user=db.execute('SELECT user_id FROM shared_users WHERE share_id=? AND reference=?',(row['id'],reference)).fetchone()
            if action=='revoke':
                if user is None or not db.execute('DELETE FROM node_users WHERE node_id=? AND user_id=?',(ident,user[0])).rowcount: raise Error('分享用户未授权',404)
                self.prune(db,row); self.registry._bump(db,row['server_id']); return {'revoked':True}
            if not node['payload'] or json.loads(node['payload']).get('protocol')!='vless': raise Error('仅VLESS节点支持分享用户授权',409)
            if user is None:
                if self.summary(db,row)['users_used']>=row['max_users']: raise Error('已达到分享用户配额',409)
                self.quota(db,'users')
                uid=db.execute('INSERT INTO users(name) VALUES(?)',('shared-'+row['id'][:8]+'-'+reference,)).lastrowid
                db.execute('INSERT INTO shared_users VALUES(?,?,?)',(uid,row['id'],reference))
            else: uid=user[0]
            existing=db.execute('SELECT credential FROM node_users WHERE node_id=? AND user_id=?',(ident,uid)).fetchone()
            if existing: return {'reference':reference,'uuid':existing[0]}
            credential=str(uuid.uuid4()); db.execute('INSERT INTO node_users VALUES(?,?,?)',(ident,uid,credential))
            self.registry._bump(db,row['server_id']); return {'reference':reference,'uuid':credential}

    @staticmethod
    def prune(db,row):
        db.execute('DELETE FROM users WHERE id IN (SELECT s.user_id FROM shared_users s WHERE s.share_id=? AND NOT EXISTS(SELECT 1 FROM node_users a WHERE a.user_id=s.user_id))',(row['id'],))
