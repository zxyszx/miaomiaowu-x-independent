"""Panel identity kernel shared by offline administration and guarded HTTP.

The control directory MUST be excluded from registry backups. All callers,
including future handlers, use this kernel; the OS operator is trusted.
"""
import contextlib
import fcntl
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import re
import secrets
import sqlite3
import stat
import tempfile
import threading
import time

from core import Error

TABLES=('panel_accounts','panel_resource_bindings','panel_sessions',
        'panel_login_challenges','panel_auth_limits','panel_auth_meta','panel_auth_audit')
SCHEMA=(
'''CREATE TABLE panel_accounts(id TEXT PRIMARY KEY, login_key TEXT UNIQUE NOT NULL,
 display_name TEXT NOT NULL, role TEXT NOT NULL CHECK(role IN ('admin','user')),
 salt TEXT, password_hash TEXT, password_alg TEXT NOT NULL, credential_state TEXT NOT NULL,
 enabled INTEGER NOT NULL, must_change_password INTEGER NOT NULL,
 credential_version INTEGER NOT NULL, auth_epoch INTEGER NOT NULL, created_at REAL NOT NULL)''',
'''CREATE TABLE panel_resource_bindings(account_id TEXT PRIMARY KEY REFERENCES panel_accounts(id),
 root_user_id INTEGER UNIQUE NOT NULL REFERENCES users(id) ON DELETE RESTRICT)''',
'''CREATE TABLE panel_sessions(id_hash TEXT PRIMARY KEY, account_id TEXT NOT NULL REFERENCES panel_accounts(id),
 epoch INTEGER NOT NULL, credential_version INTEGER NOT NULL, role_at_issue TEXT NOT NULL,
 session_generation TEXT NOT NULL, created_at REAL NOT NULL, last_seen REAL NOT NULL,
 absolute_expires_at REAL NOT NULL, revoked_at REAL, csrf_hash TEXT)''',
'''CREATE TABLE panel_login_challenges(id_hash TEXT PRIMARY KEY, csrf_hash TEXT NOT NULL,
 expires_at REAL NOT NULL, consumed_at REAL)''',
'''CREATE TABLE panel_auth_limits(key_hash TEXT PRIMARY KEY, window_start REAL NOT NULL,
 failures INTEGER NOT NULL, blocked_until REAL NOT NULL)''',
'''CREATE TABLE panel_auth_meta(singleton INTEGER PRIMARY KEY CHECK(singleton=1),schema_version INTEGER NOT NULL,
 deployment_id TEXT NOT NULL,restore_generation TEXT NOT NULL,identity_revision INTEGER NOT NULL,
 clock_floor REAL NOT NULL)''',
'''CREATE TABLE panel_auth_audit(id INTEGER PRIMARY KEY,event TEXT NOT NULL,actor_id TEXT,
 target_id TEXT,request_id TEXT NOT NULL,time REAL NOT NULL,result TEXT NOT NULL)''')
KDF=threading.BoundedSemaphore(2)
ALGORITHM='scrypt-n32768-r8-p1-dk32-v1'
# Last-resort process fence when storage cannot persist even a failure marker.
# Standalone offline callers still need trusted OS isolation. HTTP additionally
# requires PanelRuntime's pre-persisted running guard and fatal process exit.
_TIME_POISONED=set()


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),
                                     allow_nan=False).encode()).hexdigest()


def hashed(value):
    if type(value) is not str or not value or len(value)>1024:
        raise Error('身份凭据无效',401)
    return hashlib.sha256(value.encode()).hexdigest()


def password(value):
    if type(value) is not str or not 12<=len(value)<=128 or len(value.encode())>512:
        raise Error('密码格式无效',400)
    return value


def derive(value,salt):
    if not KDF.acquire(blocking=False): raise Error('身份服务繁忙',429)
    try:
        return hashlib.scrypt(value.encode(),salt=bytes.fromhex(salt),n=32768,r=8,p=1,
                              dklen=32,maxmem=128*1024*1024).hex()
    except (ValueError,MemoryError) as exc:
        raise Error('密码服务不可用',503) from exc
    finally: KDF.release()


class PanelIdentity:
    def __init__(self,db_path,control,*,clock=time.time,fault=None):
        self.path=Path(db_path).resolve()
        raw=Path(control)
        if raw.is_symlink(): raise Error('控制目录无效',503)
        self.control=raw.resolve()
        if self.control==self.path.parent or self.control in self.path.parents or self.path.parent in self.control.parents:
            raise Error('控制目录必须独立于数据库目录',503)
        self.clock=clock; self.fault=fault or (lambda _:None)
        # Real KDF self-test, no weak fallback.
        derive('kernel-self-test', '00'*16)

    def _hit(self,point): self.fault(point)

    def _db(self):
        db=sqlite3.connect(self.path.as_uri()+'?mode=rw',uri=True,timeout=10)
        db.row_factory=sqlite3.Row
        db.execute('PRAGMA foreign_keys=ON')
        db.execute('PRAGMA synchronous=FULL')
        return db

    @contextlib.contextmanager
    def _lock(self,initial=False):
        try:
            if initial: self.control.mkdir(mode=0o700,parents=False,exist_ok=True)
            info=self.control.stat()
            if info.st_uid!=os.getuid() or stat.S_IMODE(info.st_mode)!=0o700:
                raise Error('控制目录权限无效',503)
            fd=os.open(self.control/'lock',os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
            try:
                info=os.fstat(fd)
                if info.st_uid!=os.getuid() or stat.S_IMODE(info.st_mode)!=0o600 or not stat.S_ISREG(info.st_mode):
                    raise Error('控制锁权限无效',503)
                fcntl.flock(fd,fcntl.LOCK_EX)
                yield
            finally: os.close(fd)
        except Error:
            raise
        except (OSError,sqlite3.Error,ValueError,KeyError,TypeError) as exc:
            raise Error('面板身份存储不可用',503) from exc

    def _read_anchor(self):
        fd=os.open(self.control/'anchor.json',os.O_RDONLY|os.O_NOFOLLOW)
        with os.fdopen(fd) as file:
            info=os.fstat(file.fileno())
            if info.st_uid!=os.getuid() or stat.S_IMODE(info.st_mode)!=0o600 or not stat.S_ISREG(info.st_mode):
                raise Error('锚权限无效',503)
            return json.load(file)

    def _anchor(self,value,label):
        fd,name=tempfile.mkstemp(prefix='.anchor-',dir=self.control)
        try:
            with os.fdopen(fd,'w') as file:
                json.dump(value,file,sort_keys=True,allow_nan=False)
                file.flush(); self._hit(label+'.before_fsync'); os.fsync(file.fileno())
            self._hit(label+'.before_replace')
            os.replace(name,self.control/'anchor.json')
            self._hit(label+'.before_dir_fsync')
            directory=os.open(self.control,os.O_RDONLY)
            try: os.fsync(directory)
            finally: os.close(directory)
            self._hit(label+'.durable')
        finally:
            if os.path.exists(name): os.unlink(name)

    @staticmethod
    def _snapshot(db):
        return digest({table:sorted([dict(r) for r in db.execute('SELECT * FROM '+table)],
                                   key=lambda r:json.dumps(r,sort_keys=True)) for table in TABLES})

    def _time(self,meta,anchor):
        now=self.clock()
        if type(now) not in (int,float) or not math.isfinite(now) or now<max(meta['clock_floor'],anchor['clock_floor']):
            raise Error('身份时钟不可信',503)
        return now

    def _verify(self,db,anchor):
        if str(self.control) in _TIME_POISONED:
            raise Error('可信时间写入失败，需受控恢复',503)
        if anchor['state']!='clean': raise Error('身份恢复未完成',503)
        meta=db.execute('SELECT * FROM panel_auth_meta WHERE singleton=1').fetchone()
        if (meta is None or meta['schema_version']!=1 or
            any(meta[k]!=anchor[k] for k in ('deployment_id','restore_generation','identity_revision'))
            or self._snapshot(db)!=anchor['digest']):
            raise Error('身份快照与外部锚不符',503)
        return self._time(meta,anchor)

    @contextlib.contextmanager
    def _tx(self,write=False):
        with self._lock():
            anchor=self._read_anchor(); db=self._db()
            try:
                db.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
                now=self._verify(db,anchor)
                # An observed forward expiry must remain expired even when
                # the request is rejected and its SQL transaction rolls back.
                if now>anchor['clock_floor']:
                    anchor={**anchor,'clock_floor':now}
                    try:
                        self._anchor(anchor,'clock_floor')
                    except BaseException:
                        # Even a pre-replace failure leaves an OLD clean
                        # anchor. Poison current instances first, then persist
                        # a dirty fence with the observed time for reopeners.
                        _TIME_POISONED.add(str(self.control))
                        for attempt in range(2):
                            try:
                                self._anchor({**anchor,'state':'dirty'},'clock_failure_fence')
                                break
                            except OSError as failure:
                                if attempt==1:
                                    raise Error('可信时间及失败栅栏无法持久化，必须由OS停服隔离',503) from failure
                        raise
                yield db,now
                if write:
                    dirty={**anchor,'state':'dirty'}
                    self._anchor(dirty,'dirty')
                    db.execute('UPDATE panel_auth_meta SET identity_revision=identity_revision+1,clock_floor=?',(now,))
                    self._hit('db.before_commit'); db.commit(); self._hit('db.committed')
                    meta=dict(db.execute('SELECT * FROM panel_auth_meta').fetchone())
                    clean={**anchor,**meta,'state':'clean','digest':self._snapshot(db)}
                    try:
                        self._anchor(clean,'clean')
                    except BaseException:
                        # A failed acknowledgement must not silently clear a
                        # durable dirty fence, even after replace succeeded.
                        self._anchor(dirty,'clean_failure_fence')
                        raise
            finally:
                db.rollback(); db.close()

    @staticmethod
    def _audit(db,now,event,actor=None,target=None,result='ok'):
        db.execute('INSERT INTO panel_auth_audit(event,actor_id,target_id,request_id,time,result) VALUES(?,?,?,?,?,?)',
                   (event,actor,target,secrets.token_hex(16),now,result))

    @staticmethod
    def _new(db,now,login,pw,role,must_change=False):
        if type(login) is not str or not re.fullmatch('[a-z0-9_.-]{3,64}',login): raise Error('账号名无效',400)
        if role not in ('admin','user'): raise Error('角色无效',400)
        if db.execute('SELECT 1 FROM panel_accounts WHERE login_key=?',(login,)).fetchone(): raise Error('账号名已存在',409)
        salt=secrets.token_hex(16); encoded=derive(password(pw),salt); ident=secrets.token_hex(16)
        db.execute('INSERT INTO panel_accounts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   (ident,login,login,role,salt,encoded,ALGORITHM,'active',1,int(must_change),1,1,now))
        return ident

    def initialize(self,login,pw):
        with self._lock(initial=True):
            if (self.control/'anchor.json').exists(): raise Error('外部锚已存在',409)
            db=self._db()
            try:
                existing=db.execute("SELECT name FROM sqlite_master WHERE name LIKE 'panel_%'").fetchall()
                if existing: raise Error('已有身份库不能自动初始化',409)
                now=self.clock(); deployment=secrets.token_hex(16); generation=secrets.token_hex(16)
                anchor=dict(state='recovering',deployment_id=deployment,restore_generation=generation,
                            identity_revision=0,clock_floor=now,recoveries=[])
                self._anchor(anchor,'init')
                db.execute('BEGIN IMMEDIATE')
                for sql in SCHEMA: db.execute(sql)
                db.execute('INSERT INTO panel_auth_meta VALUES(1,1,?,?,0,?)',(deployment,generation,now))
                ident=self._new(db,now,login,pw,'admin')
                self._audit(db,now,'initialize',target=ident)
                self._hit('init.before_commit'); db.commit(); self._hit('init.committed')
                try:
                    self._anchor({**anchor,'state':'clean','digest':self._snapshot(db)},'init_clean')
                except BaseException:
                    self._anchor(anchor,'init_failure_fence')
                    raise
                return ident
            finally: db.rollback(); db.close()

    def _session(self,db,now,token,admin=False,restricted=False):
        row=db.execute('''SELECT a.*,s.id_hash,s.epoch,s.credential_version AS sv,s.role_at_issue,
            s.session_generation,s.created_at AS issued,s.last_seen,s.absolute_expires_at,s.revoked_at
            FROM panel_sessions s JOIN panel_accounts a ON a.id=s.account_id WHERE s.id_hash=?''',(hashed(token),)).fetchone()
        generation=db.execute('SELECT restore_generation FROM panel_auth_meta').fetchone()[0]
        if (row is None or not row['enabled'] or row['credential_state']!='active' or row['revoked_at'] is not None
            or row['epoch']!=row['auth_epoch'] or row['sv']!=row['credential_version'] or row['role_at_issue']!=row['role']
            or row['session_generation']!=generation or not row['issued']<=now<min(row['absolute_expires_at'],row['last_seen']+1800)):
            raise Error('面板会话无效',401)
        if (admin and row['role']!='admin') or (row['must_change_password'] and not restricted): raise Error('面板权限不足',403)
        binding=db.execute('SELECT root_user_id FROM panel_resource_bindings WHERE account_id=?',(row['id'],)).fetchone()
        if binding: self._resource(db,binding[0])
        return row

    @staticmethod
    def _resource(db,uid):
        if type(uid) is not int or db.execute('SELECT 1 FROM users WHERE id=?',(uid,)).fetchone() is None:
            raise Error('资源不存在',404)
        if any(db.execute('SELECT 1 FROM '+table+' WHERE user_id=?',(uid,)).fetchone() for table in ('plan_accounts','shared_users')):
            raise Error('资源不能作为面板主身份',409)

    @staticmethod
    def _revoke(db,now,ident):
        db.execute('UPDATE panel_accounts SET auth_epoch=auth_epoch+1 WHERE id=?',(ident,))
        db.execute('UPDATE panel_sessions SET revoked_at=? WHERE account_id=? AND revoked_at IS NULL',(now,ident))

    def authenticate(self,token,*,admin=False,restricted=False):
        with self._tx(True) as (db,now):
            row=self._session(db,now,token,admin,restricted)
            if now-row['last_seen']>=60: db.execute('UPDATE panel_sessions SET last_seen=? WHERE id_hash=?',(now,hashed(token)))
            return dict(id=row['id'],role=row['role'],must_change_password=bool(row['must_change_password']))

    def challenge(self):
        token=secrets.token_urlsafe(32); csrf=secrets.token_urlsafe(32)
        with self._tx(True) as (db,now):
            db.execute('DELETE FROM panel_login_challenges WHERE expires_at<=? OR consumed_at IS NOT NULL',(now,))
            db.execute('INSERT INTO panel_login_challenges VALUES(?,?,?,NULL)',(hashed(token),hashed(csrf),now+300))
        return token,csrf

    def consume_challenge(self,token,csrf):
        valid=False
        with self._tx(True) as (db,now):
            row=db.execute('SELECT * FROM panel_login_challenges WHERE id_hash=?',(hashed(token),)).fetchone()
            if row and row['consumed_at'] is None:
                db.execute('UPDATE panel_login_challenges SET consumed_at=? WHERE id_hash=?',(now,hashed(token)))
                valid=now<row['expires_at'] and hmac.compare_digest(row['csrf_hash'],hashed(csrf))
        if not valid: raise Error('登录挑战无效',403)

    def session_info(self,token):
        csrf=secrets.token_urlsafe(32)
        with self._tx(True) as (db,now):
            row=self._session(db,now,token,restricted=True)
            db.execute('UPDATE panel_sessions SET csrf_hash=?,last_seen=CASE WHEN last_seen<=? THEN ? ELSE last_seen END WHERE id_hash=?',
                       (hashed(csrf),now-60,now,hashed(token)))
            result=dict(id=row['id'],role=row['role'],must_change_password=bool(row['must_change_password']),csrf_token=csrf)
        return result

    @staticmethod
    def _csrf(db,token,csrf):
        if csrf is None: return  # offline kernel callers; HTTP always supplies a value
        row=db.execute('SELECT csrf_hash FROM panel_sessions WHERE id_hash=?',(hashed(token),)).fetchone()
        if row is None or row[0] is None or not hmac.compare_digest(row[0],hashed(csrf)):
            raise Error('CSRF无效',403)

    @contextlib.contextmanager
    def authorized(self,token,*,admin=False):
        """Future P2 write callers must act inside this transaction, not after me()."""
        with self._tx(True) as (db,now):
            row=self._session(db,now,token,admin)
            yield db,dict(id=row['id'],role=row['role'])

    def login(self,login,pw,ip):
        password(pw)
        if type(login) is not str or type(ip) is not str or len(login)>128 or len(ip)>128: raise Error('登录输入无效',400)
        # Count attempts durably BEFORE KDF; every failure remains rate limited.
        keys=[('global',60,60),(hashed('account:'+login),900,5),(hashed('ip:'+ip),900,5)]
        with self._tx(True) as (db,now):
            for key,window,maximum in keys:
                old=db.execute('SELECT * FROM panel_auth_limits WHERE key_hash=?',(key,)).fetchone()
                if old and now<old['blocked_until']: raise Error('登录暂时受限',429)
                count=old['failures'] if old and now<old['window_start']+window else 0
                start=old['window_start'] if count else now
                db.execute('INSERT OR REPLACE INTO panel_auth_limits VALUES(?,?,?,?)',
                           (key,start,count+1,now+window if count+1>=maximum else 0))
            row=db.execute('SELECT * FROM panel_accounts WHERE login_key=?',(login,)).fetchone()
            original=dict(row) if row else None
        valid=hmac.compare_digest(derive(pw,original['salt'] if original and original['salt'] else '00'*16),
                                  original['password_hash'] if original and original['password_hash'] else '00'*32)
        token=None
        with self._tx(True) as (db,now):
            row=db.execute('SELECT * FROM panel_accounts WHERE login_key=?',(login,)).fetchone()
            if valid and original and row and dict(row)==original and row['enabled'] and row['credential_state']=='active' and row['password_alg']==ALGORITHM:
                token=secrets.token_urlsafe(32)
                generation=db.execute('SELECT restore_generation FROM panel_auth_meta').fetchone()[0]
                db.execute('INSERT INTO panel_sessions VALUES(?,?,?,?,?,?,?,?,?,NULL,NULL)',
                           (hashed(token),row['id'],row['auth_epoch'],row['credential_version'],row['role'],generation,now,now,now+28800))
                for key,_,_ in keys[1:]: db.execute('DELETE FROM panel_auth_limits WHERE key_hash=?',(key,))
            self._audit(db,now,'login',target=row['id'] if row else None,result='ok' if token else 'denied')
        if token is None: raise Error('登录凭据无效',401)
        return token

    def logout(self,token,*,csrf=None):
        with self._tx(True) as (db,now):
            if csrf is not None:
                self._session(db,now,token,restricted=True); self._csrf(db,token,csrf)
            db.execute('UPDATE panel_sessions SET revoked_at=? WHERE id_hash=? AND revoked_at IS NULL',(now,hashed(token)))
            self._audit(db,now,'logout')

    def change_password(self,token,old,new,*,csrf=None):
        password(old); password(new)
        with self._tx() as (db,now):
            original=dict(self._session(db,now,token,restricted=True)); self._csrf(db,token,csrf)
        if not hmac.compare_digest(derive(old,original['salt']),original['password_hash']): raise Error('登录凭据无效',401)
        salt=secrets.token_hex(16); encoded=derive(new,salt)
        self._hit('password.verified')
        with self._tx(True) as (db,now):
            row=self._session(db,now,token,restricted=True)
            self._csrf(db,token,csrf)
            if row['credential_version']!=original['credential_version']: raise Error('凭据版本冲突',409)
            db.execute('UPDATE panel_accounts SET salt=?,password_hash=?,credential_version=credential_version+1,must_change_password=0 WHERE id=?',(salt,encoded,row['id']))
            self._hit('password.updated')
            self._revoke(db,now,row['id']); self._audit(db,now,'password',row['id'],row['id'])

    def revoke_all(self,token,pw,*,csrf=None):
        password(pw)
        with self._tx() as (db,now):
            original=dict(self._session(db,now,token)); self._csrf(db,token,csrf)
        if not hmac.compare_digest(derive(pw,original['salt']),original['password_hash']): raise Error('登录凭据无效',401)
        with self._tx(True) as (db,now):
            row=self._session(db,now,token)
            self._csrf(db,token,csrf)
            if row['credential_version']!=original['credential_version']: raise Error('凭据版本冲突',409)
            self._revoke(db,now,row['id']); self._audit(db,now,'revoke_all',row['id'],row['id'])

    def _admin_password(self,token,pw):
        password(pw)
        with self._tx() as (db,now): row=dict(self._session(db,now,token,True))
        if not hmac.compare_digest(derive(pw,row['salt']),row['password_hash']): raise Error('登录凭据无效',401)
        return row['credential_version']

    def create_account(self,token,login,pw,*,actor_password,role='user',root_user_id=None):
        version=self._admin_password(token,actor_password)
        with self._tx(True) as (db,now):
            actor=self._session(db,now,token,True)
            if actor['credential_version']!=version: raise Error('凭据版本冲突',409)
            ident=self._new(db,now,login,pw,role,True)
            if root_user_id is not None:
                if role!='user': raise Error('仅普通账号绑定资源',400)
                self._resource(db,root_user_id)
                if db.execute('SELECT 1 FROM panel_resource_bindings WHERE root_user_id=?',(root_user_id,)).fetchone(): raise Error('资源已绑定',409)
                db.execute('INSERT INTO panel_resource_bindings VALUES(?,?)',(ident,root_user_id))
                self._revoke(db,now,ident)
            self._audit(db,now,'create',actor['id'],ident)
            return ident

    def update_account(self,token,ident,*,actor_password,enabled=None,role=None,root_user_id=None,bind=False):
        version=self._admin_password(token,actor_password)
        with self._tx(True) as (db,now):
            actor=self._session(db,now,token,True)
            if actor['credential_version']!=version: raise Error('凭据版本冲突',409)
            row=db.execute('SELECT * FROM panel_accounts WHERE id=?',(ident,)).fetchone()
            if row is None: raise Error('账号不存在',404)
            if enabled is not None and type(enabled) is not bool: raise Error('状态无效',400)
            target_role=role if role is not None else row['role']
            if target_role not in ('admin','user'): raise Error('角色无效',400)
            target_enabled=row['enabled'] if enabled is None else int(enabled)
            if row['role']=='admin' and row['enabled'] and (target_role!='admin' or not target_enabled):
                if db.execute("SELECT COUNT(*) FROM panel_accounts WHERE role='admin' AND enabled=1 AND credential_state='active'").fetchone()[0]<=1:
                    raise Error('必须保留有效管理员',409)
            if target_role=='admin' and db.execute('SELECT 1 FROM panel_resource_bindings WHERE account_id=?',(ident,)).fetchone():
                raise Error('先解绑资源',409)
            if bind:
                if target_role!='user': raise Error('仅普通账号绑定资源',400)
                if root_user_id is not None: self._resource(db,root_user_id)
                if root_user_id is not None and db.execute('SELECT 1 FROM panel_resource_bindings WHERE root_user_id=? AND account_id!=?',(root_user_id,ident)).fetchone(): raise Error('资源已绑定',409)
                db.execute('DELETE FROM panel_resource_bindings WHERE account_id=?',(ident,))
                self._hit('binding.deleted')
                if root_user_id is not None: db.execute('INSERT INTO panel_resource_bindings VALUES(?,?)',(ident,root_user_id))
            db.execute('UPDATE panel_accounts SET role=?,enabled=? WHERE id=?',(target_role,target_enabled,ident))
            self._revoke(db,now,ident); self._audit(db,now,'update',actor['id'],ident)
            self._hit('account.updated')

    def resources(self,token):
        with self._tx(True) as (db,now):
            row=self._session(db,now,token)
            if now-row['last_seen']>=60: db.execute('UPDATE panel_sessions SET last_seen=? WHERE id_hash=?',(now,hashed(token)))
            binding=db.execute('SELECT root_user_id FROM panel_resource_bindings WHERE account_id=?',(row['id'],)).fetchone()
            if binding is None: return []
            root=binding[0]; self._resource(db,root)
            result=[dict(r) for r in db.execute('SELECT id,name FROM users WHERE id=? OR id IN (SELECT user_id FROM plan_accounts WHERE parent_id=?) ORDER BY id',(root,root))]
            from usage_cycles import boundary
            for resource in result:
                uid=resource['id']
                lifecycle=db.execute('SELECT enabled,expires_at,active FROM plan_lifecycle WHERE user_id=?',(uid,)).fetchone()
                enabled,end,active=tuple(lifecycle) if lifecycle else (True,None,True)
                cycle=boundary(db,uid,now)
                if cycle is not None:
                    if cycle[0] is None: active=False
                    else: end=cycle[1] if end is None else min(end,cycle[1])
                resource['plan']=dict(enabled=bool(enabled),expires_at=end,
                    status='disabled' if not enabled else ('active' if active and (end is None or now<end) else 'expired'))
            return result

    def offline_reset(self,ident,pw,*,role,enabled,root_user_id=None):
        """OS-authorized CLI only; explicit per-account recovery, not a handler API."""
        salt=secrets.token_hex(16); encoded=derive(password(pw),salt)
        if role not in ('user','admin') or type(enabled) is not bool: raise Error('恢复状态无效',400)
        with self._tx(True) as (db,now):
            row=db.execute('SELECT * FROM panel_accounts WHERE id=?',(ident,)).fetchone()
            if row is None: raise Error('账号不存在',404)
            if row['role']=='admin' and row['enabled'] and (role!='admin' or not enabled):
                if db.execute("SELECT COUNT(*) FROM panel_accounts WHERE role='admin' AND enabled=1 AND credential_state='active'").fetchone()[0]<=1:
                    raise Error('必须保留有效管理员',409)
            if root_user_id is not None:
                if role!='user': raise Error('仅普通账号绑定资源',400)
                self._resource(db,root_user_id)
                if db.execute('SELECT 1 FROM panel_resource_bindings WHERE root_user_id=? AND account_id!=?',(root_user_id,ident)).fetchone():
                    raise Error('资源已绑定',409)
            db.execute('DELETE FROM panel_resource_bindings WHERE account_id=?',(ident,))
            if root_user_id is not None: db.execute('INSERT INTO panel_resource_bindings VALUES(?,?)',(ident,root_user_id))
            db.execute("UPDATE panel_accounts SET salt=?,password_hash=?,password_alg=?,credential_state='active',enabled=?,role=?,must_change_password=1,credential_version=credential_version+1 WHERE id=?",
                       (salt,encoded,ALGORITHM,int(enabled),role,ident))
            self._revoke(db,now,ident); self._audit(db,now,'offline_reset',target=ident)

    def restore(self,backup,new_login,new_password):
        """Offline only: stop the service first; never restores the external anchor.

        Resource tables are restored exactly from the caller-selected consistent
        backup. Only panel identity state is quarantined, never auto-reactivated.
        """
        password(new_password)
        if Path(backup).resolve()==self.path: raise Error('备份不能是活动数据库',400)
        with self._lock():
            anchor=self._read_anchor()
            generation=secrets.token_hex(16)
            anchor={**anchor,'state':'recovering','restore_generation':generation,
                    'recoveries':[*anchor.get('recoveries',[]),dict(event='restore_started',id=generation)]}
            self._anchor(anchor,'restore_start')
            src=sqlite3.connect('file:'+str(Path(backup).resolve())+'?mode=ro',uri=True)
            db=self._db()
            try:
                if (src.execute('PRAGMA integrity_check').fetchone()[0]!='ok' or src.execute('PRAGMA foreign_key_check').fetchone()):
                    raise Error('备份完整性无效',503)
                self._hit('restore.before_backup'); src.backup(db); self._hit('restore.copied')
                db.execute('BEGIN IMMEDIATE')
                now=max(self.clock(),anchor['clock_floor'])
                old_digest=self._snapshot(db)
                counts={name:db.execute('SELECT COUNT(*) FROM '+name).fetchone()[0]
                        for name in ('panel_accounts','panel_sessions','panel_login_challenges','panel_resource_bindings')}
                db.execute("UPDATE panel_accounts SET enabled=0,must_change_password=1,salt=NULL,password_hash=NULL,credential_state='quarantined',credential_version=credential_version+1,auth_epoch=auth_epoch+1")
                db.execute('UPDATE panel_sessions SET revoked_at=COALESCE(revoked_at,?)',(now,))
                db.execute('UPDATE panel_login_challenges SET consumed_at=COALESCE(consumed_at,?)',(now,))
                db.execute('DELETE FROM panel_resource_bindings')
                db.execute('UPDATE panel_auth_meta SET deployment_id=?,restore_generation=?,identity_revision=?,clock_floor=?',
                           (anchor['deployment_id'],generation,anchor['identity_revision']+1,now))
                self._audit(db,now,'restore_quarantine:'+old_digest+':'+json.dumps(counts,sort_keys=True))
                ident=self._new(db,now,new_login,new_password,'admin')
                self._audit(db,now,'restore_admin',target=ident)
                self._hit('restore.before_commit'); db.commit(); self._hit('restore.committed')
                meta=dict(db.execute('SELECT * FROM panel_auth_meta').fetchone())
                try:
                    self._anchor({**anchor,**meta,'state':'clean','digest':self._snapshot(db),
                                  'recoveries':[*anchor['recoveries'],dict(event='quarantine_complete',id=generation)]},'restore_clean')
                except BaseException:
                    self._anchor(anchor,'restore_failure_fence')
                    raise
                _TIME_POISONED.discard(str(self.control))
                return ident
            finally: src.close(); db.rollback(); db.close()
