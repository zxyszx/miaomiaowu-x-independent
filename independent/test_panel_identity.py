"""Real SQLite, files and subprocesses; random disposable credentials only."""
import concurrent.futures
import json
import os
from pathlib import Path
import secrets
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from core import Registry,Error
from panel_identity import PanelIdentity, TABLES, KDF, hashed, derive


def pw(): return secrets.token_urlsafe(24)


class PanelTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.root=Path(self.tmp.name)
        self.data=self.root/'data'; self.data.mkdir()
        self.registry=Registry(self.data/'registry.db')
        self.control=self.root/'control'; self.now=2000000000.
        self.kernel=PanelIdentity(self.registry.path,self.control,clock=lambda:self.now)
        self.password=pw(); self.admin=self.kernel.initialize('admin',self.password)
        self.token=self.kernel.login('admin',self.password,'127.0.0.1')

    def tearDown(self): self.tmp.cleanup()

    def denied(self,status,function):
        with self.assertRaises(Error) as caught: function()
        self.assertEqual(caught.exception.status,status)

    def new_user(self,root=None,role='user'):
        password=pw(); ident=self.kernel.create_account(self.token,'u'+secrets.token_hex(5),password,actor_password=self.password,role=role,root_user_id=root)
        with self.registry.connect() as db: login=db.execute('SELECT login_key FROM panel_accounts WHERE id=?',(ident,)).fetchone()[0]
        token=self.kernel.login(login,password,'local-'+ident)
        changed=pw(); self.kernel.change_password(token,password,changed)
        return ident,login,changed,self.kernel.login(login,changed,'local-'+ident)

    def backup(self,name='snapshot.db'):
        path=self.root/name
        with sqlite3.connect(self.registry.path) as src,sqlite3.connect(path) as dst: src.backup(dst)
        return path

    def business(self):
        with self.registry.connect() as db:
            names=[r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'panel_%' ORDER BY name")]
            return {name:[tuple(r) for r in db.execute('SELECT * FROM '+name)] for name in names}

    def test_persist_password_revoke_and_business_exact(self):
        before=self.business(); ident,login,password,token=self.new_user()
        self.denied(403,lambda:self.kernel.authenticate(token,admin=True))
        reopened=PanelIdentity(self.registry.path,self.control,clock=lambda:self.now)
        self.assertEqual(reopened.authenticate(token)['id'],ident)
        changed=pw(); reopened.change_password(token,password,changed)
        self.denied(401,lambda:reopened.authenticate(token))
        self.denied(401,lambda:reopened.login(login,password,'retry'))
        token=reopened.login(login,changed,'retry'); reopened.revoke_all(token,changed)
        self.denied(401,lambda:reopened.authenticate(token))
        self.assertEqual(before,self.business())

    def test_limits_unknown_dummy_and_clock_expiry(self):
        for _ in range(5): self.denied(401,lambda:self.kernel.login('missing',pw(),'ip'))
        self.denied(429,lambda:self.kernel.login('missing',pw(),'ip'))
        self.now+=901; self.denied(401,lambda:self.kernel.login('missing',pw(),'ip'))
        self.now+=1800; self.denied(401,lambda:self.kernel.authenticate(self.token))
        self.now=2000000000.; self.denied(503,lambda:self.kernel.authenticate(self.token))

    def test_binding_rejects_unknown_child_shared_and_fk(self):
        parent=self.registry.save('users',{'name':'parent'})['id']
        child=self.registry.save('users',{'name':'child'})['id']
        shared=self.registry.save('users',{'name':'shared'})['id']
        with self.registry.connect() as db:
            # Populate real legacy relationship tables using their actual schema.
            plan=db.execute("INSERT INTO plans(name,bandwidth) VALUES('plan','{}')").lastrowid
            db.execute('INSERT INTO plan_accounts VALUES(?,?,?)',(child,parent,plan))
            columns=[r[1] for r in db.execute('PRAGMA table_info(shared_users)')]
        sid=self.registry.save('servers',{'name':'share-host'})['id']
        from sharing import Shares
        from test_sharing import settings
        share=Shares(self.registry).create(sid,settings())['id']
        with self.registry.connect() as db: db.execute('INSERT INTO shared_users VALUES(?,?,?)',(shared,share,'ref'))
        self.denied(409,lambda:self.kernel.create_account(self.token,'shared',pw(),actor_password=self.password,root_user_id=shared))
        self.denied(404,lambda:self.kernel.create_account(self.token,'unknown',pw(),actor_password=self.password,root_user_id=99999))
        self.denied(409,lambda:self.kernel.create_account(self.token,'child',pw(),actor_password=self.password,root_user_id=child))
        ident,login,password,token=self.new_user(parent)
        self.assertEqual({r['id'] for r in self.kernel.resources(token)},{parent,child})
        with self.assertRaises(sqlite3.IntegrityError):
            with self.registry.connect() as db: db.execute('DELETE FROM users WHERE id=?',(parent,))
        self.kernel.update_account(self.token,ident,actor_password=self.password,bind=True)
        self.denied(401,lambda:self.kernel.authenticate(token))
        self.assertEqual(self.kernel.resources(self.kernel.login(login,password,'unbound')),[])
        self.assertIn('share_id',columns)

    def test_last_admin_and_concurrent_revocation(self):
        other,_,other_pw,second=self.new_user(role='admin')
        def disable(pair):
            token,ident,password=pair
            try: self.kernel.update_account(token,ident,actor_password=password,enabled=False); return 'ok'
            except Error: return 'denied'
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            results=list(pool.map(disable,[(self.token,self.admin,self.password),(second,other,other_pw)]))
        self.assertEqual(sorted(results),['denied','ok'])
        with self.registry.connect() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM panel_accounts WHERE role='admin' AND enabled=1").fetchone()[0],1)

    def test_restore_quarantines_old_password_status_binding(self):
        parent=self.registry.save('users',{'name':'parent'})['id']
        ident,login,password,token=self.new_user(parent); backup=self.backup()
        self.kernel.update_account(self.token,ident,actor_password=self.password,enabled=False,bind=True)
        replacement=pw(); self.kernel.restore(backup,'replacement',replacement)
        self.denied(401,lambda:self.kernel.login(login,password,'restored'))
        self.denied(401,lambda:self.kernel.authenticate(token))
        self.denied(401,lambda:self.kernel.login('admin',self.password,'old-admin'))
        self.assertEqual(self.kernel.authenticate(self.kernel.login('replacement',replacement,'new'))['role'],'admin')
        with self.registry.connect() as db:
            row=db.execute('SELECT * FROM panel_accounts WHERE id=?',(ident,)).fetchone()
            self.assertEqual(row['credential_state'],'quarantined'); self.assertIsNone(row['password_hash'])
            self.assertEqual(db.execute('SELECT COUNT(*) FROM panel_resource_bindings').fetchone()[0],0)

    def test_uncontrolled_rollback_and_missing_anchor(self):
        backup=self.backup(); self.kernel.logout(self.token)
        with sqlite3.connect(backup) as src,sqlite3.connect(self.registry.path) as dst: src.backup(dst)
        self.denied(503,lambda:self.kernel.login('admin',self.password,'rollback'))
        (self.control/'anchor.json').unlink()
        self.denied(503,lambda:self.kernel.authenticate(self.token))

    def test_fault_boundaries_reopen_fail_closed(self):
        points=('dirty.before_fsync','dirty.before_replace','dirty.before_dir_fsync','dirty.durable',
                'db.before_commit','db.committed','clean.before_fsync','clean.before_replace','clean.before_dir_fsync','clean.durable')
        for point in points:
            with self.subTest(point=point),tempfile.TemporaryDirectory() as temp:
                root=Path(temp); (root/'db').mkdir(); r=Registry(root/'db'/'r.db')
                k=PanelIdentity(r.path,root/'control',clock=lambda:self.now)
                password=pw(); k.initialize('admin',password); token=k.login('admin',password,'local')
                def fault(actual):
                    if actual==point: raise OSError('injected')
                k.fault=fault
                self.denied(503,lambda:k.logout(token))
                k.fault=lambda _:None
                anchor=json.loads((root/'control'/'anchor.json').read_text())
                if anchor['state']!='clean': self.denied(503,lambda:k.authenticate(token))
                elif point.startswith('clean.'): self.denied(401,lambda:k.authenticate(token))
                else: self.assertEqual(k.authenticate(token)['role'],'admin')

    def test_subprocess_crash_after_commit(self):
        code="""import os,sys
from panel_identity import PanelIdentity
k=PanelIdentity(sys.argv[1],sys.argv[2],clock=lambda:2000000000.)
k.fault=lambda p: os._exit(73) if p=='db.committed' else None
k.logout(sys.stdin.read())
"""
        result=subprocess.run([sys.executable,'-c',code,str(self.registry.path),str(self.control)],input=self.token,text=True,capture_output=True,cwd=Path(__file__).parent)
        self.assertEqual(result.returncode,73)
        self.denied(503,lambda:self.kernel.authenticate(self.token))

    def test_hashes_audit_redaction_and_cli_no_tty(self):
        with self.registry.connect() as db:
            text='\n'.join(str(tuple(r)) for table in TABLES for r in db.execute('SELECT * FROM '+table))
        self.assertNotIn(self.password,text); self.assertNotIn(self.token,text)
        self.assertIn(hashed(self.token),text)
        result=subprocess.run([sys.executable,str(Path(__file__).with_name('panel_accounts_cli.py')),
            '--database',str(self.registry.path),'--control-directory',str(self.control),'reset',self.admin,
            '--role','admin','--enabled','yes'],input=pw(),text=True,capture_output=True)
        self.assertEqual(result.returncode,1)
        self.assertNotIn(self.password,result.stdout+result.stderr)

    def test_absolute_and_idle_deadlines_are_exclusive(self):
        for offset in range(1700,28800,1700):
            self.now=2000000000.+offset
            self.kernel.authenticate(self.token)
        self.now=2000000000.+28800
        self.denied(401,lambda:self.kernel.authenticate(self.token))

    def test_observed_future_expiry_cannot_revive_on_clock_return(self):
        self.now+=1800
        self.denied(401,lambda:self.kernel.authenticate(self.token))
        self.now-=1
        self.denied(503,lambda:self.kernel.authenticate(self.token))

    def test_password_cas_race_real_kdf(self):
        barrier=threading.Barrier(2)
        self.kernel.fault=lambda point: barrier.wait(timeout=10) if point=='password.verified' else None
        next_passwords=[pw(),pw()]
        def change(value):
            try: self.kernel.change_password(self.token,self.password,value); return True
            except Error: return False
        with concurrent.futures.ThreadPoolExecutor(2) as pool: results=list(pool.map(change,next_passwords))
        self.assertEqual(sum(results),1)
        self.kernel.fault=lambda _:None
        winner=next_passwords[results.index(True)]
        self.assertEqual(self.kernel.authenticate(self.kernel.login('admin',winner,'cas'))['id'],self.admin)

    def test_transaction_fault_password_and_binding_roll_back(self):
        parent=self.registry.save('users',{'name':'parent'})['id']
        ident,_,password,token=self.new_user(parent)
        for point,call in [('password.updated',lambda:self.kernel.change_password(token,password,pw())),
                           ('binding.deleted',lambda:self.kernel.update_account(self.token,ident,actor_password=self.password,bind=True)),
                           ('account.updated',lambda:self.kernel.update_account(self.token,ident,actor_password=self.password,enabled=False))]:
            with self.subTest(point=point):
                with self.registry.connect() as db: before=self.kernel._snapshot(db)
                def fault(actual):
                    if actual==point: raise OSError('write failure')
                self.kernel.fault=fault; self.denied(503,call); self.kernel.fault=lambda _:None
                with self.registry.connect() as db: self.assertEqual(before,self.kernel._snapshot(db))
                self.assertEqual(self.kernel.resources(token)[0]['id'],parent)

    def test_restore_failure_boundaries_remain_closed(self):
        backup=self.backup()
        for point in ('restore_start.before_fsync','restore_start.before_replace','restore_start.before_dir_fsync',
                      'restore_start.durable','restore.before_backup','restore.copied','restore.before_commit',
                      'restore.committed','restore_clean.before_fsync','restore_clean.before_replace',
                      'restore_clean.before_dir_fsync','restore_clean.durable'):
            with self.subTest(point=point),tempfile.TemporaryDirectory() as temp:
                root=Path(temp); (root/'db').mkdir(); r=Registry(root/'db'/'r.db')
                k=PanelIdentity(r.path,root/'control',clock=lambda:self.now)
                old=pw(); k.initialize('old',old)
                def fault(actual):
                    if actual==point: raise OSError('restore failure')
                k.fault=fault
                self.denied(503,lambda:k.restore(backup,'replacement',pw()))
                k.fault=lambda _:None
                anchor=json.loads((root/'control'/'anchor.json').read_text())
                if point in ('restore_start.before_fsync','restore_start.before_replace'):
                    self.assertEqual(anchor['state'],'clean')  # replacement never began
                elif point.startswith('restore_clean.') and anchor['state']=='clean':
                    self.denied(401,lambda:k.login('admin',self.password,'old'))
                else: self.denied(503,lambda:k.login('admin',self.password,'old'))

    def test_digest_covers_each_identity_table(self):
        with self.registry.connect() as db: original=self.kernel._snapshot(db)
        for table in TABLES:
            with self.subTest(table=table),self.registry.connect() as db:
                db.execute('BEGIN IMMEDIATE')
                # Test SQLite visible tampering, rolled back afterwards.
                if table in ('panel_resource_bindings','panel_login_challenges','panel_auth_limits'):
                    if table=='panel_resource_bindings':
                        uid=db.execute("INSERT INTO users(name) VALUES('temp')").lastrowid
                        db.execute('INSERT INTO panel_resource_bindings VALUES(?,?)',(self.admin,uid))
                    elif table=='panel_login_challenges': db.execute("INSERT INTO panel_login_challenges VALUES('a','b',1,NULL)")
                    else: db.execute("INSERT INTO panel_auth_limits VALUES('tamper',1,1,1)")
                elif table=='panel_accounts': db.execute("UPDATE panel_accounts SET display_name='changed'")
                elif table=='panel_sessions': db.execute('UPDATE panel_sessions SET last_seen=last_seen+1')
                elif table=='panel_auth_meta': db.execute('UPDATE panel_auth_meta SET identity_revision=identity_revision+1')
                else: db.execute("UPDATE panel_auth_audit SET result='changed'")
                self.assertNotEqual(original,self.kernel._snapshot(db)); db.rollback()
        with self.registry.connect() as db: db.execute("UPDATE panel_accounts SET role='user' WHERE id=?",(self.admin,))
        self.denied(503,lambda:self.kernel.authenticate(self.token))

    def test_offline_rebuild_requires_new_password_and_explicit_mapping(self):
        parent=self.registry.save('users',{'name':'parent'})['id']
        ident,login,old,token=self.new_user(parent); backup=self.backup()
        self.kernel.restore(backup,'recovery',pw())
        fresh=pw(); self.kernel.offline_reset(ident,fresh,role='user',enabled=True,root_user_id=parent)
        self.denied(401,lambda:self.kernel.login(login,old,'old'))
        new=self.kernel.login(login,fresh,'new')
        self.denied(403,lambda:self.kernel.resources(new))
        changed=pw(); self.kernel.change_password(new,fresh,changed)
        self.assertEqual(self.kernel.resources(self.kernel.login(login,changed,'new'))[0]['id'],parent)

    def test_permission_checks_and_precise_resource_revocation(self):
        ident,_,_,token=self.new_user()
        self.denied(403,lambda:self.kernel.create_account(token,'bad',pw(),actor_password=pw()))
        self.denied(401,lambda:self.kernel.update_account(self.token,ident,actor_password=pw(),role='admin'))
        self.kernel.update_account(self.token,ident,actor_password=self.password,role='admin')
        self.denied(401,lambda:self.kernel.authenticate(token))
        self.denied(409,lambda:self.kernel.create_account(self.token,'admin',pw(),actor_password=self.password))

    def test_os_lock_is_exclusive_across_processes(self):
        code="""import fcntl,os,sys
fd=os.open(sys.argv[1],os.O_RDWR)
try: fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
except BlockingIOError: sys.exit(72)
sys.exit(0)
"""
        with self.kernel.authorized(self.token,admin=True):
            result=subprocess.run([sys.executable,'-c',code,str(self.control/'lock')],capture_output=True)
            self.assertEqual(result.returncode,72)
        result=subprocess.run([sys.executable,'-c',code,str(self.control/'lock')],capture_output=True)
        self.assertEqual(result.returncode,0)

    def test_real_scrypt_salts_and_capacity(self):
        self.kernel.create_account(self.token,'same_password',self.password,actor_password=self.password)
        with self.registry.connect() as db:
            rows=db.execute('SELECT salt,password_hash FROM panel_accounts ORDER BY login_key').fetchall()
        self.assertNotEqual(rows[0]['salt'],rows[1]['salt'])
        self.assertNotEqual(rows[0]['password_hash'],rows[1]['password_hash'])
        self.assertEqual(derive(self.password,rows[0]['salt']),rows[0]['password_hash'])
        KDF.acquire(); KDF.acquire()
        try: self.denied(429,lambda:derive(self.password,rows[0]['salt']))
        finally: KDF.release(); KDF.release()

    def test_permission_mode_and_recovering_fail_closed(self):
        self.control.chmod(0o755)
        self.denied(503,lambda:self.kernel.authenticate(self.token)); self.control.chmod(0o700)
        anchor=self.kernel._read_anchor()
        self.kernel._anchor({**anchor,'state':'recovering'},'test_recovery')
        self.denied(503,lambda:self.kernel.login('admin',self.password,'recovering'))
        self.denied(503,lambda:self.kernel.authenticate(self.token))

    def test_restore_preserves_resource_tables_exactly(self):
        uid=self.registry.save('users',{'name':'resource'})['id']
        self.new_user(uid); backup=self.backup(); before=self.business()
        self.kernel.restore(backup,'new_admin',pw())
        self.assertEqual(before,self.business())

    def test_initialization_faults_never_auto_reinitialize(self):
        for point in ('init.before_fsync','init.before_replace','init.before_dir_fsync','init.durable',
                      'init.before_commit','init.committed','init_clean.before_fsync','init_clean.before_replace',
                      'init_clean.before_dir_fsync','init_clean.durable'):
            with self.subTest(point=point),tempfile.TemporaryDirectory() as temp:
                root=Path(temp); (root/'db').mkdir(); r=Registry(root/'db'/'r.db')
                k=PanelIdentity(r.path,root/'control',clock=lambda:self.now)
                def fault(actual):
                    if actual==point: raise OSError('initialization failure')
                k.fault=fault
                self.denied(503,lambda:k.initialize('admin',pw())); k.fault=lambda _:None
                self.denied(503,lambda:k.login('admin',pw(),'init'))
                if (root/'control'/'anchor.json').exists():
                    self.denied(409,lambda:k.initialize('admin',pw()))

    def test_clock_write_failures_fence_rollback_reopen_and_other_process(self):
        for boundary in ('before_fsync','before_replace','before_dir_fsync','durable'):
            for label in ('clock_floor','clock_failure_fence'):
                point=label+'.'+boundary
                with self.subTest(point=point),tempfile.TemporaryDirectory() as temp:
                    root=Path(temp); (root/'db').mkdir(); r=Registry(root/'db'/'r.db')
                    clock=[self.now]; k=PanelIdentity(r.path,root/'control',clock=lambda:clock[0])
                    secret=pw(); k.initialize('admin',secret); token=k.login('admin',secret,'local')
                    clock[0]+=1800; fired=set()
                    def fault(actual):
                        targets={point}
                        if label=='clock_failure_fence': targets.add('clock_floor.before_fsync')
                        if actual in targets and actual not in fired:
                            fired.add(actual); raise OSError('clock write failed once')
                    k.fault=fault; self.denied(503,lambda:k.authenticate(token)); k.fault=lambda _:None
                    self.assertIn(point,fired)
                    self.assertEqual(k._read_anchor()['state'],'dirty')
                    clock[0]-=1; self.denied(503,lambda:k.authenticate(token))
                    reopened=PanelIdentity(r.path,root/'control',clock=lambda:clock[0])
                    self.denied(503,lambda:reopened.authenticate(token))
                    code="""import sys
from panel_identity import PanelIdentity
from core import Error
k=PanelIdentity(sys.argv[1],sys.argv[2],clock=lambda:2000001799.)
try: k.authenticate(sys.stdin.read())
except Error as e: sys.exit(0 if e.status==503 else 2)
sys.exit(3)
"""
                    result=subprocess.run([sys.executable,'-c',code,str(r.path),str(root/'control')],
                                          input=token,text=True,capture_output=True,cwd=Path(__file__).parent)
                    self.assertEqual(result.returncode,0)

    def test_continuous_clock_storage_failure_latches_process_until_restore(self):
        def fault(point):
            if point in ('clock_floor.before_fsync','clock_failure_fence.before_fsync'):
                raise OSError('continuous failure')
        self.now+=1800; self.kernel.fault=fault
        self.denied(503,lambda:self.kernel.authenticate(self.token))
        self.kernel.fault=lambda _:None; self.now-=1
        self.denied(503,lambda:self.kernel.authenticate(self.token))
        reopened=PanelIdentity(self.registry.path,self.control,clock=lambda:self.now)
        self.denied(503,lambda:reopened.authenticate(self.token))
        # No durable fence is claimed when ALL persistence failed. A new OS
        # process cannot infer this volatile fact from an unchanged clean file.
        self.assertEqual(self.kernel._read_anchor()['state'],'clean')
        self.kernel.restore(self.backup(),'after_storage_repair',pw())
        self.assertEqual(self.kernel._read_anchor()['state'],'clean')

    def test_offline_duplicate_binding_conflict_is_zero_identity_write(self):
        first_root=self.registry.save('users',{'name':'first-root'})['id']
        second_root=self.registry.save('users',{'name':'second-root'})['id']
        self.new_user(first_root)
        target,_,_,token=self.new_user(second_root)
        before_business=self.business()
        with self.registry.connect() as db: before=self.kernel._snapshot(db)
        anchor=self.kernel._read_anchor()
        self.denied(409,lambda:self.kernel.offline_reset(target,pw(),role='user',enabled=True,root_user_id=first_root))
        with self.registry.connect() as db: self.assertEqual(before,self.kernel._snapshot(db))
        self.assertEqual(anchor,self.kernel._read_anchor())
        self.assertEqual(before_business,self.business())
        self.assertEqual(self.kernel.authenticate(token)['id'],target)
