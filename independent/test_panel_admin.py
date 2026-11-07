"""P2b original Handler / original business SQL / durable identity boundary."""
import concurrent.futures
import http.client
import json
from pathlib import Path
import secrets
import sqlite3
import subprocess
import sys
import threading
import unittest
from unittest import mock

from core import Error,Registry
from panel_admin import route
from panel_identity import PanelIdentity,hashed
import test_panel_http as http_fixture


class AdminTests(unittest.TestCase):
    setUp=http_fixture.HTTPTests.setUp
    tearDown=http_fixture.HTTPTests.tearDown
    request=http_fixture.HTTPTests.request
    panel=http_fixture.HTTPTests.panel
    challenge=http_fixture.HTTPTests.challenge
    login=http_fixture.HTTPTests.login

    def start_admin(self):
        self.server.panel_admin_enabled=True
        return self.login()

    def call(self,path,method='GET',data=None,*,cookie=None,csrf=None,headers=()):
        if method=='DELETE' and data is None: data={}
        return self.request(path,method,data,cookie=cookie,csrf=csrf,headers=headers)

    def bound(self):
        uid=self.registry.save('users',{'name':'bound'})['id']; secret=secrets.token_urlsafe(24)
        ident=self.kernel.create_account(self.admin_token,'u'+secrets.token_hex(4),secret,actor_password=self.password,root_user_id=uid)
        with self.registry.connect() as db: login=db.execute('SELECT login_key FROM panel_accounts WHERE id=?',(ident,)).fetchone()[0]
        token=self.kernel.login(login,secret,'bound'); changed=secrets.token_urlsafe(24)
        self.kernel.change_password(token,secret,changed); token=self.kernel.login(login,changed,'bound')
        return uid,ident,token,login,changed

    def snapshot(self):
        with self.registry.connect() as db:
            names=[r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")]
            return {t:[tuple(r) for r in db.execute('SELECT * FROM '+t)] for t in names}

    def test_cookie_crud_server_cas_node_cas_quotas_and_original_business(self):
        cookie,csrf=self.start_admin()
        def call(path,method='GET',data=None,headers=()):
            r=self.call(path,method,data,cookie=cookie,csrf=csrf,headers=headers)
            return r[0],json.loads(r[2])
        status,host=call('/api/manage/servers','POST',{'name':'host'}); self.assertEqual(status,201)
        path='/api/manage/servers/'+str(host['id'])
        self.assertEqual(call(path,'PUT',{'name':'new'},[('If-Match',host['version'])])[0],200)
        self.assertEqual(call(path,'PUT',{'name':'stale'},[('If-Match',host['version'])])[0],412)
        status,node=call('/api/nodes','POST',{'name':'node','server_id':host['id']}); self.assertEqual(status,201)
        self.assertEqual(call('/api/nodes/'+str(node['id']),'PUT',{'name':'changed','server_id':host['id']},[('If-Match',node['version'])])[0],200)
        self.assertEqual(call('/api/nodes/'+str(node['id']),'PUT',{'name':'stale','server_id':host['id']},[('If-Match',node['version'])])[0],412)
        self.assertEqual(call('/api/servers/'+str(host['id']),'DELETE')[0],409)
        self.assertEqual(call('/api/quotas/users','PUT',{'limit':1})[0],200)
        status,user=call('/api/users','POST',{'name':'first'}); self.assertEqual(status,201)
        self.assertEqual(call('/api/users','POST',{'name':'over-quota'})[0],409)
        self.assertEqual(call('/api/users/'+str(user['id']),'PUT',{'name':'renamed'})[0],200)
        self.assertEqual(call('/api/users/'+str(user['id']),'DELETE')[0],200)
        self.assertEqual(call('/api/nodes/'+str(node['id']),'DELETE')[0],200)
        self.assertEqual(call('/api/servers/'+str(host['id']),'DELETE')[0],200)
        self.assertEqual(call('/api/quotas')[1]['users']['used'],0)

    def test_binding_delete_cookie_and_legacy_atomic_revoke_audit_reopen(self):
        cookie,csrf=self.start_admin()
        for legacy in (False,True):
            uid,ident,token,login,password=self.bound()
            kwargs={'headers':[('Authorization','Bearer '+self.server.token)]} if legacy else {'cookie':cookie,'csrf':csrf}
            response=self.call('/api/users/'+str(uid),'DELETE',**kwargs)
            self.assertEqual(response[0],200,response[2])
            with self.registry.connect() as db:
                self.assertIsNone(db.execute('SELECT * FROM panel_resource_bindings WHERE account_id=?',(ident,)).fetchone())
                self.assertIsNotNone(db.execute('SELECT revoked_at FROM panel_sessions WHERE id_hash=?',(hashed(token),)).fetchone()[0])
                audit=db.execute("SELECT actor_id FROM panel_auth_audit WHERE event='resource_unbind_delete' AND target_id=?",(ident,)).fetchone()
                self.assertEqual(audit[0],'legacy_admin' if legacy else self.admin)
            reopened=PanelIdentity(self.registry.path,self.root/'control',clock=lambda:self.now)
            with self.assertRaises(Error) as caught: reopened.authenticate(token)
            self.assertEqual(caught.exception.status,401)
            fresh=reopened.login(login,password,'reopen'); self.assertEqual(reopened.resources(fresh),[])
            self.assertEqual(self.call('/api/users/'+str(uid),'DELETE',**kwargs)[0],404)

    def test_delete_publication_failure_and_child_rejection_roll_back_every_table(self):
        cookie,csrf=self.start_admin(); uid,ident,token,_,_=self.bound()
        host=self.registry.save('servers',{'name':'host'})['id']
        node=self.registry.save('nodes',{'name':'node','server_id':host})['id']
        with self.registry.connect() as db:
            db.execute('INSERT INTO node_users VALUES(?,?,?)',(node,uid,secrets.token_hex(16)))
            db.execute("CREATE TRIGGER reject_publish BEFORE UPDATE ON server_configs BEGIN SELECT RAISE(ABORT,'publication failure'); END")
        before=self.snapshot(); anchor=self.kernel._read_anchor()
        self.assertEqual(self.call('/api/users/'+str(uid),'DELETE',cookie=cookie,csrf=csrf)[0],409)
        self.assertEqual(before,self.snapshot()); self.assertEqual(anchor,self.kernel._read_anchor())
        self.assertEqual(self.kernel.authenticate(token)['id'],ident)
        child=self.registry.save('users',{'name':'child'})['id']
        plan=self.registry.save_plan({'name':'p','bandwidth':{'upload_bps':None,'download_bps':None}})['id']
        with self.registry.connect() as db: db.execute('INSERT INTO plan_accounts VALUES(?,?,?)',(child,uid,plan))
        before=self.snapshot()
        self.assertEqual(self.call('/api/users/'+str(uid),'DELETE',cookie=cookie,csrf=csrf)[0],409)
        self.assertEqual(before,self.snapshot())

    def test_business_error_rolls_back_unbinding_before_delete_for_legacy(self):
        self.start_admin(); uid,ident,token,_,_=self.bound()
        before=self.snapshot()
        with mock.patch.object(self.registry,'delete',side_effect=Error('delete failed',409)):
            response=self.call('/api/users/'+str(uid),'DELETE',headers=[('Authorization','Bearer '+self.server.token)])
        self.assertEqual(response[0],409); self.assertEqual(before,self.snapshot())
        self.assertEqual(self.kernel.authenticate(token)['id'],ident)

    def test_auth_csrf_state_and_transport_all_before_business(self):
        cookie,csrf=self.start_admin(); uid,ident,user,_,_=self.bound()
        paths=['/api/users','/api/users/1','/api/plans','/api/plans/1',
            '/api/plans/1/nodes','/api/plans/1/nodes/1/connections','/api/plans/1/nodes/1/bandwidth',
            '/api/speed-tests','/api/speed-tests/status','/api/speed-tests/id',
            '/api/servers/1/shares','/api/shares/id','/api/shares/id/rotate','/api/shares/id/usage',
            '/api/shares/id/usage/periods','/api/shares/id/usage/reset',
            '/api/users/1/usage','/api/users/1/plan-order','/api/users/1/plan-renewals',
            '/api/plan-renewals/1/decision','/api/users/1/plan-lifecycle','/api/users/1/accounts','/api/users/1/accounts/2',
            '/api/automatic-bandwidth','/api/users/1/automatic-bandwidth',
            '/api/reality-domains','/api/reality-domains/selection','/api/reality-domains/1',
            '/api/reality-domains/1/probe','/api/reality-domains/1/probe-schedule',
            '/api/servers/1/reality-domains','/api/servers/1/reality-domains/selection','/api/servers/1/reality-domains/1/probe-agent',
            '/api/node-groups','/api/node-groups/1','/api/users/1/connections-policy','/api/users/1/connections',
            '/api/users/1/temporary-bandwidth','/api/users/1/plan','/api/users/1/bandwidth-policy',
            '/api/users/1/bandwidth','/api/servers/1/bandwidth','/api/agent-status','/api/nodes/1/users/1',
            '/api/nodes/1/config','/api/nodes/1/reality-failover','/api/nodes/1/reality-domain','/api/servers/1/agent',
            '/api/manage/servers','/api/manage/servers/1','/api/quotas','/api/quotas/users','/api/unknown',
            '/agent/session','/share/status','/manage/users','/health','/unknown']
        with mock.patch.object(self.registry,'list',side_effect=AssertionError('business invoked')):
            for path in paths:
                for method in ('GET','POST','PUT','DELETE','PATCH','HEAD','OPTIONS','TRACE','CONNECT','UNKNOWN'):
                    self.assertEqual(self.call(path,method,cookie='mmw_panel_test='+user,csrf=csrf)[0],403,(path,method))
        for value in (None,secrets.token_urlsafe(32)):
            self.assertEqual(self.call('/api/users','POST',{'name':'denied'},cookie=cookie,csrf=value)[0],403)
        self.assertEqual(self.request('/api/users','POST',{'name':'denied'},cookie=cookie,csrf=csrf,origin=False)[0],403)
        self.assertEqual(self.call('/api/users',cookie=cookie,headers=[('Authorization','Bearer '+self.server.token)])[0],400)
        self.assertEqual(self.call('/api/users',cookie='mmw_panel_test='+secrets.token_urlsafe(32))[0],401)
        for path in ('/api/speed-tests','/api/reality-domains/1/probe','/api/node-groups','/api/agent-status','/api/users?x=y'):
            self.assertEqual(self.call(path,cookie=cookie)[0],403)
        self.assertEqual(self.request('/api/users','POST',cookie=cookie,csrf=csrf,raw=b'{}',headers=[('Content-Type','text/plain')])[0],415)

    def test_revoke_before_business_zero_calls_business_before_revoke_commits(self):
        cookie,csrf=self.start_admin(); token=cookie.split('=')[1]
        # First order: revoked before request enters the critical transaction.
        self.kernel.revoke_all(token,self.password)
        with mock.patch.object(self.registry,'save',side_effect=AssertionError('revoked business')):
            self.assertEqual(self.call('/api/users','POST',{'name':'no'},cookie=cookie,csrf=csrf)[0],401)
        cookie,csrf=self.login(); token=cookie.split('=')[1]
        entered=threading.Event(); release=threading.Event(); attempting=threading.Event()
        def fault(point):
            if point=='admin.authorized': entered.set(); self.assertTrue(release.wait(10))
        self.kernel.fault=fault
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            business=pool.submit(self.call,'/api/users','POST',{'name':'ordered'},cookie=cookie,csrf=csrf)
            self.assertTrue(entered.wait(5))
            def revoke(): attempting.set(); self.kernel.revoke_all(token,self.password)
            revocation=pool.submit(revoke); self.assertTrue(attempting.wait(5))
            self.assertFalse(revocation.done()); release.set()
            self.assertEqual(business.result(timeout=10)[0],201); revocation.result(timeout=10)
        self.kernel.fault=lambda _:None
        self.assertEqual(self.registry.list('users'),[{'id':1,'name':'ordered'}])
        self.assertEqual(self.call('/api/users',cookie=cookie)[0],401)

    def test_logout_after_body_read_revokes_before_final_authorization(self):
        cookie,csrf=self.start_admin(); token=cookie.split('=')[1]
        payload=json.dumps({'name':'blocked'}).encode()
        conn=http.client.HTTPConnection('127.0.0.1',self.server.server_port,timeout=5)
        conn.putrequest('POST','/api/users')
        for k,v in [('Cookie',cookie),('Origin',self.origin),('Content-Type','application/json'),('Content-Length',str(len(payload))),('X-CSRF-Token',csrf)]: conn.putheader(k,v)
        conn.endheaders(); conn.send(payload[:1]); self.kernel.logout(token)
        conn.send(payload[1:]); response=conn.getresponse(); self.assertEqual(response.status,401); response.read(); conn.close()
        self.assertEqual(self.registry.list('users'),[])

    def test_exact_same_connection_no_independent_registry_connect_and_secret_absence(self):
        cookie,csrf=self.start_admin()
        with mock.patch.object(self.registry,'connect',side_effect=AssertionError('independent DB forbidden')):
            response=self.call('/api/users','POST',{'name':'shared-tx'},cookie=cookie,csrf=csrf)
        self.assertEqual(response[0],201,response[2])
        with self.registry.connect() as db: audit=json.dumps([dict(r) for r in db.execute('SELECT * FROM panel_auth_audit')])
        for secret in (self.password,self.server.token,self.admin_token,cookie.split('=')[1],csrf):
            self.assertNotIn(secret,audit); self.assertNotIn(secret.encode(),response[2])

    def test_response_loss_after_commit_does_not_undo_delete_or_revoke(self):
        cookie,csrf=self.start_admin(); uid,ident,token,_,_=self.bound()
        from server import Handler
        original=Handler.reply
        def lost(handler,status,value):
            if handler.path=='/api/users/'+str(uid) and status==200: return None
            return original(handler,status,value)
        with mock.patch.object(Handler,'reply',lost):
            with self.assertRaises(http.client.RemoteDisconnected):
                self.call('/api/users/'+str(uid),'DELETE',cookie=cookie,csrf=csrf)
        self.assertEqual(self.call('/api/users/'+str(uid),'DELETE',cookie=cookie,csrf=csrf)[0],404)
        with self.assertRaises(Error) as caught: self.kernel.authenticate(token)
        self.assertEqual(caught.exception.status,401)

    def test_explicit_whitelist_unknown_and_method_variants_rejected(self):
        for path,method in [('/api/users/1/accounts','POST'),('/api/users/1','POST'),('/api/users','PATCH'),('/api/servers/1/agent','DELETE'),('/api/manage/servers/1','DELETE'),('/api/quotas','POST')]:
            self.assertIsNone(route(path,method))

    def test_legacy_delete_trailing_slash_query_and_padded_id_remain_compatible(self):
        self.start_admin(); uid,ident,token,_,_=self.bound()
        response=self.request('/api/users/0'+str(uid)+'/?old=script','DELETE',origin=False,
                              headers=[('Authorization','Bearer '+self.server.token)])
        self.assertEqual(response[0],200,response[2])
        with self.assertRaises(Error) as caught: self.kernel.authenticate(token)
        self.assertEqual(caught.exception.status,401)

    def test_cas_competition_and_delete_unbind_race_keep_original_rules(self):
        cookie,csrf=self.start_admin()
        host=self.call('/api/manage/servers','POST',{'name':'host'},cookie=cookie,csrf=csrf)
        row=json.loads(host[2]); gate=threading.Barrier(2)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            def put(name):
                gate.wait()
                return self.call('/api/manage/servers/'+str(row['id']),'PUT',{'name':name},cookie=cookie,csrf=csrf,
                                 headers=[('If-Match',row['version'])])[0]
            self.assertEqual(sorted(pool.map(put,('one','two'))),[200,412])
        uid,ident,token,_,_=self.bound()
        entered=threading.Event(); release=threading.Event(); attempted=threading.Event()
        def pause(point):
            if point=='admin.unbound': entered.set(); self.assertTrue(release.wait(10))
        self.kernel.fault=pause
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            deletion=pool.submit(self.call,'/api/users/'+str(uid),'DELETE',cookie=cookie,csrf=csrf)
            self.assertTrue(entered.wait(5))
            def revoke(): attempted.set(); self.kernel.logout(token)
            revocation=pool.submit(revoke); self.assertTrue(attempted.wait(5)); self.assertFalse(revocation.done())
            release.set(); self.assertEqual(deletion.result(timeout=10)[0],200); revocation.result(timeout=10)
        self.kernel.fault=lambda _:None
        with self.assertRaises(Error): self.kernel.authenticate(token)

    def test_csrf_rotation_and_account_downgrade_before_final_transaction_deny(self):
        cookie,csrf=self.start_admin(); entered=threading.Event(); release=threading.Event()
        import panel_admin
        original=panel_admin._body
        def pause(*args):
            data=original(*args); entered.set(); self.assertTrue(release.wait(10)); return data
        with mock.patch.object(panel_admin,'_body',pause),concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending=pool.submit(self.call,'/api/users','POST',{'name':'no'},cookie=cookie,csrf=csrf)
            self.assertTrue(entered.wait(5)); self.kernel.session_info(cookie.split('=')[1]); release.set()
            self.assertEqual(pending.result(timeout=10)[0],403)
        self.assertEqual(self.registry.list('users'),[])
        # A separate admin can be disabled/downgraded without touching last-admin rules.
        password=secrets.token_urlsafe(24)
        ident=self.kernel.create_account(self.admin_token,'second_admin',password,actor_password=self.password,role='admin')
        token=self.kernel.login('second_admin',password,'local'); changed=secrets.token_urlsafe(24)
        self.kernel.change_password(token,password,changed); second,secondcsrf=self.login('second_admin',changed)
        self.kernel.update_account(self.admin_token,ident,actor_password=self.password,role='user')
        with mock.patch.object(self.registry,'save',side_effect=AssertionError('downgraded business invoked')):
            self.assertEqual(self.call('/api/users','POST',{'name':'no'},cookie=second,csrf=secondcsrf)[0],401)


class AdminCrashTests(unittest.TestCase):
    def test_real_main_admin_cookie_wiring_and_bearer_bound_delete(self):
        import tempfile,select,socket
        with tempfile.TemporaryDirectory() as name:
            root=Path(name); (root/'data').mkdir(); registry=Registry(root/'data'/'registry.db')
            kernel=PanelIdentity(registry.path,root/'control'); password=secrets.token_urlsafe(24)
            kernel.initialize('admin',password); token=kernel.login('admin',password,'local')
            csrf=kernel.session_info(token)['csrf_token']; user=registry.save('users',{'name':'bound'})['id']
            ident=kernel.create_account(token,'bound_user',secrets.token_urlsafe(24),actor_password=password,root_user_id=user)
            with socket.socket() as probe: probe.bind(('127.0.0.1',0)); port=probe.getsockname()[1]
            origin=f'http://127.0.0.1:{port}'
            command=[sys.executable,'server.py','--data',str(root/'data'),'--port',str(port),'--panel-control',str(root/'control'),
                     '--panel-origin',origin,'--panel-loopback-test']
            child=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,cwd=Path(__file__).parent)
            try:
                ready,_,_=select.select([child.stdout],[],[],10); self.assertTrue(ready)
                self.assertIn('Independent API:',child.stdout.readline())
                conn=http.client.HTTPConnection('127.0.0.1',port,timeout=5)
                conn.request('POST','/api/users',json.dumps({'name':'real-main'}),{'Origin':origin,'Cookie':'mmw_panel_test='+token,
                    'X-CSRF-Token':csrf,'Content-Type':'application/json'})
                response=conn.getresponse(); self.assertEqual(response.status,201,response.read()); response.read(); conn.close()
                bearer=(root/'data'/'admin-token').read_text().strip()
                conn=http.client.HTTPConnection('127.0.0.1',port,timeout=5)
                conn.request('DELETE','/api/users/'+str(user),headers={'Authorization':'Bearer '+bearer})
                response=conn.getresponse(); self.assertEqual(response.status,200,response.read()); response.read(); conn.close()
                child.terminate(); out,err=child.communicate(timeout=10); self.assertEqual(child.returncode,0,err)
                for secret in (token,csrf,password,bearer): self.assertNotIn(secret,out+err)
                self.assertFalse((root/'control'/'running.json').exists())
                with registry.connect() as db:
                    self.assertIsNone(db.execute('SELECT * FROM panel_resource_bindings WHERE account_id=?',(ident,)).fetchone())
                reopened=PanelIdentity(registry.path,root/'control'); self.assertEqual(reopened.authenticate(token)['role'],'admin')
            finally:
                if child.poll() is None: child.kill(); child.communicate()

    def test_real_http_audit_or_anchor_fault_stops_process_no_success_response(self):
        import tempfile
        for point in ('audit','dirty.before_fsync','db.committed','clean.before_replace'):
            with self.subTest(point=point),tempfile.TemporaryDirectory() as name:
                root=Path(name); (root/'data').mkdir(); registry=Registry(root/'data'/'r.db')
                kernel=PanelIdentity(registry.path,root/'control',clock=lambda:2000000000.)
                password=secrets.token_urlsafe(24); kernel.initialize('admin',password)
                token=kernel.login('admin',password,'local'); csrf=kernel.session_info(token)['csrf_token']
                code='''import sys,json,threading,http.client
from core import Registry
from panel_identity import PanelIdentity
from panel_runtime import PanelRuntime
from panel_authorization import PanelConfig
from server import PanelHTTPServer,Handler
data=json.loads(sys.stdin.read()); registry=Registry(sys.argv[1])
k=PanelIdentity(sys.argv[1],sys.argv[2],clock=lambda:2000000000.)
r=PanelRuntime(k)
if data['point']=='audit':
    with registry.connect() as db: db.execute("CREATE TRIGGER fail_audit BEFORE INSERT ON panel_auth_audit BEGIN SELECT RAISE(ABORT,'audit failed'); END")
def fail(point):
    if point==data['point']: raise OSError('durability failure')
k.fault=fail
server=PanelHTTPServer(('127.0.0.1',0),Handler)
origin='http://127.0.0.1:'+str(server.server_port)
server.panel_config=PanelConfig(origin,loopback_test=True); server.panel_runtime=r
server.panel_admin_enabled=True; server.registry=registry
threading.Thread(target=server.serve_forever,daemon=True).start()
conn=http.client.HTTPConnection('127.0.0.1',server.server_port)
conn.request('POST','/api/users',json.dumps({'name':'atomic'}),{'Content-Type':'application/json','Origin':origin,
    'Cookie':'mmw_panel_test='+data['token'],'X-CSRF-Token':data['csrf']})
response=conn.getresponse()
print('unexpected HTTP response',response.status,flush=True)
'''
                result=subprocess.run([sys.executable,'-c',code,str(registry.path),str(root/'control')],input=json.dumps(dict(point=point,token=token,csrf=csrf)),text=True,capture_output=True,cwd=Path(__file__).parent,timeout=10)
                self.assertEqual(result.returncode,78,result.stderr)
                self.assertEqual(result.stdout,'')
                self.assertTrue((root/'control'/'running.json').exists())
                with registry.connect() as db:
                    users=db.execute('SELECT COUNT(*) FROM users').fetchone()[0]
                    audit=db.execute("SELECT COUNT(*) FROM panel_auth_audit WHERE event='admin_post_users'").fetchone()[0]
                committed=point in ('db.committed','clean.before_replace')
                self.assertEqual(users,int(committed)); self.assertEqual(audit,int(committed))
                if committed: self.assertEqual(kernel._read_anchor()['state'],'dirty')
                for secret in (token,csrf,password): self.assertNotIn(secret,result.stdout+result.stderr)

    def test_deletion_crash_boundaries_commit_and_external_anchor(self):
        # Fresh OS processes: no volatile latch or mocked exit can stand in for
        # the durable dirty fence. Read values via stdin, never output secrets.
        import tempfile
        for point in ('admin.unbound','admin.business_done','db.committed','clean.before_replace'):
            with self.subTest(point=point),tempfile.TemporaryDirectory() as name:
                root=Path(name); (root/'data').mkdir(); registry=Registry(root/'data'/'r.db')
                kernel=PanelIdentity(registry.path,root/'control',clock=lambda:2000000000.)
                password=secrets.token_urlsafe(24); admin=kernel.initialize('admin',password)
                token=kernel.login('admin',password,'local'); uid=registry.save('users',{'name':'bound'})['id']
                ident=kernel.create_account(token,'ordinary',secrets.token_urlsafe(24),actor_password=password,root_user_id=uid)
                code='''import sys,json,os
from core import Registry
from panel_identity import PanelIdentity
from panel_runtime import PanelRuntime
from panel_admin import unbind
data=json.loads(sys.stdin.read()); registry=Registry(sys.argv[1])
k=PanelIdentity(sys.argv[1],sys.argv[2],clock=lambda:2000000000.)
r=PanelRuntime(k)
def crash(point):
    if point==data['point']: os._exit(79)
k.fault=crash
with k._tx(True) as (db,now):
    k._session(db,now,data['token'],admin=True)
    unbind(k,db,now,data['uid'],data['admin'])
    registry.delete('users',data['uid'],db=db)
    k._hit('admin.business_done')
    k._audit(db,now,'admin_delete_users',data['admin'],str(data['uid']))
'''
                result=subprocess.run([sys.executable,'-c',code,str(registry.path),str(root/'control')],
                    input=json.dumps(dict(point=point,token=token,uid=uid,admin=admin)),text=True,capture_output=True,cwd=Path(__file__).parent,timeout=10)
                self.assertEqual(result.returncode,79,result.stderr)
                with registry.connect() as db:
                    deleted=db.execute('SELECT 1 FROM users WHERE id=?',(uid,)).fetchone() is None
                    unbound=db.execute('SELECT 1 FROM panel_resource_bindings WHERE account_id=?',(ident,)).fetchone() is None
                self.assertEqual(deleted,point in ('db.committed','clean.before_replace'))
                self.assertEqual(deleted,unbound)
                anchor=kernel._read_anchor()
                self.assertEqual(anchor['state'],'dirty' if deleted else 'clean')
                self.assertTrue((root/'control'/'running.json').exists())
                check='''import sys
from panel_identity import PanelIdentity
from panel_runtime import PanelRuntime
from core import Error
try: PanelRuntime(PanelIdentity(sys.argv[1],sys.argv[2],clock=lambda:2000000000.))
except Error as e: sys.exit(72 if e.status==503 else 73)
sys.exit(74)
'''
                result=subprocess.run([sys.executable,'-c',check,str(registry.path),str(root/'control')],capture_output=True,cwd=Path(__file__).parent,timeout=10)
                self.assertEqual(result.returncode,72)
