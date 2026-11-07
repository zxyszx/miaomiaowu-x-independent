"""Actual original Handler on disposable loopback, no production secrets."""
import concurrent.futures
import http.client
import json
from pathlib import Path
import re
import secrets
import ssl
import subprocess
import tempfile
import threading
import unittest
from unittest import mock
from http.server import ThreadingHTTPServer

from core import Registry,Error
from panel_authorization import PanelConfig,TEST,PRODUCTION
from panel_identity import PanelIdentity
from panel_runtime import PanelRuntime
from server import Handler


class HTTPTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.root=Path(self.temp.name)
        (self.root/'data').mkdir(); self.registry=Registry(self.root/'data'/'r.db')
        self.now=2000000000.; self.password=secrets.token_urlsafe(24)
        self.kernel=PanelIdentity(self.registry.path,self.root/'control',clock=lambda:self.now)
        self.admin=self.kernel.initialize('admin',self.password)
        self.admin_token=self.kernel.login('admin',self.password,'fixture')
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        self.origin=f'http://127.0.0.1:{self.server.server_port}'
        self.server.registry=self.registry; self.server.token=secrets.token_urlsafe(32)
        self.server.panel_config=PanelConfig(self.origin,loopback_test=True)
        self.server.panel_runtime=PanelRuntime(self.kernel)
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True); self.thread.start()

    def tearDown(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join()
        self.server.panel_runtime.close(); self.temp.cleanup()

    def request(self,path,method='GET',data=None,headers=(),cookie=None,csrf=None,origin=True,raw=None):
        conn=http.client.HTTPConnection('127.0.0.1',self.server.server_port,timeout=10)
        conn.putrequest(method,path,skip_host=True)
        pairs=[('Host',f'127.0.0.1:{self.server.server_port}')]
        if origin: pairs.append(('Origin',self.origin))
        if cookie is not None: pairs.append(('Cookie',cookie))
        if csrf is not None: pairs.append(('X-CSRF-Token',csrf))
        if data is not None:
            raw=json.dumps(data).encode(); pairs.append(('Content-Type','application/json'))
        if raw is not None: pairs.append(('Content-Length',str(len(raw))))
        for key,value in pairs+list(headers): conn.putheader(key,value)
        conn.endheaders(raw)
        response=conn.getresponse(); body=response.read(); result=(response.status,dict(response.getheaders()),body,response.getheaders())
        conn.close(); return result

    def panel(self,name,*args,**kwargs): return self.request('/panel/api/v1/'+name,*args,**kwargs)

    def challenge(self):
        result=self.panel('login-challenge'); self.assertEqual(result[0],200)
        return result[1]['Set-Cookie'].split(';')[0],json.loads(result[2])['csrf_token']

    def login(self,login='admin',password=None):
        cookie,csrf=self.challenge()
        result=self.panel('login','POST',{'login':login,'password':password or self.password},cookie=cookie,csrf=csrf)
        self.assertEqual(result[0],200,result[2])
        cookie=[v for k,v in result[3] if k=='Set-Cookie' and v.startswith(TEST[0]+'=')][0].split(';')[0]
        me=self.panel('me',cookie=cookie); self.assertEqual(me[0],200,me[2])
        return cookie,json.loads(me[2])['csrf_token']

    def test_login_me_logout_password_and_all_session_revoke(self):
        cookie,csrf=self.login(); cookie2,_=self.login()
        response=self.panel('me',cookie=cookie)
        csrf=json.loads(response[2])['csrf_token']
        self.assertEqual(json.loads(response[2])['id'],self.admin)
        result=self.panel('revoke-all','POST',{'password':self.password},cookie=cookie,csrf=csrf)
        self.assertEqual(result[0],200,result[2]); self.assertIn('Max-Age=0',result[1]['Set-Cookie'])
        for value in (cookie,cookie2): self.assertEqual(self.panel('me',cookie=value)[0],401)
        cookie,csrf=self.login(); changed=secrets.token_urlsafe(24)
        result=self.panel('password','POST',{'old_password':self.password,'new_password':changed},cookie=cookie,csrf=csrf)
        self.assertEqual(result[0],200,result[2]); self.assertEqual(self.panel('me',cookie=cookie)[0],401)
        cookie,csrf=self.login(password=changed)
        self.assertEqual(self.panel('logout','POST',{},cookie=cookie,csrf=csrf)[0],200)
        self.assertEqual(self.panel('me',cookie=cookie)[0],401)

    def test_challenge_bound_single_use_wrong_csrf_consumes_and_expires(self):
        cookie,csrf=self.challenge()
        data={'login':'admin','password':self.password}
        self.assertEqual(self.panel('login','POST',data,cookie=cookie,csrf=secrets.token_urlsafe(32))[0],403)
        self.assertEqual(self.panel('login','POST',data,cookie=cookie,csrf=csrf)[0],403)
        cookie,csrf=self.challenge(); self.now+=300
        self.assertEqual(self.panel('login','POST',data,cookie=cookie,csrf=csrf)[0],403)
        self.assertEqual(self.panel('login','POST',data,cookie=cookie,csrf=csrf)[0],403)

    def test_concurrent_challenge_replay_exactly_one_session(self):
        cookie,csrf=self.challenge()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda _:self.panel('login','POST',{'login':'admin','password':self.password},cookie=cookie,csrf=csrf)[0],range(2)))
        self.assertEqual(sorted(results),[200,403])

    def test_me_csrf_rotation_missing_wrong_and_cross_tab_rejected(self):
        cookie,csrf=self.login()
        other=json.loads(self.panel('me',cookie=cookie)[2])['csrf_token']
        for value in (None,csrf,secrets.token_urlsafe(32)):
            self.assertEqual(self.panel('logout','POST',{},cookie=cookie,csrf=value)[0],403)
        self.assertEqual(self.panel('logout','POST',{},cookie=cookie,csrf=other)[0],200)

    def test_strict_host_origin_json_fetch_metadata_and_duplicate_fields(self):
        cookie,csrf=self.login()
        for headers,origin in [([],False),([('Origin','null')],False),([('Origin','https://evil.invalid')],False),([('Origin',self.origin)],True),([('Host','evil.invalid')],True),([('Sec-Fetch-Site','cross-site')],True)]:
            self.assertIn(self.panel('logout','POST',{},headers=headers,cookie=cookie,csrf=csrf,origin=origin)[0],(400,403))
        for raw,headers,status in [(b'{}',[('Content-Type','text/plain')],415),(b'{"password":"x","password":"y"}',[('Content-Type','application/json')],400),(b'{"password":NaN}',[('Content-Type','application/json')],400)]:
            self.assertEqual(self.panel('revoke-all','POST',raw=raw,headers=headers,cookie=cookie,csrf=csrf)[0],status)
        self.assertEqual(self.panel('logout','POST',{},headers=[('Transfer-Encoding','chunked')],cookie=cookie,csrf=csrf)[0],400)

    def test_all_namespace_methods_deny_panel_before_any_business(self):
        cookie,_=self.login()
        # Exercise the literal Handler branch inventory, not just one admin GET.
        text=Path(__file__).with_name('server.py').read_text()
        paths=set(re.findall(r"['\"](/(?:api|agent|share)/[^'\"]+)['\"]",text))
        paths.update(('/api/users','/api/users/1','/agent/missing','/share/missing','/health','/manage/users','/unknown'))
        with mock.patch.object(self.registry,'list',side_effect=AssertionError('business called')),mock.patch.object(self.registry,'save',side_effect=AssertionError('business called')),mock.patch.object(self.registry,'heartbeat',side_effect=AssertionError('agent called')):
            for path in sorted(paths):
                for method in ('GET','POST','PUT','DELETE','PATCH','HEAD','OPTIONS','TRACE','CONNECT','UNKNOWN'):
                    self.assertEqual(self.request(path,method,cookie=cookie)[0],403,(path,method))
        self.assertEqual(self.registry.list('users'),[])

    def test_malformed_duplicate_ambiguous_credentials_all_methods(self):
        cookie,_=self.login(); bearer='Bearer '+self.server.token
        headersets=[([('Authorization',bearer)],cookie),([('Authorization',bearer),('Authorization',bearer)],None),([('Authorization','Basic xyz')],None),([('Authorization',bearer+', '+bearer)],None),([],cookie+'; '+cookie),([('Cookie',cookie)],cookie),([],TEST[0]+'="quoted"')]
        for method in ('GET','POST','PUT','DELETE','PATCH','HEAD','OPTIONS','UNKNOWN'):
            for headers,value in headersets:
                self.assertEqual(self.request('/api/users',method,headers=headers,cookie=value)[0],400,(method,headers))
        self.assertEqual(self.panel('me',headers=[('Authorization',bearer)])[0],403)

    def test_legacy_sole_bearer_no_origin_and_fixed_origin_compatibility(self):
        headers=[('Authorization','Bearer '+self.server.token)]
        for origin in (False,True):
            self.assertEqual(self.request('/api/users',headers=headers,origin=origin)[0],200)
        self.assertEqual(self.request('/api/users','POST',{'name':'legacy'},headers=headers,origin=False)[0],201)
        self.assertEqual(self.request('/api/users',headers=headers+[('Origin','http://evil.invalid')],origin=False)[0],403)
        self.assertEqual(self.request('/api/users',headers=[('Authorization','Bearer invalid')],origin=False)[0],401)
        self.assertEqual(self.request('/api/users',headers=[('Authorization','Bearer ')],origin=False)[0],401)
        self.assertEqual(self.request('/api/users',headers=[('Authorization','Bearer ')],cookie=TEST[0]+'='+secrets.token_urlsafe(32),origin=False)[0],400)

    def test_user_forced_change_and_only_bound_resource_summaries(self):
        root=self.registry.save('users',{'name':'bound'})['id']
        self.registry.save('users',{'name':'unrelated'})
        secret=secrets.token_urlsafe(24)
        ident=self.kernel.create_account(self.admin_token,'ordinary',secret,actor_password=self.password,root_user_id=root)
        cookie,csrf=self.login('ordinary',secret)
        self.assertTrue(json.loads(self.panel('me',cookie=cookie)[2])['must_change_password'])
        csrf=json.loads(self.panel('me',cookie=cookie)[2])['csrf_token']
        self.assertEqual(self.panel('resources',cookie=cookie)[0],403)
        changed=secrets.token_urlsafe(24)
        self.assertEqual(self.panel('password','POST',{'old_password':secret,'new_password':changed},cookie=cookie,csrf=csrf)[0],200)
        cookie,csrf=self.login('ordinary',changed)
        result=self.panel('resources',cookie=cookie)
        resources=json.loads(result[2])['resources']; self.assertEqual([r['id'] for r in resources],[root])
        self.assertEqual(set(resources[0]),{'id','name','plan'})
        self.assertEqual(self.request('/api/users',cookie=cookie)[0],403)
        self.kernel.update_account(self.admin_token,ident,actor_password=self.password,bind=True,root_user_id=None)
        self.assertEqual(self.panel('resources',cookie=cookie)[0],401)

    def test_resource_get_does_not_mutate_business_and_fk_restrict_remains(self):
        root=self.registry.save('users',{'name':'root'})['id']
        child=self.registry.save('users',{'name':'child'})['id']
        plan=self.registry.save_plan({'name':'child-plan','bandwidth':{'upload_bps':None,'download_bps':None}})['id']
        with self.registry.connect() as db:
            db.execute('INSERT INTO plan_accounts VALUES(?,?,?)',(child,root,plan))
            db.execute('INSERT INTO user_plans VALUES(?,?)',(child,plan))
            db.execute('INSERT INTO plan_lifecycle VALUES(?,?,?,?)',(child,1,self.now-1,1))
        secret=secrets.token_urlsafe(24)
        self.kernel.create_account(self.admin_token,'reader',secret,actor_password=self.password,root_user_id=root)
        token=self.kernel.login('reader',secret,'offline'); changed=secrets.token_urlsafe(24)
        self.kernel.change_password(token,secret,changed); cookie,_=self.login('reader',changed)
        def snapshot():
            with self.registry.connect() as db:
                tables=[r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'panel_%' AND name!='sqlite_sequence'")]
                return {t:[tuple(r) for r in db.execute('SELECT * FROM '+t)] for t in tables}
        before=snapshot(); response=self.panel('resources',cookie=cookie); self.assertEqual(response[0],200)
        resources=json.loads(response[2])['resources']
        self.assertEqual([r['id'] for r in resources],[root,child])
        self.assertEqual(resources[1]['plan']['status'],'expired')
        self.assertEqual(before,snapshot())
        with self.assertRaises(Exception): self.registry.delete('users',root)
        self.assertEqual(before,snapshot())

    def test_password_cas_rechecks_csrf_after_real_kdf(self):
        cookie,csrf=self.login(); changed=secrets.token_urlsafe(24)
        token=cookie.split('=')[1]
        def rotate(point):
            if point=='password.verified': self.kernel.session_info(token)
        self.kernel.fault=rotate
        try:
            result=self.panel('password','POST',{'old_password':self.password,'new_password':changed},cookie=cookie,csrf=csrf)
            self.assertEqual(result[0],403,result[2])
        finally: self.kernel.fault=lambda _:None
        self.assertEqual(self.panel('me',cookie=cookie)[0],200)
        self.assertIsInstance(self.kernel.login('admin',self.password,'after-cas'),str)

    def test_challenge_and_session_csrf_only_hashes_persist_replay_after_reopen_denied(self):
        challenge,csrf=self.challenge(); token=challenge.split('=')[1]
        with self.registry.connect() as db:
            row=dict(db.execute('SELECT * FROM panel_login_challenges').fetchone())
        self.assertNotIn(csrf,json.dumps(row)); self.assertNotIn(token,json.dumps(row))
        result=self.panel('login','POST',{'login':'admin','password':self.password},cookie=challenge,csrf=csrf)
        self.assertEqual(result[0],200)
        reopened=PanelIdentity(self.registry.path,self.root/'control',clock=lambda:self.now)
        with self.assertRaises(Error) as caught: reopened.consume_challenge(token,csrf)
        self.assertEqual(caught.exception.status,403)
        cookie,session_csrf=self.login()
        with self.registry.connect() as db:
            rows=[dict(r) for r in db.execute('SELECT * FROM panel_sessions')]
        self.assertNotIn(session_csrf,json.dumps(rows)); self.assertNotIn(cookie.split('=')[1],json.dumps(rows))

    def test_secret_absence_headers_and_wrong_cookie_mode(self):
        cookie,csrf=self.challenge()
        result=self.panel('login','POST',{'login':'admin','password':self.password},cookie=cookie,csrf=csrf)
        self.assertEqual(result[0],200)
        for secret in (self.password,self.server.token,self.admin_token,cookie.split('=')[1],csrf): self.assertNotIn(secret.encode(),result[2])
        for key,value in result[3]:
            if key=='Set-Cookie':
                self.assertIn('HttpOnly',value); self.assertIn('SameSite=Strict',value); self.assertIn('Path=/',value)
                self.assertNotIn('Domain=',value); self.assertNotIn('Secure',value)
        self.assertEqual(result[1]['Cache-Control'],'no-store')
        self.assertEqual(self.panel('me',cookie=PRODUCTION[0]+'='+secrets.token_urlsafe(32))[0],403)

    def test_no_http_production_fallback_and_no_implicit_runtime(self):
        self.server.panel_config=PanelConfig('https://trusted.invalid')
        # Host mismatch is refused before transport; a matching Host still cannot bypass TLS.
        conn=http.client.HTTPConnection('127.0.0.1',self.server.server_port)
        conn.request('GET','/panel/api/v1/login-challenge',headers={'Host':'trusted.invalid'})
        response=conn.getresponse(); self.assertEqual(response.status,403); response.read(); conn.close()
        self.server.panel_config=PanelConfig(self.origin,loopback_test=True)
        runtime=self.server.panel_runtime; del self.server.panel_runtime
        try: self.assertEqual(self.panel('login-challenge')[0],503)
        finally: self.server.panel_runtime=runtime

    def test_route_allowlist_methods_and_no_admin_or_recovery_http(self):
        cookie,_=self.login()
        for name in ('accounts','restore','offline-reset','resources/1','me?token=forbidden'):
            self.assertEqual(self.panel(name,cookie=cookie)[0],404)
        for method in ('POST','PUT','PATCH','DELETE','HEAD','OPTIONS','TRACE'):
            self.assertEqual(self.panel('me',method,cookie=cookie)[0],405)

    def test_config_rejects_unsafe_origins(self):
        for origin,test in [('http://public.invalid',False),('http://public.invalid',True),('https://host/path',False),('https://user@host',False),('https://host:bad',False),('https://host/',False)]:
            with self.assertRaises(Error): PanelConfig(origin,loopback_test=test)

    def test_actual_tls_production_secure_cookie_and_no_cors(self):
        cert=self.root/'cert.pem'; key=self.root/'key.pem'
        result=subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
            '-subj','/CN=localhost','-keyout',str(key),'-out',str(cert)],capture_output=True,timeout=15)
        self.assertEqual(result.returncode,0)
        self.server.shutdown(); self.thread.join()
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); context.load_cert_chain(cert,key)
        self.server.socket=context.wrap_socket(self.server.socket,server_side=True)
        origin=f'https://127.0.0.1:{self.server.server_port}'
        self.server.panel_config=PanelConfig(origin)
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True); self.thread.start()
        conn=http.client.HTTPSConnection('127.0.0.1',self.server.server_port,context=ssl._create_unverified_context())
        conn.request('GET','/panel/api/v1/login-challenge')
        response=conn.getresponse(); self.assertEqual(response.status,200)
        cookie=response.getheader('Set-Cookie'); csrf=json.loads(response.read())['csrf_token']
        self.assertIn(PRODUCTION[1]+'=',cookie); self.assertIn('; Secure',cookie)
        self.assertIsNone(response.getheader('Access-Control-Allow-Origin')); conn.close()
        conn=http.client.HTTPSConnection('127.0.0.1',self.server.server_port,context=ssl._create_unverified_context())
        body=json.dumps({'login':'admin','password':self.password})
        conn.request('POST','/panel/api/v1/login',body,{'Origin':origin,'Content-Type':'application/json',
            'Cookie':cookie.split(';')[0],'X-CSRF-Token':csrf})
        response=conn.getresponse(); self.assertEqual(response.status,200,response.read())
        cookies=[v for k,v in response.getheaders() if k=='Set-Cookie']
        self.assertTrue(any(v.startswith(PRODUCTION[0]+'=') and '; Secure' in v for v in cookies))
        response.read(); conn.close()
