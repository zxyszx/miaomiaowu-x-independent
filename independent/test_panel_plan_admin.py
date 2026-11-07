"""Real Handler plan sessions, final caller-TX checks and durable failures."""
import concurrent.futures
import http.client
import json
from pathlib import Path
import secrets
import select
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from core import Error,Registry
from panel_identity import PanelIdentity
from panel_plan_admin import route
from plan_nodes import PlanNodes
from plan_node_connections import PlanNodeConnections
from plan_node_bandwidth import PlanNodeBandwidth
import test_panel_admin as admin_fixture
import test_xray


def plan_data(name='plan',rate=32768):
    return dict(name=name,bandwidth=dict(upload_bps=rate,download_bps=None),
                connections=dict(max_connections=3,max_ips=2))


class PlanHTTPTests(unittest.TestCase):
    tearDown=admin_fixture.AdminTests.tearDown
    request=admin_fixture.AdminTests.request
    panel=admin_fixture.AdminTests.panel
    challenge=admin_fixture.AdminTests.challenge
    login=admin_fixture.AdminTests.login
    call=admin_fixture.AdminTests.call
    snapshot=admin_fixture.AdminTests.snapshot
    bound=admin_fixture.AdminTests.bound

    def setUp(self):
        admin_fixture.AdminTests.setUp(self)
        self.server.panel_admin_enabled=True
        self.cookie,self.csrf=self.login()

    def api(self,path,method='GET',data=None,version=None,headers=()):
        values=list(headers)+([('If-Match',version)] if version is not None else [])
        result=self.call(path,method,data,cookie=self.cookie,csrf=self.csrf,headers=values)
        return result[0],json.loads(result[2])

    def seed(self):
        host=self.registry.save('servers',{'name':'host'})['id']; nodes=[]
        for index in range(2):
            nid=self.registry.save('nodes',{'name':'node-'+str(index),'server_id':host})['id']
            self.registry.configure_node(nid,{**test_xray.VLESS,'listen_port':25400+index})
            nodes.append(nid)
        pid=self.registry.save_plan(plan_data())['id']
        uid=self.registry.save('users',{'name':'executing'})['id']
        self.registry.assign_plan(uid,{'plan_id':pid})
        for nid in nodes: self.registry.authorize_node_user(nid,uid)
        return host,nodes,pid,uid

    def test_plan_roundtrip_pl1_cas_exact_headers_and_legacy_response(self):
        status,row=self.api('/api/plans','POST',plan_data()); self.assertEqual(status,201)
        path='/api/plans/'+str(row['id']); old=row['version']
        self.assertEqual(self.api('/api/plans')[1],[row])
        updated=plan_data('changed',1)
        status,new=self.api(path,'PUT',updated,old); self.assertEqual(status,200)
        self.assertEqual(new['bandwidth']['upload_bps'],1)
        self.assertEqual(self.api(path,'PUT',updated,old)[0],412)
        self.assertEqual(self.api(path,'DELETE',version=old)[0],412)
        for version in ('*','W/'+new['version'],new['version'].replace('pl1','bw1')):
            self.assertEqual(self.api(path,'PUT',updated,version)[0],400)
        self.assertEqual(self.api('/api/plans','POST',plan_data(),new['version'])[0],400)
        self.assertEqual(self.api(path,'PUT',updated,new['version'],[('If-Match',new['version'])])[0],400)
        headers=[('Authorization','Bearer '+self.server.token)]
        legacy=self.request('/api/plans',headers=headers,origin=False)
        self.assertEqual(legacy[0],200); self.assertEqual(json.loads(legacy[2]),[new])
        self.assertEqual(self.api(path,'DELETE',version=new['version'])[0],200)
        self.assertEqual(self.api(path,'DELETE',version=new['version'])[0],404)

    def test_scope_group_union_and_policy_inherit_null_exact_bytes(self):
        from node_groups import NodeGroups
        host,nodes,pid,uid=self.seed(); scope=f'/api/plans/{pid}/nodes'
        group=NodeGroups(self.registry).save({'name':'existing-group','node_ids':[nodes[1]]})
        status,row=self.api(scope,'PUT',{'node_ids':[nodes[0]],'group_ids':[group['id']]})
        self.assertEqual(status,200); self.assertEqual(row['node_ids'],nodes)
        self.assertEqual(row['direct_node_ids'],[nodes[0]])
        connections=f'{scope}/{nodes[0]}/connections'; bandwidth=f'{scope}/{nodes[0]}/bandwidth'
        status,row=self.api(connections); self.assertEqual(status,200); self.assertIsNone(row['override'])
        status,row=self.api(connections,'PUT',{'max_connections':None})
        self.assertEqual(status,200); self.assertEqual(row['override'],{'max_connections':None})
        self.assertEqual(self.api(connections,'DELETE')[1]['override'],None)
        status,initial=self.api(bandwidth); self.assertEqual(status,200)
        rate={'upload_bps':1,'download_bps':1073741824}
        status,limited=self.api(bandwidth,'PUT',rate,initial['version'])
        self.assertEqual(status,200); self.assertEqual(limited['override'],rate)
        self.assertEqual(self.api(bandwidth,'PUT',rate,initial['version'])[0],412)
        null={'upload_bps':None,'download_bps':None}
        status,unlimited=self.api(bandwidth,'PUT',null,limited['version'])
        self.assertEqual(status,200); self.assertEqual(unlimited['override'],null)
        status,inherited=self.api(bandwidth,'DELETE',version=unlimited['version'])
        self.assertEqual(status,200); self.assertIsNone(inherited['override'])
        self.assertEqual(inherited['effective'],plan_data()['bandwidth'])
        self.assertEqual(self.api(scope,'DELETE')[1]['restricted'],False)

    def test_original_scope_enforcement_revokes_only_outside_nodes(self):
        host,nodes,pid,uid=self.seed()
        with self.registry.connect() as db: original=db.execute('SELECT revision FROM server_configs WHERE server_id=?',(host,)).fetchone()[0]
        path=f'/api/plans/{pid}/nodes'
        self.assertEqual(self.api(path,'PUT',{'node_ids':[nodes[0]]})[0],200)
        with self.registry.connect() as db:
            self.assertEqual([r[0] for r in db.execute('SELECT node_id FROM node_users WHERE user_id=?',(uid,))],[nodes[0]])
            self.assertEqual(db.execute('SELECT revision FROM server_configs WHERE server_id=?',(host,)).fetchone()[0],original+1)
        for suffix,body in (('connections',{'max_connections':1}),('bandwidth',{'upload_bps':1,'download_bps':None})):
            self.assertEqual(self.api(f'{path}/{nodes[1]}/{suffix}','PUT',body)[0],403)
        with self.registry.connect() as db: self.assertEqual(db.execute('SELECT COUNT(*) FROM node_users WHERE user_id=?',(uid,)).fetchone()[0],1)

    def test_all_eight_entrypoints_share_final_identity_connection(self):
        host,nodes,pid,uid=self.seed(); scope=f'/api/plans/{pid}/nodes'
        with mock.patch.object(self.registry,'connect',side_effect=AssertionError('independent connection forbidden')):
            self.assertEqual(self.api('/api/plans')[0],200)
            self.assertEqual(self.api(f'/api/plans/{pid}','PUT',plan_data('changed'))[0],200)
            for method,body in (('GET',None),('PUT',{'node_ids':nodes}),('DELETE',None)):
                self.assertEqual(self.api(scope,method,body)[0],200)
            for suffix,body in (('connections',{'max_connections':2}),('bandwidth',{'upload_bps':2,'download_bps':None})):
                path=f'{scope}/{nodes[0]}/{suffix}'
                for method,data in (('GET',None),('PUT',body),('DELETE',None)):
                    self.assertEqual(self.api(path,method,data)[0],200)
            status,new=self.api('/api/plans','POST',plan_data('disposable')); self.assertEqual(status,201)
            self.assertEqual(self.api('/api/plans/'+str(new['id']),'DELETE',version=new['version'])[0],200)

    def test_plan_and_bandwidth_concurrent_cas_exactly_one_winner(self):
        host,nodes,pid,uid=self.seed()
        plan=self.api('/api/plans')[1][0]
        bandwidth=f'/api/plans/{pid}/nodes/{nodes[0]}/bandwidth'; current=self.api(bandwidth)[1]
        for path,version,bodies in [(f'/api/plans/{pid}',plan['version'],[plan_data('one'),plan_data('two')]),
            (bandwidth,current['version'],[{'upload_bps':1,'download_bps':None},{'upload_bps':2,'download_bps':None}])]:
            gate=threading.Barrier(2)
            def edit(body): gate.wait(); return self.api(path,'PUT',body,version)[0]
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                self.assertEqual(sorted(pool.map(edit,bodies)),[200,412])

    def test_delete_user_and_downgrade_dependencies_and_original_cascade(self):
        from plan_grace import GracePolicies
        host,nodes,pid,uid=self.seed(); path=f'/api/plans/{pid}'
        version=self.api('/api/plans')[1][0]['version']; before=self.snapshot()
        self.assertEqual(self.api(path,'DELETE',version=version)[0],409); self.assertEqual(before,self.snapshot())
        self.registry.assign_plan(uid,{'plan_id':None})
        source=self.registry.save_plan(plan_data('source'))['id']
        GracePolicies(self.registry).save(source,{'target_plan_id':pid,'days':1})
        before=self.snapshot(); self.assertEqual(self.api(path,'DELETE',version=version)[0],409)
        self.assertEqual(before,self.snapshot()); GracePolicies(self.registry).delete(source)
        self.api(path+'/nodes','PUT',{'node_ids':nodes})
        self.api(path+f'/nodes/{nodes[0]}/bandwidth','PUT',{'upload_bps':1,'download_bps':None})
        self.assertEqual(self.api(path,'DELETE',version=version)[0],200)
        with self.registry.connect() as db:
            for table in ('plan_node_scopes','plan_nodes','plan_node_bandwidth'):
                self.assertEqual(db.execute('SELECT COUNT(*) FROM '+table+' WHERE plan_id=?',(pid,)).fetchone()[0],0)

    def test_business_publication_error_rolls_back_identity_audit_and_expiry_maintenance(self):
        host,nodes,pid,uid=self.seed()
        with self.registry.connect() as db:
            db.execute('INSERT INTO temporary_bandwidth VALUES(?,?,?)',(uid,json.dumps({'upload_bps':1,'download_bps':None}),time.time()-1))
        before=self.snapshot(); anchor=self.kernel._read_anchor()
        with mock.patch.object(self.registry,'_bump',side_effect=Error('publication failed',409)):
            self.assertEqual(self.api(f'/api/plans/{pid}','PUT',plan_data('reject',2))[0],409)
        self.assertEqual(before,self.snapshot()); self.assertEqual(anchor,self.kernel._read_anchor())
        self.assertEqual(self.api(f'/api/plans/{pid}','PUT',plan_data('success',2))[0],200)
        with self.registry.connect() as db: self.assertEqual(db.execute('SELECT COUNT(*) FROM temporary_bandwidth').fetchone()[0],0)

    def test_admitted_gets_are_business_read_only_and_scope_checks_unchanged(self):
        host,nodes,pid,uid=self.seed()
        with self.registry.connect() as db:
            db.execute('INSERT INTO temporary_bandwidth VALUES(?,?,?)',(uid,json.dumps({'upload_bps':1,'download_bps':None}),time.time()-1))
        def business(): return {k:v for k,v in self.snapshot().items() if not k.startswith('panel_') and k!='sqlite_sequence'}
        before=business()
        for path in ('/api/plans',f'/api/plans/{pid}/nodes',f'/api/plans/{pid}/nodes/{nodes[0]}/connections',f'/api/plans/{pid}/nodes/{nodes[0]}/bandwidth'):
            self.assertEqual(self.api(path)[0],200)
        self.assertEqual(before,business())

    def test_plain_user_all_plan_routes_methods_zero_business_and_outside_scope_denied(self):
        uid,ident,token,_,_=self.bound()
        paths=['/api/plans','/api/plans/1','/api/plans/1/nodes','/api/plans/1/nodes/1/connections','/api/plans/1/nodes/1/bandwidth',
               '/api/node-groups','/api/users/1/plan-lifecycle','/api/users/1/plan-renewals','/api/users/1/accounts','/api/automatic-bandwidth','/api/speed-tests']
        with (mock.patch.object(self.registry,'plans',side_effect=AssertionError('ordinary business')),
              mock.patch.object(self.registry,'save_plan',side_effect=AssertionError('ordinary business')),
              mock.patch.object(PlanNodes,'get',side_effect=AssertionError('ordinary business'))):
            for path in paths:
                for method in ('GET','POST','PUT','DELETE','PATCH','HEAD','OPTIONS','TRACE','CONNECT','UNKNOWN'):
                    self.assertEqual(self.call(path,method,cookie='mmw_panel_test='+token,csrf=self.csrf)[0],403,(path,method))
        for path in [p for p in paths[5:] if p not in ('/api/automatic-bandwidth','/api/users/1/plan-lifecycle')]: self.assertEqual(self.api(path)[0],403)
        for path,method in [('/api/plans/1','GET'),('/api/plans/1/nodes/1','PUT'),('/api/plans/1/nodes','POST'),('/api/plans/1/nodes/1/connections','POST'),('/api/plans/1/nodes/1/bandwidth/extra','PUT'),('/api/plans/0','DELETE')]:
            self.assertIsNone(route(path,method)); self.assertEqual(self.api(path,method,{} if method in ('POST','PUT') else None)[0],403)

    def test_strict_write_json_csrf_dual_and_delete_empty_object(self):
        pid=self.registry.save_plan(plan_data())['id']; path=f'/api/plans/{pid}'
        for data in ({'extra':1},[],None):
            response=self.request(path,'DELETE',data,cookie=self.cookie,csrf=self.csrf)
            self.assertIn(response[0],(400,413,415))
        self.assertEqual(self.request('/api/plans','POST',plan_data(),cookie=self.cookie,origin=True)[0],403)
        self.assertEqual(self.request('/api/plans','POST',plan_data(),cookie=self.cookie,csrf=self.csrf,origin=False)[0],403)
        self.assertEqual(self.call('/api/plans',cookie=self.cookie,headers=[('Authorization','Bearer '+self.server.token)])[0],400)
        self.assertEqual(self.request('/api/plans','POST',cookie=self.cookie,csrf=self.csrf,raw=b'{"name":"a","name":"b"}',headers=[('Content-Type','application/json')])[0],400)
        self.assertEqual(self.api('/api/plans?extra=1')[0],403)

    def test_body_interval_csrf_rotation_logout_and_final_role_recheck(self):
        import panel_admin
        for change,expected in (('csrf',403),('logout',401)):
            if change=='logout': self.cookie,self.csrf=self.login()
            entered=threading.Event(); release=threading.Event(); original=panel_admin._body
            def pause(*args):
                result=original(*args); entered.set(); self.assertTrue(release.wait(10)); return result
            with mock.patch.object(panel_admin,'_body',pause),concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                pending=pool.submit(self.api,'/api/plans','POST',plan_data('denied'))
                self.assertTrue(entered.wait(5))
                if change=='csrf': self.kernel.session_info(self.cookie.split('=')[1])
                else: self.kernel.logout(self.cookie.split('=')[1])
                release.set(); self.assertEqual(pending.result(timeout=10)[0],expected)
        self.assertEqual(self.registry.plans(),[])

    def test_business_before_revoke_commits_and_revoke_before_business_zero_calls(self):
        entered=threading.Event(); release=threading.Event(); attempted=threading.Event()
        def pause(point):
            if point=='admin.authorized': entered.set(); self.assertTrue(release.wait(10))
        self.kernel.fault=pause
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            business=pool.submit(self.api,'/api/plans','POST',plan_data('ordered'))
            self.assertTrue(entered.wait(5))
            def revoke(): attempted.set(); self.kernel.logout(self.cookie.split('=')[1])
            revoked=pool.submit(revoke); self.assertTrue(attempted.wait(5)); self.assertFalse(revoked.done())
            release.set(); self.assertEqual(business.result(timeout=10)[0],201); revoked.result(timeout=10)
        self.kernel.fault=lambda _:None
        with mock.patch.object(self.registry,'save_plan',side_effect=AssertionError('revoked business')):
            self.assertEqual(self.api('/api/plans','POST',plan_data('no'))[0],401)
        self.assertEqual(len(self.registry.plans()),1)

    def test_response_loss_committed_delete_reopen_does_not_replay(self):
        from server import Handler
        pid=self.registry.save_plan(plan_data())['id']; path=f'/api/plans/{pid}'; original=Handler.reply
        def lost(handler,status,value):
            if handler.path==path and status==200: return None
            return original(handler,status,value)
        with mock.patch.object(Handler,'reply',lost):
            with self.assertRaises(http.client.RemoteDisconnected): self.api(path,'DELETE')
        self.assertEqual(self.api(path,'DELETE')[0],404)
        reopened=PanelIdentity(self.registry.path,self.root/'control',clock=lambda:self.now)
        self.assertEqual(reopened.authenticate(self.cookie.split('=')[1])['role'],'admin')
        with self.registry.connect() as db:
            audit=[dict(r) for r in db.execute("SELECT * FROM panel_auth_audit WHERE event='admin_delete_plans'")]
        self.assertEqual(len(audit),1)
        for secret in (self.cookie.split('=')[1],self.csrf,self.password,self.server.token): self.assertNotIn(secret,json.dumps(audit))


class PlanProcessTests(unittest.TestCase):
    def test_real_http_publication_audit_cascade_and_commit_boundaries(self):
        for point in ('publication','audit','cascade','admin.business_done','db.committed','clean.before_replace'):
            with self.subTest(point=point),tempfile.TemporaryDirectory() as name:
                root=Path(name); (root/'data').mkdir(); registry=Registry(root/'data'/'r.db')
                kernel=PanelIdentity(registry.path,root/'control',clock=lambda:2000000000.)
                password=secrets.token_urlsafe(24); kernel.initialize('admin',password)
                token=kernel.login('admin',password,'local'); csrf=kernel.session_info(token)['csrf_token']
                host=registry.save('servers',{'name':'host'})['id']
                node=registry.save('nodes',{'name':'node','server_id':host})['id']; registry.configure_node(node,test_xray.VLESS)
                pid=registry.save_plan(plan_data())['id']; user=registry.save('users',{'name':'user'})['id']
                registry.assign_plan(user,{'plan_id':pid}); registry.authorize_node_user(node,user)
                if point=='cascade':
                    PlanNodeBandwidth(registry).manage(pid,node,{'upload_bps':2,'download_bps':None})
                    registry.assign_plan(user,{'plan_id':None})
                with registry.connect() as db:
                    tables=[r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")]
                    before={t:[tuple(r) for r in db.execute('SELECT * FROM '+t)] for t in tables}
                code='''import sys,json,threading,http.client,os
from core import Registry
from panel_identity import PanelIdentity
from panel_runtime import PanelRuntime
from panel_authorization import PanelConfig
from server import PanelHTTPServer,Handler
data=json.loads(sys.stdin.read()); registry=Registry(sys.argv[1])
k=PanelIdentity(sys.argv[1],sys.argv[2],clock=lambda:2000000000.)
r=PanelRuntime(k)
triggers={
 'publication':"CREATE TRIGGER fail_publish BEFORE UPDATE ON server_configs BEGIN SELECT RAISE(ABORT,'publish failed'); END",
 'audit':"CREATE TRIGGER fail_audit BEFORE INSERT ON panel_auth_audit BEGIN SELECT RAISE(ABORT,'audit failed'); END",
 'cascade':"CREATE TRIGGER fail_cascade BEFORE DELETE ON plan_node_bandwidth BEGIN SELECT RAISE(ABORT,'cascade failed'); END"}
if data['point'] in triggers:
    with registry.connect() as db: db.execute(triggers[data['point']])
def fault(point):
    if point==data['point']:
        if point in ('admin.business_done','db.committed'): os._exit(79)
        raise OSError('durability failed')
k.fault=fault
server=PanelHTTPServer(('127.0.0.1',0),Handler); origin='http://127.0.0.1:'+str(server.server_port)
server.panel_config=PanelConfig(origin,loopback_test=True); server.panel_runtime=r
server.panel_admin_enabled=True; server.registry=registry
threading.Thread(target=server.serve_forever,daemon=True).start()
conn=http.client.HTTPConnection('127.0.0.1',server.server_port)
conn.request(data['method'],data['path'],json.dumps(data['body']),{'Content-Type':'application/json',
 'Origin':origin,'Cookie':'mmw_panel_test='+data['token'],'X-CSRF-Token':data['csrf']})
response=conn.getresponse(); print('unexpected response',response.status,flush=True)
'''
                path=f'/api/plans/{pid}' if point=='cascade' else f'/api/plans/{pid}/nodes/{node}/bandwidth'
                data=dict(point=point,method='DELETE' if point=='cascade' else 'PUT',path=path,
                          body={} if point=='cascade' else {'upload_bps':1,'download_bps':None},token=token,csrf=csrf)
                result=subprocess.run([sys.executable,'-c',code,str(registry.path),str(root/'control')],input=json.dumps(data),
                    text=True,capture_output=True,cwd=Path(__file__).parent,timeout=10)
                self.assertEqual(result.returncode,79 if point in ('admin.business_done','db.committed') else 78,result.stderr)
                self.assertEqual(result.stdout,''); self.assertTrue((root/'control'/'running.json').exists())
                committed=point in ('db.committed','clean.before_replace')
                with registry.connect() as db:
                    after={t:[tuple(r) for r in db.execute('SELECT * FROM '+t)] for t in tables}
                    audits=db.execute("SELECT COUNT(*) FROM panel_auth_audit WHERE event='admin_put_plan_node_bandwidth'").fetchone()[0]
                    override=db.execute('SELECT payload FROM plan_node_bandwidth WHERE plan_id=? AND node_id=?',(pid,node)).fetchone()
                if not committed: self.assertEqual(before,after)
                else:
                    self.assertEqual(audits,1); self.assertEqual(json.loads(override[0])['upload_bps'],1)
                    self.assertEqual(kernel._read_anchor()['state'],'dirty')
                check='''import sys
from core import Error
from panel_identity import PanelIdentity
from panel_runtime import PanelRuntime
try: PanelRuntime(PanelIdentity(sys.argv[1],sys.argv[2],clock=lambda:2000000000.))
except Error as e: sys.exit(72 if e.status==503 else 73)
sys.exit(74)
'''
                reopened=subprocess.run([sys.executable,'-c',check,str(registry.path),str(root/'control')],capture_output=True,cwd=Path(__file__).parent,timeout=10)
                self.assertEqual(reopened.returncode,72)
                for secret in (password,token,csrf): self.assertNotIn(secret,result.stdout+result.stderr)

    def test_real_main_plan_cookie_and_policy_routes_and_legacy_bearer(self):
        with tempfile.TemporaryDirectory() as name:
            root=Path(name); (root/'data').mkdir(); registry=Registry(root/'data'/'registry.db')
            kernel=PanelIdentity(registry.path,root/'control'); password=secrets.token_urlsafe(24)
            kernel.initialize('admin',password); token=kernel.login('admin',password,'local')
            csrf=kernel.session_info(token)['csrf_token']
            host=registry.save('servers',{'name':'host'})['id']; node=registry.save('nodes',{'name':'node','server_id':host})['id']
            registry.configure_node(node,test_xray.VLESS)
            with socket.socket() as probe: probe.bind(('127.0.0.1',0)); port=probe.getsockname()[1]
            origin=f'http://127.0.0.1:{port}'
            command=[sys.executable,'server.py','--data',str(root/'data'),'--port',str(port),'--panel-control',str(root/'control'),
                     '--panel-origin',origin,'--panel-loopback-test']
            child=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,cwd=Path(__file__).parent)
            try:
                ready,_,_=select.select([child.stdout],[],[],10); self.assertTrue(ready)
                self.assertIn('Independent API:',child.stdout.readline())
                def call(path,method='GET',body=None,legacy=False):
                    conn=http.client.HTTPConnection('127.0.0.1',port,timeout=5)
                    headers={'Authorization':'Bearer '+(root/'data'/'admin-token').read_text().strip()} if legacy else {
                        'Origin':origin,'Cookie':'mmw_panel_test='+token,'X-CSRF-Token':csrf,'Content-Type':'application/json'}
                    conn.request(method,path,json.dumps(body) if body is not None else None,headers)
                    response=conn.getresponse(); status=response.status; value=json.loads(response.read()); conn.close(); return status,value
                status,row=call('/api/plans','POST',plan_data('main')); self.assertEqual(status,201)
                path=f'/api/plans/{row["id"]}/nodes'
                self.assertEqual(call(path,'PUT',{'node_ids':[node]})[0],200)
                self.assertEqual(call(path+f'/{node}/connections','PUT',{'max_connections':None})[0],200)
                status,value=call(path+f'/{node}/bandwidth'); self.assertEqual(status,200)
                self.assertTrue(value['version'].startswith('"bw1-'))
                self.assertEqual(call('/api/plans',legacy=True)[1],[row])
                child.terminate(); out,err=child.communicate(timeout=10); self.assertEqual(child.returncode,0,err)
                self.assertFalse((root/'control'/'running.json').exists())
                self.assertEqual(PanelIdentity(registry.path,root/'control').authenticate(token)['role'],'admin')
                for secret in (password,token,csrf): self.assertNotIn(secret,out+err)
            finally:
                if child.poll() is None: child.kill(); child.communicate()
