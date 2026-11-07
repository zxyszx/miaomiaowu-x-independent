import asyncio
import concurrent.futures
import json
from pathlib import Path
import tempfile
import os
import subprocess
import sys
import time
import unittest
from unittest import mock
import urllib.error
import urllib.request
import uuid

from core import Error,Registry
from sharing import Shares
from node_config import validate_snapshot
import test_core
from test_xray import VLESS,BINARY
from test_runtime import free_port
from vless_runtime import VlessRuntime


def settings(**changes):
    return dict(name='guest',max_nodes=2,max_users=2,port_start=20000,port_end=60000,
                expires_at=time.time()+3600,**changes)


class ShareTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.registry=Registry(Path(self.temp.name)/'db')
        self.shares=Shares(self.registry); self.sid=self.registry.save('servers',{'name':'owner'})['id']
        self.agent=self.registry.provision_agent(self.sid)['token']

    def tearDown(self): self.temp.cleanup()
    def grant(self,**changes):
        data=settings(); data.update(changes)
        invite=self.shares.create(self.sid,data); claim=self.shares.claim(invite['invite_token'])
        return invite,claim['access_token']
    def node(self,token,port=24500):
        return self.shares.guest(token,'create',data={'name':'guest-node','config':{**VLESS,'listen_port':port}})
    def denied(self,status,operation):
        with self.assertRaises(Error) as result: operation()
        self.assertEqual(result.exception.status,status)

    def test_single_use_invitation_hashes_rotation_and_persistence(self):
        invite,token=self.grant(); self.denied(401,lambda:self.shares.claim(invite['invite_token']))
        with self.registry.connect() as db:
            text=json.dumps([dict(x) for x in db.execute('SELECT * FROM server_shares')])
        self.assertNotIn(token,text); self.assertNotIn(invite['invite_token'],text)
        self.assertNotIn('hash',json.dumps(self.shares.owner(server_id=self.sid)))
        self.assertEqual(Shares(Registry(self.registry.path)).guest(token,'status')['status'],'active')
        rotated=self.shares.owner(ident=invite['id'],action='rotate')
        self.denied(401,lambda:self.shares.guest(token,'list'))
        new=self.shares.claim(rotated['invite_token'])['access_token']; self.node(new)
        self.assertEqual(self.shares.guest(new,'status')['nodes_used'],1)

    def test_invitation_concurrent_claim_only_one_recipient(self):
        invite=self.shares.create(self.sid,settings())
        def attempt(_):
            try: return self.shares.claim(invite['invite_token'])['access_token']
            except Error as e: return e.status
        with concurrent.futures.ThreadPoolExecutor(4) as pool: outcomes=list(pool.map(attempt,range(4)))
        self.assertEqual(outcomes.count(401),3)

    def test_scopes_port_collision_quota_and_atomic_failures(self):
        first,a=self.grant(max_nodes=1,max_users=1); second,b=self.grant()
        node=self.node(a); before=self.registry.configuration(self.agent)
        self.denied(409,lambda:self.node(a,24501))
        self.denied(409,lambda:self.node(b))
        self.denied(403,lambda:self.node(b,1000))
        self.denied(404,lambda:self.shares.guest(b,'delete',node['id']))
        self.denied(404,lambda:self.shares.guest(b,'authorize',node['id'],reference='x'))
        self.assertEqual(before,self.registry.configuration(self.agent))
        self.assertEqual(self.shares.guest(b,'list'),[])
        auth=self.shares.guest(a,'authorize',node['id'],reference='alice')
        self.assertEqual(self.shares.guest(a,'authorize',node['id'],reference='alice'),auth)
        self.denied(409,lambda:self.shares.guest(a,'authorize',node['id'],reference='bob'))
        snapshot=self.registry.configuration(self.agent); validate_snapshot(snapshot)
        client=snapshot['nodes'][0]['clients'][0]
        self.assertEqual(client['uuid'],auth['uuid']); self.assertEqual(client['valid_until'],first['expires_at'])
        self.assertNotIn('user_id',self.shares.guest(a,'list')[0]['clients'][0])
        self.shares.guest(a,'revoke',node['id'],reference='alice')
        self.assertEqual(self.shares.guest(a,'status')['users_used'],0)
        self.shares.guest(a,'authorize',node['id'],reference='bob')
        self.shares.guest(a,'delete',node['id'])
        self.assertEqual(self.shares.guest(a,'status')['users_used'],0)

    def test_owner_global_resource_limits_are_not_bypassed(self):
        _,token=self.grant(); self.registry.set_quota('nodes',0)
        self.denied(409,lambda:self.node(token))
        self.registry.set_quota('nodes',None); node=self.node(token)
        self.registry.set_quota('users',0)
        self.denied(409,lambda:self.shares.guest(token,'authorize',node['id'],reference='alice'))
        self.assertEqual(self.shares.guest(token,'status')['users_used'],0)

    def test_revoke_and_expire_delete_only_grant_resources_bump_snapshot(self):
        first,a=self.grant(); _,b=self.grant()
        owner=self.registry.save('nodes',{'name':'owner-node','server_id':self.sid})['id']
        self.registry.configure_node(owner,{**VLESS,'listen_port':24501})
        node_a=self.node(a); node_b=self.node(b,24502)
        self.shares.guest(a,'authorize',node_a['id'],reference='alice')
        self.shares.guest(b,'authorize',node_b['id'],reference='alice')
        before=self.registry.configuration(self.agent)
        result=self.shares.owner(ident=first['id'],action='revoke')
        self.assertEqual(result['status'],'revoked'); self.denied(401,lambda:self.shares.guest(a,'list'))
        after=self.registry.configuration(self.agent)
        self.assertGreater(after['revision'],before['revision']); self.assertEqual(len(after['nodes']),2)
        with mock.patch('sharing.time.time',return_value=first['expires_at']+1):
            expired=self.registry.configuration(self.agent)
            self.denied(401,lambda:self.shares.guest(b,'list'))
        self.assertEqual([n['id'] for n in expired['nodes']],[owner]); validate_snapshot(expired)
        self.assertEqual(self.registry.list('users'),[])

    def test_invalid_inputs_and_owner_migration_do_not_break_scope(self):
        for change in ({'max_nodes':True},{'max_users':0},{'port_start':60001},{'expires_at':float('nan')},{'expires_at':time.time()},{'unknown':1}):
            data=settings(); data.update(change)
            self.denied(400,lambda:self.shares.create(self.sid,data))
        _,token=self.grant(); node=self.node(token)
        other=self.registry.save('servers',{'name':'other'})['id']
        self.denied(409,lambda:self.registry.save('nodes',{'name':'moved','server_id':other},node['id']))
        before=self.registry.configuration(self.agent)
        self.shares.guest(token,'update',node['id'],{'name':node['name'],'config':node['config']})
        self.assertEqual(before,self.registry.configuration(self.agent))
        for ref in ('../owner','x'*65,''):
            self.denied(400,lambda:self.shares.guest(token,'authorize',node['id'],reference=ref))

    def test_concurrent_node_creates_cannot_exceed_grant_quota(self):
        _,token=self.grant(max_nodes=1)
        def create(index):
            try: return self.node(token,24500+index)['id']
            except Error as error: return str(error.status)
        with concurrent.futures.ThreadPoolExecutor(4) as pool: results=list(pool.map(create,range(4)))
        self.assertEqual(results.count('409'),3); self.assertEqual(self.shares.guest(token,'status')['nodes_used'],1)

    def test_expired_invitation_is_cleaned_and_cannot_claim(self):
        invite=self.shares.create(self.sid,settings())
        with mock.patch('sharing.time.time',return_value=invite['expires_at']+1):
            self.denied(401,lambda:self.shares.claim(invite['invite_token']))
        self.assertEqual(self.shares.owner(ident=invite['id'])['status'],'expired')

    def test_share_usage_period_reset_preserves_history_and_scopes_default_window(self):
        invite,token=self.grant(); node=self.node(token); auth=self.shares.guest(token,'authorize',node['id'],reference='alice')
        uid=self.registry.list('users')[0]['id']; now=time.time()
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('INSERT INTO usage_deltas(server_id,session_id,sequence,node_id,user_id,credential,cycle_id,received_at,upload_bytes,download_bytes) VALUES(?,?,?,?,?,?,?,?,?,?)',(self.sid,'period-a',1,node['id'],uid,auth['uuid'],None,now,4,3))
        first=self.shares.usage_for_owner(invite['id']); self.assertEqual((first['upload_bytes'],first['download_bytes']),(4,3))
        period=self.shares.reset_usage(invite['id'],'monthly')
        self.assertEqual(period['reason'],'monthly'); self.assertEqual(period['status'],'active')
        second=self.shares.usage_for_owner(invite['id']); self.assertEqual((second['upload_bytes'],second['download_bytes']),(0,0))
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('INSERT INTO usage_deltas(server_id,session_id,sequence,node_id,user_id,credential,cycle_id,received_at,upload_bytes,download_bytes) VALUES(?,?,?,?,?,?,?,?,?,?)',(self.sid,'period-b',1,node['id'],uid,auth['uuid'],None,time.time(),2,1))
            self.assertEqual(db.execute("SELECT COUNT(*) FROM share_usage_periods WHERE share_id=?",(invite['id'],)).fetchone()[0],2)
        latest=self.shares.usage_for_owner(invite['id']); self.assertEqual((latest['upload_bytes'],latest['download_bytes']),(2,1))
        explicit=self.shares.usage_for_owner(invite['id'],first['start'],period['starts_at'])
        self.assertEqual((explicit['upload_bytes'],explicit['download_bytes']),(4,3))

    def test_owner_protocol_change_rejects_guest_authorization_without_orphan_user(self):
        _,token=self.grant(); node=self.node(token)
        self.registry.configure_node(node['id'],dict(listen_host='127.0.0.1',listen_port=24500,
                                                     target_host='example.com',target_port=443,enabled=True))
        self.denied(409,lambda:self.shares.guest(token,'authorize',node['id'],reference='alice'))
        self.assertEqual(self.registry.list('users'),[])
        self.denied(409,lambda:self.shares.guest(token,'create',data={'name':'tcp','config':dict(listen_host='127.0.0.1',listen_port=24501,target_host='example.com',target_port=443,enabled=True)}))

    def test_revoked_shared_user_bumps_other_owner_authorized_hosts(self):
        invite,token=self.grant(); node=self.node(token)
        self.shares.guest(token,'authorize',node['id'],reference='alice')
        uid=self.registry.list('users')[0]['id']
        sid=self.registry.save('servers',{'name':'extra'})['id']; agent=self.registry.provision_agent(sid)['token']
        nid=self.registry.save('nodes',{'name':'extra','server_id':sid})['id']
        self.registry.configure_node(nid,VLESS); self.registry.authorize_node_user(nid,uid)
        before=self.registry.configuration(agent)
        self.shares.owner(ident=invite['id'],action='revoke'); after=self.registry.configuration(agent)
        self.assertGreater(after['revision'],before['revision']); self.assertEqual(after['nodes'][0]['clients'],[])


class ApiTests(unittest.TestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown
    request=test_core.ApiTests.request
    def request_token(self,path,token,method='GET',data=None):
        req=urllib.request.Request(self.base+path,method=method,data=None if data is None else json.dumps(data).encode(),headers={'Authorization':'Bearer '+token})
        return self.opener.open(req,timeout=5)

    def test_http_claim_node_user_revoke_and_admin_agent_isolation(self):
        sid=self.server.registry.save('servers',{'name':'owner'})['id']
        invite=json.load(self.request(f'/api/servers/{sid}/shares','POST',settings()))
        with self.request_token('/share/claim',invite['invite_token'],'POST',{}) as response:
            self.assertEqual(response.headers['Cache-Control'],'no-store'); token=json.load(response)['access_token']
        for path in ('/api/servers','/agent/config'):
            with self.assertRaises(urllib.error.HTTPError) as caught: self.request_token(path,token)
            self.assertEqual(caught.exception.code,401)
        with self.assertRaises(urllib.error.HTTPError): self.request_token('/share/status',invite['invite_token'])
        node=json.load(self.request_token('/share/nodes',token,'POST',{'name':'shared','config':VLESS}))
        auth=json.load(self.request_token(f'/share/nodes/{node["id"]}/users/one',token,'PUT',{}))
        self.assertIn('uuid',auth)
        self.assertEqual(len(json.load(self.request_token('/share/nodes',token))),1)
        self.request('/api/shares/'+invite['id'],'DELETE').close()
        with self.assertRaises(urllib.error.HTTPError) as caught: self.request_token('/share/status',token)
        self.assertEqual(caught.exception.code,401)

    def test_shared_usage_endpoint_reports_only_confirmed_deltas_and_period(self):
        sid=self.server.registry.save('servers',{'name':'owner'})['id']
        invite=json.load(self.request(f'/api/servers/{sid}/shares','POST',settings()))
        with self.request_token('/share/claim',invite['invite_token'],'POST',{}) as response:
            access=json.load(response)['access_token']
        node=json.load(self.request_token('/share/nodes',access,'POST',{'name':'shared','config':VLESS}))
        auth=json.load(self.request_token(f'/share/nodes/{node["id"]}/users/one',access,'PUT',{}))
        with self.server.registry.connect() as db:
            user=db.execute('SELECT user_id FROM shared_users WHERE share_id=?',(invite['id'],)).fetchone()[0]
            db.execute('INSERT INTO usage_deltas(server_id,session_id,sequence,node_id,user_id,credential,cycle_id,received_at,upload_bytes,download_bytes) VALUES(?,?,?,?,?,?,?,?,?,?)',
                       (sid,'confirmed',1,node['id'],user,auth['uuid'],None,100.0,12,8))
            # This unrelated identity must not leak into the share aggregate.
            other=db.execute('INSERT INTO users(name) VALUES(?)',('other',)).lastrowid
            db.execute('INSERT INTO usage_deltas(server_id,session_id,sequence,node_id,user_id,credential,cycle_id,received_at,upload_bytes,download_bytes) VALUES(?,?,?,?,?,?,?,?,?,?)',
                       (sid,'other',1,node['id'],other,str(uuid.uuid4()),None,100.0,99,99))
        summary=json.load(self.request_token('/share/usage?start=50&end=150',access))
        self.assertEqual((summary['upload_bytes'],summary['download_bytes']),(12,8))
        self.assertTrue(summary['unknown_bytes_excluded']); self.assertEqual(summary['basis'],'confirmed_deltas')
        self.assertEqual(summary['items'][0]['reference'],'one')
        owner=json.load(self.request('/api/shares/'+invite['id']+'/usage?start=50&end=150'))
        self.assertEqual((owner['upload_bytes'],owner['download_bytes']),(12,8))
        reset=json.load(self.request('/api/shares/'+invite['id']+'/usage/reset','POST',{}))
        self.assertEqual((reset['status'],reset['reason']),('active','manual'))
        current=json.load(self.request('/api/shares/'+invite['id']+'/usage'))
        self.assertEqual((current['upload_bytes'],current['download_bytes']),(0,0))
        owner_periods=json.load(self.request('/api/shares/'+invite['id']+'/usage/periods'))
        guest_periods=json.load(self.request_token('/share/usage/periods',access))
        self.assertEqual(len(owner_periods),len(guest_periods)); self.assertEqual(len(owner_periods),2)
        self.assertEqual(owner_periods[0]['status'],'active'); self.assertEqual(owner_periods[1]['status'],'closed')
        self.assertEqual(owner_periods[1]['reason'],'initial')
        with self.assertRaises(urllib.error.HTTPError) as caught: self.request_token('/share/usage?start=150&end=50',access)
        self.assertEqual(caught.exception.code,400)


class ClientTests(ApiTests):
    # Keep only new tests; API flow itself is covered in ApiTests above.
    test_http_claim_node_user_revoke_and_admin_agent_isolation=None

    def test_actual_recipient_cli_claim_create_authorize_and_revoke(self):
        sid=self.server.registry.save('servers',{'name':'source'})['id']
        invite=Shares(self.server.registry).create(sid,settings())
        config=Path(self.temp.name)/'recipient.json'; source=Path(__file__).with_name('share_client.py')
        def run(*args,input=None):
            return subprocess.run([sys.executable,str(source),'--config',str(config),*args],input=input,text=True,capture_output=True,timeout=10)
        result=run('claim','--source',self.base,input=invite['invite_token']+'\n')
        self.assertEqual(result.returncode,0,result.stderr); self.assertNotIn('access_token',result.stdout)
        self.assertNotIn(invite['invite_token'],result.stdout+result.stderr)
        self.assertEqual(config.stat().st_mode&0o777,0o600)
        result=run('claim','--source',self.base,input=invite['invite_token']+'\n'); self.assertEqual(result.returncode,1)
        payload=Path(self.temp.name)/'node.json'; payload.write_text(json.dumps({'name':'imported','config':VLESS}))
        result=run('create','--data',str(payload)); self.assertEqual(result.returncode,0,result.stderr)
        ident=json.loads(result.stdout)['id']
        result=run('authorize','--node',str(ident),'--reference','user-one'); self.assertEqual(result.returncode,0,result.stderr)
        self.assertIn('uuid',json.loads(result.stdout))
        self.assertEqual(len(json.loads(run('nodes').stdout)),1)
        Shares(self.server.registry).owner(ident=invite['id'],action='revoke')
        result=run('status'); self.assertEqual(result.returncode,1); self.assertIn('失效',result.stderr)
        self.assertNotIn(json.loads(config.read_text())['token'],result.stderr)

    def test_client_rejects_remote_plaintext_and_exposed_local_secrets(self):
        from share_client import endpoint,load
        for value in ('http://example.com','https://u:p@example.com','https://example.com/other','https://example.com/#x','https://example.com:99999'):
            with self.assertRaises(ValueError): endpoint(value)
        config=Path(self.temp.name)/'unsafe'; config.write_text('{}'); config.chmod(0o644)
        with self.assertRaisesRegex(ValueError,'600'): load(config)


class ClientTlsTests(unittest.TestCase):
    from test_agent_tls import AgentTlsTests
    setUp=AgentTlsTests.setUp

    def test_shared_client_https_trust_and_redirect_does_not_forward(self):
        from share_client import request
        shares=Shares(self.server.registry); invite=shares.create(1,settings())
        with mock.patch.dict(os.environ,{'SSL_CERT_FILE':str(self.root/'ca.pem')}):
            claim=request(self.base,invite['invite_token'],'/share/claim','POST',{})
            self.assertEqual(claim['status'],'active')
            self.assertEqual(request(self.base,claim['access_token'],'/share/status')['id'],invite['id'])
            before=len(self.hits)
            with self.assertRaises(urllib.error.HTTPError): request(self.base,claim['access_token'],'/redirect')
            self.assertEqual(self.hits[before:],['/redirect'])
        with mock.patch.dict(os.environ,{'SSL_CERT_FILE':''}):
            with self.assertRaises(urllib.error.URLError): request(self.base,claim['access_token'],'/share/status')


@unittest.skipUnless(BINARY.is_file(),'Real Xray required')
class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.registry=Registry(Path(self.temp.name)/'db')
        self.shares=Shares(self.registry); self.sid=self.registry.save('servers',{'name':'own'})['id']
        self.agent=self.registry.provision_agent(self.sid)['token']; self.writers=[]
        async def echo(reader,writer):
            try:
                while data:=await reader.read(4096): writer.write(data); await writer.drain()
            except OSError: pass
            finally: writer.close(); await writer.wait_closed()
        self.echo=await asyncio.start_server(echo,'127.0.0.1',0); self.target=self.echo.sockets[0].getsockname()[1]
        self.runtime=VlessRuntime(BINARY,Path(self.temp.name)/'state',local_test_target=('127.0.0.1',self.target))
    async def asyncTearDown(self):
        for writer in self.writers: writer.close()
        await self.runtime.close(); self.echo.close(); await self.echo.wait_closed(); self.temp.cleanup()
    async def open(self,port,credential):
        reader,writer=await asyncio.open_connection('127.0.0.1',port); self.writers.append(writer)
        writer.write(b'\0'+uuid.UUID(credential).bytes+b'\0\1'+self.target.to_bytes(2,'big')+b'\1\x7f\0\0\1'+b'hello'); await writer.drain()
        self.assertEqual(await asyncio.wait_for(reader.readexactly(7),2),b'\0\0hello')
        return reader,writer
    async def test_real_revoke_terminates_guest_and_owner_can_reconnect(self):
        port=free_port(); data=settings(); data.update(port_start=port,port_end=port)
        invite=self.shares.create(self.sid,data); token=self.shares.claim(invite['invite_token'])['access_token']
        guest=self.shares.guest(token,'create',data={'name':'guest','config':{**VLESS,'listen_port':port}})
        credential=self.shares.guest(token,'authorize',guest['id'],reference='remote')
        nid=self.registry.save('nodes',{'name':'owner','server_id':self.sid})['id']; owner_port=free_port()
        self.registry.configure_node(nid,{**VLESS,'listen_port':owner_port})
        uid=self.registry.save('users',{'name':'owner-user'})['id']; own=self.registry.authorize_node_user(nid,uid)
        await self.runtime.apply(self.registry.configuration(self.agent))
        reader,writer=await self.open(port,credential['uuid']); owner_reader,owner_writer=await self.open(owner_port,own['uuid'])
        self.shares.owner(ident=invite['id'],action='revoke')
        await self.runtime.apply(self.registry.configuration(self.agent))
        self.assertEqual(await asyncio.wait_for(reader.read(),2),b'')
        # Structural configuration changes restart the shared private Xray core.
        # Owner authorization remains, but active owner streams reconnect.
        self.assertEqual(await asyncio.wait_for(owner_reader.read(),2),b'')
        await self.open(owner_port,own['uuid'])
        with self.assertRaises(OSError): await asyncio.open_connection('127.0.0.1',port)

    async def test_expiry_enforced_offline_without_expiring_owner(self):
        port=free_port(); data=settings(); data.update(port_start=port,port_end=port)
        invite=self.shares.create(self.sid,data); token=self.shares.claim(invite['invite_token'])['access_token']
        node=self.shares.guest(token,'create',data={'name':'guest','config':{**VLESS,'listen_port':port}})
        credential=self.shares.guest(token,'authorize',node['id'],reference='remote')
        nid=self.registry.save('nodes',{'name':'owner','server_id':self.sid})['id']; owner_port=free_port()
        self.registry.configure_node(nid,{**VLESS,'listen_port':owner_port})
        uid=self.registry.save('users',{'name':'owner'})['id']; own=self.registry.authorize_node_user(nid,uid)
        with self.registry.connect() as db: db.execute('UPDATE server_shares SET expires_at=? WHERE id=?',(time.time()+1.2,invite['id']))
        snapshot=self.registry.configuration(self.agent); validate_snapshot(snapshot); await self.runtime.apply(snapshot)
        reader,_=await self.open(port,credential['uuid']); owner_reader,owner_writer=await self.open(owner_port,own['uuid'])
        self.assertEqual(await asyncio.wait_for(reader.read(),3),b'')
        owner_writer.write(b'alive'); await owner_writer.drain()
        self.assertEqual(await asyncio.wait_for(owner_reader.readexactly(5),2),b'alive')
