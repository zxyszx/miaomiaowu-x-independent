import concurrent.futures
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest import mock
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from core import Registry, Error
from server import Handler
from agent import send

class RegistryTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.path=Path(self.temp.name)/'registry.db'
        self.registry=Registry(self.path)
    def tearDown(self): self.temp.cleanup()
    def test_default_no_vendor_quantity_limits_and_restart_persistence(self):
        for kind in ('servers','users'):
            for index in range(8): self.registry.save(kind,{'name':f'{kind}-{index}'})
        for index in range(10): self.registry.save('nodes',{'name':f'node-{index}','server_id':1})
        restarted=Registry(self.path)
        self.assertEqual([len(restarted.list(k)) for k in ('servers','nodes','users')],[8,10,8])
        self.assertTrue(all(v['limit'] is None for v in restarted.quotas().values()))
    def test_quota_creation_update_delete_and_release(self):
        self.registry.set_quota('servers',1)
        resource=self.registry.save('servers',{'name':'first'})
        with self.assertRaises(Error): self.registry.save('servers',{'name':'second'})
        self.registry.save('servers',{'name':'renamed'},resource['id'])
        self.registry.delete('servers',resource['id'])
        self.registry.save('servers',{'name':'replacement'})
        self.assertEqual(self.registry.quotas()['servers'],{'limit':1,'used':1})
    def test_concurrent_creates_do_not_exceed_quota(self):
        self.registry.set_quota('users',3)
        def create(index):
            try: self.registry.save('users',{'name':str(index)}); return True
            except Error as error: self.assertEqual(error.status,409); return False
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            self.assertEqual(sum(pool.map(create,range(24))),3)
        self.assertEqual(len(self.registry.list('users')),3)
    def test_foreign_key_and_deletion_protect_nodes(self):
        with self.assertRaises(Error): self.registry.save('nodes',{'name':'missing','server_id':1})
        server=self.registry.save('servers',{'name':'server'})
        node=self.registry.save('nodes',{'name':'node','server_id':server['id']})
        with self.assertRaises(Error): self.registry.delete('servers',server['id'])
        self.registry.delete('nodes',node['id'])
        self.registry.delete('servers',server['id'])
    def test_bad_fields_and_quotas_are_rejected(self):
        for data in ({'name':''},{'name':'x','role':'admin'},[],{'name':'x'*121},{'name':None}):
            with self.subTest(data=data), self.assertRaises(Error): self.registry.save('users',data)
        for value in (-1,True,1.5,'3'):
            with self.subTest(value=value), self.assertRaises(Error): self.registry.set_quota('users',value)
        self.registry.save('users',{'name':'existing'})
        with self.assertRaises(Error): self.registry.set_quota('users',0)
        self.registry.set_quota('users',None)
    def test_zero_quota_blocks_and_unknown_ids_are_not_success(self):
        self.registry.set_quota('users',0)
        with self.assertRaises(Error): self.registry.save('users',{'name':'blocked'})
        for operation in (lambda:self.registry.delete('users',999),lambda:self.registry.save('users',{'name':'missing'},999),lambda:self.registry.list('users; DROP TABLE servers')):
            with self.assertRaises(Error) as caught: operation()
            self.assertEqual(caught.exception.status,404)

    def test_agent_expiry_rotation_and_persistence(self):
        server=self.registry.save('servers',{'name':'local'})
        first=self.registry.provision_agent(server['id'])['token']
        self.assertFalse(self.registry.agent_status()[0]['online'])
        self.registry.heartbeat(first,{'uptime':1,'hostname':'fixture'})
        restored=Registry(self.path)
        self.assertTrue(restored.agent_status()[0]['online'])
        with mock.patch('core.time.time',return_value=restored.agent_status()[0]['last_seen']+46):
            self.assertFalse(restored.agent_status()[0]['online'])
        second=restored.provision_agent(server['id'])['token']
        with self.assertRaises(Error) as caught: restored.heartbeat(first,{'uptime':2,'hostname':'old'})
        self.assertEqual(caught.exception.status,401)
        restored.heartbeat(second,{'uptime':2,'hostname':'new'})
        restored.revoke_agent(server['id'])
        with self.assertRaises(Error): restored.heartbeat(second,{'uptime':3,'hostname':'revoked'})

    def test_invalid_heartbeat_does_not_mark_online(self):
        server=self.registry.save('servers',{'name':'test'})
        token=self.registry.provision_agent(server['id'])['token']
        for uptime in (-1,True,float('nan'),float('inf'),'1'):
            with self.subTest(uptime=uptime),self.assertRaises(Error):
                self.registry.heartbeat(token,{'uptime':uptime,'hostname':'fixture'})
        self.assertFalse(self.registry.agent_status()[0]['online'])
        with self.registry.connect() as db:
            stored=db.execute('SELECT token_hash FROM agents').fetchone()[0]
            self.assertNotEqual(stored,token)

class ApiTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        self.server.registry=Registry(Path(self.temp.name)/'registry.db')
        self.server.token='fixture-only'
        threading.Thread(target=self.server.serve_forever,daemon=True).start()
        self.base=f'http://127.0.0.1:{self.server.server_port}'
        self.opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
    def tearDown(self):
        self.server.shutdown();self.server.server_close();self.temp.cleanup()
    def request(self,path,method='GET',data=None,auth=True):
        headers={'Authorization':'Bearer fixture-only'} if auth else {}
        request=urllib.request.Request(self.base+path,method=method,data=None if data is None else json.dumps(data).encode(),headers=headers)
        return self.opener.open(request,timeout=5)
    def test_auth_health_and_no_public_resource_data(self):
        self.assertTrue(json.load(self.request('/health',auth=False))['ready'])
        for method in ('GET','POST','PUT','DELETE'):
            with self.assertRaises(urllib.error.HTTPError) as caught:
                self.request('/api/users',method,{'name':'no'},auth=False)
            self.assertEqual(caught.exception.code,401)
        self.assertEqual(self.server.registry.list('users'),[])
    def test_api_resource_and_quota_flow(self):
        with self.request('/api/quotas/users','PUT',{'limit':1}) as response:
            self.assertEqual(response.headers['Cache-Control'],'no-store')
        with self.request('/api/users','POST',{'name':'one'}) as response:
            self.assertEqual(response.status,201)
            ident=json.load(response)['id']
        with self.assertRaises(urllib.error.HTTPError) as caught:
            self.request('/api/users','POST',{'name':'two'})
        self.assertEqual(caught.exception.code,409)
        self.request(f'/api/users/{ident}','PUT',{'name':'updated'}).close()
        self.assertEqual(json.load(self.request('/api/users'))[0]['name'],'updated')
        self.request(f'/api/users/{ident}','DELETE').close()
        self.assertEqual(json.load(self.request('/api/users')),[])
    def test_malformed_body_does_not_create_resource(self):
        request=urllib.request.Request(self.base+'/api/users',method='POST',data=b'{',headers={'Authorization':'Bearer fixture-only'})
        with self.assertRaises(urllib.error.HTTPError) as caught: self.opener.open(request)
        self.assertEqual(caught.exception.code,400)
        self.assertEqual(self.server.registry.list('users'),[])

    def test_actual_agent_http_registration_revoke_and_admin_isolation(self):
        server=json.load(self.request('/api/servers','POST',{'name':'fixture'}))
        credentials=json.load(self.request(f'/api/servers/{server["id"]}/agent','POST'))
        self.assertTrue(send(self.base,credentials['token'],12)['accepted'])
        state=json.load(self.request('/api/agent-status'))[0]
        self.assertTrue(state['online'])
        self.assertEqual(state['uptime'],12)
        self.assertNotIn('token',state)
        request=urllib.request.Request(self.base+'/api/users',headers={'Authorization':'Bearer '+credentials['token']})
        with self.assertRaises(urllib.error.HTTPError) as caught: self.opener.open(request)
        self.assertEqual(caught.exception.code,401)
        self.request(f'/api/servers/{server["id"]}/agent','DELETE').close()
        with self.assertRaises(urllib.error.HTTPError) as caught: send(self.base,credentials['token'],13)
        self.assertEqual(caught.exception.code,401)

    def test_agent_rejects_plaintext_remote_master(self):
        with self.assertRaisesRegex(ValueError,'HTTPS'):
            send('http://example.com','fixture-token',1)

if __name__=='__main__': unittest.main()
