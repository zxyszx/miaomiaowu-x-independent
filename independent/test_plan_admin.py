"""Versioned base-plan HTTP tests. Ephemeral registry and random administrator."""
import concurrent.futures
import http.client
import json
import threading
import unittest
import urllib.error
import urllib.request
from unittest import mock
from urllib.parse import urlsplit
from core import Registry, Error
import test_core
import test_xray

RATES={'upload_bps':125000,'download_bps':None}

class PlanAdminTests(unittest.TestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown

    def call(self,path='/api/plans',method='GET',data=None,version=None,token=None):
        headers={'Authorization':'Bearer '+(self.server.token if token is None else token)}
        if version is not None: headers['If-Match']=version
        req=urllib.request.Request(self.base+path,method=method,headers=headers,data=json.dumps(data).encode() if data is not None else None)
        try:
            with self.opener.open(req,timeout=5) as response:return response.status,json.load(response)
        except urllib.error.HTTPError as error:return error.code,json.load(error)

    def create(self,**extra):
        result=self.call(method='POST',data={'name':'temporary','bandwidth':RATES,**extra})
        self.assertEqual(result[0],201);return result[1]

    def test_create_edit_preserve_full_connections_and_reopen(self):
        first=self.create(connections={'max_connections':8,'max_ips':3});path=f'/api/plans/{first["id"]}'
        self.assertEqual(self.call()[1],[first]);self.assertRegex(first['version'],r'^"pl1-[0-9a-f]{64}"$')
        body={'name':'renamed','bandwidth':RATES}
        status,current=self.call(path,'PUT',body,first['version']);self.assertEqual(status,200)
        self.assertEqual(current['connections'],first['connections'])
        self.assertEqual(Registry(self.server.registry.path).plans(versioned=True),[current])
        body['connections']={'max_connections':None}
        status,updated=self.call(path,'PUT',body,current['version']);self.assertEqual(status,200)
        self.assertEqual(updated['connections'],{'max_connections':None})
        self.assertNotEqual(current['version'],updated['version'])

    def test_concurrent_conditional_edits_single_winner(self):
        first=self.create();path=f'/api/plans/{first["id"]}';gate=threading.Barrier(2)
        def write(name):gate.wait();return self.call(path,'PUT',{'name':name,'bandwidth':RATES},first['version'])
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(write,['first','second']))
        self.assertEqual(sorted(s for s,_ in results),[200,412])
        winner=next(row for status,row in results if status==200)
        self.assertEqual(self.call()[1],[winner])

    def test_versions_are_resource_specific_legacy_content_aba(self):
        first=self.create();other=self.create(name='other');path=f'/api/plans/{first["id"]}';body={'name':'changed','bandwidth':RATES}
        self.assertEqual(self.call(path,'PUT',body,other['version'])[0],412)
        self.assertEqual(self.call(path,'PUT',body)[0],200)
        self.assertEqual(self.call(path,'PUT',body,first['version'])[0],412)
        self.assertEqual(self.call(path,'PUT',{'name':first['name'],'bandwidth':RATES})[0],200)
        self.assertEqual(self.call()[1][0]['version'],first['version'])
        self.assertEqual(self.call(path,'PUT',body,first['version'])[0],200)

    def test_auth_malformed_conditions_duplicates_and_invalid_body_zero_write(self):
        first=self.create();path=f'/api/plans/{first["id"]}';body={'name':'changed','bandwidth':RATES}
        r=self.server.registry;sid=r.save('servers',{'name':'host'})['id'];agent=r.provision_agent(sid)['token']
        for token in ['',agent]:
            for method,payload,endpoint in [('POST',body,'/api/plans'),('PUT',body,path),('GET',None,'/api/plans')]:self.assertEqual(self.call(endpoint,method,payload,first['version'] if method=='PUT' else None,token)[0],401)
        for bad in ['', '*','W/'+first['version'],first['version'].replace('pl1','bw1')]:self.assertEqual(self.call(path,'PUT',body,bad)[0],400)
        self.assertEqual(self.call(method='POST',data=body,version=first['version'])[0],400)
        for bad in [{'name':' ','bandwidth':RATES},{'name':'x','bandwidth':{'upload_bps':0,'download_bps':None}},{**body,'connections':{'max_connections':1,'max_ips':True}},{**body,'role':'admin'}]:self.assertEqual(self.call(path,'PUT',bad,first['version'])[0],400)
        c=http.client.HTTPConnection(urlsplit(self.base).hostname,urlsplit(self.base).port,timeout=5)
        try:
            c.putrequest('PUT',path);c.putheader('Authorization','Bearer '+self.server.token);c.putheader('If-Match',first['version']);c.putheader('If-Match',first['version']);c.putheader('Content-Length','2');c.endheaders(b'{}');reply=c.getresponse();self.assertEqual(reply.status,400);reply.read()
        finally:c.close()
        self.assertEqual(self.call()[1],[first])

    def test_publication_failure_rolls_back_plan_connections_and_revision(self):
        first=self.create(connections={'max_connections':8,'max_ips':3});r=self.server.registry
        sid=r.save('servers',{'name':'host'})['id'];nid=r.save('nodes',{'name':'node','server_id':sid})['id'];r.configure_node(nid,test_xray.VLESS)
        uid=r.save('users',{'name':'user'})['id'];r.assign_plan(uid,{'plan_id':first['id']});r.authorize_node_user(nid,uid)
        with r.connect() as db:
            before=db.execute('SELECT revision FROM server_configs WHERE server_id=?',(sid,)).fetchone()[0]
            db.execute("CREATE TRIGGER fail_plan_publish BEFORE UPDATE ON server_configs BEGIN SELECT RAISE(ABORT,'fixture'); END")
        status,_=self.call(f'/api/plans/{first["id"]}','PUT',{'name':'changed','bandwidth':{'upload_bps':1,'download_bps':None},'connections':{'max_connections':1}},first['version'])
        self.assertEqual(status,500);self.assertEqual(self.call()[1],[first])
        with r.connect() as db:self.assertEqual(db.execute('SELECT revision FROM server_configs WHERE server_id=?',(sid,)).fetchone()[0],before)
