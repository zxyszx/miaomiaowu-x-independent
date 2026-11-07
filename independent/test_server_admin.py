"""Temporary original Handler server-name administration boundaries."""
import concurrent.futures
import http.client
import json
import threading
import unittest
from urllib.parse import urlsplit
from core import Registry
import test_core
import test_plan_admin
import test_xray

BASE='/api/manage/servers'
class ServerAdminTests(unittest.TestCase):
 setUp=test_core.ApiTests.setUp
 tearDown=test_core.ApiTests.tearDown
 call=test_plan_admin.PlanAdminTests.call
 def create(self,name='host'):
  status,row=self.call(BASE,'POST',{'name':name});self.assertEqual(status,201);return row
 def test_create_rename_preserves_agent_nodes_config_and_legacy_shape(self):
  row=self.create();r=self.server.registry;sid=row['id'];token=r.provision_agent(sid)['token'];r.heartbeat(token,{'uptime':1,'hostname':'temporary'})
  node=r.save('nodes',{'name':'node','server_id':sid});r.configure_node(node['id'],test_xray.VLESS)
  with r.connect() as db:
   before={t:[tuple(x) for x in db.execute('SELECT * FROM '+t)] for t in ['agents','nodes','node_configs','runtime_receipts']};rev=db.execute('SELECT revision FROM server_configs WHERE server_id=?',(sid,)).fetchone()[0]
  status,new=self.call(BASE+'/'+str(sid),'PUT',{'name':' renamed '},row['version']);self.assertEqual(status,200);self.assertEqual(new['name'],'renamed');self.assertEqual(set(new),{'id','name','version'})
  self.assertEqual(Registry(r.path).list('servers',versioned=True),[new]);self.assertEqual(self.call('/api/servers')[1],[{'id':sid,'name':'renamed'}]);self.assertEqual(r.list('servers'),[{'id':sid,'name':'renamed'}])
  with r.connect() as db:
   for t,values in before.items():self.assertEqual([tuple(x) for x in db.execute('SELECT * FROM '+t)],values)
   self.assertEqual(db.execute('SELECT revision FROM server_configs WHERE server_id=?',(sid,)).fetchone()[0],rev+1)
  r.heartbeat(token,{'uptime':2,'hostname':'temporary'})
 def test_competing_edits_have_one_winner_and_legacy_aba_is_explicit(self):
  row=self.create();path=BASE+'/'+str(row['id']);gate=threading.Barrier(2)
  def put(name):gate.wait();return self.call(path,'PUT',{'name':name},row['version'])
  with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:result=list(pool.map(put,['a','b']))
  self.assertEqual(sorted(s for s,_ in result),[200,412])
  self.assertEqual(self.call('/api/servers/'+str(row['id']),'PUT',{'name':row['name']})[0],200)
  self.assertEqual(self.call(BASE)[1],[row]);self.assertEqual(self.call(path,'PUT',{'name':'after-aba'},row['version'])[0],200)
 def test_auth_version_repeated_headers_body_quota_and_unknown_routes(self):
  row=self.create();r=self.server.registry;path=BASE+'/'+str(row['id']);body={'name':'changed'};agent=r.provision_agent(row['id'])['token']
  for token in ['',agent]:
   for endpoint,method,data in [(BASE,'GET',None),(BASE,'POST',body),(path,'PUT',body)]:self.assertEqual(self.call(endpoint,method,data,row['version'] if method=='PUT' else None,token)[0],401)
  for bad in [None,'','*','W/'+row['version'],row['version'].replace('sv1','nd1')]:self.assertEqual(self.call(path,'PUT',body,bad)[0],400)
  for bad in [{}, {'name':True},{'name':' '},{'name':'x'*121},{'name':'ok','token':'no'},{'name':'ok','server_id':1}]:self.assertEqual(self.call(path,'PUT',bad,row['version'])[0],400)
  self.assertEqual(self.call(BASE,'POST',body,row['version'])[0],400)
  url=urlsplit(self.base);conn=http.client.HTTPConnection(url.hostname,url.port,timeout=5)
  try:
   conn.putrequest('PUT',path);conn.putheader('Authorization','Bearer '+self.server.token);payload=json.dumps(body).encode();conn.putheader('Content-Length',str(len(payload)));conn.putheader('If-Match',row['version']);conn.putheader('If-Match',row['version']);conn.endheaders(payload);reply=conn.getresponse();self.assertEqual(reply.status,400);reply.read()
  finally:conn.close()
  for endpoint,method in [(path,'DELETE'),(BASE+'/1/agent','POST'),(BASE+'/x','PUT')]:self.assertEqual(self.call(endpoint,method,body)[0],404)
  self.assertEqual(self.call(BASE+'/99999','PUT',body,row['version'])[0],404)
  r.set_quota('servers',1);self.assertEqual(self.call(BASE,'POST',body)[0],409);self.assertEqual(self.call(BASE)[1],[row])
 def test_publish_failure_rolls_back_and_create_does_not_leave_orphan(self):
  row=self.create();r=self.server.registry
  with r.connect() as db:
   before={t:[tuple(x) for x in db.execute('SELECT * FROM '+t)] for t in ['servers','server_configs']}
   db.execute("CREATE TRIGGER reject_publish BEFORE UPDATE ON server_configs BEGIN SELECT RAISE(ABORT,'fixture'); END")
  self.assertEqual(self.call(BASE+'/'+str(row['id']),'PUT',{'name':'failed'},row['version'])[0],409)
  with r.connect() as db:
   for t,values in before.items():self.assertEqual([tuple(x) for x in db.execute('SELECT * FROM '+t)],values)
   db.execute("CREATE TRIGGER reject_initial BEFORE INSERT ON server_configs BEGIN SELECT RAISE(ABORT,'fixture'); END")
  self.assertEqual(self.call(BASE,'POST',{'name':'orphan'})[0],409);self.assertEqual(self.call(BASE)[1],[row])
