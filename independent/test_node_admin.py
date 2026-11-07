"""Versioned node-base management; only temporary registry/loopback."""
import concurrent.futures
import json
import threading
import unittest
from core import Registry
import test_core
import test_plan_admin
import test_xray
import test_sharing
from sharing import Shares

class NodeAdminTests(unittest.TestCase):
 setUp=test_core.ApiTests.setUp
 tearDown=test_core.ApiTests.tearDown
 call=test_plan_admin.PlanAdminTests.call
 def fixture(self):
  r=self.server.registry
  hosts=[r.save('servers',{'name':n})['id'] for n in ['a','b']]
  status,row=self.call('/api/nodes','POST',{'name':'node','server_id':hosts[0]});self.assertEqual(status,201)
  return r,hosts,row
 def test_create_conditional_migrate_publish_and_reopen(self):
  r,hosts,row=self.fixture();path=f'/api/nodes/{row["id"]}'
  self.assertRegex(row['version'],r'^"nd1-[0-9a-f]{64}"$');self.assertEqual(self.call('/api/nodes')[1],[row])
  r.configure_node(row['id'],test_xray.VLESS);current=self.call('/api/nodes')[1][0]
  with r.connect() as db:
   revisions=[db.execute('SELECT revision FROM server_configs WHERE server_id=?',(h,)).fetchone()[0] for h in hosts]
   config=db.execute('SELECT payload FROM node_configs WHERE node_id=?',(row['id'],)).fetchone()[0]
  self.assertEqual(self.call(path,'PUT',{'name':'changed','server_id':hosts[1]},row['version'])[0],412)
  status,new=self.call(path,'PUT',{'name':'changed','server_id':hosts[1]},current['version']);self.assertEqual(status,200)
  self.assertEqual(Registry(r.path).list('nodes',versioned=True),[new]);self.assertEqual(set(new),{'id','name','server_id','version'})
  with r.connect() as db:
   self.assertEqual(db.execute('SELECT payload FROM node_configs WHERE node_id=?',(row['id'],)).fetchone()[0],config)
   for h,before in zip(hosts,revisions):self.assertEqual(db.execute('SELECT revision FROM server_configs WHERE server_id=?',(h,)).fetchone()[0],before+1)
 def test_competing_edits_and_legacy_content_aba(self):
  r,hosts,row=self.fixture();path=f'/api/nodes/{row["id"]}';gate=threading.Barrier(2)
  def write(name):gate.wait();return self.call(path,'PUT',{'name':name,'server_id':hosts[0]},row['version'])
  with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(write,['first','second']))
  self.assertEqual(sorted(s for s,_ in results),[200,412])
  self.assertEqual(self.call(path,'PUT',{'name':row['name'],'server_id':hosts[0]})[0],200)
  self.assertEqual(self.call('/api/nodes')[1][0]['version'],row['version'])
  self.assertEqual(self.call(path,'PUT',{'name':'legacy-aba','server_id':hosts[0]},row['version'])[0],200)
 def test_auth_inputs_quota_and_missing_server(self):
  r,hosts,row=self.fixture();path=f'/api/nodes/{row["id"]}';body={'name':'other','server_id':hosts[0]}
  for token in ['',r.provision_agent(hosts[0])['token']]:
   for endpoint,method,data in [('/api/nodes','GET',None),('/api/servers','GET',None),('/api/nodes','POST',body),(path,'PUT',body)]:self.assertEqual(self.call(endpoint,method,data,row['version'] if method=='PUT' else None,token)[0],401)
  for bad in ['','*',row['version'].replace('nd1','pl1')]:self.assertEqual(self.call(path,'PUT',body,bad)[0],400)
  self.assertEqual(self.call('/api/nodes','POST',body,row['version'])[0],400)
  for bad in [{**body,'server_id':True},{**body,'server_id':0},{**body,'config':{}},{**body,'name':' '}]:self.assertEqual(self.call(path,'PUT',bad,row['version'])[0],400)
  self.assertEqual(self.call(path,'PUT',{**body,'server_id':99999},row['version'])[0],409)
  r.set_quota('nodes',1);self.assertEqual(self.call('/api/nodes','POST',body)[0],409)
  self.assertEqual(self.call('/api/nodes')[1],[row])
 def test_port_collision_and_shared_migration_still_rejected(self):
  r,hosts,row=self.fixture();r.configure_node(row['id'],test_xray.VLESS)
  other=r.save('nodes',{'name':'collision','server_id':hosts[1]});r.configure_node(other['id'],test_xray.VLESS)
  current=self.call('/api/nodes')[1][0]
  self.assertEqual(self.call(f'/api/nodes/{row["id"]}','PUT',{'name':'blocked','server_id':hosts[1]},current['version'])[0],409)
  sharing=Shares(r);invite=sharing.create(hosts[0],test_sharing.settings());access=sharing.claim(invite['invite_token'])['access_token']
  guest=sharing.guest(access,'create',data={'name':'guest','config':{**test_xray.VLESS,'listen_port':24581}})
  snapshot=next(n for n in self.call('/api/nodes')[1] if n['id']==guest['id'])
  self.assertEqual(self.call(f'/api/nodes/{guest["id"]}','PUT',{'name':'move','server_id':hosts[1]},snapshot['version'])[0],409)
  self.assertEqual(self.call(f'/api/nodes/{guest["id"]}','PUT',{'name':'rename','server_id':hosts[0]},snapshot['version'])[0],200)
 def test_second_host_publication_failure_rolls_back_both_hosts_and_node(self):
  r,hosts,row=self.fixture();r.configure_node(row['id'],test_xray.VLESS);current=self.call('/api/nodes')[1][0]
  with r.connect() as db:
   before={t:[tuple(x) for x in db.execute('SELECT * FROM '+t+' ORDER BY 1')] for t in ['nodes','node_configs','server_configs']}
   db.execute("CREATE TRIGGER fail_host_publish BEFORE UPDATE ON server_configs WHEN NEW.server_id="+str(hosts[1])+" BEGIN SELECT RAISE(ABORT,'fixture'); END")
  # Existing Registry maps sqlite integrity errors to its legacy409; full rollback still required.
  self.assertEqual(self.call(f'/api/nodes/{row["id"]}','PUT',{'name':'failed','server_id':hosts[1]},current['version'])[0],409)
  self.assertEqual(self.call('/api/nodes')[1],[current])
  with r.connect() as db:
   for t,values in before.items():self.assertEqual([tuple(x) for x in db.execute('SELECT * FROM '+t+' ORDER BY 1')],values)
