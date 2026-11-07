"""Conditional deletion: current dependencies, atomic rollback, and no orphan links."""
import concurrent.futures
import http.client
import threading
import unittest
from urllib.parse import urlsplit
from core import Registry,Error
from plan_grace import GracePolicies
from plan_nodes import PlanNodes
from plan_node_bandwidth import PlanNodeBandwidth
import test_plan_admin
from test_plan_admin import RATES
import test_core
import test_xray

class PlanDeleteTests(unittest.TestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown
    call=test_plan_admin.PlanAdminTests.call
    create=test_plan_admin.PlanAdminTests.create

    def test_condition_auth_duplicate_header_and_legacy_semantics(self):
        row=self.create();path=f'/api/plans/{row["id"]}';r=self.server.registry
        sid=r.save('servers',{'name':'temporary'})['id'];agent=r.provision_agent(sid)['token']
        for token in ['',agent]:self.assertEqual(self.call(path,'DELETE',version=row['version'],token=token)[0],401)
        for bad in ['', '*','W/'+row['version'],row['version'].replace('pl1','bw1')]:self.assertEqual(self.call(path,'DELETE',version=bad)[0],400)
        c=http.client.HTTPConnection(urlsplit(self.base).hostname,urlsplit(self.base).port,timeout=5)
        try:
            c.putrequest('DELETE',path);c.putheader('Authorization','Bearer '+self.server.token);c.putheader('If-Match',row['version']);c.putheader('If-Match',row['version']);c.endheaders();reply=c.getresponse();self.assertEqual(reply.status,400);reply.read()
        finally:c.close()
        self.call(path,'PUT',{'name':'changed','bandwidth':RATES})
        self.assertEqual(self.call(path,'DELETE',version=row['version'])[0],412)
        self.assertEqual(self.call(path,'DELETE'),(200,{'deleted':True}))
        self.assertEqual(self.call(path,'DELETE',version=row['version'])[0],404)

    def test_bound_user_and_grace_target_reject_without_unbinding(self):
        row=self.create();source=self.create(name='downgrade source');path=f'/api/plans/{row["id"]}';r=self.server.registry
        uid=r.save('users',{'name':'temporary'})['id'];r.assign_plan(uid,{'plan_id':row['id']})
        self.assertEqual(self.call(path,'DELETE',version=row['version'])[0],409)
        self.assertEqual(r.bandwidth_policy(uid)['plan_id'],row['id'])
        r.assign_plan(uid,{'plan_id':None});GracePolicies(r).save(source['id'],{'target_plan_id':row['id'],'days':1})
        self.assertEqual(self.call(path,'DELETE',version=row['version'])[0],409)
        self.assertEqual(GracePolicies(r).get(source['id'])['target_plan_id'],row['id'])
        GracePolicies(r).delete(source['id']);self.assertEqual(self.call(path,'DELETE',version=row['version'])[0],200)
        self.assertEqual([p['id'] for p in r.plans()],[source['id']])

    def test_edit_delete_and_double_delete_serialize(self):
        for rival in ['PUT','DELETE']:
            row=self.create();path=f'/api/plans/{row["id"]}';gate=threading.Barrier(2)
            def write(method):gate.wait();return self.call(path,method,{'name':'changed','bandwidth':RATES} if method=='PUT' else None,row['version'])
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(write,['DELETE',rival]))
            statuses=sorted(s for s,_ in results)
            self.assertIn(statuses,[[200,404],[200,412]])
            if rival=='DELETE':self.assertEqual(statuses,[200,404])

    def test_assignment_and_grace_creation_race_no_dangling_reference(self):
        for dependency in ['user','grace']:
            row=self.create();source=self.create(name='source');r=self.server.registry
            uid=r.save('users',{'name':dependency})['id'];gate=threading.Barrier(2)
            def remove():gate.wait();return self.call(f'/api/plans/{row["id"]}','DELETE',version=row['version'])[0]
            def bind():
                gate.wait()
                try:
                    if dependency=='user':r.assign_plan(uid,{'plan_id':row['id']})
                    else:GracePolicies(r).save(source['id'],{'target_plan_id':row['id'],'days':1})
                    return 200
                except Error as error:return error.status
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                a=pool.submit(remove);b=pool.submit(bind);result=(a.result(),b.result())
            self.assertIn(result,[(200,404),(409,200)])
            with r.connect() as db:self.assertEqual(db.execute('PRAGMA foreign_key_check').fetchall(),[])

    def test_real_cascade_failure_rolls_back_and_success_is_scoped(self):
        row=self.create(connections={'max_connections':8,'max_ips':3});other=self.create(name='unrelated');r=self.server.registry
        sid=r.save('servers',{'name':'host'})['id'];nid=r.save('nodes',{'name':'node','server_id':sid})['id'];r.configure_node(nid,test_xray.VLESS)
        PlanNodes(r).save(row['id'],{'node_ids':[nid]});PlanNodeBandwidth(r).manage(row['id'],nid,{'upload_bps':1,'download_bps':None})
        GracePolicies(r).save(row['id'],{'target_plan_id':other['id'],'days':1})
        tables=['plans','plan_connections','plan_node_scopes','plan_nodes','plan_node_bandwidth','plan_grace_policies','server_configs','nodes','users']
        def snapshot():
            with r.connect() as db:return {t:[tuple(x) for x in db.execute('SELECT * FROM '+t+' ORDER BY 1')] for t in tables}
        before=snapshot()
        with r.connect() as db:db.execute("CREATE TRIGGER fail_plan_cascade BEFORE DELETE ON plan_node_bandwidth BEGIN SELECT RAISE(ABORT,'fixture'); END")
        path=f'/api/plans/{row["id"]}';self.assertEqual(self.call(path,'DELETE',version=row['version'])[0],500);self.assertEqual(snapshot(),before)
        with r.connect() as db:db.execute('DROP TRIGGER fail_plan_cascade')
        self.assertEqual(self.call(path,'DELETE',version=row['version']),(200,{'deleted':True}))
        reopened=Registry(r.path);self.assertEqual([p['id'] for p in reopened.plans()],[other['id']]);after=snapshot()
        for table in tables[1:6]:self.assertEqual(after[table],[])
        for table in ['server_configs','nodes','users']:self.assertEqual(after[table],before[table])
