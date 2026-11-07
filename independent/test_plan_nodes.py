import asyncio
import concurrent.futures
import json
import sqlite3
import unittest
import urllib.error
import urllib.request
from core import Registry,Error
from accounts import Accounts
from plan_nodes import PlanNodes
import test_core
import test_xray
import test_user_bandwidth
import test_user_connections
from test_accounts import plan


class ScopeTests(unittest.TestCase):
    setUp=test_core.RegistryTests.setUp
    tearDown=test_core.RegistryTests.tearDown

    def fixtures(self):
        r=self.registry; parent=r.save('users',{'name':'parent'})['id']; p=plan(r)
        nodes=[]; tokens=[]
        for i in range(2):
            sid=r.save('servers',{'name':str(i)})['id']; tokens.append(r.provision_agent(sid)['token'])
            nid=r.save('nodes',{'name':str(i),'server_id':sid})['id']; nodes.append(nid)
            r.configure_node(nid,test_xray.VLESS)
        return r,parent,p,nodes,tokens

    def test_default_compatibility_empty_scope_clear_and_persistence(self):
        r,parent,p,nodes,tokens=self.fixtures(); scope=PlanNodes(r)
        self.assertFalse(scope.get(p)['restricted'])
        child=Accounts(r).create(parent,dict(name='child',plan_id=p,node_ids=nodes))
        value=scope.save(p,{'node_ids':[]})
        self.assertTrue(value['restricted']); self.assertEqual(value['node_ids'],[])
        self.assertEqual(Accounts(r).list(parent)[0]['nodes'],[])
        self.assertEqual(PlanNodes(Registry(r.path)).get(p),value)
        for nid in nodes:
            with self.assertRaises(Error): r.authorize_node_user(nid,child['user_id'])
        self.assertFalse(scope.clear(p)['restricted'])
        self.assertEqual(Accounts(r).list(parent)[0]['nodes'],[])
        r.authorize_node_user(nodes[0],child['user_id'])

    def test_shrink_revokes_only_bound_users_and_changed_hosts_once(self):
        r,parent,p,nodes,tokens=self.fixtures(); r.assign_plan(parent,{'plan_id':p})
        other=r.save('users',{'name':'unbound'})['id']
        for n in nodes:
            r.authorize_node_user(n,parent); r.authorize_node_user(n,other)
        before=[r.configuration(t) for t in tokens]
        PlanNodes(r).save(p,{'node_ids':[nodes[0]]})
        after=[r.configuration(t) for t in tokens]
        self.assertEqual(before[0],after[0]); self.assertEqual(after[1]['revision'],before[1]['revision']+1)
        self.assertEqual([c['user_id'] for c in after[1]['nodes'][0]['clients']],[other])
        PlanNodes(r).save(p,{'node_ids':[nodes[0]]})
        self.assertEqual([r.configuration(t) for t in tokens],after)
        PlanNodes(r).save(p,{'node_ids':nodes})
        self.assertEqual([r.configuration(t) for t in tokens],after)

    def test_create_edit_generic_authorize_and_assign_cannot_bypass_range(self):
        r,parent,p,nodes,tokens=self.fixtures(); PlanNodes(r).save(p,{'node_ids':[nodes[0]]})
        with self.assertRaises(Error): Accounts(r).create(parent,dict(name='bad',plan_id=p,node_ids=nodes))
        child=Accounts(r).create(parent,dict(name='valid',plan_id=p,node_ids=[nodes[0]]))
        with self.assertRaises(Error): Accounts(r).update(parent,child['user_id'],dict(name='bad',node_ids=nodes))
        with self.assertRaises(Error): r.authorize_node_user(nodes[1],child['user_id'])
        r.authorize_node_user(nodes[1],parent)
        with self.assertRaises(Error): r.assign_plan(parent,{'plan_id':p})
        with r.connect() as db:
            self.assertIsNone(db.execute('SELECT 1 FROM user_plans WHERE user_id=?',(parent,)).fetchone())
        self.assertEqual(Accounts(r).list(parent),[child])

    def test_invalid_inputs_and_failed_revision_rollback_scope_and_credentials(self):
        r,parent,p,nodes,tokens=self.fixtures(); scope=PlanNodes(r)
        child=Accounts(r).create(parent,dict(name='child',plan_id=p,node_ids=nodes))
        before=[r.configuration(t) for t in tokens]
        for data in ({'node_ids':[True]},{'node_ids':[nodes[0],nodes[0]]},{'node_ids':[999]}, {'node_ids':[],'extra':1},None):
            with self.assertRaises(Error): scope.save(p,data)
            self.assertFalse(scope.get(p)['restricted'])
        from sharing import Shares
        from test_sharing import settings
        with r.connect() as db: sid=db.execute('SELECT server_id FROM nodes WHERE id=?',(nodes[0],)).fetchone()[0]
        shares=Shares(r); invitation=shares.create(sid,settings())
        token=shares.claim(invitation['invite_token'])['access_token']
        guest=shares.guest(token,'create',data={'name':'delegated','config':{**test_xray.VLESS,'listen_port':24502}})
        with self.assertRaises(Error): scope.save(p,{'node_ids':[guest['id']]})
        self.assertFalse(scope.get(p)['restricted'])
        # The delegated fixture legitimately changes its host snapshot.
        before=[r.configuration(t) for t in tokens]
        with r.connect() as db: db.execute("CREATE TRIGGER fail_scope BEFORE UPDATE ON server_configs BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): scope.save(p,{'node_ids':[]})
        self.assertFalse(scope.get(p)['restricted']); self.assertEqual(Accounts(r).list(parent),[child])
        self.assertEqual([r.configuration(t) for t in tokens],before)

    def test_deleting_last_allowed_node_keeps_explicit_deny_all(self):
        r,parent,p,nodes,tokens=self.fixtures(); scope=PlanNodes(r)
        scope.save(p,{'node_ids':[nodes[0]]}); r.delete('nodes',nodes[0])
        self.assertEqual(scope.get(p),{'plan_id':p,'restricted':True,'node_ids':[]})
        r.delete_plan(p)
        with self.assertRaises(Error): scope.get(p)

    def test_concurrent_shrink_and_authorize_cannot_leave_disallowed_grant(self):
        r,parent,p,nodes,tokens=self.fixtures(); r.assign_plan(parent,{'plan_id':p})
        def grant():
            try: r.authorize_node_user(nodes[0],parent)
            except Error: pass
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            a=pool.submit(grant); b=pool.submit(PlanNodes(r).save,p,{'node_ids':[]}); a.result(); b.result()
        self.assertEqual(r.configuration(tokens[0])['nodes'][0]['clients'],[])

class ScopeApiTests(unittest.TestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown
    request=test_core.ApiTests.request

    def test_admin_http_lifecycle_and_unknown_plan(self):
        p=plan(self.server.registry); path=f'/api/plans/{p}/nodes'
        for method,data in (('GET',None),('PUT',{'node_ids':[]}),('DELETE',None)):
            with self.assertRaises(urllib.error.HTTPError) as caught: self.request(path,method,data,auth=False)
            self.assertEqual(caught.exception.code,401)
        sid=self.server.registry.save('servers',{'name':'agent'})['id']
        token=self.server.registry.provision_agent(sid)['token']
        req=urllib.request.Request(self.base+path,headers={'Authorization':'Bearer '+token})
        with self.assertRaises(urllib.error.HTTPError) as caught: self.opener.open(req,timeout=3)
        self.assertEqual(caught.exception.code,401)
        self.assertFalse(json.load(self.request(path))['restricted'])
        self.assertTrue(json.load(self.request(path,'PUT',{'node_ids':[]}))['restricted'])
        self.assertFalse(json.load(self.request(path,'DELETE'))['restricted'])
        with self.assertRaises(urllib.error.HTTPError) as caught: self.request('/api/plans/999/nodes')
        self.assertEqual(caught.exception.code,404)

@unittest.skipUnless(test_xray.BINARY.is_file(),'Real Xray required')
class ScopeRuntimeTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_xray.XrayRealTests.setUp
    tearDown=test_xray.XrayRealTests.tearDown
    asyncSetUp=test_user_bandwidth.UserBandwidthRealTests.asyncSetUp
    asyncTearDown=test_xray.XrayRealTests.asyncTearDown
    request=test_xray.XrayRealTests.request
    open=test_user_connections.ConnectionRealTests.open
    close_writer=test_user_connections.ConnectionRealTests.close_writer
    accepted=test_user_connections.ConnectionRealTests.accepted
    rejected=test_user_connections.ConnectionRealTests.rejected

    async def test_range_shrink_rejects_old_uuid_and_other_identity_reconnects(self):
        r=self.server.registry; p=plan(r); r.assign_plan(self.uid,{'plan_id':p})
        other=r.save('users',{'name':'other'})['id']; auth=r.authorize_node_user(self.nid,other)
        await self.runtime.apply(r.configuration(self.token))
        reader,_=await self.open(); await self.accepted(reader)
        PlanNodes(r).save(p,{'node_ids':[]}); await self.runtime.apply(r.configuration(self.token))
        reader,_=await self.open(); await self.rejected(reader)
        reader,_=await self.open(auth['uuid']); await self.accepted(reader)
