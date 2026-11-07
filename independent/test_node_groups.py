import concurrent.futures
import json
import sqlite3
import unittest
import urllib.error
import urllib.request
from core import Registry,Error
from accounts import Accounts
from node_groups import NodeGroups
from plan_nodes import PlanNodes
import test_core
import test_plan_nodes
import test_xray
from test_accounts import plan

class GroupsTests(unittest.TestCase):
    setUp=test_core.RegistryTests.setUp
    tearDown=test_core.RegistryTests.tearDown
    fixtures=test_plan_nodes.ScopeTests.fixtures

    def test_union_tracks_group_edits_without_granting_new_credentials(self):
        r,parent,p,nodes,tokens=self.fixtures(); groups=NodeGroups(r); scopes=PlanNodes(r)
        g=groups.save(dict(name='region',node_ids=[nodes[0]]))
        g2=groups.save(dict(name='backup',node_ids=nodes))
        value=scopes.save(p,dict(node_ids=[],group_ids=[g['id'],g2['id']]))
        self.assertEqual(value['node_ids'],nodes); self.assertEqual(value['direct_node_ids'],[])
        child=Accounts(r).create(parent,dict(name='child',plan_id=p,node_ids=nodes))
        before=[r.configuration(t) for t in tokens]
        groups.save(dict(name='backup',node_ids=[]),g2['id'])
        self.assertEqual([n['node_id'] for n in Accounts(r).list(parent)[0]['nodes']],[nodes[0]])
        self.assertEqual(r.configuration(tokens[0]),before[0])
        self.assertEqual(r.configuration(tokens[1])['revision'],before[1]['revision']+1)
        groups.save(dict(name='region',node_ids=nodes),g['id'])
        self.assertEqual(len(Accounts(r).list(parent)[0]['nodes']),1)
        r.authorize_node_user(nodes[1],child['user_id'])
        self.assertEqual(NodeGroups(Registry(r.path)).get(),groups.get())

    def test_direct_overlap_and_other_groups_prevent_unnecessary_revocation(self):
        r,parent,p,nodes,tokens=self.fixtures(); groups=NodeGroups(r); scopes=PlanNodes(r)
        g=groups.save(dict(name='all',node_ids=nodes)); scopes.save(p,dict(node_ids=[nodes[0]],group_ids=[g['id']]))
        child=Accounts(r).create(parent,dict(name='child',plan_id=p,node_ids=nodes))
        before=r.configuration(tokens[0]); groups.save(dict(name='renamed',node_ids=[nodes[1]]),g['id'])
        self.assertEqual(r.configuration(tokens[0]),before)
        self.assertEqual(Accounts(r).list(parent)[0]['nodes'],child['nodes'])
        groups.delete(g['id']); self.assertEqual(scopes.get(p)['node_ids'],[nodes[0]])
        self.assertEqual(Accounts(r).list(parent)[0]['nodes'],[child['nodes'][0]])

    def test_shared_group_multiple_plans_versions_each_host_once_and_delete_denies_all(self):
        r,parent,p,nodes,tokens=self.fixtures(); groups=NodeGroups(r); scope=PlanNodes(r)
        g=groups.save(dict(name='shared',node_ids=nodes))
        for pid in (p,plan(r,'second')):
            scope.save(pid,dict(node_ids=[],group_ids=[g['id']]))
            Accounts(r).create(parent,dict(name=str(pid),plan_id=pid,node_ids=nodes))
        before=[r.configuration(t) for t in tokens]; groups.delete(g['id'])
        after=[r.configuration(t) for t in tokens]
        for a,b in zip(after,before):
            self.assertEqual(a['revision'],b['revision']+1); self.assertEqual(a['nodes'][0]['clients'],[])
        for a in Accounts(r).list(parent):
            self.assertEqual(scope.get(a['plan_id']),dict(plan_id=a['plan_id'],restricted=True,node_ids=[]))
            with self.assertRaises(Error): r.authorize_node_user(nodes[0],a['user_id'])

    def test_invalid_names_members_and_group_binding_are_atomic(self):
        r,parent,p,nodes,tokens=self.fixtures(); groups=NodeGroups(r); scope=PlanNodes(r)
        g=groups.save(dict(name='group',node_ids=nodes))
        for data in (dict(name='group',node_ids=[]),dict(name='',node_ids=[]),dict(name='bad',node_ids=[True]),dict(name='bad',node_ids=[999]),dict(name='bad',node_ids=nodes,extra=1)):
            with self.assertRaises(Error): groups.save(data)
        for ids in ([999],[True],[g['id'],g['id']]):
            with self.assertRaises(Error): scope.save(p,dict(node_ids=[],group_ids=ids))
            self.assertFalse(scope.get(p)['restricted'])
        with self.assertRaises(Error): groups.save(dict(name='missing',node_ids=[]),999)
        r.delete('nodes',nodes[0]); self.assertEqual(groups.get(g['id'])['node_ids'],[nodes[1]])

    def test_revision_failure_rolls_back_group_and_all_plan_grants(self):
        r,parent,p,nodes,tokens=self.fixtures(); groups=NodeGroups(r)
        g=groups.save(dict(name='group',node_ids=nodes)); PlanNodes(r).save(p,dict(node_ids=[],group_ids=[g['id']]))
        child=Accounts(r).create(parent,dict(name='child',plan_id=p,node_ids=nodes)); before=[r.configuration(t) for t in tokens]
        with r.connect() as db: db.execute("CREATE TRIGGER fail_group BEFORE UPDATE ON server_configs BEGIN SELECT RAISE(ABORT,'fixture'); END")
        for action in (lambda:groups.save(dict(name='changed',node_ids=[]),g['id']),lambda:groups.delete(g['id'])):
            with self.assertRaises(sqlite3.IntegrityError): action()
            self.assertEqual(groups.get(g['id']),g); self.assertEqual(Accounts(r).list(parent),[child])
            self.assertEqual([r.configuration(t) for t in tokens],before)

    def test_legacy_database_upgrade_and_scope_clear_preserve_unrelated_state(self):
        r,parent,p,nodes,tokens=self.fixtures(); r.assign_plan(parent,{'plan_id':p})
        auth=r.authorize_node_user(nodes[0],parent); before=[r.configuration(t) for t in tokens]
        with r.connect() as db:
            db.execute('DROP TABLE plan_node_groups'); db.execute('DROP TABLE node_group_members'); db.execute('DROP TABLE node_groups')
        upgraded=Registry(r.path); groups=NodeGroups(upgraded)
        self.assertEqual(groups.get(),[]); self.assertEqual([upgraded.configuration(t) for t in tokens],before)
        g=groups.save(dict(name='legacy',node_ids=nodes)); scope=PlanNodes(upgraded)
        scope.save(p,dict(node_ids=[],group_ids=[g['id']]))
        groups.save(dict(name='rename-only',node_ids=nodes),g['id'])
        self.assertEqual(upgraded.authorize_node_user(nodes[0],parent),auth)
        self.assertEqual([upgraded.configuration(t) for t in tokens],before)
        scope.clear(p); groups.save(dict(name='empty-unbound',node_ids=[]),g['id'])
        self.assertEqual([upgraded.configuration(t) for t in tokens],before)

    def test_delegated_nodes_are_rejected_without_changing_group(self):
        r,parent,p,nodes,tokens=self.fixtures(); groups=NodeGroups(r)
        from sharing import Shares
        from test_sharing import settings
        with r.connect() as db: sid=db.execute('SELECT server_id FROM nodes WHERE id=?',(nodes[0],)).fetchone()[0]
        shares=Shares(r); invitation=shares.create(sid,settings())
        token=shares.claim(invitation['invite_token'])['access_token']
        guest=shares.guest(token,'create',data={'name':'delegated','config':{**test_xray.VLESS,'listen_port':24502}})
        g=groups.save(dict(name='owner',node_ids=nodes))
        with self.assertRaises(Error): groups.save(dict(name='changed',node_ids=[guest['id']]),g['id'])
        self.assertEqual(groups.get(),[g])

    def test_concurrent_group_shrink_cannot_leave_outside_credentials(self):
        r,parent,p,nodes,tokens=self.fixtures(); groups=NodeGroups(r)
        g=groups.save(dict(name='group',node_ids=nodes)); PlanNodes(r).save(p,dict(node_ids=[],group_ids=[g['id']]))
        r.assign_plan(parent,{'plan_id':p})
        def grant():
            try: r.authorize_node_user(nodes[0],parent)
            except Error: pass
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            a=pool.submit(grant); b=pool.submit(groups.save,dict(name='group',node_ids=[]),g['id']); a.result(); b.result()
        self.assertEqual(r.configuration(tokens[0])['nodes'][0]['clients'],[])

class GroupsApiTests(unittest.TestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown
    request=test_core.ApiTests.request

    def test_admin_crud_plan_binding_and_credential_boundaries(self):
        base='/api/node-groups'; r=self.server.registry; p=plan(r)
        for method,data in (('GET',None),('POST',dict(name='x',node_ids=[]))):
            with self.assertRaises(urllib.error.HTTPError) as caught: self.request(base,method,data,auth=False)
            self.assertEqual(caught.exception.code,401)
        g=json.load(self.request(base,'POST',dict(name='g',node_ids=[]))); path=base+'/'+str(g['id'])
        token=r.provision_agent(r.save('servers',{'name':'agent'})['id'])['token']
        for method,data in (('GET',None),('PUT',dict(name='wrong',node_ids=[])),('DELETE',None)):
            req=urllib.request.Request(self.base+path,method=method,data=json.dumps(data).encode() if data else None,headers={'Authorization':'Bearer '+token})
            with self.assertRaises(urllib.error.HTTPError) as caught: self.opener.open(req,timeout=3)
            self.assertEqual(caught.exception.code,401)
        self.assertEqual(json.load(self.request(base)),[g]); self.assertEqual(json.load(self.request(path)),g)
        self.assertEqual(json.load(self.request(path,'PUT',dict(name='renamed',node_ids=[])))['name'],'renamed')
        scope=json.load(self.request(f'/api/plans/{p}/nodes','PUT',dict(node_ids=[],group_ids=[g['id']])))
        self.assertEqual(scope['group_ids'],[g['id']]); self.request(path,'DELETE')
        self.assertTrue(json.load(self.request(f'/api/plans/{p}/nodes'))['restricted'])
        with self.assertRaises(urllib.error.HTTPError) as caught: self.request(path)
        self.assertEqual(caught.exception.code,404)

@unittest.skipUnless(test_xray.BINARY.is_file(),'Real Xray required')
class GroupsRuntimeTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_plan_nodes.ScopeRuntimeTests.setUp
    tearDown=test_plan_nodes.ScopeRuntimeTests.tearDown
    asyncSetUp=test_plan_nodes.ScopeRuntimeTests.asyncSetUp
    asyncTearDown=test_plan_nodes.ScopeRuntimeTests.asyncTearDown
    request=test_plan_nodes.ScopeRuntimeTests.request
    open=test_plan_nodes.ScopeRuntimeTests.open
    close_writer=test_plan_nodes.ScopeRuntimeTests.close_writer
    accepted=test_plan_nodes.ScopeRuntimeTests.accepted
    rejected=test_plan_nodes.ScopeRuntimeTests.rejected

    async def test_group_edit_rejects_old_uuid_and_other_user_can_reconnect(self):
        r=self.server.registry; p=plan(r); r.assign_plan(self.uid,{'plan_id':p})
        groups=NodeGroups(r); g=groups.save(dict(name='live',node_ids=[self.nid]))
        PlanNodes(r).save(p,dict(node_ids=[],group_ids=[g['id']]))
        other=r.save('users',{'name':'other'})['id']; auth=r.authorize_node_user(self.nid,other)
        await self.runtime.apply(r.configuration(self.token)); reader,_=await self.open(); await self.accepted(reader)
        groups.save(dict(name='live',node_ids=[]),g['id'])
        await self.runtime.apply(r.configuration(self.token)); reader,_=await self.open(); await self.rejected(reader)
        reader,_=await self.open(auth['uuid']); await self.accepted(reader)
