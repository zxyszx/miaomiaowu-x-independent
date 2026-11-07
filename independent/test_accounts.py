"""Per-plan credentials must stay independent through updates and removal."""
import asyncio
import concurrent.futures
import json
import sqlite3
import unittest
import urllib.error
import urllib.request
from core import Registry,Error
from accounts import Accounts
from node_config import validate_snapshot
import test_core
import test_xray
import test_user_connections
import test_user_bandwidth
from sharing import Shares
from test_sharing import settings


def plan(r,name='one',rate=32768,maximum=1):
    return r.save_plan(dict(name=name,bandwidth=dict(upload_bps=rate,download_bps=None),connections={'max_connections':maximum}))['id']

class AccountTests(unittest.TestCase):
    setUp=test_core.RegistryTests.setUp
    tearDown=test_core.RegistryTests.tearDown
    def fixtures(self):
        r=self.registry; parent=r.save('users',{'name':'parent'})['id']
        sid=r.save('servers',{'name':'host'})['id']; nid=r.save('nodes',{'name':'node','server_id':sid})['id']
        r.configure_node(nid,test_xray.VLESS)
        return r,parent,nid,r.provision_agent(sid)['token']
    def denied(self,status,call):
        with self.assertRaises(Error) as caught: call()
        self.assertEqual(caught.exception.status,status)
    def create(self,parent,p,nodes):
        return Accounts(self.registry).create(parent,dict(name='child',plan_id=p,node_ids=nodes))

    def test_multiple_plan_identities_persist_isolate_rules_and_existing_parent(self):
        r,parent,nid,token=self.fixtures(); a=plan(r); b=plan(r,'two',65536,2)
        r.assign_plan(parent,{'plan_id':a}); old=r.authorize_node_user(nid,parent)
        child=self.create(parent,b,[nid]); uid=child['user_id']
        self.assertEqual(child['parent_id'],parent); self.assertEqual(child['bandwidth']['plan_id'],b)
        self.assertNotEqual(child['nodes'][0]['uuid'],old['uuid'])
        self.assertEqual(Accounts(Registry(r.path)).list(parent),[child])
        r.user_bandwidth(parent,dict(upload_bps=1,download_bps=None))
        self.assertEqual(r.user_bandwidth(uid)['upload_bps'],65536)
        r.save_plan(dict(name='two',bandwidth=dict(upload_bps=12345,download_bps=None),connections={'max_connections':3}),b)
        clients={c['user_id']:c for c in validate_snapshot(r.configuration(token))['nodes'][0]['clients']}
        self.assertEqual(clients[uid]['bandwidth']['upload_bps'],12345)
        self.assertEqual(clients[uid]['connections'],{'max_connections':3})
        self.assertEqual(clients[parent]['bandwidth']['upload_bps'],1)
        r.user_connections(uid,{'max_connections':None})
        self.assertNotIn('connections',r.configuration(token)['nodes'][0]['clients'][1])
        r.user_connections(uid,inherit=True)
        self.assertEqual(r.user_connections(uid),{'max_connections':3})

    def test_parent_and_plan_lifecycle_and_wrong_parent_scope(self):
        r,parent,nid,token=self.fixtures(); p=plan(r); child=self.create(parent,p,[nid]); uid=child['user_id']
        other=r.save('users',{'name':'other'})['id']
        self.denied(404,lambda:Accounts(r).delete(other,uid))
        self.denied(409,lambda:r.delete('users',parent))
        self.denied(409,lambda:r.delete_plan(p))
        self.denied(409,lambda:r.assign_plan(uid,{'plan_id':None}))
        self.denied(409,lambda:r.assign_plan(parent,{'plan_id':p}))
        self.denied(409,lambda:self.create(uid,p,[]))
        self.denied(409,lambda:self.create(parent,p,[]))
        before=r.configuration(token); Accounts(r).delete(parent,uid)
        after=r.configuration(token); self.assertGreater(after['revision'],before['revision'])
        self.assertEqual(after['nodes'][0]['clients'],[])
        r.delete_plan(p); r.delete('users',parent)

    def test_invalid_targets_quota_and_failure_are_atomic(self):
        r,parent,nid,token=self.fixtures(); p=plan(r); before=r.configuration(token)
        data=dict(name='child',plan_id=p,node_ids=[nid])
        for changes in ({'plan_id':True},{'node_ids':[True]},{'node_ids':[nid,nid]},{'node_ids':[999]},{'extra':1},{'name':''}):
            value={**data,**changes}
            with self.assertRaises(Error): Accounts(r).create(parent,value)
            self.assertEqual(len(r.list('users')),1)
        r.set_quota('users',1); self.denied(409,lambda:self.create(parent,p,[nid]))
        r.set_quota('users',None)
        with r.connect() as db:
            db.execute("CREATE TRIGGER fail_bump BEFORE UPDATE ON server_configs BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.create(parent,p,[nid])
        self.assertEqual(r.configuration(token),before); self.assertEqual(Accounts(r).list(parent),[])
        self.assertEqual(len(r.list('users')),1)

    def test_empty_authorization_explicit_and_generic_child_delete_releases_mapping(self):
        r,parent,nid,token=self.fixtures(); p=plan(r); before=r.configuration(token)
        child=self.create(parent,p,[]); self.assertEqual(child['nodes'],[])
        self.assertEqual(r.configuration(token),before)
        r.delete('users',child['user_id']); self.assertEqual(Accounts(r).list(parent),[])
        self.assertEqual(r.quotas()['users']['used'],1)
        self.create(parent,p,[nid])

    def test_shared_resources_cannot_hide_or_orphan_plan_identities(self):
        r,parent,nid,token=self.fixtures(); p=plan(r)
        with r.connect() as db: sid=db.execute('SELECT server_id FROM nodes WHERE id=?',(nid,)).fetchone()[0]
        shares=Shares(r); invite=shares.create(sid,settings()); access=shares.claim(invite['invite_token'])['access_token']
        node=shares.guest(access,'create',data=dict(name='guest',config={**test_xray.VLESS,'listen_port':24501}))
        self.denied(409,lambda:self.create(parent,p,[node['id']]))
        shares.guest(access,'authorize',node['id'],reference='guest')
        with r.connect() as db: guest=db.execute('SELECT user_id FROM shared_users').fetchone()[0]
        self.denied(409,lambda:self.create(guest,p,[]))
        self.assertEqual(Accounts(r).list(parent),[])

    def test_concurrent_duplicate_and_user_quota_do_not_leak_resources(self):
        r,parent,nid,token=self.fixtures(); p=plan(r)
        def attempt(_):
            try: return self.create(parent,p,[nid])['user_id']
            except Error as e: return -e.status
        with concurrent.futures.ThreadPoolExecutor(4) as pool: result=list(pool.map(attempt,range(4)))
        self.assertEqual(result.count(-409),3); self.assertEqual(len(r.list('users')),2)
        self.assertEqual(len(r.configuration(token)['nodes'][0]['clients']),1)

    def test_concurrent_different_plans_respect_global_quota(self):
        r,parent,nid,token=self.fixtures(); plans=[plan(r,str(n)) for n in range(4)]
        r.set_quota('users',3)
        def attempt(p):
            try: return self.create(parent,p,[nid])['user_id']
            except Error as e: return -e.status
        with concurrent.futures.ThreadPoolExecutor(4) as pool: result=list(pool.map(attempt,plans))
        self.assertEqual(result.count(-409),2)
        self.assertEqual(len(Accounts(r).list(parent)),2)
        self.assertEqual(r.quotas()['users']['used'],3)
        self.assertEqual(len(r.configuration(token)['nodes'][0]['clients']),2)

    def test_edit_node_set_preserves_existing_credentials_and_versions_changed_hosts(self):
        r,parent,nid,token=self.fixtures(); p=plan(r); accounts=Accounts(r)
        child=self.create(parent,p,[nid]); uid=child['user_id']; original=child['nodes'][0]['uuid']
        sid2=r.save('servers',{'name':'second'})['id']; token2=r.provision_agent(sid2)['token']
        second=r.save('nodes',{'name':'second','server_id':sid2})['id']
        r.configure_node(second,{**test_xray.VLESS,'listen_port':24502})
        before=r.configuration(token); other=r.configuration(token2)
        updated=accounts.update(parent,uid,dict(name='renamed',node_ids=[second,nid]))
        self.assertEqual(updated['name'],'renamed'); self.assertEqual(updated['nodes'][0]['uuid'],original)
        self.assertEqual(r.configuration(token),before)
        self.assertEqual(r.configuration(token2)['revision'],other['revision']+1)
        unchanged=r.configuration(token2)
        accounts.update(parent,uid,dict(name='renamed-again',node_ids=[nid,second]))
        self.assertEqual(r.configuration(token2),unchanged)
        accounts.update(parent,uid,dict(name='only-second',node_ids=[second]))
        self.assertEqual(r.configuration(token)['nodes'][0]['clients'],[])
        self.assertEqual(r.configuration(token)['revision'],before['revision']+1)
        self.assertEqual(r.configuration(token2),unchanged)
        restored=accounts.update(parent,uid,dict(name='restored',node_ids=[nid,second]))
        self.assertNotEqual(restored['nodes'][0]['uuid'],original)
        self.assertEqual(restored['nodes'][1]['uuid'],updated['nodes'][1]['uuid'])

    def test_edit_invalid_scope_or_failed_publication_rolls_back(self):
        r,parent,nid,token=self.fixtures(); p=plan(r); accounts=Accounts(r)
        child=self.create(parent,p,[nid]); uid=child['user_id']; before=r.configuration(token)
        another=r.save('users',{'name':'other-parent'})['id']
        self.denied(404,lambda:accounts.update(another,uid,dict(name='wrong',node_ids=[])))
        for nodes in ([nid,999],[True],[nid,nid]):
            with self.assertRaises(Error): accounts.update(parent,uid,dict(name='bad',node_ids=nodes))
        with self.assertRaises(Error): accounts.update(parent,uid,dict(name='bad',node_ids=[],plan_id=p))
        self.assertEqual(accounts.list(parent),[child]); self.assertEqual(r.configuration(token),before)
        with r.connect() as db:
            db.execute("CREATE TRIGGER fail_edit_bump BEFORE UPDATE ON server_configs BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): accounts.update(parent,uid,dict(name='rollback',node_ids=[]))
        self.assertEqual(accounts.list(parent),[child]); self.assertEqual(r.configuration(token),before)

class AccountApiTests(unittest.TestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown
    request=test_core.ApiTests.request
    def test_admin_only_crud_with_real_http_and_no_node_implicit_grant(self):
        r=self.server.registry; parent=r.save('users',{'name':'parent'})['id']; p=plan(r)
        path=f'/api/users/{parent}/accounts'
        data=dict(name='child',plan_id=p,node_ids=[])
        with self.assertRaises(urllib.error.HTTPError) as caught: self.request(path,'POST',data,auth=False)
        self.assertEqual(caught.exception.code,401)
        sid=r.save('servers',{'name':'agent-host'})['id']
        token=r.provision_agent(sid)['token']
        request=urllib.request.Request(self.base+path,headers={'Authorization':'Bearer '+token})
        with self.assertRaises(urllib.error.HTTPError) as caught: self.opener.open(request,timeout=3)
        self.assertEqual(caught.exception.code,401)
        child=json.load(self.request(path,'POST',data)); self.assertEqual(json.load(self.request(path)),[child])
        updated=json.load(self.request(path+'/'+str(child['user_id']),'PUT',dict(name='edited',node_ids=[])))
        self.assertEqual(updated['name'],'edited')
        json.load(self.request(path+'/'+str(child['user_id']),'DELETE'))
        self.assertEqual(json.load(self.request(path)),[])

@unittest.skipUnless(test_xray.BINARY.is_file(),'Real Xray required')
class AccountRuntimeTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_xray.XrayRealTests.setUp
    tearDown=test_xray.XrayRealTests.tearDown
    request=test_xray.XrayRealTests.request
    asyncSetUp=test_user_bandwidth.UserBandwidthRealTests.asyncSetUp
    asyncTearDown=test_xray.XrayRealTests.asyncTearDown
    transfer=test_user_bandwidth.UserBandwidthRealTests.transfer
    open=test_user_connections.ConnectionRealTests.open
    close_writer=test_user_connections.ConnectionRealTests.close_writer
    accepted=test_user_connections.ConnectionRealTests.accepted
    rejected=test_user_connections.ConnectionRealTests.rejected
    async def test_actual_plan_caps_independent_credentials_update_and_delete(self):
        r=self.server.registry; accounts=Accounts(r)
        a=plan(r,'limited',32768,1); b=plan(r,'fast',None,2)
        first=accounts.create(self.uid,dict(name='one',plan_id=a,node_ids=[self.nid]))
        second=accounts.create(self.uid,dict(name='two',plan_id=b,node_ids=[self.nid]))
        c1=first['nodes'][0]['uuid']; c2=second['nodes'][0]['uuid']
        await self.runtime.apply(r.configuration(self.token))
        # A slow plan must not throttle its sibling plan, nor share its admission cap.
        elapsed=await self.transfer(c1); self.assertGreaterEqual(elapsed,.48)
        self.assertLess(await self.transfer(c2),.4)
        async def released():
            while self.runtime.task_users: await asyncio.sleep(.02)
        # Xray may retain the remote half-close for about five seconds.
        # Require observed release rather than retrying a denied admission.
        await asyncio.wait_for(released(),8)
        reader,w=await self.open(c1); await self.accepted(reader)
        denied,_=await self.open(c1); await self.rejected(denied)
        sibling,sw=await self.open(c2); await self.accepted(sibling)
        await self.close_writer(w); await self.close_writer(sw)
        accounts.delete(self.uid,first['user_id']); await self.runtime.apply(r.configuration(self.token))
        denied,_=await self.open(c1); await self.rejected(denied)
        sibling,_=await self.open(c2); await self.accepted(sibling)
        accounts.delete(self.uid,second['user_id'])
        await self.runtime.apply(r.configuration(self.token))
        parent,_=await self.open(); await self.accepted(parent)
