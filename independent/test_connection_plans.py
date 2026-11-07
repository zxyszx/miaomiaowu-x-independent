"""Connection-policy inheritance must change both snapshots and real admission."""
import asyncio
import json
import sqlite3
import unittest
import urllib.error

from core import Registry
import test_core
import test_xray
import test_user_connections


def limit(value): return {'max_connections':value}
def plan_data(value):
    return dict(name='standard',bandwidth={'upload_bps':None,'download_bps':None},connections=limit(value))


class ConnectionPlanTests(unittest.TestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown
    request=test_core.ApiTests.request

    def fixtures(self):
        r=self.server.registry
        uid=r.save('users',{'name':'one'})['id']
        sid=r.save('servers',{'name':'one'})['id']
        nid=r.save('nodes',{'name':'one','server_id':sid})['id']
        r.configure_node(nid,test_xray.VLESS); r.authorize_node_user(nid,uid)
        return r,uid,r.provision_agent(sid)['token']

    def test_inherit_explicit_unlimited_clear_switch_and_restart(self):
        r,uid,token=self.fixtures()
        path=f'/api/users/{uid}/connections'
        policy=f'/api/users/{uid}/connections-policy'
        self.assertEqual(json.load(self.request(policy)),dict(source='default',plan_id=None,override=None,effective=limit(None)))
        plan=json.load(self.request('/api/plans','POST',plan_data(1)))
        self.assertEqual(plan['connections'],limit(1))
        r.assign_plan(uid,{'plan_id':plan['id']})
        self.assertEqual(json.load(self.request(path)),limit(1))
        self.assertEqual(json.load(self.request(policy))['source'],'plan')
        self.assertEqual(r.configuration(token)['nodes'][0]['clients'][0]['connections'],limit(1))
        json.load(self.request(path,'PUT',limit(None)))
        self.assertEqual(json.load(self.request(policy))['source'],'user')
        self.assertNotIn('connections',r.configuration(token)['nodes'][0]['clients'][0])
        r.save_plan(plan_data(2),plan['id'])
        self.assertEqual(json.load(self.request(path)),limit(None))
        self.assertEqual(json.load(self.request(path,'DELETE')),limit(2))
        other=r.save_plan(plan_data(3)); r.assign_plan(uid,{'plan_id':other['id']})
        self.assertEqual(r.user_connections(uid),limit(3))
        restored=Registry(r.path)
        self.assertEqual(restored.user_connections(uid),limit(3))
        self.assertEqual(restored.plans()[1]['connections'],limit(3))
        restored.assign_plan(uid,{'plan_id':None})
        self.assertEqual(restored.user_connections(uid),limit(None))

    def test_revision_scope_ignores_masked_users_and_preserves_missing_field_on_update(self):
        r,uid,token=self.fixtures()
        plan=r.save_plan(plan_data(1)); r.assign_plan(uid,{'plan_id':plan['id']})
        old=r.configuration(token)
        r.save_plan(plan_data(2),plan['id'])
        self.assertEqual(r.configuration(token)['revision'],old['revision']+1)
        r.user_connections(uid,limit(None)); old=r.configuration(token)
        r.save_plan(plan_data(3),plan['id'])
        self.assertEqual(r.configuration(token),old)
        # Legacy bandwidth-only writers must not silently remove connection rules.
        value=plan_data(4); value.pop('connections'); value['name']='renamed'
        updated=r.save_plan(value,plan['id'])
        self.assertEqual(updated['connections'],limit(3))
        self.assertEqual(r.configuration(token),old)
        r.user_connections(uid,inherit=True)
        self.assertEqual(r.configuration(token)['nodes'][0]['clients'][0]['connections'],limit(3))

    def test_combined_plan_changes_bump_each_affected_server_once(self):
        r,uid,token=self.fixtures()
        second=r.save('servers',{'name':'second'})['id']
        node=r.save('nodes',{'name':'second','server_id':second})['id']
        r.configure_node(node,test_xray.VLESS); r.authorize_node_user(node,uid)
        second_token=r.provision_agent(second)['token']
        masked=r.save('users',{'name':'masked'})['id']
        r.authorize_node_user(node,masked)
        plan=r.save_plan(plan_data(1))
        for user in (uid,masked): r.assign_plan(user,{'plan_id':plan['id']})
        r.user_connections(masked,limit(None))
        r.user_bandwidth(masked,{'upload_bps':None,'download_bps':None})
        old=[r.configuration(t) for t in (token,second_token)]
        value=plan_data(2); value['bandwidth']['download_bps']=32768
        r.save_plan(value,plan['id'])
        new=[r.configuration(t) for t in (token,second_token)]
        for before,after in zip(old,new): self.assertEqual(after['revision'],before['revision']+1)
        clients={c['user_id']:c for c in new[1]['nodes'][0]['clients']}
        self.assertEqual(clients[uid]['connections'],limit(2))
        self.assertNotIn('connections',clients[masked])
        # Removing the plan restriction changes inherited users only.
        value['connections']=limit(None)
        r.save_plan(value,plan['id'])
        for t in (token,second_token):
            self.assertTrue(all('connections' not in c for c in r.configuration(t)['nodes'][0]['clients']))
        r.assign_plan(uid,{'plan_id':None}); r.assign_plan(masked,{'plan_id':None})
        r.delete_plan(plan['id'])
        with r.connect() as db: self.assertEqual(db.execute('SELECT count(*) FROM plan_connections').fetchone()[0],0)

    def test_policy_auth_validation_and_unknown_user(self):
        r,uid,_=self.fixtures()
        path=f'/api/users/{uid}/connections'
        for method,suffix in (('GET','-policy'),('DELETE','')):
            with self.assertRaises(urllib.error.HTTPError) as caught: self.request(path+suffix,method,auth=False)
            self.assertEqual(caught.exception.code,401)
        for value in ({},None,{'max_connections':True},limit(0)):
            data=plan_data(1); data['connections']=value
            with self.assertRaises(urllib.error.HTTPError) as caught: self.request('/api/plans','POST',data)
            self.assertEqual(caught.exception.code,400)
        with self.assertRaises(urllib.error.HTTPError) as caught: self.request('/api/users/999/connections-policy')
        self.assertEqual(caught.exception.code,404)

    def test_plan_write_and_override_clear_roll_back_with_revision_failure(self):
        r,uid,token=self.fixtures()
        plan=r.save_plan(plan_data(1)); r.assign_plan(uid,{'plan_id':plan['id']})
        before=r.configuration(token)
        with r.connect() as db:
            db.execute("CREATE TRIGGER fail_revision BEFORE UPDATE ON server_configs BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): r.save_plan(plan_data(2),plan['id'])
        self.assertEqual(r.plans()[0],plan)
        self.assertEqual(r.configuration(token),before)
        with r.connect() as db: db.execute('DROP TRIGGER fail_revision')
        r.user_connections(uid,limit(None)); before=r.configuration(token)
        with r.connect() as db:
            db.execute("CREATE TRIGGER fail_revision BEFORE UPDATE ON server_configs BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): r.user_connections(uid,inherit=True)
        self.assertEqual(r.user_connections(uid),limit(None))
        self.assertEqual(r.configuration(token),before)


@unittest.skipUnless(test_xray.BINARY.is_file(),'Real Xray required')
class ConnectionPlanRealTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_user_connections.ConnectionRealTests.setUp
    tearDown=test_user_connections.ConnectionRealTests.tearDown
    request=test_user_connections.ConnectionRealTests.request
    asyncSetUp=test_user_connections.ConnectionRealTests.asyncSetUp
    asyncTearDown=test_user_connections.ConnectionRealTests.asyncTearDown
    open=test_user_connections.ConnectionRealTests.open
    close_writer=test_user_connections.ConnectionRealTests.close_writer
    accepted=test_user_connections.ConnectionRealTests.accepted
    rejected=test_user_connections.ConnectionRealTests.rejected

    async def test_switch_override_clear_and_plan_update_change_real_connections(self):
        r=self.server.registry
        one=r.save_plan(plan_data(1)); two=r.save_plan(plan_data(2))
        async def sync(): await self.runtime.apply(r.configuration(self.token))
        r.assign_plan(self.uid,{'plan_id':one['id']}); await sync()
        first,_=await self.open(); await self.accepted(first)
        extra,_=await self.open(); await self.rejected(extra)
        r.assign_plan(self.uid,{'plan_id':two['id']}); await sync()
        await self.rejected(first)
        streams=await asyncio.gather(*(self.open() for _ in range(2)))
        for reader,_ in streams: await self.accepted(reader)
        extra,_=await self.open(); await self.rejected(extra)
        r.user_connections(self.uid,limit(None)); await sync()
        r.save_plan(plan_data(1),two['id']); await sync() # masked override remains unlimited
        streams=await asyncio.gather(*(self.open() for _ in range(3)))
        for reader,_ in streams: await self.accepted(reader)
        r.user_connections(self.uid,inherit=True); await sync()
        for reader,_ in streams: await self.rejected(reader)
        first,_=await self.open(); await self.accepted(first)
        extra,_=await self.open(); await self.rejected(extra)
