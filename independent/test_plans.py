import asyncio
import concurrent.futures
import json
import socket
import time
import unittest
import urllib.error
import urllib.request
from core import Error, Registry
from node_config import validate_snapshot
import test_core
import test_xray
import test_user_bandwidth


def rule(upload=None,download=None):
    return dict(upload_bps=upload,download_bps=download)


class PlanApiTests(unittest.TestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown
    request=test_core.ApiTests.request

    def user(self,name='user'):
        return self.server.registry.save('users',{'name':name})['id']

    def plan(self,limits=None):
        return json.load(self.request('/api/plans','POST',dict(name='standard',bandwidth=limits or rule(upload=32768))))

    def assign(self,user,plan):
        return json.load(self.request(f'/api/users/{user}/plan','PUT',dict(plan_id=plan)))

    def policy(self,user):
        return json.load(self.request(f'/api/users/{user}/bandwidth-policy'))

    def test_admin_auth_strict_inputs_and_no_implicit_node_authorization(self):
        user=self.user()
        registry=self.server.registry
        server=registry.save('servers',{'name':'server'})['id']
        token=registry.provision_agent(server)['token']
        node=registry.save('nodes',{'name':'node','server_id':server})['id']
        registry.configure_node(node,test_xray.VLESS)
        for path,method,data in (('/api/plans','GET',None),('/api/plans','POST',dict(name='x',bandwidth=rule())),
                                 (f'/api/users/{user}/plan','PUT',dict(plan_id=None)),
                                 (f'/api/users/{user}/bandwidth','DELETE',None),
                                 (f'/api/users/{user}/bandwidth-policy','GET',None)):
            with self.assertRaises(urllib.error.HTTPError) as caught: self.request(path,method,data,auth=False)
            self.assertEqual(caught.exception.code,401)
            req=urllib.request.Request(self.base+path,method=method,headers={'Authorization':'Bearer '+token},
                                       data=json.dumps(data).encode() if data is not None else None)
            with self.assertRaises(urllib.error.HTTPError) as caught: self.opener.open(req,timeout=5)
            self.assertEqual(caught.exception.code,401)
        for bad in ({},dict(name=''),dict(name='x',bandwidth=rule(upload=True)),dict(name='x',bandwidth={}),
                    dict(name='x',bandwidth=rule(),role='admin')):
            with self.assertRaises(urllib.error.HTTPError): self.request('/api/plans','POST',bad)
        plan=self.plan()
        for bad in ({},dict(plan_id=True),dict(plan_id=0),dict(plan_id='1'),dict(plan_id=plan['id'],other=True)):
            with self.assertRaises(urllib.error.HTTPError): self.request(f'/api/users/{user}/plan','PUT',bad)
        with self.assertRaises(urllib.error.HTTPError) as caught: self.assign(user,99999)
        self.assertEqual(caught.exception.code,404)
        with self.assertRaises(urllib.error.HTTPError): self.assign(99999,plan['id'])
        self.assign(user,plan['id'])
        self.assertEqual(registry.configuration(token)['nodes'][0]['clients'],[])
        self.assertEqual(json.load(self.request('/api/plans')),[plan])

    def test_override_clear_and_explicit_unlimited_have_distinct_semantics(self):
        user=self.user(); plan=self.plan()
        self.assertEqual(self.policy(user),dict(source='default',plan_id=None,override=None,effective=rule()))
        self.assertEqual(self.assign(user,plan['id'])['source'],'plan')
        path=f'/api/users/{user}/bandwidth'
        self.assertEqual(json.load(self.request(path)),plan['bandwidth'])
        self.assertEqual(json.load(self.request(path,'PUT',rule())),rule())
        policy=self.policy(user)
        self.assertEqual(policy['source'],'user')
        self.assertEqual(policy['plan_id'],plan['id'])
        self.assertEqual(policy['override'],rule())
        self.assertEqual(json.load(self.request(path,'DELETE')),plan['bandwidth'])
        self.assertEqual(self.policy(user)['source'],'plan')
        self.assign(user,None)
        self.assertEqual(self.policy(user)['source'],'default')
        self.assertEqual(json.load(self.request(path,'DELETE')),rule())

    def test_updates_touch_only_effective_users_and_each_server_once(self):
        registry=self.server.registry
        users=[self.user(str(i)) for i in range(3)]
        plan=self.plan()
        tokens=[]
        for index in range(4):
            server=registry.save('servers',{'name':str(index)})['id']
            node=registry.save('nodes',{'name':str(index),'server_id':server})['id']
            registry.configure_node(node,test_xray.VLESS)
            for user in (users[:2] if index==0 else [users[min(index-1,2)]]):
                registry.authorize_node_user(node,user)
            tokens.append(registry.provision_agent(server)['token'])
        for user in users: self.assign(user,plan['id'])
        registry.user_bandwidth(users[2],rule(download=12345))
        before=[registry.configuration(t) for t in tokens]
        newer=dict(name='renamed',bandwidth=rule(download=65536))
        self.request(f'/api/plans/{plan["id"]}','PUT',newer).close()
        after=[validate_snapshot(registry.configuration(t)) for t in tokens]
        for old,new in zip(before[:3],after[:3]): self.assertEqual(new['revision'],old['revision']+1)
        self.assertEqual(before[3],after[3])
        for new in after[:3]:
            self.assertTrue(all(c['bandwidth']==newer['bandwidth'] for c in new['nodes'][0]['clients']))
        self.request(f'/api/plans/{plan["id"]}','PUT',newer).close()
        self.assertEqual([registry.configuration(t) for t in tokens],after)
        self.request(f'/api/plans/{plan["id"]}','PUT',{**newer,'name':'name only'}).close()
        self.assertEqual([registry.configuration(t) for t in tokens],after)

    def test_switch_unbind_and_delete_protect_linked_users(self):
        user=self.user(); first=self.plan(); second=self.plan(rule(download=65536))
        self.assign(user,first['id'])
        with self.assertRaises(urllib.error.HTTPError) as caught: self.request(f'/api/plans/{first["id"]}','DELETE')
        self.assertEqual(caught.exception.code,409)
        self.assertEqual(self.assign(user,second['id'])['effective'],second['bandwidth'])
        self.request(f'/api/plans/{first["id"]}','DELETE').close()
        self.assign(user,None)
        self.request(f'/api/plans/{second["id"]}','DELETE').close()
        self.assertEqual(json.load(self.request('/api/plans')),[])

    def test_reopen_retains_policy_and_deleting_user_releases_plan(self):
        user=self.user(); plan=self.plan()
        self.assign(user,plan['id'])
        restored=Registry(self.server.registry.path)
        self.assertEqual(restored.bandwidth_policy(user),self.policy(user))
        restored.user_bandwidth(user,rule(download=8192))
        self.assertEqual(Registry(restored.path).bandwidth_policy(user)['source'],'user')
        restored.delete('users',user)
        with restored.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM user_plans').fetchone()[0],0)
        restored.delete_plan(plan['id'])

    def test_concurrent_assignment_and_delete_never_leave_a_dangling_plan(self):
        registry=self.server.registry
        for index in range(6):
            user=self.user(str(index)); plan=self.plan()
            def assign():
                try: registry.assign_plan(user,dict(plan_id=plan['id'])); return 'assigned'
                except Error as error:
                    self.assertEqual(error.status,404); return 'deleted'
            def delete():
                try: registry.delete_plan(plan['id']); return 'deleted'
                except Error as error:
                    self.assertEqual(error.status,409); return 'assigned'
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                first=pool.submit(assign); second=pool.submit(delete)
                self.assertEqual(first.result(),second.result())
            policy=registry.bandwidth_policy(user)
            exists=any(p['id']==plan['id'] for p in registry.plans())
            self.assertEqual(policy['plan_id']==plan['id'],exists)


@unittest.skipUnless(test_xray.BINARY.is_file(),'Set XRAY_BINARY for real inherited bandwidth tests')
class PlanRealTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_xray.XrayRealTests.setUp
    tearDown=test_xray.XrayRealTests.tearDown
    request=test_xray.XrayRealTests.request
    asyncSetUp=test_user_bandwidth.UserBandwidthRealTests.asyncSetUp
    asyncTearDown=test_xray.XrayRealTests.asyncTearDown
    sync=test_user_bandwidth.UserBandwidthRealTests.sync
    transfer=test_user_bandwidth.UserBandwidthRealTests.transfer
    limits=test_user_bandwidth.UserBandwidthRealTests.limits

    async def test_plan_override_edit_clear_and_unbind_execute_in_both_directions(self):
        registry=self.server.registry
        for direction in ('upload','download'):
            plan=registry.save_plan(dict(name=direction,bandwidth=rule(**{direction:32768})))
            registry.assign_plan(self.uid,dict(plan_id=plan['id']))
            await self.sync()
            self.assertGreaterEqual(await self.transfer(self.credential['uuid']),.48)
            self.limits(**{direction:65536}); await self.sync()
            elapsed=await self.transfer(self.credential['uuid'])
            self.assertGreaterEqual(elapsed,.23); self.assertLess(elapsed,.48)
            previous=registry.configuration(self.token)
            registry.save_plan(dict(name='tightened',bandwidth=rule(**{direction:16384})),plan['id'])
            self.assertEqual(registry.configuration(self.token),previous)
            registry.user_bandwidth(self.uid,inherit=True); await self.sync()
            self.assertGreaterEqual(await self.transfer(self.credential['uuid']),.95)
            registry.assign_plan(self.uid,dict(plan_id=None)); await self.sync()
            self.assertLess(await self.transfer(self.credential['uuid']),.4)

    async def test_same_plan_is_per_user_and_server_total_remains_effective(self):
        registry=self.server.registry
        user=registry.save('users',dict(name='other'))['id']
        credential=registry.authorize_node_user(self.nid,user)['uuid']
        plan=registry.save_plan(dict(name='shared plan',bandwidth=rule(upload=32768)))
        for uid in (self.uid,user): registry.assign_plan(uid,dict(plan_id=plan['id']))
        await self.sync()
        start=time.monotonic()
        await asyncio.gather(self.transfer(self.credential['uuid']),self.transfer(credential))
        self.assertLess(time.monotonic()-start,.95)
        registry.server_bandwidth(self.sid,rule(upload=32768)); await self.sync()
        start=time.monotonic()
        await asyncio.gather(self.transfer(self.credential['uuid']),self.transfer(credential))
        self.assertGreaterEqual(time.monotonic()-start,.95)
        await self.runtime.close(); await self.sync()
        self.assertGreaterEqual(await self.transfer(credential),.48)

    async def test_tightening_plan_then_failing_to_bind_does_not_restore_old_rule(self):
        registry=self.server.registry
        plan=registry.save_plan(dict(name='initial',bandwidth=rule()))
        registry.assign_plan(self.uid,dict(plan_id=plan['id'])); await self.sync()
        registry.save_plan(dict(name='limited',bandwidth=rule(upload=32768)),plan['id'])
        occupied=socket.socket(); occupied.bind(('127.0.0.1',0)); occupied.listen()
        try:
            registry.configure_node(self.nid,{**test_xray.VLESS,'listen_port':occupied.getsockname()[1]})
            with self.assertRaises(OSError): await self.sync()
            self.assertIsNone(self.runtime.snapshot)
            self.assertIsNone(self.runtime.core.process)
            with self.assertRaises(OSError): await asyncio.open_connection('127.0.0.1',self.port)
        finally: occupied.close()
