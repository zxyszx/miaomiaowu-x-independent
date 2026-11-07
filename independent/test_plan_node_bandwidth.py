import asyncio
import copy
import json
import sqlite3
import time
import unittest
import urllib.error
import urllib.request
from core import Registry,Error
from plan_node_bandwidth import PlanNodeBandwidth
from plan_nodes import PlanNodes
from node_config import digest,validate_snapshot
from xray_runtime import configuration
import test_core
import test_plan_nodes
import test_xray
import test_user_bandwidth
import test_reality
from test_accounts import plan
from test_runtime import free_port


def rates(upload=None,download=None): return dict(upload_bps=upload,download_bps=download)

class NodeRateTests(unittest.TestCase):
    setUp=test_core.RegistryTests.setUp
    tearDown=test_core.RegistryTests.tearDown
    fixtures=test_plan_nodes.ScopeTests.fixtures

    def test_inherit_explicit_unlimited_persistence_idempotence_and_clear(self):
        r,parent,p,nodes,tokens=self.fixtures(); r.assign_plan(parent,{'plan_id':p})
        r.authorize_node_user(nodes[0],parent); r.authorize_node_user(nodes[1],parent)
        policy=PlanNodeBandwidth(r); before=[r.configuration(t) for t in tokens]
        self.assertIsNone(policy.manage(p,nodes[0])['override'])
        value=policy.manage(p,nodes[0],rates()); after=[r.configuration(t) for t in tokens]
        self.assertEqual(value['effective'],rates()); self.assertEqual(after[0]['revision'],before[0]['revision']+1)
        self.assertEqual(after[1],before[1]); self.assertEqual(after[0]['nodes'][0]['clients'][0]['node_bandwidth'],rates())
        self.assertEqual(PlanNodeBandwidth(Registry(r.path)).manage(p,nodes[0]),value)
        policy.manage(p,nodes[0],rates()); self.assertEqual(r.configuration(tokens[0]),after[0])
        restored=policy.manage(p,nodes[0],inherit=True)
        self.assertEqual(restored['effective'],rates(32768)); self.assertIsNone(restored['override'])
        self.assertNotIn('node_bandwidth',r.configuration(tokens[0])['nodes'][0]['clients'][0])

    def test_equal_user_override_and_clear_still_version_route_policy(self):
        r,parent,p,nodes,tokens=self.fixtures(); r.assign_plan(parent,{'plan_id':p}); r.authorize_node_user(nodes[0],parent)
        policy=PlanNodeBandwidth(r); policy.manage(p,nodes[0],rates(65536)); old=r.configuration(tokens[0])
        r.user_bandwidth(parent,rates(32768)); hidden=r.configuration(tokens[0])
        self.assertEqual(hidden['revision'],old['revision']+1); self.assertNotIn('node_bandwidth',hidden['nodes'][0]['clients'][0])
        policy.manage(p,nodes[0],rates(16384)); self.assertEqual(r.configuration(tokens[0]),hidden)
        r.user_bandwidth(parent,inherit=True); restored=r.configuration(tokens[0])
        self.assertEqual(restored['revision'],hidden['revision']+1)
        self.assertEqual(restored['nodes'][0]['clients'][0]['node_bandwidth'],rates(16384))

    def test_temporary_expiry_and_plan_switch_restore_correct_route_policy(self):
        from unittest import mock
        r,parent,p,nodes,tokens=self.fixtures(); r.assign_plan(parent,{'plan_id':p}); r.authorize_node_user(nodes[0],parent)
        PlanNodeBandwidth(r).manage(p,nodes[0],rates(65536)); deadline=time.time()+30
        r.temporary_bandwidth(parent,{'bandwidth':rates(8192),'expires_at':deadline})
        old=r.configuration(tokens[0]); self.assertNotIn('node_bandwidth',old['nodes'][0]['clients'][0])
        with mock.patch('core.time.time',return_value=deadline+1): new=r.configuration(tokens[0])
        self.assertGreater(new['revision'],old['revision']); self.assertEqual(new['nodes'][0]['clients'][0]['node_bandwidth'],rates(65536))
        second=plan(r,'second'); r.assign_plan(parent,{'plan_id':second})
        current=r.configuration(tokens[0]); self.assertNotIn('node_bandwidth',current['nodes'][0]['clients'][0])
        self.assertGreater(current['revision'],new['revision'])

    def test_invalid_outside_scope_and_publication_failure_rollback(self):
        r,parent,p,nodes,tokens=self.fixtures(); policy=PlanNodeBandwidth(r)
        PlanNodes(r).save(p,dict(node_ids=[nodes[0]]))
        for data in (rates(True),rates(0),rates(-1),dict(upload_bps=1),dict(upload_bps=1,download_bps=None,extra=1)):
            with self.assertRaises(Error): policy.manage(p,nodes[0],data)
        with self.assertRaises(Error): policy.manage(p,nodes[1],rates(1))
        with self.assertRaises(Error): policy.manage(999,nodes[0],rates(1))
        r.assign_plan(parent,{'plan_id':p}); r.authorize_node_user(nodes[0],parent); before=r.configuration(tokens[0])
        with r.connect() as db: db.execute("CREATE TRIGGER fail_rate BEFORE UPDATE ON server_configs BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): policy.manage(p,nodes[0],rates(1))
        self.assertIsNone(policy.manage(p,nodes[0])['override']); self.assertEqual(r.configuration(tokens[0]),before)

    def test_malformed_snapshot_and_unpaced_core_are_rejected(self):
        r,parent,p,nodes,tokens=self.fixtures(); r.assign_plan(parent,{'plan_id':p}); r.authorize_node_user(nodes[0],parent)
        PlanNodeBandwidth(r).manage(p,nodes[0],rates(8192)); good=r.configuration(tokens[0]); validate_snapshot(good)
        with self.assertRaises(ValueError): configuration(good)
        bad=copy.deepcopy(good); bad['nodes'][0]['clients'][0]['node_bandwidth']=rates(False)
        bad['digest']=digest({k:v for k,v in bad.items() if k!='digest'})
        with self.assertRaises(ValueError): validate_snapshot(bad)

    def test_unused_policy_and_plan_delete_do_not_modify_execution(self):
        r,parent,p,nodes,tokens=self.fixtures(); before=r.configuration(tokens[0]); policy=PlanNodeBandwidth(r)
        policy.manage(p,nodes[0],rates(8192)); self.assertEqual(r.configuration(tokens[0]),before)
        r.delete_plan(p)
        with r.connect() as db: self.assertEqual(db.execute('SELECT count(*) FROM plan_node_bandwidth').fetchone()[0],0)

class NodeRateApiTests(unittest.TestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown
    request=test_core.ApiTests.request

    def test_admin_get_put_delete_and_agent_boundary(self):
        r=self.server.registry; sid=r.save('servers',{'name':'host'})['id']; nid=r.save('nodes',dict(name='node',server_id=sid))['id']
        r.configure_node(nid,test_xray.VLESS); p=plan(r); path=f'/api/plans/{p}/nodes/{nid}/bandwidth'
        token=r.provision_agent(sid)['token']
        for method,data in (('GET',None),('PUT',rates(8192)),('DELETE',None)):
            with self.assertRaises(urllib.error.HTTPError) as caught: self.request(path,method,data,auth=False)
            self.assertEqual(caught.exception.code,401)
            req=urllib.request.Request(self.base+path,method=method,data=json.dumps(data).encode() if data else None,headers={'Authorization':'Bearer '+token})
            with self.assertRaises(urllib.error.HTTPError) as caught: self.opener.open(req,timeout=3)
            self.assertEqual(caught.exception.code,401)
        self.assertIsNone(json.load(self.request(path))['override'])
        self.assertEqual(json.load(self.request(path,'PUT',rates()))['override'],rates())
        self.assertIsNone(json.load(self.request(path,'DELETE'))['override'])

@unittest.skipUnless(test_xray.BINARY.is_file(),'Real Xray required')
class NodeRateRealTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_xray.XrayRealTests.setUp
    tearDown=test_xray.XrayRealTests.tearDown
    request=test_xray.XrayRealTests.request
    asyncSetUp=test_user_bandwidth.UserBandwidthRealTests.asyncSetUp
    asyncTearDown=test_xray.XrayRealTests.asyncTearDown
    transfer=test_user_bandwidth.UserBandwidthRealTests.transfer
    sync=test_user_bandwidth.UserBandwidthRealTests.sync

    async def test_node_override_replaces_plan_in_both_directions_and_clear_restores(self):
        r=self.server.registry; p=plan(r,rate=8192); r.assign_plan(self.uid,{'plan_id':p}); policy=PlanNodeBandwidth(r)
        for direction in ('upload','download'):
            policy.manage(p,self.nid,rates(**{direction:32768})); await self.sync()
            self.assertGreaterEqual(await self.transfer(self.credential['uuid']),.48)
            policy.manage(p,self.nid,rates()); await self.sync()
            self.assertLess(await self.transfer(self.credential['uuid']),.4)
        policy.manage(p,self.nid,inherit=True); await self.sync()
        self.assertGreaterEqual(await self.transfer(self.credential['uuid'],size=8192),.95)

    async def test_node_connections_share_override_but_other_node_is_independent(self):
        r=self.server.registry; p=plan(r,rate=8192); r.assign_plan(self.uid,{'plan_id':p})
        # Avoid the fixture plan's separate TCP connection cap.
        r.user_connections(self.uid,{'max_connections':None})
        port=free_port(); nid=r.save('nodes',dict(name='second',server_id=self.sid))['id']
        r.configure_node(nid,{**test_xray.VLESS,'listen_port':port}); auth=r.authorize_node_user(nid,self.uid)
        policy=PlanNodeBandwidth(r); policy.manage(p,self.nid,rates(32768)); policy.manage(p,nid,rates())
        await self.sync(); start=time.monotonic()
        elapsed=await asyncio.gather(self.transfer(self.credential['uuid']),self.transfer(self.credential['uuid']),self.transfer(auth['uuid'],port))
        self.assertGreaterEqual(time.monotonic()-start,.95); self.assertLess(elapsed[2],.4)
        r.server_bandwidth(self.sid,rates(32768)); await self.sync()
        self.assertGreaterEqual(await self.transfer(auth['uuid'],port),.48)

    async def test_user_rule_still_supersedes_node_unlimited(self):
        r=self.server.registry; p=plan(r,rate=8192); r.assign_plan(self.uid,{'plan_id':p})
        PlanNodeBandwidth(r).manage(p,self.nid,rates()); r.user_bandwidth(self.uid,rates(32768))
        await self.sync(); self.assertGreaterEqual(await self.transfer(self.credential['uuid']),.48)
        r.user_bandwidth(self.uid,inherit=True); await self.sync()
        self.assertLess(await self.transfer(self.credential['uuid']),.4)

@unittest.skipUnless(test_xray.BINARY.is_file(),'Real Xray required')
class NodeRateRealityTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=test_reality.RealityRealTests.asyncSetUp
    asyncTearDown=test_reality.RealityRealTests.asyncTearDown
    echo=test_reality.RealityRealTests.echo
    apply=test_reality.RealityRealTests.apply
    client=test_reality.RealityRealTests.client
    exchange=test_reality.RealityRealTests.exchange

    async def test_reality_node_limit_applies_after_decryption_and_unlimited_restores(self):
        p=plan(self.registry,rate=8192); self.registry.assign_plan(self.uid,{'plan_id':p})
        policy=PlanNodeBandwidth(self.registry); policy.manage(p,self.nid,rates(download=32768)); await self.apply()
        payload=b'x'*16384; start=time.monotonic(); self.assertEqual(await self.exchange(payload=payload),payload)
        self.assertGreaterEqual(time.monotonic()-start,.48)
        policy.manage(p,self.nid,rates()); await self.apply()
        self.assertEqual(await self.exchange(payload=payload),payload)
