import asyncio
import copy
import json
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest import mock
import urllib.error

from automatic_bandwidth import DownloadController, DownloadStore
from core import Registry
from node_config import automatic_bandwidth, digest, validate_snapshot
from xray_runtime import configuration
import test_core
import test_user_bandwidth
import test_xray
from test_runtime import free_port

RULE=dict(enabled=True,threshold_bps=1000,sustain_seconds=2,download_bps=500,duration_seconds=3)


class ControllerTests(unittest.TestCase):
    def setUp(self):
        self.now=100
        self.saved=[]
        self.patch=mock.patch('automatic_bandwidth.time',SimpleNamespace(monotonic=lambda:self.now,time=lambda:1000+self.now))
        self.patch.start(); self.addCleanup(self.patch.stop)
        self.control=DownloadController(RULE,self.saved.append)

    def window(self,count=2000,seconds=1):
        self.control.record(count); self.now+=seconds
        return self.control.sample()

    def trigger(self):
        self.assertIsNone(self.window())
        self.assertEqual(self.window().rate,500)

    def test_sustained_windows_trigger_and_expiry_restores_without_master(self):
        self.trigger()
        self.assertEqual(len(self.saved),1)
        self.now+=2.9; self.assertEqual(self.control.sample().rate,500)
        self.now+=0.1; self.assertIsNone(self.control.sample())
        self.assertIsNone(self.saved[-1])
        self.assertEqual(self.control.hot,0)

    def test_burst_idle_low_window_and_stall_reset_sustain(self):
        self.window(); self.window(0)
        self.assertIsNone(self.window())
        self.assertIsNone(self.window(100000,seconds=3))
        self.assertEqual(self.control.hot,0)
        self.assertIsNone(self.window())
        self.assertEqual(self.window().rate,500)

    def test_polling_without_bytes_cannot_trigger(self):
        for _ in range(10): self.assertIsNone(self.window(0))
        self.assertEqual(self.saved,[])

    def test_durable_trigger_survives_restart_clock_rollback_is_bounded(self):
        self.trigger()
        restored=DownloadController(RULE,self.saved.append,self.saved[-1])
        self.assertEqual(restored.sample().rate,500)
        self.now-=100
        restored=DownloadController(RULE,self.saved.append,self.saved[-1])
        self.assertLessEqual(restored.until-self.now,3)
        for state in ({'rule':RULE,'until':float('nan'),'recorded_at':0},'bad'):
            with self.assertRaises(ValueError): DownloadController(RULE,self.saved.append,state)

    def test_failed_persistence_does_not_commit_trigger(self):
        self.window()
        self.control.save=mock.Mock(side_effect=OSError('disk full'))
        with self.assertRaises(OSError): self.window()
        self.assertEqual(self.control.until,0)

    def test_store_private_atomic_restart_and_removed_rules(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'auto.json'; store=DownloadStore(path)
            store.configure(1,{2:RULE})
            self.control=store.controllers[2]; self.trigger()
            self.assertEqual(path.stat().st_mode&0o777,0o600)
            again=DownloadStore(path); again.configure(1,{2:RULE})
            self.assertEqual(again.controllers[2].sample().rate,500)
            again.configure(1,{})
            self.assertEqual(json.loads(path.read_text())['users'],{})
            with self.assertRaises(ValueError): DownloadStore(path).configure(2,{2:RULE})


class AsyncControllerTests(unittest.IsolatedAsyncioTestCase):
    async def scenario(self, failure=None):
        clock=[100.0]
        entered=asyncio.Event(); release=asyncio.Event(); saved=[]
        async def save(state):
            if not saved:
                saved.append(state)
                entered.set()
                await release.wait()
                if failure is not None: raise failure
            else:
                saved.append(state)
        with mock.patch('automatic_bandwidth.time',SimpleNamespace(
                monotonic=lambda:clock[0],time=lambda:1000+clock[0])):
            control=DownloadController(RULE,lambda state:None,async_save=save)
            for _ in range(2):
                control.record(2000); clock[0]+=1
                control.sample(defer_persist=True)
            self.assertGreater(control.until,clock[0])
            first=asyncio.create_task(control.persist_pending())
            await asyncio.wait_for(entered.wait(),2)
            # A second live connection observes expiry during the first fsync.
            clock[0]+=RULE['duration_seconds']
            self.assertIsNone(control.sample(defer_persist=True))
            second=asyncio.create_task(control.persist_pending())
            await asyncio.sleep(0)
            self.assertFalse(second.done())
            release.set()
            if failure is None:
                await asyncio.wait_for(first,2)
            else:
                with self.assertRaises(type(failure)):
                    await asyncio.wait_for(first,2)
            await asyncio.wait_for(second,2)
            # The older trigger must not clear or resurrect the newer expiry.
            self.assertEqual(len(saved),2)
            self.assertEqual(saved[0]['rule'],RULE)
            self.assertIsNone(saved[1])
            await control.persist_pending()
            self.assertEqual(len(saved),2)
            self.assertEqual(control.until,0)

    async def test_expiry_during_trigger_save_is_durable_before_waiters_resume(self):
        await self.scenario()

    async def test_failed_old_save_does_not_overwrite_newer_expiry(self):
        await self.scenario(OSError('injected storage failure'))

    async def test_cancelled_old_save_does_not_overwrite_newer_expiry(self):
        await self.scenario(asyncio.CancelledError())


class ApiTests(unittest.TestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown
    request=test_core.ApiTests.request

    def fixture(self):
        r=self.server.registry
        uid=r.save('users',{'name':'one'})['id']
        tokens=[]
        for index in range(2):
            sid=r.save('servers',{'name':str(index)})['id']
            nid=r.save('nodes',{'name':'one','server_id':sid})['id']
            r.configure_node(nid,test_xray.VLESS); r.authorize_node_user(nid,uid)
            tokens.append(r.provision_agent(sid)['token'])
        return r,uid,tokens

    def test_global_user_disable_inherit_idempotence_and_restart(self):
        r,uid,tokens=self.fixture(); path=f'/api/users/{uid}/automatic-bandwidth'
        old=[r.configuration(token) for token in tokens]
        result=json.load(self.request('/api/automatic-bandwidth','PUT',RULE))
        self.assertEqual(result['effective'],RULE)
        snapshots=[r.configuration(token) for token in tokens]
        for previous,snapshot in zip(old,snapshots):
            self.assertEqual(snapshot['revision'],previous['revision']+1)
            self.assertEqual(snapshot['nodes'][0]['clients'][0]['automatic_bandwidth'],RULE)
            with self.assertRaises(ValueError): configuration(snapshot)
        self.request('/api/automatic-bandwidth','PUT',RULE).close()
        self.assertEqual(Registry(r.path).configuration(tokens[0]),snapshots[0])
        self.request(path,'PUT',{'enabled':False}).close()
        self.assertNotIn('automatic_bandwidth',r.configuration(tokens[0])['nodes'][0]['clients'][0])
        self.request('/api/automatic-bandwidth','DELETE').close()
        rev=r.configuration(tokens[0])['revision']
        self.request('/api/automatic-bandwidth','PUT',RULE).close()
        self.assertEqual(r.configuration(tokens[0])['revision'],rev)
        self.request(path,'DELETE').close()
        self.assertEqual(json.load(self.request(path))['effective'],RULE)
        r.delete('users',uid)
        with r.connect() as db: self.assertEqual(db.execute('SELECT count(*) FROM automatic_bandwidth_users').fetchone()[0],0)

    def test_validation_auth_and_base_limits_unchanged(self):
        r,uid,tokens=self.fixture(); path=f'/api/users/{uid}/automatic-bandwidth'
        with self.assertRaises(urllib.error.HTTPError) as caught: self.request(path,auth=False)
        self.assertEqual(caught.exception.code,401)
        for bad in (None,{}, {'enabled':0},{**RULE,'sustain_seconds':True},{**RULE,'download_bps':1000},{**RULE,'extra':1}):
            with self.assertRaises(urllib.error.HTTPError): self.request(path,'PUT',bad)
        r.user_bandwidth(uid,{'upload_bps':100,'download_bps':200})
        r.temporary_bandwidth(uid,{'bandwidth':{'upload_bps':10,'download_bps':20},'expires_at':time.time()+60})
        before=r.bandwidth_policy(uid)
        with self.request(path,'PUT',RULE) as response:
            self.assertEqual(response.headers['Cache-Control'],'no-store')
        self.assertEqual(r.bandwidth_policy(uid),before)
        snap=r.configuration(tokens[0])
        for change in ({**RULE,'duration_seconds':False},{**RULE,'download_bps':0}):
            invalid=copy.deepcopy(snap); invalid['nodes'][0]['clients'][0]['automatic_bandwidth']=change
            invalid['digest']=digest({key:value for key,value in invalid.items() if key!='digest'})
            with self.assertRaises(ValueError): validate_snapshot(invalid)


@unittest.skipUnless(test_xray.BINARY.is_file(),'Real Xray required')
class RealTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_user_bandwidth.UserBandwidthRealTests.setUp
    tearDown=test_user_bandwidth.UserBandwidthRealTests.tearDown
    asyncSetUp=test_user_bandwidth.UserBandwidthRealTests.asyncSetUp
    asyncTearDown=test_user_bandwidth.UserBandwidthRealTests.asyncTearDown
    request=test_core.ApiTests.request
    sync=test_user_bandwidth.UserBandwidthRealTests.sync
    transfer=test_user_bandwidth.UserBandwidthRealTests.transfer

    async def test_real_detection_shared_cap_and_offline_expiry(self):
        rule={**RULE,'sustain_seconds':1,'download_bps':8192,'threshold_bps':10000,'duration_seconds':4}
        r=self.server.registry
        sid=r.configuration(self.token)['server_id']
        port=free_port()
        nid=r.save('nodes',{'name':'second','server_id':sid})['id']
        r.configure_node(nid,{**test_xray.VLESS,'listen_port':port})
        credential=r.authorize_node_user(nid,self.uid)['uuid']
        r.automatic_bandwidth(self.uid,rule)
        await self.sync()
        await self.transfer(self.credential['uuid'],size=32768)
        await asyncio.sleep(1.05)
        elapsed=await self.transfer(self.credential['uuid'],size=8192)
        self.assertGreater(elapsed,0.85)
        controller=self.runtime.automatic.controllers[self.uid]
        self.assertGreater(controller.until,time.monotonic())
        elapsed=await asyncio.gather(self.transfer(self.credential['uuid'],size=4096),self.transfer(credential,port=port,size=4096))
        self.assertGreater(max(elapsed),0.85)
        # No master sync is needed to release the automatic cap.
        await asyncio.sleep(max(0,controller.until-time.monotonic())+0.02)
        self.assertLess(await self.transfer(self.credential['uuid'],size=8192),0.7)

    async def test_real_manual_cap_remains_stricter_and_rule_change_revokes_old_policy(self):
        r=self.server.registry
        r.automatic_bandwidth(self.uid,{**RULE,'threshold_bps':10000,'download_bps':8192,'sustain_seconds':1})
        r.user_bandwidth(self.uid,dict(upload_bps=None,download_bps=4096))
        await self.sync()
        control=self.runtime.automatic.controllers[self.uid]
        # Force only the triggering evidence here; actual bytes must still traverse both limits.
        control.start-=1.01; control.record(20000)
        self.assertGreater(await self.transfer(self.credential['uuid'],size=4096),0.85)
        self.assertIsNone(self.runtime.pacers[self.uid][0].rate)
        r.automatic_bandwidth(self.uid,{'enabled':False})
        await self.sync()
        self.assertNotIn(self.uid,self.runtime.automatic.controllers)
        self.assertGreater(await self.transfer(self.credential['uuid'],size=4096),0.85)

    async def test_real_restart_retains_active_automatic_cap(self):
        from vless_runtime import VlessRuntime
        r=self.server.registry
        r.automatic_bandwidth(self.uid,{**RULE,'threshold_bps':10000,'download_bps':8192,'sustain_seconds':1,'duration_seconds':10})
        await self.sync()
        control=self.runtime.automatic.controllers[self.uid]
        control.start-=1.01; control.record(20000)
        self.assertIsNotNone(control.sample())
        root=self.runtime.core.state_dir
        until=control.until
        await self.runtime.close()
        self.runtime=VlessRuntime(test_xray.BINARY,root,local_test_target=('127.0.0.1',self.target_port))
        await self.sync()
        restored=self.runtime.automatic.controllers[self.uid]
        self.assertAlmostEqual(restored.until,until,delta=0.2)
        self.assertGreater(await self.transfer(self.credential['uuid'],size=8192),0.85)

    async def test_detection_under_existing_manual_pacing(self):
        r=self.server.registry
        r.user_bandwidth(self.uid,dict(upload_bps=None,download_bps=8192))
        r.automatic_bandwidth(self.uid,{**RULE,'threshold_bps':6000,'download_bps':4096,'sustain_seconds':1})
        await self.sync()
        elapsed=await self.transfer(self.credential['uuid'],size=12288)
        self.assertGreater(elapsed,1.8)
        self.assertGreater(self.runtime.automatic.controllers[self.uid].until,time.monotonic())
