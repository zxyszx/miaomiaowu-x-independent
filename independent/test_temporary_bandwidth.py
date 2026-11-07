import asyncio
import copy
import json
from pathlib import Path
import tempfile
from agent import sync
import time
import unittest
from unittest import mock
import urllib.error
from bandwidth import BytePacer, CombinedPacer
from core import Registry
from node_config import digest, validate_snapshot
from xray_runtime import configuration
import test_core
import test_xray
import test_user_bandwidth


def limits(upload=None,download=None):
    return dict(upload_bps=upload,download_bps=download)


class TemporaryBandwidthApiTests(unittest.TestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown
    request=test_core.ApiTests.request

    def fixture(self):
        registry=self.server.registry
        self.uid=registry.save('users',dict(name='user'))['id']
        self.sid=registry.save('servers',dict(name='server'))['id']
        node=registry.save('nodes',dict(name='node',server_id=self.sid))['id']
        registry.configure_node(node,test_xray.VLESS)
        registry.authorize_node_user(node,self.uid)
        self.token=registry.provision_agent(self.sid)['token']
        self.path=f'/api/users/{self.uid}/temporary-bandwidth'
        return registry

    def put(self,rule,deadline):
        return json.load(self.request(self.path,'PUT',dict(bandwidth=rule,expires_at=deadline)))

    def test_upgrade_migrates_old_disk_snapshot_without_weakening_version_fence(self):
        registry=self.fixture()
        self.put(limits(upload=32768),time.time()+100)
        current=registry.configuration(self.token)
        legacy=copy.deepcopy(current)
        for node in legacy['nodes']:
            for client in node.get('clients',[]): client.pop('valid_until',None)
        legacy['digest']=digest({k:v for k,v in legacy.items() if k!='digest'})
        unaffected=registry.save('servers',{'name':'no temporary rules'})['id']
        other_token=registry.provision_agent(unaffected)['token']
        other_before=registry.configuration(other_token)
        receipt={k:legacy[k] for k in ('revision','digest')}
        # Reconstruct a pre-upgrade DB plus its staged and running receipts.
        with registry.connect() as db:
            db.execute('DROP TABLE IF EXISTS schema_migrations')
            db.execute('UPDATE server_configs SET staged_revision=?,staged_digest=?,staged_at=? WHERE server_id=?',
                       (legacy['revision'],legacy['digest'],time.time(),self.sid))
            db.execute('INSERT OR REPLACE INTO runtime_receipts VALUES(?,?,?,?)',
                       (self.sid,legacy['revision'],legacy['digest'],time.time()))
        with tempfile.TemporaryDirectory() as directory:
            state=Path(directory); disk=state/'staged-config.json'
            disk.write_text(json.dumps(legacy))
            upgraded=Registry(registry.path)
            desired=upgraded.configuration(self.token)
            self.assertEqual(desired['revision'],legacy['revision']+1)
            self.assertNotEqual(desired['digest'],legacy['digest'])
            self.assertEqual(upgraded.configuration(other_token),other_before)
            self.assertEqual(Registry(registry.path).configuration(self.token),desired)
            status=next(x for x in upgraded.agent_status() if x['server_id']==self.sid)
            self.assertFalse(status['listeners_current'])
            for acknowledge in (upgraded.acknowledge_configuration,upgraded.acknowledge_runtime):
                with self.assertRaises(ValueError): acknowledge(self.token,receipt)
            self.assertTrue(sync(self.base,self.token,state)['accepted'])
            self.assertEqual(json.loads(disk.read_text()),desired)
            # Do not relax same-revision digest checking to get migration working.
            tampered=copy.deepcopy(desired)
            tampered['server_name']='different'
            tampered['digest']=digest({k:v for k,v in tampered.items() if k!='digest'})
            with mock.patch('agent.request',return_value=tampered):
                with self.assertRaises(ValueError): sync(self.base,self.token,state)
            self.assertEqual(json.loads(disk.read_text()),desired)

    def test_auth_validation_persistence_and_delete_cascade(self):
        registry=self.fixture()
        for method in ('GET','PUT','DELETE'):
            with self.assertRaises(urllib.error.HTTPError) as caught:
                self.request(self.path,method,dict(bandwidth=limits(),expires_at=time.time()+100),auth=False)
            self.assertEqual(caught.exception.code,401)
        self.assertIsNone(json.load(self.request(self.path)))
        before=registry.configuration(self.token)
        for expiry in (True,0,-1,'tomorrow',None,time.time()-1,time.time()+31*86400):
            with self.assertRaises(urllib.error.HTTPError): self.put(limits(),expiry)
        for rule in ({},limits(upload=False),limits(upload=0)):
            with self.assertRaises(urllib.error.HTTPError): self.put(rule,time.time()+100)
        for value in ({},dict(bandwidth=limits(),expires_at=time.time()+100,extra=True)):
            with self.assertRaises(urllib.error.HTTPError): self.request(self.path,'PUT',value)
        self.assertEqual(registry.configuration(self.token),before)
        value=self.put(limits(upload=32768),time.time()+100)
        self.assertEqual(Registry(registry.path).temporary_bandwidth(self.uid),value)
        current=registry.configuration(self.token)
        self.assertEqual(current['valid_until'],value['expires_at'])
        self.assertEqual(validate_snapshot(current),current)
        with self.assertRaises(ValueError): configuration(current)
        registry.delete('users',self.uid)
        with registry.connect() as db: self.assertEqual(db.execute('SELECT count(*) FROM temporary_bandwidth').fetchone()[0],0)

    def test_expiry_restores_latest_override_then_plan_then_default_and_versions(self):
        registry=self.fixture()
        plan=registry.save_plan(dict(name='base',bandwidth=limits(upload=16384)))
        registry.assign_plan(self.uid,dict(plan_id=plan['id']))
        registry.user_bandwidth(self.uid,limits(upload=8192))
        for source in ('user','plan','default'):
            deadline=time.time()+100
            self.put(limits(upload=65536),deadline)
            active=registry.configuration(self.token)
            self.assertEqual(registry.bandwidth_policy(self.uid)['source'],'temporary')
            if source=='user': registry.user_bandwidth(self.uid,limits(download=4096))
            if source=='plan': registry.save_plan(dict(name='changed',bandwidth=limits(download=2048)),plan['id'])
            self.assertEqual(registry.configuration(self.token),active)
            with mock.patch('core.time.time',return_value=deadline):
                restored=registry.configuration(self.token)
                self.assertNotIn('valid_until',restored)
                self.assertEqual(restored['revision'],active['revision']+1)
                self.assertIsNone(registry.temporary_bandwidth(self.uid))
                self.assertEqual(registry.bandwidth_policy(self.uid)['source'],source)
                self.assertEqual(registry.configuration(self.token),restored)
            if source=='user':
                self.assertEqual(registry.user_bandwidth(self.uid),limits(download=4096))
                registry.user_bandwidth(self.uid,inherit=True)
            elif source=='plan':
                self.assertEqual(registry.user_bandwidth(self.uid),limits(download=2048))
                registry.assign_plan(self.uid,dict(plan_id=None))
            else: self.assertEqual(registry.user_bandwidth(self.uid),limits())

    def test_replace_extend_clear_and_idempotence_are_versioned(self):
        registry=self.fixture()
        deadline=time.time()+100
        rule=limits(upload=32768)
        self.put(rule,deadline)
        original=registry.configuration(self.token)
        self.put(rule,deadline)
        self.assertEqual(registry.configuration(self.token),original)
        self.put(rule,deadline+10)
        extended=registry.configuration(self.token)
        self.assertEqual(extended['revision'],original['revision']+1)
        self.assertEqual(extended['valid_until'],deadline+10)
        self.put(limits(),deadline+20)
        self.assertEqual(registry.user_bandwidth(self.uid),limits())
        json.load(self.request(self.path,'DELETE'))
        cleared=registry.configuration(self.token)
        self.assertNotIn('valid_until',cleared)
        json.load(self.request(self.path,'DELETE'))
        self.assertEqual(registry.configuration(self.token),cleared)

    def test_two_expirations_on_one_server_bump_once_and_next_deadline_is_preserved(self):
        registry=self.fixture()
        second=registry.save('users',dict(name='second'))['id']
        third=registry.save('users',dict(name='third'))['id']
        node=registry.list('nodes')[0]['id']
        for user in (second,third): registry.authorize_node_user(node,user)
        deadline=time.time()+100
        for user,expiry in ((self.uid,deadline),(second,deadline),(third,deadline+50)):
            registry.temporary_bandwidth(user,dict(bandwidth=limits(upload=32768),expires_at=expiry))
        before=registry.configuration(self.token)
        with mock.patch('core.time.time',return_value=deadline):
            after=registry.configuration(self.token)
            self.assertEqual(after['revision'],before['revision']+1)
            self.assertEqual(after['valid_until'],deadline+50)
            self.assertEqual(registry.agent_status()[0]['desired_revision'],after['revision'])

    def test_expired_receipts_rejected_and_boundary_does_not_mutate_same_revision(self):
        registry=self.fixture()
        deadline=time.time()+100
        self.put(limits(upload=32768),deadline)
        current=registry.configuration(self.token)
        receipt={k:current[k] for k in ('revision','digest')}
        registry.acknowledge_configuration(self.token,receipt)
        registry.acknowledge_runtime(self.token,receipt)
        with mock.patch('core.time.time',return_value=deadline):
            for acknowledge in (registry.acknowledge_configuration,registry.acknowledge_runtime):
                with self.assertRaises(ValueError): acknowledge(self.token,receipt)
            self.assertFalse(registry.agent_status()[0]['listeners_current'])
            newer=registry.configuration(self.token)
            self.assertGreater(newer['revision'],current['revision'])
            self.assertNotEqual(newer['digest'],current['digest'])

    def test_invalid_deadline_metadata_is_rejected(self):
        registry=self.fixture()
        value=registry.configuration(self.token)
        for deadline in (True,'10',0,-1,None):
            bad={**value,'valid_until':deadline}
            bad['digest']=digest({k:v for k,v in bad.items() if k!='digest'})
            with self.assertRaises(ValueError): validate_snapshot(bad)

    def test_per_user_deadlines_require_consistent_global_deadline(self):
        registry=self.fixture()
        self.put(limits(),time.time()+100)
        value=registry.configuration(self.token)
        self.assertEqual(value['nodes'][0]['clients'][0]['valid_until'],value['valid_until'])
        variants=[]
        for deadline in (True,'10',0,-1,None):
            bad=copy.deepcopy(value)
            bad['nodes'][0]['clients'][0]['valid_until']=deadline
            variants.append(bad)
        for change in ('missing','mismatch','node'):
            bad=copy.deepcopy(value)
            if change=='missing': bad.pop('valid_until')
            elif change=='mismatch': bad['valid_until']+=1
            else:
                node=copy.deepcopy(bad['nodes'][0]); node['id']+=100
                node['config']['listen_port']+=1
                node['clients'][0].pop('valid_until')
                bad['nodes'].append(node)
            variants.append(bad)
        for bad in variants:
            bad['digest']=digest({k:v for k,v in bad.items() if k!='digest'})
            with self.assertRaises(ValueError): validate_snapshot(bad)
        # The bare core must not silently ignore per-user expiry metadata.
        with self.assertRaises(ValueError): configuration(value)


class TemporaryPacerTests(unittest.IsolatedAsyncioTestCase):
    async def test_expiry_blocks_payload_even_when_shutdown_is_waiting_for_lifecycle_lock(self):
        class Writer:
            data=b''
            def write(self,data): self.data+=data
            async def drain(self): pass
        for rate in (None,1):
            writer=Writer()
            pacer=CombinedPacer(BytePacer(rate),deadline=time.time()+.02)
            if rate is None: await asyncio.sleep(.03)
            with self.assertRaises(asyncio.TimeoutError): await pacer.write(writer,b'x')
            self.assertEqual(writer.data,b'')


@unittest.skipUnless(test_xray.BINARY.is_file(),'Set XRAY_BINARY for temporary bandwidth tests')
class TemporaryBandwidthRealTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_xray.XrayRealTests.setUp
    tearDown=test_xray.XrayRealTests.tearDown
    request=test_xray.XrayRealTests.request
    asyncSetUp=test_user_bandwidth.UserBandwidthRealTests.asyncSetUp
    asyncTearDown=test_xray.XrayRealTests.asyncTearDown
    sync=test_user_bandwidth.UserBandwidthRealTests.sync
    transfer=test_user_bandwidth.UserBandwidthRealTests.transfer

    async def wait_expired(self,deadline):
        await asyncio.sleep(max(0,deadline-time.time())+.05)
        for _ in range(100):
            if self.runtime.expiry_task is None: return
            await asyncio.sleep(.02)
        self.fail('expiry task did not finish')

    async def test_offline_expiration_closes_connections_then_sync_restores_latest_rule(self):
        registry=self.server.registry
        registry.user_bandwidth(self.uid,limits(upload=32768))
        deadline=time.time()+3
        registry.temporary_bandwidth(self.uid,dict(bandwidth=limits(),expires_at=deadline))
        await self.sync()
        self.assertLess(await self.transfer(self.credential['uuid']),.4)
        reader,writer=await test_xray.XrayRealTests.connect_client(self,self.credential['uuid'])
        self.assertEqual(await asyncio.wait_for(reader.readexactly(20),2),b'\0\0authenticated data')
        try:
            # No control-plane requests while the deadline passes.
            await self.wait_expired(deadline)
            try: self.assertEqual(await asyncio.wait_for(reader.read(),.5),b'')
            except ConnectionResetError: pass
            self.assertIsNone(self.runtime.core.process.returncode)
            self.assertFalse(self.runtime.tasks)
            self.assertIsNone(self.runtime.expiry_task)
            denied,attempt=await test_xray.XrayRealTests.connect_client(self,self.credential['uuid'])
            try:
                try: self.assertEqual(await asyncio.wait_for(denied.read(),1),b'')
                except ConnectionResetError: pass
            finally:
                attempt.close()
                try: await attempt.wait_closed()
                except OSError: pass
        finally:
            writer.close()
            try: await writer.wait_closed()
            except ConnectionResetError: pass
        await self.sync()
        self.assertGreaterEqual(await self.transfer(self.credential['uuid']),.48)

    async def test_expired_snapshot_is_rejected_and_replacement_cancels_old_timer(self):
        registry=self.server.registry
        deadline=time.time()+3
        registry.temporary_bandwidth(self.uid,dict(bandwidth=limits(download=32768),expires_at=deadline))
        await self.sync()
        stale=copy.deepcopy(self.runtime.snapshot)
        old_timer=self.runtime.expiry_task
        registry.temporary_bandwidth(self.uid,remove=True)
        await self.sync()
        self.assertTrue(old_timer.done())
        self.assertIsNone(self.runtime.expiry_task)
        await asyncio.sleep(max(0,deadline-time.time())+.05)
        self.assertLess(await self.transfer(self.credential['uuid']),.4)
        with self.assertRaises(ValueError): await self.runtime.apply(stale)
        self.assertLess(await self.transfer(self.credential['uuid']),.4)

    async def test_legacy_expiry_waiting_on_reload_lock_cannot_close_replacement(self):
        # Force the old timer to reach close() while the reload owns lifecycle.
        # Call _apply under that same lock, as public apply() does, so this
        # ordering is deterministic instead of relying on a scheduler race.
        from types import SimpleNamespace
        clock=[time.time()]
        # VM wall clocks can jump independently of asyncio's monotonic clock.
        # Control only this runtime's clock; do not patch global time.time.
        self.enterContext(mock.patch('vless_runtime.time',
                                    SimpleNamespace(time=lambda:clock[0])))
        legacy=copy.deepcopy(self.server.registry.configuration(self.token))
        legacy['valid_until']=clock[0]+60
        legacy['digest']=digest({k:v for k,v in legacy.items() if k!='digest'})
        await self.runtime.apply(legacy)
        timer=self.runtime.expiry_task
        replacement=copy.deepcopy(legacy)
        replacement.pop('valid_until')
        replacement['revision']+=1
        replacement['digest']=digest({k:v for k,v in replacement.items() if k!='digest'})
        entered=asyncio.Event()
        started=time.monotonic()
        wall=time.time()
        close=self.runtime.close
        async def waiting_close():
            entered.set()
            await close()
        with mock.patch.object(self.runtime,'close',side_effect=waiting_close):
            async with self.runtime.lifecycle:
                clock[0]=legacy['valid_until']
                try:
                    await asyncio.wait_for(entered.wait(),3)
                except TimeoutError as error:
                    raise AssertionError(dict(deadline=legacy['valid_until'],
                        wall_time=time.time(),elapsed=time.monotonic()-started,
                        clock_shift=time.time()-wall-(time.monotonic()-started),
                        timer_done=timer.done(),
                        timer_stack=[frame.f_code.co_name+':'+str(frame.f_lineno)
                                     for frame in timer.get_stack()],
                        deadlines=self.runtime.deadlines.copy())) from error
                self.assertFalse(timer.done())
                await asyncio.wait_for(self.runtime._apply(replacement),3)
        self.assertTrue(timer.cancelled())
        self.assertIsNone(self.runtime.expiry_task)
        self.assertEqual(self.runtime.snapshot,replacement)
        process=self.runtime.core.process
        await self.transfer(self.credential['uuid'],size=32)
        self.assertIs(self.runtime.core.process,process)
        self.assertIsNone(process.returncode)

    async def test_temporary_rules_are_limited_by_server_total_in_both_directions(self):
        registry=self.server.registry
        for direction in ('upload','download'):
            registry.server_bandwidth(self.sid,limits(**{direction:32768}))
            registry.temporary_bandwidth(self.uid,dict(bandwidth=limits(),expires_at=time.time()+10))
            await self.sync()
            self.assertGreaterEqual(await self.transfer(self.credential['uuid']),.48)
        await self.runtime.close()
        self.assertIsNone(self.runtime.expiry_task)

    async def test_running_legacy_snapshot_upgrades_and_restarts_with_saved_new_snapshot(self):
        registry=self.server.registry
        registry.temporary_bandwidth(self.uid,dict(bandwidth=limits(upload=32768),expires_at=time.time()+30))
        legacy=copy.deepcopy(registry.configuration(self.token))
        for node in legacy['nodes']:
            for client in node['clients']: client.pop('valid_until',None)
        legacy['digest']=digest({k:v for k,v in legacy.items() if k!='digest'})
        self.state.mkdir(parents=True,exist_ok=True)
        disk=self.state/'staged-config.json'
        disk.write_text(json.dumps(legacy))
        await self.runtime.apply(legacy)
        old_timer=self.runtime.expiry_task
        self.assertGreaterEqual(await self.transfer(self.credential['uuid']),.48)
        with registry.connect() as db: db.execute('DROP TABLE schema_migrations')
        Registry(registry.path)
        await self.sync()
        self.assertTrue(old_timer.done())
        self.assertEqual(self.runtime.snapshot['revision'],legacy['revision']+1)
        self.assertIn('valid_until',self.runtime.snapshot['nodes'][0]['clients'][0])
        self.assertEqual(json.loads(disk.read_text()),self.runtime.snapshot)
        self.assertGreaterEqual(await self.transfer(self.credential['uuid']),.48)
        await self.runtime.close()
        await self.sync()
        self.assertGreaterEqual(await self.transfer(self.credential['uuid']),.48)

    async def test_one_user_expiry_keeps_other_users_existing_connection_active(self):
        registry=self.server.registry
        other=registry.save('users',dict(name='unaffected'))['id']
        credential=registry.authorize_node_user(self.nid,other)['uuid']
        deadline=time.time()+3
        registry.temporary_bandwidth(self.uid,dict(bandwidth=limits(),expires_at=deadline))
        await self.sync()
        reader,writer=await test_xray.XrayRealTests.connect_client(self,credential)
        try:
            self.assertEqual(await asyncio.wait_for(reader.readexactly(20),2),b'\0\0authenticated data')
            await asyncio.sleep(max(0,deadline-time.time())+.1)
            writer.write(b'still active'); await writer.drain()
            self.assertEqual(await asyncio.wait_for(reader.readexactly(12),1),b'still active')
            self.assertIsNotNone(self.runtime.core.process)
            self.assertIsNone(self.runtime.core.process.returncode)
        finally:
            writer.close()
            try: await writer.wait_closed()
            except OSError: pass
