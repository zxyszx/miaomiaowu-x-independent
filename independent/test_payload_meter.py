"""Real authenticated payload metering excludes protocol and rejected traffic."""
import asyncio
import json
import unittest
import uuid
import time
from types import SimpleNamespace
from unittest import mock
from payload_meter import PayloadMeter, MeteredWriter, CycleQuota, QuotaExceeded
from bandwidth import BytePacer, CombinedPacer
from automatic_bandwidth import DownloadController
import test_xray
import test_user_connections
import test_reality
from test_runtime import free_port


def fixed_cycle_clock(test):
    """Keep quota fixtures in-period without replacing the process wall clock."""
    now=time.time()
    test.enterContext(mock.patch('usage_cycles.time',SimpleNamespace(time=lambda:now)))
    return now


class Writer:
    def __init__(self): self.blocks=[]
    def write(self,data): self.blocks.append(bytes(data))
    async def drain(self): pass


class MeterTests(unittest.IsolatedAsyncioTestCase):
    def fixture(self):
        meter=PayloadMeter(); meter.bind(1); writer=Writer()
        return meter,writer,MeteredWriter(writer,meter,(2,3,str(uuid.uuid4())),0)

    async def test_paced_blocks_count_once_and_snapshot_is_detached(self):
        meter,writer,measured=self.fixture()
        await CombinedPacer(BytePacer(10000)).write(measured,b'x'*1300)
        self.assertGreater(len(writer.blocks),1)
        self.assertEqual(sum(map(len,writer.blocks)),1300)
        result=meter.snapshot(); self.assertEqual(result['items'][0]['upload_bytes'],1300)
        result['items'][0]['upload_bytes']=0; result['items'].clear()
        self.assertEqual(meter.snapshot()['items'][0]['upload_bytes'],1300)
        meter.bind(1)
        with self.assertRaises(ValueError): meter.bind(2)

    async def test_write_failure_does_not_count_drain_failure_counts_queued_bytes(self):
        meter,writer,measured=self.fixture()
        writer.write=mock.Mock(side_effect=OSError('closed'))
        with self.assertRaises(OSError): await BytePacer(None).write(measured,b'failed')
        self.assertEqual(meter.snapshot()['items'],[])
        writer.write=mock.Mock(); writer.drain=mock.AsyncMock(side_effect=OSError('drain failed'))
        with self.assertRaises(OSError): await BytePacer(None).write(measured,b'queued')
        self.assertEqual(meter.snapshot()['items'][0]['upload_bytes'],6)

    async def test_cancel_during_pacing_counts_only_written_blocks(self):
        meter,writer,measured=self.fixture()
        first=asyncio.Event(); original=writer.write
        def write(data): original(data); first.set()
        writer.write=write
        task=asyncio.create_task(CombinedPacer(BytePacer(1000)).write(measured,b'x'*10000))
        await asyncio.wait_for(first.wait(),1); task.cancel()
        with self.assertRaises(asyncio.CancelledError): await task
        total=sum(map(len,writer.blocks))
        self.assertTrue(0<total<10000)
        self.assertEqual(meter.snapshot()['items'][0]['upload_bytes'],total)

    async def test_cancel_in_drain_does_not_lose_queued_block(self):
        meter,writer,measured=self.fixture(); entered=asyncio.Event()
        async def drain(): entered.set(); await asyncio.Future()
        writer.drain=drain
        task=asyncio.create_task(BytePacer(None).write(measured,b'sent'))
        await entered.wait(); task.cancel()
        with self.assertRaises(asyncio.CancelledError): await task
        self.assertEqual(meter.snapshot()['items'][0]['upload_bytes'],4)

    async def test_automatic_download_cap_counts_payload_once(self):
        meter,writer,measured=self.fixture(); measured.direction=1
        rule=dict(enabled=True,threshold_bps=1000,sustain_seconds=1,download_bps=10000,duration_seconds=5)
        controller=DownloadController(rule,lambda _:None)
        import time
        controller.until=time.monotonic()+5
        await controller.write(measured,b'd'*1300,BytePacer(None),BytePacer(None),None)
        self.assertEqual(meter.snapshot()['items'][0]['download_bytes'],1300)
        self.assertEqual(sum(map(len,writer.blocks)),1300)

    async def test_cycle_quota_caps_payload_and_disconnects_after_final_bytes(self):
        meter,writer,_=self.fixture()
        quota=CycleQuota(upload_limit=5,download_limit=7,upload_used=1,download_used=3)
        measured=MeteredWriter(writer,meter,(2,3,str(uuid.uuid4())),0,quota)
        measured.write(b'ab')
        with self.assertRaises(QuotaExceeded): measured.write(b'1234')
        self.assertEqual(b''.join(writer.blocks),b'ab12')
        self.assertEqual(meter.snapshot()['items'][0]['upload_bytes'],4)
        with self.assertRaises(QuotaExceeded): measured.write(b'x')

    async def test_cycle_quota_is_shared_by_directions_and_rejects_bad_baseline(self):
        with self.assertRaises(ValueError): CycleQuota(upload_limit=1,upload_used=2)
        quota=CycleQuota(upload_limit=2,download_limit=3)
        self.assertEqual((quota.reserve(0,2),quota.reserve(1,3)),(2,3))
        self.assertEqual((quota.reserve(0,1),quota.reserve(1,1)),(0,0))

    async def test_cycle_quota_reconcile_keeps_usage_through_stale_and_unlimited_rules(self):
        quota=CycleQuota(upload_limit=10,download_limit=20,upload_used=2)
        self.assertEqual(quota.reserve(0,3),3)
        quota.reconcile(10,20,0,0)
        self.assertEqual(quota.remaining,[5,20])
        quota.reconcile(10,20,7,0)
        self.assertEqual(quota.remaining,[3,20])
        quota.reconcile(None,None)
        quota.reserve(0,4)
        quota.reconcile(10,20)
        self.assertEqual(quota.reserve(0,1),0)
        quota.reconcile(15,20)
        self.assertEqual(quota.reserve(0,20),4)
        self.assertEqual(quota.used,[15,0])
        with self.assertRaises(ValueError): quota.reconcile(15,20,True,0)
        self.assertEqual(quota.used,[15,0])


@unittest.skipUnless(test_xray.BINARY.is_file(),'Set XRAY_BINARY')
class PayloadRealTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_user_connections.ConnectionRealTests.setUp
    tearDown=test_user_connections.ConnectionRealTests.tearDown
    request=test_user_connections.ConnectionRealTests.request
    asyncSetUp=test_user_connections.ConnectionRealTests.asyncSetUp
    asyncTearDown=test_user_connections.ConnectionRealTests.asyncTearDown
    open=test_user_connections.ConnectionRealTests.open
    accepted=test_user_connections.ConnectionRealTests.accepted
    rejected=test_user_connections.ConnectionRealTests.rejected
    close_writer=test_user_connections.ConnectionRealTests.close_writer

    async def sync(self): await self.runtime.apply(self.server.registry.configuration(self.token))
    def item(self):
        values=self.runtime.usage_snapshot()['items']
        self.assertEqual(len(values),1)
        return values[0]

    async def test_asymmetric_target_greeting_keeps_upload_download_direction(self):
        self.target.close(); await self.target.wait_closed()
        greeting=b'target greeting!'
        async def greet(reader,writer):
            try:
                writer.write(greeting); await writer.drain()
                while data:=await reader.read(65536):
                    writer.write(data); await writer.drain()
            finally:
                writer.close()
                try: await writer.wait_closed()
                except OSError: pass
        self.target=await asyncio.start_server(greet,'127.0.0.1',self.target_port)
        await self.sync(); reader,writer=await self.open()
        self.assertEqual(await asyncio.wait_for(reader.readexactly(2+len(greeting)+5),3),
                         b'\0\0'+greeting+b'hello')
        self.assertEqual(self.item()['upload_bytes'],5)
        self.assertEqual(self.item()['download_bytes'],len(greeting)+5)
        await self.close_writer(writer)

    async def test_large_echo_exact_payload_and_reload_close_retain_counters(self):
        await self.sync(); reader,writer=await self.open(); await self.accepted(reader)
        payload=b'payload'*20000; writer.write(payload); await writer.drain()
        self.assertEqual(await asyncio.wait_for(reader.readexactly(len(payload)),4),payload)
        self.assertEqual((self.item()['upload_bytes'],self.item()['download_bytes']),(len(payload)+5,)*2)
        await self.sync(); self.assertEqual(self.item()['upload_bytes'],len(payload)+5)
        await self.runtime.close(); self.assertEqual(self.item()['download_bytes'],len(payload)+5)
        await self.sync(); reader,_=await self.open(); await self.accepted(reader)
        self.assertEqual(self.item()['upload_bytes'],len(payload)+10)

    async def test_cycle_quota_disconnects_vless_after_final_payload(self):
        from usage_cycles import UsageCycles
        import time
        r=self.server.registry; now=fixed_cycle_clock(self)
        # Insert the execution period directly so the real transport test stays
        # focused on cycle attribution/quota enforcement; the public cycle API
        # separately requires a plan binding and is covered by period tests.
        cycle_id='vless-quota-cycle'
        with r.connect() as db:
            db.execute('INSERT INTO usage_cycles(id,user_id,starts_at,ends_at) VALUES(?,?,?,?)',
                       (cycle_id,self.uid,now-1,now+120))
            from usage_accounting import initialize_cycle
            initialize_cycle(db,cycle_id)
        cycle={'id':cycle_id}
        UsageCycles(r).set_budget(cycle['id'],{'upload_bytes':7,'download_bytes':2**63-1})
        snapshot=r.configuration(self.token)
        self.assertIn('nodes',snapshot,repr(snapshot))
        await self.runtime.apply(snapshot)
        reader,writer=await self.open(); await self.accepted(reader)
        writer.write(b'abcd'); await writer.drain()
        # The runtime queues the final two permitted bytes, then closes the
        # stream as soon as the quota rejects the remainder of this write.
        self.assertEqual(await asyncio.wait_for(reader.read(1),3),b'')
        self.assertEqual(self.item()['upload_bytes'],7)

    async def test_usage_baseline_sync_keeps_live_connection_and_quota_object(self):
        import copy
        from node_config import digest
        from usage_cycles import UsageCycles
        import time
        r=self.server.registry; now=fixed_cycle_clock(self)
        with r.connect() as db:
            db.execute('INSERT INTO usage_cycles(id,user_id,starts_at,ends_at) VALUES(?,?,?,?)',
                       ('hot-usage',self.uid,now-1,now+120))
            from usage_accounting import initialize_cycle
            initialize_cycle(db,'hot-usage')
        UsageCycles(r).set_budget('hot-usage',dict(upload_bytes=20,download_bytes=100))
        await self.sync()
        reader,writer=await self.open(); await self.accepted(reader)
        process=self.runtime.core.process
        quota=self.runtime.quotas[(self.uid,'hot-usage')]
        for index in range(3):
            value=copy.deepcopy(self.runtime.snapshot)
            value['revision']+=1
            for node in value['nodes']:
                for client in node['clients']:
                    client['cycle_usage']=dict(upload_bytes=5+index,download_bytes=5+index)
            value['digest']=digest({k:v for k,v in value.items() if k!='digest'})
            await self.runtime.apply(value)
            self.assertIs(self.runtime.core.process,process)
            self.assertIs(self.runtime.quotas[(self.uid,'hot-usage')],quota)
            writer.write(b'ab'); await writer.drain()
            self.assertEqual(await asyncio.wait_for(reader.readexactly(2),3),b'ab')
        self.assertEqual(quota.used,[11,11])
        self.assertEqual(self.item()['upload_bytes'],11)
        await self.close_writer(writer)

    async def test_usage_baseline_storage_failure_closes_connections_and_fences_revision(self):
        import copy
        from node_config import digest
        await self.sync()
        reader,writer=await self.open(); await self.accepted(reader)
        # Add a period through a normal policy reload first.
        value=copy.deepcopy(self.runtime.snapshot); value['revision']+=1
        for node in value['nodes']:
            for client in node['clients']:
                client.update(cycle_id='failure-period',cycle_budget=dict(upload_bytes=100,download_bytes=100),
                              cycle_usage=dict(upload_bytes=0,download_bytes=0))
        value['digest']=digest({k:v for k,v in value.items() if k!='digest'})
        await self.runtime.apply(value)
        reader,writer=await self.open(); await self.accepted(reader)
        newer=copy.deepcopy(value); newer['revision']+=1
        newer['nodes'][0]['clients'][0]['cycle_usage']['upload_bytes']=6
        newer['digest']=digest({k:v for k,v in newer.items() if k!='digest'})
        quota=self.runtime.quotas[(self.uid,'failure-period')]
        with mock.patch.object(quota,'reconcile',side_effect=OSError('fixture disk failure')):
            with self.assertRaises(OSError): await self.runtime.apply(newer)
        self.assertEqual(await asyncio.wait_for(reader.read(),3),b'')
        self.assertIsNone(self.runtime.snapshot)
        with self.assertRaises(ValueError): await self.runtime.apply(value)
        await self.runtime.apply(newer)
        self.assertEqual(self.runtime.snapshot,newer)

    async def test_cycle_quota_survives_close_and_changed_configuration(self):
        from usage_cycles import UsageCycles
        import time
        import inspect
        started=time.monotonic()
        started_wall=time.time()
        shutdowns=[]
        original_close=self.runtime._close
        async def traced_close():
            shutdowns.append(dict(
                caller=inspect.currentframe().f_back.f_code.co_name,
                expiry_task=asyncio.current_task() is self.runtime.expiry_task,
                elapsed=time.monotonic()-started,wall_time=time.time(),
                revision=(self.runtime.snapshot or {}).get('revision'),
                valid_until=(self.runtime.snapshot or {}).get('valid_until'),
                deadlines=self.runtime.deadlines.copy()))
            await original_close()
        close_trace=mock.patch.object(self.runtime,'_close',new=traced_close)
        close_trace.start()
        self.addCleanup(close_trace.stop)
        diagnostics=mock.patch('vless_runtime.logger.debug')
        close_log=diagnostics.start()
        self.addCleanup(diagnostics.stop)
        def reload_state():
            quota=self.runtime.quotas.get((self.uid,'reload-cycle'))
            process=self.runtime.core.process
            return dict(quota_used=quota.used[:] if quota else None,
                        quota_remaining=quota.remaining[:] if quota else None,
                        tasks=len(self.runtime.tasks),updating=self.runtime.updating,
                        wall_time=time.time(),cycle_ends_at=now+120,
                        elapsed=time.monotonic()-started,
                        clock_shift=time.time()-started_wall-(time.monotonic()-started),
                        shutdowns=shutdowns[:],
                        snapshot_valid_until=(self.runtime.snapshot or {}).get('valid_until'),
                        deadlines=self.runtime.deadlines.copy(),
                        process_returncode=process.returncode if process else 'not running',
                        close_events=[call.args for call in close_log.call_args_list])
        async def accepted(stage):
            reader,_=await self.open()
            try: await self.accepted(reader)
            except Exception as error:
                raise AssertionError(dict(stage=stage,**reload_state())) from error
        r=self.server.registry; now=time.time()
        from types import SimpleNamespace
        # This case tests reload durability, not movement across cycle starts.
        # Keep the cycle clock stable; a separate real-transport regression
        # covers clock rollback withdrawing and restoring this identity.
        self.enterContext(mock.patch('usage_cycles.time',SimpleNamespace(time=lambda:now)))
        with r.connect() as db:
            db.execute('INSERT INTO usage_cycles(id,user_id,starts_at,ends_at) VALUES(?,?,?,?)',
                       ('reload-cycle',self.uid,now-1,now+120))
            from usage_accounting import initialize_cycle
            initialize_cycle(db,'reload-cycle')
        UsageCycles(r).set_budget('reload-cycle',{'upload_bytes':12,'download_bytes':100})
        await self.sync()
        await accepted('initial configuration')
        await self.runtime.close()
        await self.sync() # master still reports zero; local five must survive
        await accepted('reopened configuration')
        self.assertEqual(self.item()['upload_bytes'],10)
        r.user_connections(self.uid,dict(max_connections=2))
        await self.sync() # policy change closes old runtime before activation
        reader,_=await self.open()
        await asyncio.wait_for(reader.read(),3)
        quota=self.runtime.quotas.get((self.uid,'reload-cycle'))
        self.assertIsNotNone(quota,reload_state())
        self.assertEqual(self.item()['upload_bytes'],12,reload_state())
        self.assertEqual(quota.remaining[0],0)
        self.assertEqual([event['caller'] for event in shutdowns],['close','_apply'],reload_state())
        self.assertFalse(any(event['expiry_task'] for event in shutdowns),reload_state())
        close_log.assert_not_called() # quota exhaustion is expected

    async def test_clock_before_cycle_start_revokes_then_restores_durable_quota(self):
        from usage_cycles import UsageCycles
        from types import SimpleNamespace
        import time
        registry=self.server.registry
        now=fixed_cycle_clock(self)
        with registry.connect() as db:
            db.execute('INSERT INTO usage_cycles(id,user_id,starts_at,ends_at) VALUES(?,?,?,?)',
                       ('clock-cycle',self.uid,now-1,now+120))
            from usage_accounting import initialize_cycle
            initialize_cycle(db,'clock-cycle')
        UsageCycles(registry).set_budget('clock-cycle',{'upload_bytes':7,'download_bytes':100})
        await self.sync()
        reader,_=await self.open()
        await self.accepted(reader)
        self.assertEqual(self.item()['upload_bytes'],5)
        # A rollback before starts_at legitimately withdraws the identity.
        # Patch only the cycle clock during synchronous snapshot generation.
        with mock.patch('usage_cycles.time',SimpleNamespace(time=lambda:now-2)):
            withdrawn=registry.configuration(self.token)
        self.assertEqual(withdrawn['nodes'][0]['clients'],[])
        await self.runtime.apply(withdrawn)
        await self.rejected(reader)
        denied,_=await self.open()
        await self.rejected(denied)
        self.assertEqual(self.runtime.quotas,{})
        restored=registry.configuration(self.token)
        self.assertGreater(restored['revision'],withdrawn['revision'])
        await self.runtime.apply(restored)
        reader,_=await self.open()
        await asyncio.wait_for(reader.read(),3)
        self.assertEqual(self.item()['upload_bytes'],7)
        self.assertEqual(self.runtime.quotas[(self.uid,'clock-cycle')].remaining[0],0)

    async def test_clock_rollback_after_ten_bytes_does_not_refund_reload_quota(self):
        from usage_cycles import UsageCycles
        registry=self.server.registry; now=fixed_cycle_clock(self)
        with registry.connect() as db:
            db.execute('INSERT INTO usage_cycles(id,user_id,starts_at,ends_at) VALUES(?,?,?,?)',
                       ('ten-byte-cycle',self.uid,now-1,now+120))
            from usage_accounting import initialize_cycle
            initialize_cycle(db,'ten-byte-cycle')
        UsageCycles(registry).set_budget('ten-byte-cycle',dict(upload_bytes=12,download_bytes=100))
        await self.sync()
        reader,_=await self.open(); await self.accepted(reader)
        await self.runtime.close()
        await self.sync()
        reader,_=await self.open(); await self.accepted(reader)
        self.assertEqual(self.item()['upload_bytes'],10)
        registry.user_connections(self.uid,dict(max_connections=2))
        with mock.patch('usage_cycles.time',SimpleNamespace(time=lambda:now-9)):
            withdrawn=registry.configuration(self.token)
        self.assertEqual(withdrawn['nodes'][0]['clients'],[])
        await self.runtime.apply(withdrawn)
        await self.rejected(reader)
        reader,_=await self.open(); await self.rejected(reader)
        # Reproduce the observed ten-byte counter without assuming that this
        # injected clock shift caused the earlier uninstrumented failure.
        self.assertEqual(self.item()['upload_bytes'],10)
        self.assertEqual(self.runtime.quotas,{})
        await self.sync()
        reader,_=await self.open()
        await asyncio.wait_for(reader.read(),3)
        self.assertEqual(self.item()['upload_bytes'],12)
        self.assertEqual(self.runtime.quotas[(self.uid,'ten-byte-cycle')].remaining[0],0)

    async def test_core_connection_failure_has_private_stage_diagnostic(self):
        import errno
        await self.sync()
        connect=asyncio.open_connection
        inner_ports={entry.inner_port for entry in self.runtime.listeners.values()}
        async def refused_core(host,port,*args,**kwargs):
            if port in inner_ports:
                # Exception messages may contain addresses or credentials.
                raise ConnectionRefusedError(errno.ECONNREFUSED,'private fixture information')
            return await connect(host,port,*args,**kwargs)
        with mock.patch('vless_runtime.asyncio.open_connection',side_effect=refused_core), \
             self.assertLogs('vless_runtime',level='DEBUG') as events:
            reader,_=await self.open()
            self.assertEqual(await asyncio.wait_for(reader.read(),3),b'')
        self.assertEqual(self.runtime.usage_snapshot()['items'],[])
        self.assertEqual(len(events.output),1)
        self.assertIn('phase=core_connect error=ConnectionRefusedError errno=',events.output[0])
        self.assertIn('revision='+str(self.runtime.snapshot['revision']),events.output[0])
        self.assertIn('core_pid='+str(self.runtime.core.process.pid),events.output[0])
        self.assertIn('core_returncode=None',events.output[0])
        self.assertNotIn('private fixture information',events.output[0])
        self.assertNotIn(self.credential['uuid'],events.output[0])

    async def test_partial_handshakes_do_not_survive_repeated_policy_reload(self):
        await self.sync()
        previous=None
        for maximum in (1,2)*4:
            with self.subTest(maximum=maximum,upload=self.runtime.usage_snapshot()['items']):
                # Stop authentication after version + partial UUID. A reload
                # must close this admitted socket without counting header bytes.
                existing=len(self.runtime.tasks)
                partial,writer=await asyncio.open_connection('127.0.0.1',self.port)
                self.addAsyncCleanup(self.close_writer,writer)
                writer.write(b'\0'+uuid.UUID(self.credential['uuid']).bytes[:7])
                await writer.drain()
                async def started():
                    while len(self.runtime.tasks)<=existing: await asyncio.sleep(0)
                await asyncio.wait_for(started(),3)
                self.server.registry.user_connections(self.uid,dict(max_connections=maximum))
                await asyncio.wait_for(self.sync(),3)
                await self.rejected(partial)
                if previous is not None: await self.rejected(previous)
                self.assertFalse(self.runtime.tasks)
                self.assertFalse(self.runtime.accept_tasks)
                previous,writer=await self.open()
                await self.accepted(previous)
        self.assertEqual((self.item()['upload_bytes'],self.item()['download_bytes']),(40,40))

    async def test_dead_core_failure_records_exit_status_without_counting_payload(self):
        await self.sync()
        process=self.runtime.core.process
        process.terminate()
        await asyncio.wait_for(process.wait(),3)
        self.assertIsNotNone(process.returncode)
        with self.assertLogs('vless_runtime',level='DEBUG') as events:
            reader,_=await self.open()
            self.assertEqual(await asyncio.wait_for(reader.read(),3),b'')
        self.assertEqual(self.runtime.usage_snapshot()['items'],[])
        self.assertEqual(len(events.output),1)
        self.assertIn('phase=core_connect',events.output[0])
        self.assertIn('core_pid='+str(process.pid),events.output[0])
        self.assertIn('core_returncode='+str(process.returncode),events.output[0])

    async def test_unknown_and_connection_limit_rejection_count_no_payload(self):
        r=self.server.registry; r.user_connections(self.uid,dict(max_connections=1)); await self.sync()
        reader,_=await self.open(str(uuid.uuid4())); await self.rejected(reader)
        self.assertEqual(self.runtime.usage_snapshot()['items'],[])
        reader,_=await self.open(); await self.accepted(reader)
        denied,_=await self.open(); await self.rejected(denied)
        self.assertEqual((self.item()['upload_bytes'],self.item()['download_bytes']),(5,5))

    async def test_vless_new_runtime_keeps_quota_without_a_usage_receipt(self):
        from usage_cycles import UsageCycles
        from vless_runtime import VlessRuntime
        import time
        r=self.server.registry; now=fixed_cycle_clock(self)
        with r.connect() as db:
            db.execute('INSERT INTO usage_cycles(id,user_id,starts_at,ends_at) VALUES(?,?,?,?)',
                       ('restart-cycle',self.uid,now-1,now+120))
            from usage_accounting import initialize_cycle
            initialize_cycle(db,'restart-cycle')
        UsageCycles(r).set_budget('restart-cycle',dict(upload_bytes=7,download_bytes=100))
        await self.sync()
        reader,_=await self.open(); await self.accepted(reader)
        state=self.runtime.quota_path.parent
        await self.runtime.close()
        self.runtime=VlessRuntime(test_xray.BINARY,state,
            local_test_target=('127.0.0.1',self.target_port))
        await self.sync()
        reader,_=await self.open()
        await asyncio.wait_for(reader.read(),3)
        self.assertEqual(self.item()['upload_bytes'],2)
        self.assertEqual(self.runtime.quotas[(self.uid,'restart-cycle')].used[0],7)

    async def test_inactive_cycle_cache_is_released_without_refunding_old_budget(self):
        import copy
        import sqlite3
        from node_config import digest
        r=self.server.registry; base=r.configuration(self.token)
        async def apply_cycle(cycle,revision):
            snapshot=copy.deepcopy(base); snapshot['revision']+=revision
            for node in snapshot['nodes']:
                for client in node['clients']:
                    client.update(cycle_id=cycle,
                        cycle_budget=dict(upload_bytes=7,download_bytes=100),
                        cycle_usage=dict(upload_bytes=0,download_bytes=0))
            snapshot['digest']=digest({k:v for k,v in snapshot.items() if k!='digest'})
            await self.runtime.apply(snapshot)
        # Distinct isolated execution periods exercise reload and actual writes.
        # The controller's cycle lifecycle is separately covered by its API tests.
        for index in range(8):
            cycle=f'cache-period-{index}'
            await apply_cycle(cycle,index+1)
            reader,writer=await self.open(); await self.accepted(reader)
            await self.close_writer(writer)
            self.assertEqual(set(self.runtime.cycle_quotas),{(self.uid,cycle)})
        await self.runtime.close()
        self.assertEqual(self.runtime.cycle_quotas,{})
        with sqlite3.connect(self.runtime.quota_path) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM quotas').fetchone()[0],8)
        # Re-add the old period with a stale zero baseline: only two bytes remain.
        await apply_cycle('cache-period-0',9)
        reader,_=await self.open()
        # The protocol header is unmetered and can be sent while the durable
        # two-byte upload allowance is awaited; no application echo is allowed.
        self.assertIn(await asyncio.wait_for(reader.read(),3),(b'',b'\x00\x00'))
        self.assertEqual(self.runtime.quotas[(self.uid,'cache-period-0')].used[0],7)
        self.assertEqual(self.item()['upload_bytes'],42)

    async def test_separate_users_and_nodes_are_metered_independently(self):
        r=self.server.registry
        other=r.save('users',dict(name='other'))['id']; auth=r.authorize_node_user(self.nid,other)['uuid']
        nid=r.save('nodes',dict(name='second',server_id=r.configuration(self.token)['server_id']))['id']
        port=free_port(); r.configure_node(nid,dict(test_xray.VLESS,listen_port=port))
        second=r.authorize_node_user(nid,self.uid)['uuid']; await self.sync()
        pairs=await asyncio.gather(self.open(),self.open(auth),self.open(second,port))
        for reader,_ in pairs: await self.accepted(reader)
        values=self.runtime.usage_snapshot()['items']; self.assertEqual(len(values),3)
        self.assertEqual({(x['node_id'],x['user_id']) for x in values},{(self.nid,self.uid),(self.nid,other),(nid,self.uid)})
        self.assertTrue(all(x['upload_bytes']==5 and x['download_bytes']==5 for x in values))

    async def test_credential_rotation_keeps_old_stream_total_separate(self):
        r=self.server.registry; await self.sync()
        reader,_=await self.open(); await self.accepted(reader)
        r.authorize_node_user(self.nid,self.uid,revoke=True); await self.sync()
        auth=r.authorize_node_user(self.nid,self.uid)['uuid']; await self.sync()
        reader,_=await self.open(auth); await self.accepted(reader)
        values=self.runtime.usage_snapshot()['items']; self.assertEqual(len(values),2)
        self.assertEqual({x['credential'] for x in values},{auth,self.credential['uuid']})
        self.assertEqual(sum(x['upload_bytes'] for x in values),10)

    async def test_real_payload_manually_reported_over_http_deduplicates(self):
        r=self.server.registry; await self.sync(); reader,writer=await self.open(); await self.accepted(reader)
        session=str(uuid.uuid4()); r.begin_session(self.token,dict(session_id=session))
        from agent import request
        async def report(seq):
            data=dict(session_id=session,sequence=seq,items=self.runtime.usage_snapshot()['items'])
            return await asyncio.to_thread(request,self.base,self.token,'/agent/usage',data)
        self.assertFalse((await report(1))['duplicate']); self.assertTrue((await report(1))['duplicate'])
        writer.write(b'next'); await writer.drain(); self.assertEqual(await asyncio.wait_for(reader.readexactly(4),2),b'next')
        await report(2)
        with self.request(f'/api/users/{self.uid}/usage') as response: total=json.load(response)
        self.assertEqual((total['upload_bytes'],total['download_bytes']),(9,9))

    async def test_enabled_automatic_rule_and_header_only_connection(self):
        r=self.server.registry; r.automatic_bandwidth(self.uid,dict(enabled=True,threshold_bps=100000,sustain_seconds=2,download_bps=20000,duration_seconds=3))
        await self.sync()
        reader,writer=await asyncio.open_connection('127.0.0.1',self.port)
        self.addAsyncCleanup(self.close_writer,writer)
        header=b'\0'+uuid.UUID(self.credential['uuid']).bytes+b'\0\1'+self.target_port.to_bytes(2,'big')+b'\1\x7f\0\0\1'
        writer.write(header); await writer.drain(); await asyncio.sleep(.1)
        self.assertEqual(self.runtime.usage_snapshot()['items'],[])
        payload=b'd'*40000; writer.write(payload); await writer.drain()
        self.assertEqual(await asyncio.wait_for(reader.readexactly(len(payload)+2),3),b'\0\0'+payload)
        self.assertEqual((self.item()['upload_bytes'],self.item()['download_bytes']),(len(payload),)*2)


@unittest.skipUnless(test_xray.BINARY.is_file(),'Set XRAY_BINARY')
class PayloadRealityTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=test_reality.RealityRealTests.asyncSetUp
    asyncTearDown=test_reality.RealityRealTests.asyncTearDown
    echo=test_reality.RealityRealTests.echo
    client=test_reality.RealityRealTests.client
    exchange=test_reality.RealityRealTests.exchange

    def budget(self, upload, download):
        from usage_cycles import UsageCycles
        import time
        now=fixed_cycle_clock(self)
        with self.registry.connect() as db:
            db.execute('INSERT INTO usage_cycles(id,user_id,starts_at,ends_at) VALUES(?,?,?,?)',
                       ('encrypted-budget',self.uid,now-1,now+120))
            from usage_accounting import initialize_cycle
            initialize_cycle(db,'encrypted-budget')
        UsageCycles(self.registry).set_budget('encrypted-budget',
            dict(upload_bytes=upload,download_bytes=download))

    def measured(self):
        items=self.runtime.usage_snapshot()['items']
        self.assertEqual(len(items),1)
        self.assertEqual((items[0]['node_id'],items[0]['user_id'],items[0]['credential']),
                         (self.nid,self.uid,self.credential))
        return items[0]

    async def exhaust(self, payload):
        reader,writer=await self.client()
        try:
            writer.write(payload); await writer.drain()
            # A real encrypted stream must terminate, not just report a capped
            # counter while continuing to pass unmetered application bytes.
            return await asyncio.wait_for(reader.read(),5)
        finally:
            writer.close()
            try: await writer.wait_closed()
            except OSError: pass

    async def test_encrypted_usage_sync_keeps_both_cores_and_live_stream(self):
        import copy
        from node_config import digest
        self.budget(20,100)
        await self.runtime.apply(self.registry.configuration(self.token))
        reader,writer=await self.client()
        try:
            writer.write(b'hello'); await writer.drain()
            self.assertEqual(await asyncio.wait_for(reader.readexactly(5),4),b'hello')
            front=self.runtime.front.process; core=self.runtime.paced.core.process
            quota=self.runtime.paced.quotas[(self.uid,'encrypted-budget')]
            original=copy.deepcopy(self.runtime.snapshot)
            for index in range(3):
                value=copy.deepcopy(self.runtime.snapshot); value['revision']+=1
                value['nodes'][0]['clients'][0]['cycle_usage']=dict(upload_bytes=5+index,download_bytes=5+index)
                value['digest']=digest({k:v for k,v in value.items() if k!='digest'})
                await self.runtime.apply(value)
                self.assertIs(self.runtime.front.process,front)
                self.assertIs(self.runtime.paced.core.process,core)
                self.assertIs(self.runtime.paced.quotas[(self.uid,'encrypted-budget')],quota)
                writer.write(b'ab'); await writer.drain()
                self.assertEqual(await asyncio.wait_for(reader.readexactly(2),3),b'ab')
            self.assertEqual(quota.used,[11,11])
            with self.assertRaises(ValueError): await self.runtime.apply(original)
            # A higher remote baseline consumes remaining credit in the SAME writer.
            value=copy.deepcopy(self.runtime.snapshot); value['revision']+=1
            value['nodes'][0]['clients'][0]['cycle_usage']['upload_bytes']=19
            value['digest']=digest({k:v for k,v in value.items() if k!='digest'})
            await self.runtime.apply(value)
            writer.write(b'too much'); await writer.drain()
            await asyncio.wait_for(reader.read(),4)
            self.assertEqual(quota.used[0],20)
            self.assertEqual(self.measured()['upload_bytes'],12)
        finally:
            writer.close()
            try: await writer.wait_closed()
            except OSError: pass

    async def test_encrypted_upload_budget_disconnects_and_cannot_reconnect(self):
        self.budget(7,100)
        await self.runtime.apply(self.registry.configuration(self.token))
        self.assertEqual(await self.exchange(payload=b'hello'),b'hello')
        tail=await self.exhaust(b'abcd')
        self.assertLessEqual(len(tail),2)
        self.assertEqual(self.measured()['upload_bytes'],7)
        self.assertEqual(await self.exhaust(b'again'),b'')
        self.assertEqual(self.measured()['upload_bytes'],7)

    async def test_encrypted_download_budget_stops_echo_without_counting_tls(self):
        self.budget(100,7)
        await self.runtime.apply(self.registry.configuration(self.token))
        self.assertEqual(await self.exchange(payload=b'hello'),b'hello')
        tail=await self.exhaust(b'abcd')
        self.assertLessEqual(len(tail),2)
        self.assertEqual(self.measured()['download_bytes'],7)
        self.assertEqual(self.measured()['upload_bytes'],9)
        self.assertEqual(await self.exhaust(b'again'),b'')
        self.assertEqual(self.measured()['download_bytes'],7)

    async def test_encrypted_budget_survives_core_restart_and_policy_reload(self):
        self.budget(12,100)
        await self.runtime.apply(self.registry.configuration(self.token))
        self.assertEqual(await self.exchange(payload=b'hello'),b'hello')
        await self.runtime.close()
        await self.runtime.apply(self.registry.configuration(self.token))
        self.assertEqual(await self.exchange(payload=b'hello'),b'hello')
        self.registry.user_connections(self.uid,dict(max_connections=2))
        await self.runtime.apply(self.registry.configuration(self.token))
        self.assertLessEqual(len(await self.exhaust(b'abcd')),2)
        self.assertEqual(self.measured()['upload_bytes'],12)
        self.assertEqual(self.runtime.paced.quotas[(self.uid,'encrypted-budget')].remaining[0],0)

    async def test_encrypted_budget_survives_a_new_runtime_with_zero_master_usage(self):
        from reality_runtime import RealityRuntime
        self.budget(7,100)
        snapshot=self.registry.configuration(self.token)
        await self.runtime.apply(snapshot)
        self.assertEqual(await self.exchange(payload=b'hello'),b'hello')
        await self.runtime.close()
        self.runtime=RealityRuntime(test_xray.BINARY,self.root/'server',
            local_test_target=('127.0.0.1',self.target_port),
            reality_test_target=('127.0.0.1',self.decoy.sockets[0].getsockname()[1]))
        await self.runtime.apply(snapshot)
        self.assertEqual(self.runtime.paced.quotas[(self.uid,'encrypted-budget')].used[0],5)
        self.assertLessEqual(len(await self.exhaust(b'abcd')),2)
        self.assertEqual(self.measured()['upload_bytes'],2)
        self.assertEqual(self.runtime.paced.quotas[(self.uid,'encrypted-budget')].remaining[0],0)

    async def test_real_encrypted_payload_counts_once_without_tls_bridge_or_vless_headers(self):
        await self.runtime.apply(self.registry.configuration(self.token))
        payload=b'reality-payload'*4000
        self.assertEqual(await self.exchange(payload=payload),payload)
        measured=self.runtime.usage_snapshot()
        self.assertEqual(len(measured['items']),1)
        item=measured['items'][0]
        self.assertEqual((item['upload_bytes'],item['download_bytes']),(len(payload),)*2)
        self.assertEqual((item['node_id'],item['user_id'],item['credential']),(self.nid,self.uid,self.credential))
        await self.runtime.close(); self.assertEqual(self.runtime.usage_snapshot(),measured)

    async def test_failed_encrypted_authentication_never_creates_usage_entry(self):
        await self.runtime.apply(self.registry.configuration(self.token))
        reader,writer=await self.client(str(uuid.uuid4()))
        try:
            writer.write(b'not-authorized'); await writer.drain()
            try: self.assertEqual(await asyncio.wait_for(reader.read(1),3),b'')
            except ConnectionResetError: pass
            self.assertEqual(self.runtime.usage_snapshot()['items'],[])
        finally:
            writer.close()
            try: await writer.wait_closed()
            except OSError: pass
