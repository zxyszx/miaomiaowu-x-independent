import asyncio
import json
import sqlite3
from pathlib import Path
import tempfile
import threading
import time
import unittest
import urllib.error
import uuid
from types import SimpleNamespace
from unittest import mock

from usage_delivery import UsageDelivery,drained
from usage_outbox import UsageOutbox
from loop_probe import LoopProbe
import test_usage_settlement
import test_xray


def scoped_period_clock(test):
    """Advance period/proof time without changing asyncio or process clocks."""
    clock=mock.Mock(return_value=time.time())
    for module in ('usage_cycles','usage_settlement'):
        test.enterContext(mock.patch(module+'.time',SimpleNamespace(time=clock)))
    return clock


class DeliveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.session=str(uuid.uuid4()); self.state=Path(self.temp.name)
        self.item=dict(node_id=1,user_id=2,credential=str(uuid.uuid4()),upload_bytes=5,download_bytes=7)
        self.runtime=mock.Mock()
        self.runtime.usage_snapshot.side_effect=lambda:dict(server_id=1,items=[dict(self.item)])
        self.worker=UsageDelivery('http://127.0.0.1','fixture',self.state,self.runtime,self.session)

    def receipt(self,base,token,path,report):
        if path=='/agent/usage/drains':
            return dict(server_id=1,sessions=report['sessions'],drains=[],next_after=None,ready_for_execution=False)
        self.assertEqual(path,'/agent/usage/settle')
        return dict(accepted=True,server_id=1,session_id=report['session_id'],sequence=report['sequence'])

    async def test_capture_deliver_ack_then_growth_and_restart(self):
        with mock.patch('usage_delivery.request',side_effect=self.receipt) as send:
            await self.worker.step(); self.assertEqual(send.call_count,1)
            await self.worker.step(); self.assertEqual(send.call_count,1)
            self.item['upload_bytes']=8; await self.worker.step()
            self.assertEqual(send.call_args.args[3]['sequence'],2)
        self.assertEqual(UsageOutbox(self.state/'usage-outbox.sqlite',1).outstanding_sessions(),[])

    async def test_rotation_segments_cumulative_meter_and_keeps_old_frozen_report(self):
        await self.worker.capture()
        frozen=self.worker.box.next_report(self.session)
        newer=str(uuid.uuid4())
        await self.worker.rotate(newer)
        self.assertEqual(self.worker.box.next_report(self.session),frozen)
        self.item.update(upload_bytes=9,download_bytes=10)
        sent=[]
        def send(*args):
            if args[2]=='/agent/usage/settle': sent.append(args[3])
            return self.receipt(*args)
        with mock.patch('usage_delivery.request',side_effect=send):
            await self.worker.step(); await self.worker.step(); await self.worker.step()
        totals={r['session_id']:(r['items'][0]['upload_bytes'],r['items'][0]['download_bytes']) for r in sent}
        self.assertEqual(totals,{self.session:(5,7),newer:(4,3)})
        self.assertEqual(self.item['upload_bytes'],9) # runtime meter was never reset

    async def test_rotation_capture_failure_preserves_epoch_and_baseline(self):
        with mock.patch.object(self.worker,'_capture_locked',side_effect=OSError('disk full')):
            with self.assertRaises(OSError): await self.worker.rotate(str(uuid.uuid4()))
        self.assertEqual(self.worker.session,self.session)
        self.assertEqual(self.worker.baseline,{})
        await self.worker.capture()
        self.assertEqual(self.worker.box.next_report(self.session)['items'][0]['upload_bytes'],5)

    async def test_unconfirmed_epoch_is_durable_but_only_old_reports_are_sent(self):
        await self.worker.capture()
        newer=str(uuid.uuid4())
        await self.worker.rotate(newer)
        self.worker.unconfirmed_session=newer
        self.item.update(upload_bytes=9,download_bytes=10)
        with mock.patch('usage_delivery.request',side_effect=self.receipt) as send:
            await self.worker.step(); await self.worker.step()
            self.assertEqual(send.call_count,2)
            self.assertEqual(send.call_args_list[0].args[3]['session_id'],self.session)
            self.assertEqual(send.call_args_list[1].args[2],'/agent/usage/drains')
            self.assertEqual(self.worker.box.next_report(newer)['items'][0]['upload_bytes'],4)
            self.worker.unconfirmed_session=None
            await self.worker.step()
            self.assertEqual(send.call_count,3)
            self.assertEqual(send.call_args.args[3]['session_id'],newer)

    async def test_rotation_rejects_same_session_and_counter_rollback(self):
        with self.assertRaises(ValueError): await self.worker.rotate(self.session)
        await self.worker.rotate(str(uuid.uuid4()))
        self.item['upload_bytes']=4
        with self.assertRaises(ValueError): await self.worker.capture()

    async def test_lost_ack_frozen_retry_survives_worker_restart(self):
        with mock.patch('usage_delivery.request',side_effect=OSError('lost')),mock.patch('builtins.print'):
            await self.worker.step()
        report=self.worker.box.next_report(self.session)
        self.item['upload_bytes']=9
        restarted=UsageDelivery(self.worker.base,'fixture',self.state,self.runtime,self.session)
        with mock.patch('usage_delivery.request',side_effect=self.receipt) as send,mock.patch('usage_delivery.time.time',return_value=time.time()+6):
            await restarted.step(); self.assertEqual(send.call_args.args[3],report)
            await restarted.step(); self.assertEqual(send.call_args.args[3]['items'][0]['upload_bytes'],9)

    async def test_rejected_old_session_preserved_without_blocking_current(self):
        await self.worker.capture(); old=str(uuid.uuid4())
        self.worker.box.capture(old,dict(server_id=1,items=[dict(self.item)]))
        def send(base,token,path,report):
            if report['session_id']==old: raise urllib.error.HTTPError('local',403,'expired',{},None)
            return self.receipt(base,token,path,report)
        with mock.patch('usage_delivery.request',side_effect=send),mock.patch('builtins.print'):
            await self.worker.step()
            await self.worker.step()
        self.assertEqual(self.worker.box.outstanding_sessions(),[])
        self.assertIsNone(self.worker.box.next_report(old))
        self.assertEqual(self.worker.box.isolation_status()['quarantined'][0]['session'],old)

    async def test_bad_receipt_preserves_pending_and_no_secret_in_feedback(self):
        with mock.patch('usage_delivery.request',return_value=dict(accepted=False)),mock.patch('builtins.print') as output:
            await self.worker.step()
        self.assertEqual(self.worker.box.outstanding_sessions(),[self.session])
        self.assertNotIn('fixture',str(output.call_args_list))
        self.assertNotIn(self.item['credential'],str(output.call_args_list))

    async def test_unbound_runtime_and_capture_failure_do_not_send(self):
        self.runtime.usage_snapshot.return_value=None
        self.runtime.usage_snapshot.side_effect=lambda:dict(server_id=None,items=[])
        with mock.patch('usage_delivery.request') as send:
            await self.worker.step(); send.assert_not_called()
        self.runtime.usage_snapshot.side_effect=ValueError('fixture failure')
        with mock.patch('usage_delivery.INTERVAL',0.01),mock.patch('builtins.print'),mock.patch('usage_delivery.request') as send:
            task=asyncio.create_task(self.worker.run())
            await asyncio.sleep(0.04); self.assertFalse(task.done())
            await self.worker.finish(task); send.assert_not_called()

    async def test_cancel_drains_thread_before_final_capture(self):
        entered=threading.Event(); release=threading.Event(); finished=threading.Event()
        def blocking(): entered.set(); release.wait(2); finished.set()
        task=asyncio.create_task(drained(blocking))
        await asyncio.to_thread(entered.wait,1); task.cancel(); await asyncio.sleep(0.01)
        self.assertFalse(task.done()); release.set()
        with self.assertRaises(asyncio.CancelledError): await task
        self.assertTrue(finished.is_set())
        await self.worker.capture()
        loop=asyncio.create_task(asyncio.sleep(10)); self.item['upload_bytes']=10
        await self.worker.finish(loop)
        self.assertEqual(self.worker.box.next_report(self.session)['items'][0]['upload_bytes'],10)

    async def test_usage_stall_cannot_delay_configuration_lease_shutdown(self):
        import execution_agent
        from test_execution_lease import Runtime
        runtime=Runtime()
        runtime.usage_snapshot=lambda:dict(server_id=1,items=[dict(self.item)])
        release=threading.Event(); entered=threading.Event(); calls=0
        async def cycle(*args):
            nonlocal calls
            calls+=1
            if calls==1: runtime.active=True; return {}
            await asyncio.Future()
        def stalled(*args): entered.set(); release.wait(2); raise OSError('offline')
        with mock.patch('execution_agent.cycle',side_effect=cycle),mock.patch('execution_agent.LEASE_SECONDS',0.15),mock.patch('execution_agent.SYNC_INTERVAL',0.01),mock.patch('usage_delivery.request',side_effect=stalled),mock.patch('builtins.print'):
            task=asyncio.create_task(execution_agent.run(dict(master='unused',token='unused'),self.state,runtime))
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait,1))
                await asyncio.wait_for(runtime.closed.wait(),0.5)
                self.assertFalse(runtime.active)
            finally:
                release.set(); task.cancel()
                with self.assertRaises(asyncio.CancelledError): await task


@unittest.skipUnless(test_xray.BINARY.is_file(),'Set XRAY_BINARY')
class DeliveryRealTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_usage_settlement.SettlementRealTests.setUp
    tearDown=test_usage_settlement.SettlementRealTests.tearDown
    request=test_usage_settlement.SettlementRealTests.request
    asyncSetUp=test_usage_settlement.SettlementRealTests.asyncSetUp
    asyncTearDown=test_usage_settlement.SettlementRealTests.asyncTearDown
    open=test_usage_settlement.SettlementRealTests.open
    accepted=test_usage_settlement.SettlementRealTests.accepted
    close_writer=test_usage_settlement.SettlementRealTests.close_writer

    async def test_real_payload_commit_busy_preserves_exact_tail_and_recovers_once(self):
        await self.storage_failure_recovery('commit-busy')

    async def test_real_payload_sqlite_full_preserves_exact_tail_and_recovers_once(self):
        await self.storage_failure_recovery('full')

    async def storage_failure_recovery(self,mode):
        from contextlib import ExitStack
        import execution_agent
        from execution_epochs import ExecutionEpochs
        from storage_boundaries_fixture import page_limited_connections
        from usage_ledger import UsageLedger
        session=str(uuid.uuid4())
        epochs=ExecutionEpochs(self.base,self.token,self.state,self.runtime,session)
        await execution_agent.cycle(self.base,self.token,self.state,self.runtime,1,session,epochs)
        reader,_=await self.open(); await self.accepted(reader)
        box=epochs.delivery.box; frozen=box.next_report(session)
        try:
            with ExitStack() as stack:
                if mode=='commit-busy':
                    import sqlite3
                    db=sqlite3.connect(box.path,check_same_thread=False)
                    stack.callback(db.close)
                    stack.callback(db.rollback)
                    # Acquire the reader only after intent is durable, so this
                    # remains a post-write COMMIT failure test.
                    prepare=box.prepare_write
                    def prepare_then_lock(*args):
                        prepare(*args)
                        db.execute('BEGIN'); db.execute('SELECT * FROM totals').fetchall()
                    stack.enter_context(mock.patch.object(box,'prepare_write',prepare_then_lock))
                else:
                    with box.connect() as db:
                        db.executescript('''CREATE TABLE fixture_padding(data BLOB);
                            CREATE TRIGGER fixture_full BEFORE UPDATE ON totals BEGIN
                                INSERT INTO fixture_padding VALUES(zeroblob(65536)); END;''')
                    stack.enter_context(page_limited_connections(box.path))
                reader,_=await self.open()
                # The unmetered two-byte VLESS response can arrive while the
                # durable upload checkpoint is pending; no application echo may.
                self.assertIn(await asyncio.wait_for(reader.read(),3),(b'',b'\x00\x00'))
                self.assertTrue(self.runtime.meter.failed)
                # Failure is latched before another accepted payload write.
                reader,_=await self.open()
                self.assertEqual(await asyncio.wait_for(reader.read(),3),b'')
        finally:
            if mode=='full':
                with box.connect() as db: db.execute('DROP TRIGGER IF EXISTS fixture_full')
        self.assertEqual(box.next_report(session),frozen)
        await epochs.delivery.capture()
        await epochs.delivery.step()
        tail=box.next_report(session)
        await epochs.delivery.step()
        total=UsageLedger(self.server.registry).user(self.uid)
        self.assertEqual((total['upload_bytes'],total['download_bytes']),(10,5))
        self.assertEqual(box.outstanding_sessions(),[])
        self.assertEqual(box.uncertain_writes(),[])
        # The latest lost receipt is idempotent; an older sequence is rejected.
        from agent import request
        receipt=await asyncio.to_thread(request,self.base,self.token,'/agent/usage/settle',tail)
        self.assertTrue(receipt['duplicate'])
        with self.assertRaises(urllib.error.HTTPError) as stale:
            await asyncio.to_thread(request,self.base,self.token,'/agent/usage/settle',frozen)
        self.assertEqual(stale.exception.code,409)
        total=UsageLedger(self.server.registry).user(self.uid)
        self.assertEqual((total['upload_bytes'],total['download_bytes']),(10,5))

    async def test_slow_checkpoint_concurrent_payload_allows_configuration_renewal(self):
        await self.slow_checkpoint_load(8)

    async def test_sustained_slow_checkpoint_payload_keeps_renewing_and_settles_exactly(self):
        await self.slow_checkpoint_load(128)

    async def slow_checkpoint_load(self,rounds):
        import execution_agent
        from execution_epochs import ExecutionEpochs
        from usage_ledger import UsageLedger
        session=str(uuid.uuid4())
        epochs=ExecutionEpochs(self.base,self.token,self.state,self.runtime,session)
        await execution_agent.cycle(self.base,self.token,self.state,self.runtime,1,session,epochs)
        connections=await asyncio.gather(self.open(),self.open())
        await asyncio.gather(*(self.accepted(reader) for reader,_ in connections))
        committed=epochs.delivery.box.capture
        samples=[]; renewals=[]; ticks=[]; clock_offsets=[]; cpu_ticks=[]
        stages=[]
        longest_tick_stall=None
        blocking=execution_agent._blocking
        apply=self.runtime.apply
        prepare=epochs.delivery.box.prepare_write
        def record(name,start,end):
            stages.append((name,start,end))
        def slow_commit(*args):
            # Controlled slow storage around a real FULL SQLite transaction;
            # this is not a claim about physical disk latency on another host.
            start=time.monotonic()
            time.sleep(0.01)
            slept=time.monotonic()
            record('injected_sleep',start,slept)
            try:
                result=committed(*args)
                samples.append(time.monotonic())
                return result
            finally: record('sqlite_capture',slept,time.monotonic())
        def timed_prepare(*args):
            start=time.monotonic()
            try: return prepare(*args)
            finally: record('sqlite_prepare',start,time.monotonic())
        async def timed_apply(*args):
            start=time.monotonic()
            try: return await apply(*args)
            finally: record('runtime_apply',start,time.monotonic())
        async def timed_blocking(runtime,function,*args):
            # Measure worker queue, actual blocking work, and event-loop wakeup
            # separately. Never record request arguments or credentials.
            queued=time.monotonic(); finished=[]
            name=function.__name__
            def work(*values):
                start=time.monotonic()
                record(name+'.queue',queued,start)
                try: return function(*values)
                finally:
                    end=time.monotonic(); finished.append(end)
                    record(name+'.work',start,end)
            try: return await blocking(runtime,work,*args)
            finally:
                if finished: record(name+'.resume',finished[0],time.monotonic())
        async def renew():
            while True:
                await execution_agent.cycle(self.base,self.token,self.state,self.runtime,2,session,epochs)
                renewals.append(time.monotonic())
                await asyncio.sleep(0.03)
        async def observe():
            nonlocal longest_tick_stall
            while True:
                now=time.monotonic()
                cpu=time.thread_time()
                if ticks:
                    gap=now-ticks[-1]
                    if longest_tick_stall is None or gap>longest_tick_stall['duration']:
                        # Capture immediately; bounded sampler history may have
                        # expired by the time a sustained test finishes.
                        longest_tick_stall=dict(duration=gap,
                            loop_cpu_seconds=cpu-cpu_ticks[-1],
                            samples=probe.between(ticks[-1],now))
                ticks.append(now)
                cpu_ticks.append(cpu)
                clock_offsets.append(time.time()-now)
                await asyncio.sleep(0.01)
        async def transfer(reader,writer):
            data=b'x'*(128*1024)
            for _ in range(rounds):
                writer.write(data); await writer.drain()
                self.assertEqual(await asyncio.wait_for(reader.readexactly(len(data)),5),data)
        with LoopProbe() as probe, \
             mock.patch.object(epochs.delivery.box,'capture',side_effect=slow_commit), \
             mock.patch.object(epochs.delivery.box,'prepare_write',side_effect=timed_prepare), \
             mock.patch.object(self.runtime,'apply',side_effect=timed_apply), \
             mock.patch.object(execution_agent,'_blocking',side_effect=timed_blocking):
            started=time.monotonic()
            renewal=asyncio.create_task(renew()); observer=asyncio.create_task(observe())
            try:
                await asyncio.wait_for(asyncio.gather(*(transfer(*pair) for pair in connections)),60)
            finally:
                ended=time.monotonic()
                renewal.cancel(); observer.cancel()
                await asyncio.gather(renewal,observer,return_exceptions=True)
                gaps=[(b-a,a,b) for a,b in zip([started]+renewals,renewals+[ended])]
                longest=max(gaps)
                # Emit on success too: a passing rerun must not erase an earlier
                # timing failure. Intervals are relative and contain no identities.
                summary={name:round(max(b-a for n,a,b in stages if n==name),6)
                         for name in sorted({n for n,_,_ in stages})}
                overlaps=sorted(((b-a,n,a-started,b-started) for n,a,b in stages
                                 if a<longest[2] and b>longest[1]),reverse=True)[:12]
                stall=(dict(longest_tick_stall,
                            duration=round(longest_tick_stall['duration'],6),
                            loop_cpu_seconds=round(longest_tick_stall['loop_cpu_seconds'],6))
                       if longest_tick_stall is not None else None)
                print('RENEWAL_DIAGNOSTICS '+json.dumps(dict(rounds=rounds,
                    duration=round(ended-started,6),renewals=len(renewals),
                    max_renewal_gap=round(longest[0],6),
                    longest_gap=[round(longest[1]-started,6),round(longest[2]-started,6)],
                    max_tick_gap=round(max((b-a for a,b in zip(ticks,ticks[1:])),default=0),6),
                    wall_monotonic_offset_range=round(max(clock_offsets)-min(clock_offsets),6) if clock_offsets else None,
                    max_stage_seconds=summary,longest_gap_stages=overlaps,
                    longest_tick_stall=stall)),flush=True)
        self.assertGreaterEqual(len(samples),rounds*8)
        self.assertTrue(any(started<t<ended for t in renewals))
        if rounds>=128:
            self.assertGreaterEqual(len(renewals),10)
            self.assertLess(max(b-a for a,b in zip([started]+renewals,renewals+[ended])),2)
        self.assertGreater(len(ticks),5)
        self.assertLess(max(b-a for a,b in zip(ticks,ticks[1:])),0.5)
        self.assertFalse(self.runtime.meter.failed)
        await epochs.delivery.step()
        total=UsageLedger(self.server.registry).user(self.uid)
        self.assertEqual((total['upload_bytes'],total['download_bytes']),(2*(5+rounds*128*1024),)*2)

    async def test_concurrent_users_incremental_commits_preserve_frozen_batch_and_settle_exactly(self):
        import execution_agent
        from execution_epochs import ExecutionEpochs
        from usage_ledger import UsageLedger
        registry=self.server.registry
        identities=[(self.uid,self.credential['uuid'])]
        for n in range(7):
            uid=registry.save('users',dict(name=f'parallel-{n}'))['id']
            identities.append((uid,registry.authorize_node_user(self.nid,uid)['uuid']))
        session=str(uuid.uuid4())
        epochs=ExecutionEpochs(self.base,self.token,self.state,self.runtime,session)
        await execution_agent.cycle(self.base,self.token,self.state,self.runtime,1,session,epochs)
        connections=await asyncio.gather(*(self.open(credential) for _,credential in identities))
        started=time.monotonic(); offset=time.time()-started
        ticks=[started]; stages=[]
        def timed(name,operation):
            def wrapped(*args,**kwargs):
                begin=time.monotonic()
                try: return operation(*args,**kwargs)
                finally: stages.append((name,time.monotonic()-begin))
            return wrapped
        async def observe():
            while True:
                await asyncio.sleep(.01)
                ticks.append(time.monotonic())
        observer=asyncio.create_task(observe())
        try:
            with mock.patch.object(epochs.delivery.box,'prepare_write',side_effect=timed(
                    'prepare',epochs.delivery.box.prepare_write)), \
                 mock.patch.object(epochs.delivery.box,'capture',side_effect=timed(
                    'capture',epochs.delivery.box.capture)):
                await asyncio.gather(*(self.accepted(reader) for reader,_ in connections))
        except TimeoutError:
            # Preserve the original three-second assertion. Collect code locations,
            # never task reprs/locals, which could contain credentials or payloads.
            stacks=[[dict(file=Path(frame.f_code.co_filename).name,
                          function=frame.f_code.co_name,line=frame.f_lineno)
                     for frame in task.get_stack()] for task in asyncio.all_tasks()
                    if task is not asyncio.current_task()]
            print('PARALLEL_HANDSHAKE_DIAGNOSTICS '+json.dumps(dict(
                elapsed=time.monotonic()-started,
                wall_monotonic_offset_change=time.time()-time.monotonic()-offset,
                meter_failed=self.runtime.meter.failed,
                eof=[reader.at_eof() for reader,_ in connections],stacks=stacks)),flush=True)
            raise
        finally:
            ended=time.monotonic()
            observer.cancel(); await asyncio.gather(observer,return_exceptions=True)
            # Emit passing samples as well, without changing any deadline or
            # retrying a handshake; a passing run cannot close an earlier error.
            print('PARALLEL_HANDSHAKE_TIMING '+json.dumps(dict(
                elapsed=ended-started,
                max_tick_gap=max(b-a for a,b in zip(ticks,ticks[1:]+[ended])),
                wall_monotonic_offset_change=time.time()-time.monotonic()-offset,
                stages={name:dict(count=sum(n==name for n,_ in stages),
                                 max_seconds=max(t for n,t in stages if n==name))
                        for name in sorted({n for n,_ in stages})})),flush=True)
        # Receiving bytes does not imply their asynchronous durable checkpoint
        # has committed. Capture crosses the runtime's shared storage boundary
        # before inspecting the frozen batch; retain the exact 5/5 assertion.
        await epochs.delivery.capture()
        frozen=epochs.delivery.box.next_report(session)
        self.assertEqual(len(frozen['items']),8)
        self.assertTrue(all((i['upload_bytes'],i['download_bytes'])==(5,5) for i in frozen['items']))
        async def transfer(index,reader,writer):
            data=bytes([index+1])*((index+1)*4096)
            for _ in range(3):
                writer.write(data); await writer.drain()
                self.assertEqual(await asyncio.wait_for(reader.readexactly(len(data)),5),data)
        with mock.patch.object(epochs.delivery.box,'capture',wraps=epochs.delivery.box.capture) as capture:
            await asyncio.wait_for(asyncio.gather(*(transfer(i,*pair) for i,pair in enumerate(connections))),20)
            self.assertTrue(capture.call_count>=16)
            self.assertTrue(all(len(call.args[1]['items'])==1 for call in capture.call_args_list))
        self.assertEqual(epochs.delivery.box.next_report(session),frozen)
        await epochs.delivery.step(); await epochs.delivery.step()
        for index,(uid,_) in enumerate(identities):
            total=UsageLedger(registry).user(uid)
            self.assertEqual((total['upload_bytes'],total['download_bytes']),(5+3*(index+1)*4096,)*2)
        self.assertEqual(epochs.delivery.box.outstanding_sessions(),[])

    async def test_real_payload_locked_checkpoint_closes_without_stalling_loop(self):
        import execution_agent
        from execution_epochs import ExecutionEpochs
        session=str(uuid.uuid4())
        epochs=ExecutionEpochs(self.base,self.token,self.state,self.runtime,session)
        await execution_agent.cycle(self.base,self.token,self.state,self.runtime,1,session,epochs)
        reader,_=await self.open(); await self.accepted(reader)
        lock=sqlite3.connect(epochs.delivery.box.path)
        try:
            lock.execute('BEGIN IMMEDIATE')
            start=time.perf_counter()
            reader,_=await self.open()
            # The fixture offers five authenticated bytes. The new write-intent
            # transaction is blocked before transport receives those bytes.
            self.assertEqual(await asyncio.wait_for(reader.read(),2),b'')
            self.assertLess(time.perf_counter()-start,1)
            self.assertTrue(self.runtime.meter.failed)
        finally:
            lock.rollback(); lock.close()
        await epochs.delivery.capture()
        item=epochs.delivery.box.next_report(session)['items'][0]
        self.assertEqual((item['upload_bytes'],item['download_bytes']),(5,5))

    async def test_real_payload_checkpoint_recovers_without_periodic_or_shutdown_capture(self):
        await self.checkpoint_recovery()

    async def test_clock_rollback_isolates_tail_then_explicit_retry_settles_exactly(self):
        await self.checkpoint_recovery(rollback=True)

    async def checkpoint_recovery(self,rollback=False):
        clock=scoped_period_clock(self) if rollback else None
        import execution_agent
        from execution_epochs import ExecutionEpochs
        from vless_runtime import VlessRuntime
        session=str(uuid.uuid4())
        epochs=ExecutionEpochs(self.base,self.token,self.state,self.runtime,session)
        await execution_agent.cycle(self.base,self.token,self.state,self.runtime,1,session,epochs)
        reader,_=await self.open(); await self.accepted(reader)
        # No delivery.capture(), step(), or finish(): the write itself commits.
        box=UsageOutbox(self.state/'usage-outbox.sqlite',self.sid)
        report=box.next_report(session)
        self.assertEqual(box.uncertain_writes(),[])
        self.assertEqual((report['items'][0]['upload_bytes'],report['items'][0]['download_bytes']),(5,5))
        await self.runtime.close()
        self.runtime=VlessRuntime(test_xray.BINARY,self.state/'new-core',
                                 local_test_target=('127.0.0.1',self.target_port))
        new=str(uuid.uuid4())
        recovered=ExecutionEpochs(self.base,self.token,self.state,self.runtime,new)
        await execution_agent.cycle(self.base,self.token,self.state,self.runtime,2,new,recovered)
        reader,_=await self.open(); await self.accepted(reader)
        # Capture the actual HTTP refusal and both clocks on intermittent
        # failures; do not infer missing proof versus clock rollback from 403.
        import usage_delivery
        original_request=usage_delivery.request
        attempts=[]
        def diagnosed_request(*args):
            sample=dict(session=args[3]['session_id'],wall=time.time(),monotonic=time.monotonic())
            try:
                result=original_request(*args)
                sample['accepted']=True
                return result
            except urllib.error.HTTPError as error:
                sample.update(status=error.code,body=error.read(2048).decode('utf-8','replace'))
                raise
            finally:
                sample.update(wall_after=time.time(),monotonic_after=time.monotonic())
                with self.server.registry.connect() as db:
                    sample['grants']=[tuple(row) for row in db.execute(
                        'SELECT session_id,issued_at,settle_until FROM usage_execution_grants')]
                attempts.append(sample)
        with mock.patch('usage_delivery.request',side_effect=diagnosed_request):
            await recovered.delivery.step()
            if rollback: clock.return_value-=9
            await recovered.delivery.step()
            if rollback:
                self.assertEqual(attempts[-1]['status'],403,attempts)
                self.assertIn('流量尾部结算窗口已结束或时钟异常',attempts[-1]['body'])
                with self.request(f'/api/users/{self.uid}/usage') as response: partial=json.load(response)
                self.assertEqual((partial['upload_bytes'],partial['download_bytes']),(5,5))
                evidence=recovered.delivery.box.isolation_status()
                self.assertEqual(len(evidence['quarantined']),1)
                rejected=evidence['failures'][-1]['report']
                clock.return_value+=9
                # Clock recovery alone cannot silently acknowledge or discard
                # quarantined bytes; explicit retry still needs valid proof.
                await recovered.delivery.step()
                self.assertEqual(recovered.delivery.box.isolation_status(),evidence)
                recovered.delivery.box.retry_quarantined(rejected,time.time())
                await recovered.delivery.step()
                self.assertEqual(attempts[-1]['accepted'],True)
                await recovered.delivery.step()
                self.assertEqual(len(attempts),3) # no duplicate report after ack
                self.assertEqual(recovered.delivery.box.isolation_status()['quarantined'],[])
                self.assertEqual(len(recovered.delivery.box.recovery_status()),1)
        with self.request(f'/api/users/{self.uid}/usage') as response: total=json.load(response)
        self.assertEqual((total['upload_bytes'],total['download_bytes']),(10,10),
            dict(attempts=attempts,pending=recovered.delivery.box.outstanding_sessions()))
        self.assertEqual(self.runtime.usage_snapshot()['items'][0]['upload_bytes'],5)
        self.assertEqual(recovered.delivery.box.outstanding_sessions(),[])

    async def test_real_cycle_rotation_segments_old_tail_and_new_payload_via_http(self):
        import execution_agent
        from execution_epochs import ExecutionEpochs
        from usage_cycles import UsageCycles
        clock=scoped_period_clock(self); now=clock(); r=self.server.registry; session=str(uuid.uuid4())
        with r.connect() as db:
            for ident,start,end in [('old',now-1,now+30),('new',now+30,now+120)]:
                db.execute('INSERT INTO usage_cycles(id,user_id,starts_at,ends_at) VALUES(?,?,?,?)',
                           (ident,self.uid,start,end))
                from usage_accounting import initialize_cycle
                initialize_cycle(db,ident)
        for ident in ('old','new'):
            UsageCycles(r).set_budget(ident,dict(upload_bytes=5,download_bytes=5))
        epochs=ExecutionEpochs(self.base,self.token,self.state,self.runtime,session)
        await execution_agent.cycle(self.base,self.token,self.state,self.runtime,1,session,epochs)
        reader,_=await self.open(); await self.accepted(reader)
        # Do not post the first period before rotating. Its captured tail must
        # still settle under the retired proof, not under the next period.
        from usage_migration import UsageMigration
        migration=UsageMigration(r); migration.begin_drain('old')
        clock.return_value=now+30 # adjacent periods switch at the exact boundary
        await execution_agent.cycle(self.base,self.token,self.state,self.runtime,2,session,epochs)
        self.assertNotEqual(epochs.session,session)
        final=epochs.delivery.box.closed_epoch(session)
        self.assertEqual([(i['upload_bytes'],i['download_bytes']) for i in final['checkpoint']['items']],[(5,5)])
        self.assertEqual(final['checkpoint']['uncertain_writes'],[])
        reader,_=await self.open(); await self.accepted(reader)
        # Lose the first final receipt after the real master commits it. The
        # durable job must survive reopening its database and retry unchanged.
        import usage_delivery
        original_request=usage_delivery.request; lost=[]; paths=[]
        def lose_final_receipt(*args):
            paths.append(args[2])
            reply=original_request(*args)
            if args[2]=='/agent/usage/closed':
                lost.append(reply); raise OSError('fixture: receipt lost after commit')
            return reply
        with mock.patch('usage_delivery.request',side_effect=lose_final_receipt):
            await epochs.delivery.step(); await epochs.delivery.step()
            await epochs.delivery.step(); await epochs.delivery.step()
            await epochs.delivery.step()
        self.assertEqual(len(lost),1)
        self.assertEqual(paths.count('/agent/usage/drains'),1)
        self.assertLess(paths.index('/agent/usage/drains'),paths.index('/agent/usage/closed'))
        from usage_outbox import UsageOutbox
        epochs.delivery.box=UsageOutbox(epochs.delivery.box.path,epochs.delivery.box.server_id)
        with epochs.delivery.box.connect() as db:
            self.assertEqual(db.execute('SELECT status FROM closed_deliveries').fetchone()[0],'pending')
            db.execute('UPDATE closed_deliveries SET next_attempt=0') # retry clock fixture
        await epochs.delivery.step()
        with epochs.delivery.box.connect() as db:
            self.assertEqual(db.execute('SELECT status FROM closed_deliveries').fetchone()[0],'accepted')
        self.assertTrue(migration.accept_closed('old',self.token,final)['duplicate'])
        from drain_seal import DrainSeal
        sealed=DrainSeal(r).seal('old')
        self.assertEqual((sealed['final_upload_bytes'],sealed['final_download_bytes']),(5,5))
        self.assertFalse(sealed['ready_for_execution'])
        with r.connect() as db:
            rows=db.execute('SELECT cycle_id,upload_bytes,download_bytes FROM usage_deltas ORDER BY cycle_id').fetchall()
            proof=db.execute('SELECT DISTINCT session_id,cycle_id FROM usage_execution_grants ORDER BY cycle_id').fetchall()
        self.assertEqual([tuple(x) for x in rows],[('new',5,5),('old',5,5)])
        self.assertEqual({x['cycle_id'] for x in proof},{'old','new'})
        self.assertEqual(len({x['session_id'] for x in proof}),2)
        self.assertEqual(self.runtime.usage_snapshot()['items'][0]['upload_bytes'],10)
        self.assertFalse(epochs.delivery.box.outstanding_sessions())

    async def test_rotation_rollback_cannot_reopen_old_period_or_rebucket_tail(self):
        import execution_agent
        from execution_epochs import ExecutionEpochs
        from usage_cycles import UsageCycles
        clock=scoped_period_clock(self); now=clock(); r=self.server.registry
        with r.connect() as db:
            for ident,start,end in [('rollback-old',now-1,now+30),('rollback-new',now+30,now+120)]:
                db.execute('INSERT INTO usage_cycles(id,user_id,starts_at,ends_at) VALUES(?,?,?,?)',
                           (ident,self.uid,start,end))
                from usage_accounting import initialize_cycle
                initialize_cycle(db,ident)
        for ident in ('rollback-old','rollback-new'):
            UsageCycles(r).set_budget(ident,dict(upload_bytes=7,download_bytes=100))
        epochs=ExecutionEpochs(self.base,self.token,self.state,self.runtime,str(uuid.uuid4()))
        await execution_agent.cycle(self.base,self.token,self.state,self.runtime,1,epochs.session,epochs)
        old_session=epochs.session
        reader,_=await self.open(); await self.accepted(reader)
        clock.return_value=now+30
        await execution_agent.cycle(self.base,self.token,self.state,self.runtime,2,epochs.session,epochs)
        new_session=epochs.session
        self.assertNotEqual(old_session,new_session)
        reader,_=await self.open(); await self.accepted(reader)
        clock.return_value=now+10
        await execution_agent.cycle(self.base,self.token,self.state,self.runtime,3,epochs.session,epochs)
        self.assertEqual(self.runtime.snapshot['nodes'][0]['clients'],[])
        self.assertEqual(epochs.session,new_session)
        with r.connect() as db:
            self.assertEqual(db.execute("SELECT expired FROM usage_cycles WHERE id='rollback-old'").fetchone()[0],1)
        clock.return_value=now+31
        await execution_agent.cycle(self.base,self.token,self.state,self.runtime,4,epochs.session,epochs)
        self.assertEqual(epochs.session,new_session)
        reader,_=await self.open()
        self.assertIn(await asyncio.wait_for(reader.read(),3),(b'',b'\x00\x00'))
        self.assertEqual(self.runtime.usage_snapshot()['items'][0]['upload_bytes'],12)
        self.assertEqual(self.runtime.quotas[(self.uid,'rollback-new')].remaining[0],0)
        await epochs.delivery.step(); await epochs.delivery.step(); await epochs.delivery.step()
        with r.connect() as db:
            rows=db.execute('SELECT cycle_id,sum(upload_bytes) FROM usage_deltas GROUP BY cycle_id ORDER BY cycle_id').fetchall()
            proofs=db.execute('SELECT DISTINCT session_id,cycle_id FROM usage_execution_grants').fetchall()
        self.assertEqual([tuple(row) for row in rows],[('rollback-new',7),('rollback-old',5)])
        self.assertEqual({tuple(row) for row in proofs},{(old_session,'rollback-old'),(new_session,'rollback-new')})
        self.assertEqual(epochs.delivery.box.outstanding_sessions(),[])

    async def test_execution_loop_automatically_posts_real_payload_and_stops_cleanly(self):
        import execution_agent
        ready=asyncio.Event(); original=execution_agent.cycle
        async def cycle(*args):
            receipt=await original(*args); ready.set(); return receipt
        with mock.patch('execution_agent.cycle',side_effect=cycle),mock.patch('execution_agent.SYNC_INTERVAL',0.03),mock.patch('usage_delivery.INTERVAL',0.03),mock.patch('builtins.print'):
            task=asyncio.create_task(execution_agent.run(dict(master=self.base,token=self.token),self.state,self.runtime))
            try:
                await asyncio.wait_for(ready.wait(),3)
                reader,writer=await self.open(); await self.accepted(reader)
                for _ in range(100):
                    with self.request(f'/api/users/{self.uid}/usage') as response: total=json.load(response)
                    if total['download_bytes']==5: break
                    await asyncio.sleep(0.02)
                self.assertEqual((total['upload_bytes'],total['download_bytes']),(5,5))
            finally:
                task.cancel()
                with self.assertRaises(asyncio.CancelledError): await task
        box=UsageOutbox(self.state/'usage-outbox.sqlite',self.sid)
        self.assertEqual(box.outstanding_sessions(),[])

    async def test_running_agent_switches_period_then_automatically_delivers_both(self):
        import execution_agent
        clock=scoped_period_clock(self); now=clock(); r=self.server.registry
        with r.connect() as db:
            for ident,start,end in [('loop-old',now-1,now+30),('loop-new',now+30,now+120)]:
                db.execute('INSERT INTO usage_cycles(id,user_id,starts_at,ends_at) VALUES(?,?,?,?)',
                           (ident,self.uid,start,end))
                from usage_accounting import initialize_cycle
                initialize_cycle(db,ident)
        ready=asyncio.Event(); switched=asyncio.Event(); sessions=[]
        original=execution_agent.cycle
        async def cycle(*args):
            receipt=await original(*args)
            sessions.append(args[-1].session)
            ready.set()
            if sessions[-1]!=sessions[0]: switched.set()
            return receipt
        with mock.patch('execution_agent.cycle',side_effect=cycle), \
             mock.patch('execution_agent.SYNC_INTERVAL',0.03), \
             mock.patch('usage_delivery.INTERVAL',0.03),mock.patch('builtins.print'):
            task=asyncio.create_task(execution_agent.run(dict(master=self.base,token=self.token),self.state,self.runtime))
            try:
                await asyncio.wait_for(ready.wait(),3)
                reader,_=await self.open(); await self.accepted(reader)
                clock.return_value=now+31
                await asyncio.wait_for(switched.wait(),3)
                reader,_=await self.open(); await self.accepted(reader)
                for _ in range(100):
                    with r.connect() as db:
                        totals=[tuple(x) for x in db.execute('SELECT cycle_id,sum(upload_bytes),sum(download_bytes) FROM usage_deltas GROUP BY cycle_id ORDER BY cycle_id')]
                    if totals==[('loop-new',5,5),('loop-old',5,5)]: break
                    await asyncio.sleep(.02)
                self.assertEqual(totals,[('loop-new',5,5),('loop-old',5,5)])
                self.assertEqual(len(set(sessions)),2)
            finally:
                task.cancel()
                with self.assertRaises(asyncio.CancelledError): await task

    async def test_stalled_usage_network_does_not_hold_configuration_renewals(self):
        await self.stalled_usage_renewal()

    async def test_slow_configuration_renewal_completes_before_usage_is_released(self):
        # A legitimate async configuration delay must not look like deadlock.
        await self.stalled_usage_renewal(cycle_delay=0.35)

    async def stalled_usage_renewal(self,cycle_delay=0):
        import execution_agent
        entered=threading.Event(); release=threading.Event(); finished=threading.Event()
        first=asyncio.Event(); renewed=asyncio.Event(); cycles=[]
        original=execution_agent.cycle
        async def cycle(*args):
            if cycles and cycle_delay: await asyncio.sleep(cycle_delay)
            receipt=await original(*args); cycles.append(receipt)
            first.set(); renewed.set()
            return receipt
        def stalled(*args):
            entered.set()
            try:
                release.wait(5)
                raise OSError('offline')
            finally: finished.set()
        with mock.patch('execution_agent.cycle',side_effect=cycle),mock.patch('execution_agent.SYNC_INTERVAL',0.03),mock.patch('usage_delivery.INTERVAL',0.03),mock.patch('usage_delivery.request',side_effect=stalled),mock.patch('builtins.print'):
            task=asyncio.create_task(execution_agent.run(dict(master=self.base,token=self.token),self.state,self.runtime))
            try:
                await asyncio.wait_for(first.wait(),3)
                reader,writer=await self.open(); await self.accepted(reader)
                self.assertTrue(await asyncio.to_thread(entered.wait,1))
                before=len(cycles); renewed.clear()
                # Await a completed HTTP configuration/receipt cycle while the
                # usage thread is still blocked, not an arbitrary 200ms sample.
                await asyncio.wait_for(renewed.wait(),2)
                self.assertGreater(len(cycles),before)
                self.assertFalse(release.is_set())
                self.assertFalse(finished.is_set())
            finally:
                release.set(); task.cancel()
                with self.assertRaises(asyncio.CancelledError): await task
        self.assertTrue(UsageOutbox(self.state/'usage-outbox.sqlite',self.sid).outstanding_sessions())
