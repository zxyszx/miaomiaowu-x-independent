"""Durable async write primitive; runtime integration is deliberately separate."""
import asyncio
from contextlib import contextmanager
from pathlib import Path
import tempfile
import threading
import unittest
import uuid
from unittest import mock
from payload_meter import PayloadMeter, MeteredWriter, CycleQuota, QuotaExceeded
from usage_outbox import UsageOutbox
import test_payload_crash as crash_fixture


class AsyncPayloadTests(unittest.IsolatedAsyncioTestCase):
    async def test_slow_intent_does_not_block_loop_and_transport_stays_on_loop(self):
        meter=PayloadMeter(); meter.bind(1)
        entered=threading.Event(); release=threading.Event()
        self.addCleanup(release.set)
        loop_thread=threading.get_ident(); stages=[]
        def intent(snapshot):
            stages.append(('intent',threading.get_ident()))
            entered.set()
            if not release.wait(3): raise OSError('fixture timeout')
        meter.set_write_intent(intent)
        destination=mock.Mock()
        destination.write.side_effect=lambda data: stages.append(('transport',threading.get_ident()))
        meter.set_incremental_checkpoint(lambda snapshot: stages.append(('checkpoint',threading.get_ident())))
        stages.clear()
        writer=MeteredWriter(destination,meter,(1,2,'fixture'),0)
        task=asyncio.create_task(writer.awrite(b'hello'))
        try:
            await asyncio.wait_for(asyncio.to_thread(entered.wait,2),2.5)
            self.assertTrue(entered.is_set())
            # This coroutine must be scheduled while the storage callback is blocked.
            await asyncio.sleep(0)
            self.assertFalse(task.done()); destination.write.assert_not_called()
            release.set(); await task
        finally:
            release.set(); await asyncio.gather(task,return_exceptions=True)
        self.assertEqual([stage for stage,_ in stages],['intent','transport','checkpoint'])
        self.assertEqual(stages[1][1],loop_thread)
        self.assertNotEqual(stages[0][1],loop_thread)
        self.assertNotEqual(stages[2][1],loop_thread)
        self.assertEqual(meter.snapshot()['items'][0]['upload_bytes'],5)

    async def test_cancel_waits_for_storage_even_when_cancelled_twice(self):
        meter=PayloadMeter(); meter.bind(1)
        entered=threading.Event(); release=threading.Event(); finished=threading.Event()
        self.addCleanup(release.set)
        def intent(snapshot):
            entered.set()
            if not release.wait(3): raise OSError('fixture timeout')
            finished.set()
        meter.set_write_intent(intent)
        destination=mock.Mock()
        task=asyncio.create_task(MeteredWriter(destination,meter,(1,2,'fixture'),0).awrite(b'x'))
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            with self.assertRaises(OSError): MeteredWriter(destination,meter,(1,2,'fixture'),1).write(b'mixed')
            task.cancel(); await asyncio.sleep(0); task.cancel(); await asyncio.sleep(0)
            self.assertFalse(task.done()); destination.write.assert_not_called()
            release.set()
            with self.assertRaises(asyncio.CancelledError): await task
            self.assertTrue(finished.is_set()); self.assertTrue(meter.failed)
            self.assertEqual(meter.snapshot()['items'],[])
        finally:
            release.set(); await asyncio.gather(task,return_exceptions=True)

    async def test_cancel_after_transport_finishes_checkpoint_before_return(self):
        meter=PayloadMeter(); meter.bind(1)
        entered=threading.Event(); release=threading.Event(); durable=[]
        self.addCleanup(release.set)
        def checkpoint(snapshot):
            entered.set()
            if not release.wait(3): raise OSError('fixture timeout')
            durable.append(snapshot)
        meter.checkpoint=checkpoint; meter.incremental_checkpoint=True
        destination=mock.Mock()
        task=asyncio.create_task(MeteredWriter(destination,meter,(1,2,'fixture'),0).awrite(b'hello'))
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            task.cancel(); await asyncio.sleep(0)
            self.assertFalse(task.done()); destination.write.assert_called_once_with(b'hello')
            release.set()
            with self.assertRaises(asyncio.CancelledError): await task
            self.assertEqual(durable[0]['items'][0]['upload_bytes'],5)
            self.assertEqual(meter.snapshot()['items'][0]['upload_bytes'],5)
            # Cancellation after a successful durable COMMIT must not fence a
            # healthy meter or classify that committed write as unknown.
            self.assertFalse(meter.failed)
            await MeteredWriter(destination,meter,(1,2,'fixture'),0).awrite(b'next')
            self.assertEqual(meter.snapshot()['items'][0]['upload_bytes'],9)
        finally:
            release.set(); await asyncio.gather(task,return_exceptions=True)

    async def test_concurrent_directions_serialize_and_queued_cancel_does_not_write(self):
        meter=PayloadMeter(); meter.bind(1)
        entered=threading.Event(); release=threading.Event(); snapshots=[]
        self.addCleanup(release.set)
        def intent(snapshot):
            entered.set()
            if not release.wait(3): raise OSError('fixture timeout')
            snapshots.append(snapshot)
        meter.set_write_intent(intent)
        destination=mock.Mock(); key=(1,2,'fixture')
        first=asyncio.create_task(MeteredWriter(destination,meter,key,0).awrite(b'up'))
        second=None
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            second=asyncio.create_task(MeteredWriter(destination,meter,key,1).awrite(b'down'))
            await asyncio.sleep(0); second.cancel()
            with self.assertRaises(asyncio.CancelledError): await second
            release.set(); await first
            await MeteredWriter(destination,meter,key,1).awrite(b'down')
            self.assertEqual([(s['items'][0]['upload_bytes'],s['items'][0]['download_bytes'])
                              for s in snapshots],[(2,0),(2,4)])
            self.assertEqual(destination.write.call_count,2); self.assertFalse(meter.failed)
        finally:
            release.set(); await asyncio.gather(first,*([second] if second else []),return_exceptions=True)

    async def test_intent_failure_fences_before_transport(self):
        meter=PayloadMeter(); meter.bind(1)
        meter.set_write_intent(mock.Mock(side_effect=OSError('disk')))
        destination=mock.Mock(); writer=MeteredWriter(destination,meter,(1,2,'fixture'),0)
        with self.assertRaises(OSError): await writer.awrite(b'hello')
        with self.assertRaises(OSError): await writer.awrite(b'blocked')
        destination.write.assert_not_called(); self.assertTrue(meter.failed)

    async def test_checkpoint_failure_retains_successful_counters(self):
        meter=PayloadMeter(); meter.bind(1)
        meter.checkpoint=mock.Mock(side_effect=OSError('disk')); meter.incremental_checkpoint=True
        destination=mock.Mock(); writer=MeteredWriter(destination,meter,(1,2,'fixture'),0)
        with self.assertRaises(OSError): await writer.awrite(b'hello')
        self.assertEqual(meter.snapshot()['items'][0]['upload_bytes'],5)
        destination.write.assert_called_once_with(b'hello'); self.assertTrue(meter.failed)

    async def test_slow_sqlite_commit_keeps_loop_responsive_and_preserves_intent_order(self):
        with tempfile.TemporaryDirectory() as folder:
            box=UsageOutbox(Path(folder)/'usage.sqlite',1); session=str(uuid.uuid4())
            meter=PayloadMeter(); meter.bind(1); key=(1,2,str(uuid.uuid4()))
            meter.set_incremental_checkpoint(lambda snapshot:box.capture(session,snapshot))
            meter.set_write_intent(lambda snapshot:box.prepare_write(session,snapshot['items'][0]))
            entered=threading.Event(); release=threading.Event(); self.addCleanup(release.set)
            original=box.connect; thread_ids=[]
            @contextmanager
            def slow_connect(*args,**kwargs):
                with original(*args,**kwargs) as db:
                    def trace(sql):
                        if sql=='COMMIT':
                            thread_ids.append(threading.get_ident()); entered.set()
                            if not release.wait(3): raise OSError('fixture timeout')
                    db.set_trace_callback(trace)
                    yield db
            destination=mock.Mock()
            with mock.patch.object(box,'connect',slow_connect):
                task=asyncio.create_task(MeteredWriter(destination,meter,key,0).awrite(b'hello'))
                try:
                    self.assertTrue(await asyncio.to_thread(entered.wait,2))
                    for _ in range(20): await asyncio.sleep(0)
                    destination.write.assert_not_called(); self.assertFalse(task.done())
                    release.set(); await task
                finally:
                    release.set(); await asyncio.gather(task,return_exceptions=True)
            self.assertEqual(len(thread_ids),2)
            self.assertTrue(all(t!=threading.get_ident() for t in thread_ids))
            self.assertEqual(box.uncertain_writes(),[])
            self.assertEqual(UsageOutbox(box.path,1).next_report(session)['items'][0]['upload_bytes'],5)

    async def test_partial_quota_counts_only_written_bytes_without_refund(self):
        meter=PayloadMeter(); meter.bind(1); destination=mock.Mock()
        quota=CycleQuota(total_limit=3)
        writer=MeteredWriter(destination,meter,(1,2,'fixture'),0,quota)
        with self.assertRaises(QuotaExceeded): await writer.awrite(b'hello')
        destination.write.assert_called_once_with(b'hel')
        self.assertEqual(quota.total_remaining,0)
        self.assertEqual(meter.snapshot()['items'][0]['upload_bytes'],3)
        self.assertFalse(meter.failed)

    async def test_cancel_during_real_intent_commit_preserves_unknown_and_reserved_quota(self):
        with tempfile.TemporaryDirectory() as folder:
            box=UsageOutbox(Path(folder)/'usage.sqlite',1); session=str(uuid.uuid4())
            meter=PayloadMeter(); meter.bind(1); key=(1,2,str(uuid.uuid4()))
            meter.set_incremental_checkpoint(lambda snapshot:box.capture(session,snapshot))
            meter.set_write_intent(lambda snapshot:box.prepare_write(session,snapshot['items'][0]))
            entered=threading.Event(); release=threading.Event(); self.addCleanup(release.set)
            original=box.connect
            @contextmanager
            def paused_commit(*args,**kwargs):
                with original(*args,**kwargs) as db:
                    def trace(sql):
                        if sql=='COMMIT':
                            entered.set()
                            release.wait(3)
                    db.set_trace_callback(trace)
                    yield db
            destination=mock.Mock(); quota=CycleQuota(total_limit=10)
            with mock.patch.object(box,'connect',paused_commit):
                task=asyncio.create_task(MeteredWriter(destination,meter,key,0,quota).awrite(b'hello'))
                try:
                    self.assertTrue(await asyncio.to_thread(entered.wait,2))
                    task.cancel(); await asyncio.sleep(0)
                    self.assertFalse(task.done()); release.set()
                    with self.assertRaises(asyncio.CancelledError): await task
                finally:
                    release.set(); await asyncio.gather(task,return_exceptions=True)
            destination.write.assert_not_called(); self.assertTrue(meter.failed)
            reopened=UsageOutbox(box.path,1)
            self.assertIsNone(reopened.next_report(session))
            unknown=reopened.uncertain_writes()
            self.assertEqual(len(unknown),1)
            self.assertEqual((unknown[0]['committed_upload'],unknown[0]['possible_upload']),(0,5))
            self.assertEqual(quota.total_remaining,5)


class AsyncPayloadCrashTests(crash_fixture.PayloadCrashTests):
    """Reuse all four real socket + SIGKILL + FULL SQLite crash boundaries."""
    def crash(self,*args):
        script=crash_fixture.CHILD.replace('import os,signal,socket,sys','import asyncio,os,signal,socket,sys')
        script=script.replace("writer.write(b'hello')","asyncio.run(writer.awrite(b'hello'))")
        script=script.replace("writer.write(b'abcd')","asyncio.run(writer.awrite(b'abcd'))")
        with mock.patch.object(crash_fixture,'CHILD',script): super().crash(*args)
