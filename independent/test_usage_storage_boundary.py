"""Opt-in delivery coordination; the real forwarding runtime is not opted in yet."""
import asyncio
from pathlib import Path
import tempfile
import threading
import unittest
import uuid
from unittest import mock
from payload_meter import PayloadMeter, MeteredWriter
from usage_delivery import UsageDelivery


class Runtime:
    def __init__(self): self.meter=PayloadMeter()
    def usage_snapshot(self): return self.meter.snapshot()
    def set_incremental_usage_checkpoint(self,callback): self.meter.set_incremental_checkpoint(callback)
    def set_usage_write_intent(self,callback): self.meter.set_write_intent(callback)
    def usage_storage_boundary(self): return self.meter.io_lock


class BoundaryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        self.runtime=Runtime(); self.session=str(uuid.uuid4())
        self.worker=UsageDelivery('unused','fixture',Path(temp.name),self.runtime,self.session)
        self.runtime.meter.bind(1)
        self.key=(1,2,str(uuid.uuid4())); self.destination=mock.Mock()
        self.writer=MeteredWriter(self.destination,self.runtime.meter,self.key,0)

    def gate(self,function):
        entered=threading.Event(); release=threading.Event(); finished=threading.Event()
        self.addCleanup(release.set)
        def blocked(*args):
            entered.set()
            if not release.wait(4): raise OSError('fixture storage gate timeout')
            result=function(*args); finished.set(); return result
        return blocked,entered,release,finished

    async def entered(self,event):
        self.assertTrue(await asyncio.wait_for(asyncio.to_thread(event.wait,2),3))

    async def test_capture_waits_for_write_intent_and_success_checkpoint(self):
        blocked,entered,release,_=self.gate(self.worker.box.prepare_write)
        with mock.patch.object(self.worker.box,'prepare_write',side_effect=blocked):
            write=asyncio.create_task(self.writer.awrite(b'hello'))
            await self.entered(entered)
            capture=asyncio.create_task(self.worker.capture()); await asyncio.sleep(.02)
            self.assertFalse(capture.done()); self.destination.write.assert_not_called()
            release.set(); await asyncio.gather(write,capture)
        report=self.worker.box.next_report(self.session)
        self.assertEqual(report['items'][0]['upload_bytes'],5)

    async def test_capture_storage_is_off_loop_and_blocks_competing_write(self):
        blocked,entered,release,_=self.gate(self.worker._checkpoint)
        with mock.patch.object(self.worker,'_checkpoint',side_effect=blocked):
            capture=asyncio.create_task(self.worker.capture()); await self.entered(entered)
            write=asyncio.create_task(self.writer.awrite(b'hello')); await asyncio.sleep(.02)
            self.assertFalse(write.done()); self.destination.write.assert_not_called()
            release.set(); await asyncio.gather(capture,write)
        self.assertEqual(self.runtime.meter.snapshot()['items'][0]['upload_bytes'],5)

    async def test_rotation_serializes_old_write_and_new_baseline(self):
        await self.writer.awrite(b'old')
        blocked,entered,release,_=self.gate(self.worker.box.close_epoch)
        next_session=str(uuid.uuid4())
        with mock.patch.object(self.worker.box,'close_epoch',side_effect=blocked):
            rotate=asyncio.create_task(self.worker.rotate(next_session)); await self.entered(entered)
            write=asyncio.create_task(self.writer.awrite(b'newer')); await asyncio.sleep(.02)
            self.assertFalse(write.done()); self.assertEqual(self.worker.session,self.session)
            release.set(); await asyncio.gather(rotate,write)
        old=self.worker.box.next_report(self.session)['items'][0]
        new=self.worker.box.next_report(next_session)['items'][0]
        self.assertEqual((old['upload_bytes'],new['upload_bytes']),(3,5))
        self.assertEqual(self.runtime.meter.snapshot()['items'][0]['upload_bytes'],8)

    async def test_repeated_cancel_does_not_unlock_capture_storage_early(self):
        blocked,entered,release,finished=self.gate(self.worker._checkpoint)
        with mock.patch.object(self.worker,'_checkpoint',side_effect=blocked):
            capture=asyncio.create_task(self.worker.capture()); await self.entered(entered)
            capture.cancel(); await asyncio.sleep(0); capture.cancel(); await asyncio.sleep(.02)
            self.assertFalse(capture.done()); self.assertTrue(self.runtime.meter.io_lock.locked())
            release.set()
            with self.assertRaises(asyncio.CancelledError): await capture
        self.assertTrue(finished.is_set()); self.assertFalse(self.runtime.meter.io_lock.locked())
        await self.writer.awrite(b'after')

    async def test_failed_capture_does_not_change_epoch_or_baseline(self):
        await self.writer.awrite(b'old')
        with mock.patch.object(self.worker,'_checkpoint',side_effect=OSError('disk')):
            with self.assertRaises(OSError): await self.worker.rotate(str(uuid.uuid4()))
        self.assertEqual(self.worker.session,self.session); self.assertEqual(self.worker.baseline,{})
        self.assertFalse(self.runtime.meter.io_lock.locked())
        self.assertEqual(self.worker.box.next_report(self.session)['items'][0]['upload_bytes'],3)

    async def test_repeated_cancel_drains_maintenance_before_boundary_release(self):
        blocked,entered,release,finished=self.gate(self.worker.box.retire_confirmed)
        with mock.patch.object(self.worker.box,'retire_confirmed',side_effect=blocked):
            capture=asyncio.create_task(self.worker.capture()); await self.entered(entered)
            capture.cancel(); await asyncio.sleep(0); capture.cancel(); await asyncio.sleep(.02)
            self.assertFalse(capture.done()); self.assertTrue(self.runtime.meter.io_lock.locked())
            release.set()
            with self.assertRaises(asyncio.CancelledError): await capture
        self.assertTrue(finished.is_set()); self.assertFalse(self.runtime.meter.io_lock.locked())

    async def test_cancelled_close_commit_cannot_rebase_or_write_closed_epoch(self):
        await self.writer.awrite(b'old')
        blocked,entered,release,finished=self.gate(self.worker.box.close_epoch)
        with mock.patch.object(self.worker.box,'close_epoch',side_effect=blocked):
            rotate=asyncio.create_task(self.worker.rotate(str(uuid.uuid4())))
            await self.entered(entered)
            rotate.cancel(); await asyncio.sleep(0); rotate.cancel(); await asyncio.sleep(.02)
            self.assertFalse(rotate.done()); self.assertTrue(self.runtime.meter.io_lock.locked())
            release.set()
            with self.assertRaises(asyncio.CancelledError): await rotate
        self.assertTrue(finished.is_set()); self.assertEqual(self.worker.session,self.session)
        self.assertEqual(self.worker.baseline,{})
        self.destination.reset_mock()
        with self.assertRaises(OSError): await self.writer.awrite(b'must not send')
        self.destination.write.assert_not_called()
        self.assertTrue(self.runtime.meter.failed)
        self.assertEqual(self.worker.box.next_report(self.session)['items'][0]['upload_bytes'],3)

    async def test_concurrent_same_rotation_cannot_close_successor_epoch(self):
        await self.writer.awrite(b'old'); next_session=str(uuid.uuid4())
        results=await asyncio.gather(self.worker.rotate(next_session),self.worker.rotate(next_session),return_exceptions=True)
        self.assertEqual(sum(isinstance(r,ValueError) for r in results),1)
        await self.writer.awrite(b'new')
        self.assertEqual(self.worker.box.next_report(next_session)['items'][0]['upload_bytes'],3)

    async def test_finish_holds_boundary_until_final_close_commit(self):
        await self.writer.awrite(b'final')
        blocked,entered,release,finished=self.gate(self.worker.box.close_epoch)
        with mock.patch.object(self.worker.box,'close_epoch',side_effect=blocked):
            finish=asyncio.create_task(self.worker.finish(None)); await self.entered(entered)
            self.assertTrue(self.runtime.meter.io_lock.locked())
            release.set(); await finish
        self.assertTrue(finished.is_set())
        self.assertEqual(self.worker.box.next_report(self.session)['items'][0]['upload_bytes'],5)
