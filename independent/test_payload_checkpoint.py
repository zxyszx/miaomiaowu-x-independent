"""Successful writes checkpoint straight into the original epoch's outbox."""
from pathlib import Path
import sqlite3
import time
import subprocess
import sys
import tempfile
import unittest
import uuid
from unittest import mock
from payload_meter import PayloadMeter,MeteredWriter
from usage_delivery import UsageDelivery
from usage_outbox import UsageOutbox


class Runtime:
    def __init__(self): self.meter=PayloadMeter()
    def usage_snapshot(self): return self.meter.snapshot()
    def set_usage_checkpoint(self,callback): self.meter.set_checkpoint(callback)
    def set_incremental_usage_checkpoint(self,callback): self.meter.set_incremental_checkpoint(callback)


class CheckpointTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.state=Path(self.temp.name); self.session=str(uuid.uuid4())
        self.key=(1,2,str(uuid.uuid4())); self.runtime=Runtime()
        self.worker=UsageDelivery('http://local','fixture',self.state,self.runtime,self.session)
        self.runtime.meter.bind(1)
        self.writer=mock.Mock()
        self.measured=MeteredWriter(self.writer,self.runtime.meter,self.key,0)

    def test_write_does_not_enumerate_other_identities_or_request_full_snapshot(self):
        keys=[(1,n+1,str(uuid.uuid4())) for n in range(256)]
        for key in keys: MeteredWriter(self.writer,self.runtime.meter,key,0).write(b'x')
        class NoScan(dict):
            def items(self): raise AssertionError('payload write scanned all identities')
            def __iter__(self): raise AssertionError('payload write iterated all identities')
        self.runtime.meter.counters=NoScan(self.runtime.meter.counters)
        with mock.patch.object(self.runtime,'usage_snapshot',side_effect=AssertionError('full snapshot')):
            MeteredWriter(self.writer,self.runtime.meter,keys[9],1).write(b'hello')
        with self.worker.box.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM totals').fetchone()[0],256)
            selected=db.execute('SELECT upload,download FROM totals WHERE user=10').fetchone()
        self.assertEqual(tuple(selected),(1,5))

    async def test_incremental_failure_recovers_full_tail_without_rewriting_other_totals(self):
        other=(1,3,str(uuid.uuid4()))
        self.measured.write(b'hello')
        MeteredWriter(self.writer,self.runtime.meter,other,1).write(b'other')
        with mock.patch.object(self.worker.box,'capture',side_effect=sqlite3.OperationalError('disk')):
            with self.assertRaises(OSError): self.measured.write(b'tail')
        await self.worker.capture()
        items=self.worker.box.next_report(self.session)['items']
        self.assertEqual([(i['upload_bytes'],i['download_bytes']) for i in items],[(9,0),(0,5)])
        self.assertTrue(self.runtime.meter.failed)

    def test_legacy_checkpoint_runtime_still_captures_without_arguments(self):
        class Legacy:
            def __init__(self): self.meter=PayloadMeter()
            def usage_snapshot(self): return self.meter.snapshot()
            def set_usage_checkpoint(self,callback): self.meter.set_checkpoint(callback)
        runtime=Legacy()
        with tempfile.TemporaryDirectory() as state:
            delivery=UsageDelivery('unused','fixture',state,runtime,self.session)
            runtime.meter.bind(1)
            MeteredWriter(self.writer,runtime.meter,self.key,0).write(b'legacy')
            self.assertEqual(delivery.box.next_report(self.session)['items'][0]['upload_bytes'],6)

    def test_success_is_durable_without_periodic_capture(self):
        self.measured.write(b'hello')
        box=UsageOutbox(self.state/'usage-outbox.sqlite',1)
        report=box.next_report(self.session)
        self.assertEqual(report['items'][0]['upload_bytes'],5)
        self.writer.write.assert_called_once_with(b'hello')

    def test_many_identities_checkpoint_only_changed_cumulative_totals(self):
        keys=[(1,n+1,str(uuid.uuid4())) for n in range(80)]
        with mock.patch.object(self.worker.box,'capture',wraps=self.worker.box.capture) as capture:
            for key in keys:
                MeteredWriter(self.writer,self.runtime.meter,key,0).write(b'hello')
            self.assertEqual(capture.call_count,80)
            self.assertTrue(all(len(call.args[1]['items'])==1 for call in capture.call_args_list))
            capture.reset_mock()
            MeteredWriter(self.writer,self.runtime.meter,keys[0],1).write(b'new')
            self.assertEqual(capture.call_count,1)
            self.assertEqual(capture.call_args.args[1]['items'],[dict(node_id=1,user_id=1,
                credential=keys[0][2],upload_bytes=5,download_bytes=3)])
        with self.worker.box.connect() as db:
            self.assertEqual(tuple(db.execute('SELECT count(*),sum(upload),sum(download) FROM totals').fetchone()),(80,400,3))

    async def test_failed_changed_commit_is_not_cached_and_tail_retries(self):
        self.measured.write(b'hello')
        with mock.patch.object(self.worker.box,'capture',side_effect=sqlite3.OperationalError('disk')):
            with self.assertRaises(OSError): self.measured.write(b'tail')
        self.assertEqual(self.worker.checkpoint_totals[self.key],(5,0))
        await self.worker.capture()
        self.assertEqual(self.worker.checkpoint_totals[self.key],(9,0))
        self.assertEqual(self.worker.box.next_report(self.session)['items'][0]['upload_bytes'],9)
        self.assertTrue(self.runtime.meter.failed)

    async def test_rotation_resets_cache_when_new_totals_equal_previous_epoch(self):
        self.measured.write(b'old')
        old=self.worker.box.next_report(self.session)
        new=str(uuid.uuid4()); await self.worker.rotate(new)
        self.measured.write(b'new')
        self.assertEqual(self.worker.box.next_report(self.session),old)
        self.assertEqual(self.worker.box.next_report(new)['items'][0]['upload_bytes'],3)

    async def test_unchanged_periodic_capture_does_not_rewrite_totals(self):
        self.measured.write(b'hello')
        with mock.patch.object(self.worker.box,'capture',wraps=self.worker.box.capture) as capture:
            await self.worker.capture()
            capture.assert_not_called()
        self.assertEqual(self.worker.box.next_report(self.session)['items'][0]['upload_bytes'],5)

    def test_failed_transport_does_not_create_usage(self):
        self.writer.write.side_effect=OSError('closed')
        with self.assertRaises(OSError): self.measured.write(b'failed')
        self.assertIsNone(self.worker.box.next_report(self.session))

    async def test_real_sqlite_lock_fences_promptly_and_tail_recovers(self):
        self.measured.write(b'ok')
        lock=sqlite3.connect(self.worker.box.path)
        try:
            lock.execute('BEGIN IMMEDIATE')
            started=time.perf_counter()
            with self.assertRaises(OSError): self.measured.write(b'tail')
            self.assertLess(time.perf_counter()-started,0.5)
            with self.assertRaises(OSError): self.measured.write(b'blocked')
            self.assertEqual(self.writer.write.call_count,2)
        finally:
            lock.rollback(); lock.close()
        await self.worker.capture()
        item=self.worker.box.next_report(self.session)['items'][0]
        self.assertEqual((item['upload_bytes'],item['download_bytes']),(6,0))
        self.assertTrue(self.runtime.meter.failed)

    async def test_failed_checkpoint_latches_closed_and_final_capture_can_recover(self):
        with mock.patch.object(self.worker.box,'capture',side_effect=OSError('disk')):
            with self.assertRaises(OSError): self.measured.write(b'hello')
        with self.assertRaises(OSError): self.measured.write(b'blocked')
        self.assertEqual(self.writer.write.call_count,1)
        await self.worker.capture()
        self.assertEqual(self.worker.box.next_report(self.session)['items'][0]['upload_bytes'],5)
        with self.assertRaises(OSError): self.runtime.meter.bind(1)

    async def test_rotation_keeps_frozen_report_and_new_bytes_separate(self):
        self.measured.write(b'hello'); old=self.worker.box.next_report(self.session)
        newer=str(uuid.uuid4()); await self.worker.rotate(newer)
        self.measured.write(b'new')
        self.assertEqual(self.worker.box.next_report(self.session),old)
        self.assertEqual(self.worker.box.next_report(newer)['items'][0]['upload_bytes'],3)
        await self.worker.capture()

    def test_process_exit_after_write_preserves_original_epoch(self):
        script='''import os,sys
from pathlib import Path
from payload_meter import PayloadMeter,MeteredWriter
from usage_delivery import UsageDelivery
class Runtime:
 def __init__(self): self.meter=PayloadMeter()
 def usage_snapshot(self): return self.meter.snapshot()
 def set_usage_checkpoint(self,callback): self.meter.set_checkpoint(callback)
r=Runtime(); w=UsageDelivery('http://local','fixture',Path(sys.argv[1]),r,sys.argv[2]); r.meter.bind(1)
class Writer:
 def write(self,data): pass
MeteredWriter(Writer(),r.meter,(1,2,sys.argv[3]),0).write(b'hello')
os._exit(0)
'''
        subprocess.run([sys.executable,'-c',script,str(self.state),self.session,self.key[2]],
                       cwd=Path(__file__).parent,check=True,timeout=10)
        report=UsageOutbox(self.state/'usage-outbox.sqlite',1).next_report(self.session)
        self.assertEqual(report['items'][0]['upload_bytes'],5)

    async def test_fresh_process_counters_do_not_replay_confirmed_old_bytes(self):
        self.measured.write(b'hello')
        old=self.worker.box.next_report(self.session)
        self.worker.box.acknowledge(old,dict(accepted=True,server_id=1,
                                    session_id=self.session,sequence=old['sequence']))
        fresh=Runtime(); session=str(uuid.uuid4())
        worker=UsageDelivery('http://local','fixture',self.state,fresh,session)
        fresh.meter.bind(1)
        MeteredWriter(mock.Mock(),fresh.meter,self.key,0).write(b'new')
        await worker.capture()
        self.assertEqual(worker.box.outstanding_sessions(),[session])
        self.assertEqual(worker.box.next_report(session)['items'][0]['upload_bytes'],3)
