from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import subprocess
import sys
import sqlite3
import time
import unittest
from unittest import mock
from payload_meter import PayloadMeter, MeteredWriter
from quota_store import QuotaStore, PersistentCycleQuota


class QuotaStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.path=Path(self.temp.name)/'quota.sqlite'

    def quota(self,key=(1,'period'),server=1,limit=7):
        return PersistentCycleQuota(QuotaStore(self.path,server),key).reconcile(limit,20)

    @staticmethod
    def busy_or_raise(operation):
        # A bounded payload lock is allowed to deny a reservation. Do not hide
        # full/corrupt storage, constraint failures, or an unrelated OSError.
        try:
            return operation(), False
        except OSError as error:
            cause = error.__cause__
            if (not isinstance(cause, sqlite3.OperationalError)
                    or getattr(cause, 'sqlite_errorcode', None) != sqlite3.SQLITE_BUSY):
                raise
            return 0, True

    def test_new_instance_keeps_reservations_against_stale_baseline(self):
        self.assertEqual(self.quota().reserve(0,5),5)
        newer=self.quota()
        self.assertEqual(newer.used,[5,0])
        self.assertEqual(newer.reserve(0,5),2)
        self.assertEqual(self.quota().reserve(0,5),0)

    def test_cycles_users_and_directions_are_independent(self):
        first=self.quota(); first.reserve(0,7); first.reserve(1,4)
        self.assertEqual(self.quota().remaining,[0,16])
        self.assertEqual(self.quota((1,'next')).reserve(0,7),7)
        self.assertEqual(self.quota((2,'period')).reserve(0,7),7)
        with self.assertRaises(ValueError): self.quota(server=2)

    def test_unlimited_then_finite_and_higher_remote_usage_never_refund(self):
        first=self.quota(limit=None); first.reserve(0,9)
        self.assertEqual(self.quota().reserve(0,1),0)
        newer=self.quota(limit=15).reconcile(15,20,12,0)
        self.assertEqual(newer.reserve(0,10),3)
        self.assertEqual(self.quota(limit=15).used,[15,0])

    def test_storage_failure_prevents_transport_write(self):
        quota=self.quota(); meter=PayloadMeter(); target=mock.Mock()
        writer=MeteredWriter(target,meter,(1,1,'fixture'),0,quota)
        with mock.patch.object(quota.store,'update',side_effect=OSError('full')):
            with self.assertRaises(OSError): writer.write(b'hello')
        target.write.assert_not_called(); self.assertEqual(meter.snapshot()['items'],[])

    def test_real_sqlite_lock_rejects_before_write_without_burning_budget(self):
        quota=self.quota(); meter=PayloadMeter(); target=mock.Mock()
        writer=MeteredWriter(target,meter,(1,1,'fixture'),0,quota)
        lock=sqlite3.connect(self.path)
        try:
            lock.execute('BEGIN IMMEDIATE'); started=time.perf_counter()
            with self.assertRaises(OSError): writer.write(b'hello')
            self.assertLess(time.perf_counter()-started,0.5)
            target.write.assert_not_called()
            self.assertEqual(meter.snapshot()['items'],[])
        finally:
            lock.rollback(); lock.close()
        self.assertEqual(self.quota().remaining[0],7)
        writer.write(b'hello')
        self.assertEqual(self.quota().remaining[0],2)

    def test_failed_transport_burns_reservation_but_not_measured_payload(self):
        quota=self.quota(); meter=PayloadMeter(); target=mock.Mock()
        target.write.side_effect=OSError('closed')
        with self.assertRaises(OSError): MeteredWriter(target,meter,(1,1,'fixture'),0,quota).write(b'hello')
        self.assertEqual(self.quota().remaining[0],2)
        self.assertEqual(meter.snapshot()['items'],[])

    def test_corrupt_file_is_rejected_not_recreated(self):
        self.path.write_bytes(b'not a sqlite database')
        with self.assertRaises(OSError): self.quota()
        self.assertEqual(self.path.read_bytes(),b'not a sqlite database')

    def test_abrupt_process_exit_after_reservation_keeps_budget_consumed(self):
        code='''import os,sys
from quota_store import QuotaStore,PersistentCycleQuota
q=PersistentCycleQuota(QuotaStore(sys.argv[1],1),(1,'period')).reconcile(7,20)
assert q.reserve(0,5)==5
os._exit(0)
'''
        subprocess.run([sys.executable,'-c',code,str(self.path)],
                       cwd=Path(__file__).resolve().parent,check=True,timeout=10)
        self.assertEqual(self.quota().remaining[0],2)

    def test_concurrent_local_reservations_cannot_exceed_budget(self):
        self.quota()
        def reserve(_): return self.busy_or_raise(lambda: self.quota().reserve(0,2))
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(reserve,range(12)))
        admitted = sum(amount for amount, _ in results)
        self.assertLessEqual(admitted, 7)
        self.assertEqual(self.quota().used, [admitted, 0])
        # Rejected attempts must not burn budget. Finish uncontended without
        # retrying any failed transport or changing the runtime lock timeout.
        self.assertEqual(self.quota().reserve(0,7), 7-admitted)
        self.assertEqual(self.quota().used[0],7)

    def test_concurrent_busy_denials_never_write_or_consume_budget(self):
        quotas = [self.quota() for _ in range(4)]
        targets = [mock.Mock() for _ in quotas]
        meters = [PayloadMeter() for _ in quotas]
        writers = [MeteredWriter(target,meter,(1,1,'fixture'),0,quota)
                   for target,meter,quota in zip(targets,meters,quotas)]
        with sqlite3.connect(self.path) as lock:
            lock.execute('BEGIN IMMEDIATE')
            with ThreadPoolExecutor(max_workers=4) as pool:
                results = list(pool.map(lambda writer: self.busy_or_raise(lambda: writer.write(b'hello')), writers))
            self.assertEqual(results, [(0, True)]*4)
            lock.rollback()
        for target,meter in zip(targets,meters):
            target.write.assert_not_called()
            self.assertEqual(meter.snapshot()['items'], [])
        self.assertEqual(self.quota().used, [0,0])
        writers[0].write(b'hello')
        targets[0].write.assert_called_once_with(b'hello')
        self.assertEqual(self.quota().used, [5,0])

    def test_busy_handler_does_not_accept_other_storage_errors(self):
        quota = self.quota()
        with sqlite3.connect(self.path) as db:
            db.execute("CREATE TRIGGER reject_write BEFORE INSERT ON quotas BEGIN SELECT RAISE(ABORT,'fixture constraint'); END")
        with self.assertRaises(OSError) as caught:
            self.busy_or_raise(lambda: quota.reserve(0,2))
        self.assertIsInstance(caught.exception.__cause__, sqlite3.IntegrityError)
        with sqlite3.connect(self.path) as db:
            db.execute('DROP TRIGGER reject_write')
        self.assertEqual(self.quota().used, [0,0])
        def unrelated(): raise OSError('fixture storage error')
        with self.assertRaises(OSError): self.busy_or_raise(unrelated)
