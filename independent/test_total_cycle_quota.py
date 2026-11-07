"""Combined local reservation cap; not yet an execution/wire policy."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from payload_meter import CycleQuota, PayloadMeter, MeteredWriter, QuotaExceeded
from quota_store import QuotaStore, PersistentCycleQuota


class TotalCycleQuotaTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.path=Path(self.temp.name)/'quota.sqlite'

    def quota(self,key=(1,'period'),total=7,up=None,down=None,used=(0,0)):
        return PersistentCycleQuota(QuotaStore(self.path,1),key).reconcile(
            up,down,*used,total_limit=total)

    def test_combined_cap_intersects_directional_limits(self):
        q=CycleQuota(3,20,1,1,total_limit=7)
        self.assertEqual(q.reserve(0,9),2)
        self.assertEqual(q.reserve(1,9),3)
        self.assertEqual(q.used,[3,4]); self.assertEqual(q.total_remaining,0)
        self.assertEqual(q.reserve(0,1),0); self.assertEqual(q.reserve(1,1),0)
        self.assertEqual(CycleQuota(total_limit=0).reserve(1,100),0)
        self.assertEqual(CycleQuota().reserve(1,100),100)

    def test_validation_is_atomic_and_baseline_is_monotonic(self):
        q=CycleQuota(total_limit=7); q.reserve(0,5)
        for bad in (True,-1,1.5,'7'):
            with self.assertRaises(ValueError): q.reconcile(total_limit=bad)
            with self.assertRaises(ValueError): CycleQuota(total_limit=bad)
            self.assertEqual(q.used,[5,0]); self.assertEqual(q.total_limit,7)
        for bad in (True,-1,2,'0'):
            with self.assertRaises(ValueError): q.reserve(bad,1)
        with self.assertRaises(ValueError): CycleQuota(upload_used=4,download_used=4,total_limit=7)
        q.reconcile(upload_used=2,download_used=3,total_limit=6)
        self.assertEqual(q.used,[5,3]); self.assertEqual(q.reserve(1,1),0)
        q.reconcile(total_limit=10)
        self.assertEqual(q.reserve(1,10),2)
        q.reconcile(total_limit=None)
        self.assertEqual(q.used,[5,5]); self.assertEqual(q.reserve(0,2),2)

    def test_failed_write_reservation_survives_stale_baseline_and_caps_other_direction(self):
        q=self.quota(); meter=PayloadMeter(); target=mock.Mock()
        target.write.side_effect=OSError('transport uncertain')
        with self.assertRaises(OSError):
            MeteredWriter(target,meter,(1,1,'fixture'),0,q).write(b'hello')
        self.assertEqual(meter.snapshot()['items'],[])
        reopened=self.quota(); self.assertEqual(reopened.used,[5,0])
        self.assertEqual(reopened.reserve(1,9),2)
        self.assertEqual(self.quota().reserve(0,1),0)
        self.assertEqual(self.quota((1,'next')).reserve(1,7),7)
        self.assertEqual(self.quota((2,'period')).reserve(0,7),7)

    def test_sigkill_after_commit_keeps_combined_reservation(self):
        code='''import os,signal,sys
from quota_store import QuotaStore,PersistentCycleQuota
q=PersistentCycleQuota(QuotaStore(sys.argv[1],1),(1,'period')).reconcile(total_limit=7)
assert q.reserve(0,4)==4
assert q.reserve(1,2)==2
os.kill(os.getpid(),signal.SIGKILL)
'''
        result=subprocess.run([sys.executable,'-c',code,str(self.path)],
            cwd=Path(__file__).resolve().parent,timeout=10,capture_output=True)
        self.assertEqual(result.returncode,-signal.SIGKILL,result.stderr)
        q=self.quota(); self.assertEqual(q.used,[4,2])
        self.assertEqual(q.reserve(1,99),1)
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('PRAGMA integrity_check').fetchone()[0],'ok')

    def test_concurrent_opposite_directions_share_one_atomic_cap(self):
        self.quota(total=17)
        def reserve(i):
            try: return self.quota(total=17).reserve(i%2,2)
            except OSError as error:
                # The 50 ms payload lock deliberately fails closed under load.
                # Only this exact SQLite lock rejection is permitted here.
                if not isinstance(error.__cause__,sqlite3.OperationalError) or str(error.__cause__)!='database is locked':
                    raise
                return 0
        with ThreadPoolExecutor(max_workers=4) as pool:
            allowed=sum(pool.map(reserve,range(24)))
        self.assertLessEqual(allowed,17)
        q=self.quota(total=17)
        self.assertEqual(sum(q.used),allowed)
        self.assertEqual(q.reserve(1,17),17-allowed)
        self.assertEqual(sum(self.quota(total=17).used),17)

    def test_lowered_raised_and_remote_high_water_do_not_refund_unknown(self):
        q=self.quota(); q.reserve(0,5)
        self.assertEqual(self.quota(total=3).reserve(1,10),0)
        q=self.quota(total=10,used=(2,3))
        self.assertEqual(q.used,[5,3]); self.assertEqual(q.reserve(1,10),2)
        self.assertEqual(self.quota(total=11,used=(6,1)).reserve(0,10),0)
        with self.assertRaises(ValueError): self.quota(total=True)
        with self.assertRaises(ValueError):
            PersistentCycleQuota(QuotaStore(self.path,2),(1,'period')).reconcile(total_limit=11)


class TotalCycleSocketTests(unittest.IsolatedAsyncioTestCase):
    async def test_final_permitted_payload_is_queued_once_on_real_socket(self):
        received=asyncio.get_running_loop().create_future()
        async def accept(reader,writer):
            data=await reader.read(); writer.close(); await writer.wait_closed()
            received.set_result(data)
        server=await asyncio.start_server(accept,'127.0.0.1',0)
        self.addAsyncCleanup(server.wait_closed); self.addCleanup(server.close)
        reader,writer=await asyncio.open_connection(*server.sockets[0].getsockname())
        try:
            q=CycleQuota(total_limit=7); meter=PayloadMeter(); key=(1,1,'fixture')
            MeteredWriter(writer,meter,key,0,q).write(b'abcd')
            with self.assertRaises(QuotaExceeded): MeteredWriter(writer,meter,key,1,q).write(b'efghij')
            await writer.drain(); writer.write_eof()
            self.assertEqual(await asyncio.wait_for(received,3),b'abcdefg')
            self.assertEqual(q.used,[4,3])
            self.assertEqual(meter.snapshot()['items'][0]['download_bytes'],3)
        finally:
            writer.close(); await writer.wait_closed()
