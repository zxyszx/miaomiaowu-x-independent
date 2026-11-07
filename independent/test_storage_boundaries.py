"""Real SQLite commit/full failures, bounded to isolated temporary files."""
from pathlib import Path
import sqlite3
import tempfile
import unittest
import uuid
from unittest import mock

from payload_meter import PayloadMeter, MeteredWriter
from quota_store import QuotaStore, PersistentCycleQuota
from usage_delivery import UsageDelivery
from storage_boundaries_fixture import page_limited_connections


class Runtime:
    def __init__(self): self.meter=PayloadMeter()
    def usage_snapshot(self): return self.meter.snapshot()
    def set_incremental_usage_checkpoint(self,callback): self.meter.set_incremental_checkpoint(callback)


class StorageBoundaryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.state=Path(self.temp.name); self.session=str(uuid.uuid4())
        self.runtime=Runtime(); self.key=(1,2,str(uuid.uuid4()))
        self.delivery=UsageDelivery('unused','fixture',self.state,self.runtime,self.session)
        self.runtime.meter.bind(1); self.target=mock.Mock()
        self.writer=MeteredWriter(self.target,self.runtime.meter,self.key,0)

    async def assert_tail_recovery(self,frozen):
        box=self.delivery.box
        await self.delivery.capture()
        self.assertEqual(box.next_report(self.session),frozen)
        box.acknowledge(frozen,dict(accepted=True,server_id=1,
            session_id=self.session,sequence=frozen['sequence']))
        tail=box.next_report(self.session)
        self.assertEqual(tail['items'][0]['upload_bytes'],9)
        self.assertTrue(self.runtime.meter.failed)
        with self.assertRaises(OSError): self.writer.write(b'blocked')
        self.assertEqual(self.target.write.call_count,2)

    async def test_checkpoint_commit_busy_rolls_back_and_recovers_original_tail(self):
        self.writer.write(b'hello')
        box=self.delivery.box; frozen=box.next_report(self.session)
        with box.connect() as db:
            # A reader's shared lock allows BEGIN IMMEDIATE and UPDATE, but
            # prevents COMMIT from obtaining its exclusive lock.
            db.execute('BEGIN')
            db.execute('SELECT * FROM totals').fetchall()
            with self.assertRaises(OSError) as caught: self.writer.write(b'tail')
            self.assertEqual(caught.exception.__cause__.sqlite_errorcode,sqlite3.SQLITE_BUSY)
            self.assertEqual(db.execute('SELECT upload FROM totals').fetchone()[0],5)
        await self.assert_tail_recovery(frozen)

    async def test_checkpoint_sqlite_full_preserves_frozen_report_and_recovers_tail(self):
        self.writer.write(b'hello')
        box=self.delivery.box; frozen=box.next_report(self.session)
        with box.connect() as db:
            db.executescript('''CREATE TABLE fixture_padding(data BLOB);
                CREATE TRIGGER fixture_full BEFORE UPDATE ON totals BEGIN
                    INSERT INTO fixture_padding VALUES(zeroblob(65536)); END;''')
        with page_limited_connections(box.path):
            with self.assertRaises(OSError) as caught: self.writer.write(b'tail')
        self.assertEqual(caught.exception.__cause__.sqlite_errorcode,sqlite3.SQLITE_FULL)
        with box.connect() as db:
            self.assertEqual(db.execute('SELECT upload FROM totals').fetchone()[0],5)
            self.assertEqual(db.execute('SELECT count(*) FROM fixture_padding').fetchone()[0],0)
            db.execute('DROP TRIGGER fixture_full')
        await self.assert_tail_recovery(frozen)

    def quota_writer(self):
        path=self.state/'quota.sqlite'
        quota=PersistentCycleQuota(QuotaStore(path,1),(2,'period')).reconcile(7,20)
        meter=PayloadMeter(); target=mock.Mock()
        return path,quota,meter,target,MeteredWriter(target,meter,self.key,0,quota)

    def assert_quota_recovery(self,path,quota,meter,target,writer):
        target.write.assert_not_called()
        self.assertEqual(meter.snapshot()['items'],[])
        self.assertEqual(quota.used,[0,0])
        reopened=PersistentCycleQuota(QuotaStore(path,1),(2,'period')).reconcile(7,20)
        self.assertEqual(reopened.remaining,[7,20])
        writer.write(b'hello')
        self.assertEqual(quota.remaining,[2,20])
        target.write.assert_called_once_with(b'hello')

    def test_quota_commit_busy_prevents_write_and_does_not_burn_allowance(self):
        path,quota,meter,target,writer=self.quota_writer()
        db=sqlite3.connect(path)
        try:
            db.execute('BEGIN'); db.execute('SELECT * FROM quotas').fetchall()
            with self.assertRaises(OSError) as caught: writer.write(b'hello')
            self.assertEqual(caught.exception.__cause__.sqlite_errorcode,sqlite3.SQLITE_BUSY)
            self.assertEqual(db.execute('SELECT upload FROM quotas').fetchone()[0],0)
        finally: db.close()
        self.assert_quota_recovery(path,quota,meter,target,writer)

    def test_quota_sqlite_full_prevents_write_and_does_not_burn_allowance(self):
        path,quota,meter,target,writer=self.quota_writer()
        db=sqlite3.connect(path)
        try:
            db.executescript('''CREATE TABLE fixture_padding(data BLOB);
                CREATE TRIGGER fixture_full BEFORE INSERT ON quotas BEGIN
                    INSERT INTO fixture_padding VALUES(zeroblob(65536)); END;''')
        finally: db.close()
        with page_limited_connections(path):
            with self.assertRaises(OSError) as caught: writer.write(b'hello')
        self.assertEqual(caught.exception.__cause__.sqlite_errorcode,sqlite3.SQLITE_FULL)
        db=sqlite3.connect(path)
        try:
            self.assertEqual(db.execute('SELECT count(*) FROM fixture_padding').fetchone()[0],0)
            db.execute('DROP TRIGGER fixture_full'); db.commit()
        finally: db.close()
        self.assert_quota_recovery(path,quota,meter,target,writer)
