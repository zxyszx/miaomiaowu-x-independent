"""Real SQLite writer ordering: a legacy ACK is captured or rejected, never lost."""
from concurrent.futures import ThreadPoolExecutor
import sqlite3
import threading
import unittest
from unittest.mock import patch
from core import Registry, Error
from drain_seal import DrainSeal
from usage_migration import UsageMigration
import test_drain_seal as base


class DrainAckAtomicTests(unittest.TestCase):
    setUp=base.DrainSealTests.setUp
    tearDown=base.DrainSealTests.tearDown
    fixtures=base.DrainSealTests.fixtures
    fixture=base.DrainSealTests.fixture
    setup_total=base.DrainSealTests.setup_total
    setup_drain=base.DrainSealTests.setup_drain
    applied=base.DrainSealTests.applied
    report=base.DrainSealTests.report
    data=base.DrainSealTests.data
    encode=staticmethod(base.DrainSealTests.encode)
    envelope=base.DrainSealTests.envelope
    accept=base.DrainSealTests.accept

    def stage(self,r):
        snap=r.configuration(self.token)
        ack=dict(revision=snap['revision'],digest=snap['digest'],session_id=self.session)
        r.acknowledge_configuration(self.token,ack)
        return snap,ack

    def assert_frontier(self,r,count):
        with r.connect() as db:
            grants=[tuple(x) for x in db.execute('''SELECT server_id,session_id,node_id,user_id,credential
                FROM usage_execution_grants WHERE cycle_id=?''',(self.period,))]
            captured=[tuple(x) for x in db.execute('''SELECT server_id,session_id,node_id,user_id,credential
                FROM usage_drain_inventory WHERE cycle_id=?''',(self.period,))]
        self.assertEqual(len(grants),count)
        self.assertEqual(set(captured),set(grants))
        self.assertEqual(r.configuration(self.token)['nodes'][0]['clients'],[])

    def test_ack_holds_writer_first_and_drain_captures_its_committed_identity(self):
        r=self.setup_total(); _,ack=self.stage(r)
        entered=threading.Event(); release=threading.Event(); started=threading.Event()
        original=r._snapshot
        def snapshot(db,sid):
            result=original(db,sid)
            entered.set()
            if not release.wait(5): raise AssertionError('ACK transaction was not released')
            return result
        other=Registry(r.path)
        def drain():
            started.set()
            return UsageMigration(other).begin_drain(self.period)
        with ThreadPoolExecutor(2) as pool:
            with patch.object(r,'_snapshot',side_effect=snapshot):
                first=pool.submit(r.acknowledge_runtime,self.token,ack)
                try:
                    self.assertTrue(entered.wait(5))
                    second=pool.submit(drain)
                    self.assertTrue(started.wait(5))
                finally: release.set()
                self.assertTrue(first.result(timeout=5)['accepted'])
                self.assertFalse(second.result(timeout=5)['ready_for_execution'])
        self.assert_frontier(Registry(r.path),1)

    def test_drain_holds_writer_first_and_staged_ack_is_rejected(self):
        r=self.setup_total(); _,ack=self.stage(r)
        entered=threading.Event(); release=threading.Event(); started=threading.Event()
        original=r._bump
        def bump(db,sid):
            original(db,sid)
            entered.set()
            if not release.wait(5): raise AssertionError('drain transaction was not released')
        other=Registry(r.path)
        def apply():
            started.set()
            return other.acknowledge_runtime(self.token,ack)
        with ThreadPoolExecutor(2) as pool:
            with patch.object(r,'_bump',side_effect=bump):
                first=pool.submit(UsageMigration(r).begin_drain,self.period)
                try:
                    self.assertTrue(entered.wait(5))
                    second=pool.submit(apply)
                    self.assertTrue(started.wait(5))
                finally: release.set()
                first.result(timeout=5)
                with self.assertRaises(Error) as caught:second.result(timeout=5)
                self.assertEqual(caught.exception.status,409)
        self.assert_frontier(Registry(r.path),0)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM runtime_receipts').fetchone()[0],0)

    def test_frontier_insert_failure_rolls_back_drain_capture_and_revision(self):
        r=self.setup_total(); snap=self.applied(r)
        with r.connect() as db:
            db.execute("CREATE TRIGGER deny_frontier BEFORE INSERT ON usage_drain_inventory BEGIN SELECT RAISE(ABORT,'injected frontier failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):UsageMigration(r).begin_drain(self.period)
        with r.connect() as db:
            for table in ('usage_cycle_drains','usage_drain_inventory_captures','usage_drain_inventory'):
                self.assertEqual(db.execute('SELECT count(*) FROM '+table).fetchone()[0],0)
            self.assertEqual(db.execute('SELECT revision FROM server_configs WHERE server_id=?',(snap['server_id'],)).fetchone()[0],snap['revision'])
            db.execute('DROP TRIGGER deny_frontier')
        self.assertEqual(r.configuration(self.token)['digest'],snap['digest'])
        UsageMigration(Registry(r.path)).begin_drain(self.period)
        self.assert_frontier(r,1)

    def test_execution_insert_failure_cannot_leave_runtime_ack_or_fake_frontier(self):
        r=self.setup_total(); _,ack=self.stage(r)
        with r.connect() as db:
            db.execute("CREATE TRIGGER deny_execution BEFORE INSERT ON usage_execution_grants BEGIN SELECT RAISE(ABORT,'injected execution failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):r.acknowledge_runtime(self.token,ack)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM runtime_receipts').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM usage_execution_grants').fetchone()[0],0)
        UsageMigration(r).begin_drain(self.period)
        self.assert_frontier(Registry(r.path),0)
        with self.assertRaises(Error):DrainSeal(r).seal(self.period)

    def test_empty_post_drain_runtime_ack_does_not_renew_captured_execution(self):
        r=self.setup_drain(); self.accept(); sealed=DrainSeal(r).seal(self.period)
        with r.connect() as db:
            before=tuple(db.execute('SELECT issued_at,settle_until FROM usage_execution_grants').fetchone())
        snap,ack=self.stage(r)
        self.assertEqual(snap['nodes'][0]['clients'],[])
        self.assertTrue(r.acknowledge_runtime(self.token,ack)['accepted'])
        with r.connect() as db:
            self.assertEqual(tuple(db.execute('SELECT issued_at,settle_until FROM usage_execution_grants').fetchone()),before)
        self.assertTrue(self.report()['duplicate'])
        self.assertEqual(DrainSeal(Registry(r.path)).seal(self.period)['digest'],sealed['digest'])
        self.assert_frontier(r,1)
