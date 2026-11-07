"""Directional quotas share a durable, deletion-safe accounting source."""
import sqlite3
import unittest
from core import Registry,Error
from total_usage import directions,add_directions
from usage_accounting import VERSION
from usage_ledger import MAX_COUNTER
import test_total_usage as base


class DirectionTests(unittest.TestCase):
    setUp=base.ConfirmedTests.setUp
    tearDown=base.ConfirmedTests.tearDown
    fixtures=base.ConfirmedTests.fixtures
    fixture=base.ConfirmedTests.fixture
    setup_total=base.ConfirmedTests.setup_total
    report=base.ConfirmedTests.report
    second_host=base.ConfirmedTests.second_host
    share_delete=base.ConfirmedTests.share_delete
    total=base.ConfirmedTests.total

    def known(self,r):
        with r.connect() as db:
            db.execute('BEGIN IMMEDIATE'); return directions(db,self.period)

    def test_deleted_node_keeps_direction_limits_and_snapshot_usage(self):
        r=self.setup_total(40); self.cycles.set_budget(self.period,dict(upload_bytes=4,download_bytes=10))
        self.report(); token,data=self.second_host(r); r.delete('nodes',self.nid)
        self.assertEqual(self.known(Registry(r.path)),(4,3,0))
        client=r.configuration(token)['nodes'][0]['clients'][0]
        self.assertEqual(client['cycle_usage'],dict(upload_bytes=4,download_bytes=3))
        data['items'][0].update(upload_bytes=1,download_bytes=0)
        with self.assertRaises(Error): self.ledger.report(token,data)
        data['items'][0].update(upload_bytes=0,download_bytes=7)
        self.ledger.report(token,data)
        self.assertEqual(self.known(r),(4,10,0))

    def test_missing_directions_block_delete_without_repair(self):
        r=self.setup_total(40); self.report()
        with r.connect() as db: db.execute('DELETE FROM usage_cycle_confirmed_directions')
        with self.assertRaises(Error): r.delete('nodes',self.nid)
        with self.assertRaises(Error): self.known(Registry(r.path))
        with r.connect() as db:
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_cycle_confirmed_directions').fetchone()[0],0)
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_cycle_accounting_events').fetchone()[0],1)
            self.assertIsNotNone(db.execute('SELECT 1 FROM nodes WHERE id=?',(self.nid,)).fetchone())

    def test_unknown_direction_is_not_invented_or_released(self):
        r=self.setup_total(40); self.report(); token,data=self.second_host(r)
        r.delete('nodes',self.nid)
        # Emulate a pre-marker installation that retained only aggregate totals.
        with r.connect() as db:
            db.execute('DELETE FROM usage_cycle_accounting_events')
            db.execute('DELETE FROM usage_attributed_sources')
            db.execute('DELETE FROM usage_cycle_accounting_anchors')
            db.execute('DELETE FROM schema_migrations WHERE name=?',(VERSION,))
            db.execute('DELETE FROM usage_cycle_confirmed_directions')
            db.execute('DELETE FROM usage_deltas')
        r=Registry(r.path);self.ledger.registry=r
        self.cycles.set_budget(self.period,dict(upload_bytes=10,download_bytes=10))
        self.assertEqual(self.known(r),(0,0,7))
        self.assertEqual(r.configuration(token)['nodes'][0]['clients'],[])
        data['items'][0].update(upload_bytes=1,download_bytes=0)
        with self.assertRaises(Error): self.ledger.report(token,data)
        self.assertEqual(self.known(r),(0,0,7))
        # Both directions explicitly unlimited: new known usage can accumulate
        # without relabelling the unknown historical seven bytes.
        with r.connect() as db:
            db.execute('UPDATE usage_cycle_budgets SET upload_bytes=NULL,download_bytes=NULL')
        self.ledger.report(token,data)
        self.assertEqual(self.known(r),(1,0,7))
        self.assertEqual(self.total(r),8)
        self.assertEqual(r.configuration(token)['nodes'][0]['clients'][0]['cycle_usage'],
                         dict(upload_bytes=1,download_bytes=0))

    def test_arbitrary_precision_snapshot_and_retry(self):
        r=self.setup_total(None); self.item.update(upload_bytes=MAX_COUNTER,download_bytes=MAX_COUNTER)
        self.report(); self.report(); token,data=self.second_host(r)
        data['items'][0].update(upload_bytes=MAX_COUNTER,download_bytes=MAX_COUNTER)
        self.ledger.report(token,data)
        self.assertEqual(self.known(r),(MAX_COUNTER*2,MAX_COUNTER*2,0))
        self.cycles.set_budget(self.period,dict(upload_bytes=MAX_COUNTER,download_bytes=MAX_COUNTER))
        self.assertEqual(r.configuration(token)['nodes'][0]['clients'][0]['cycle_usage'],
                         dict(upload_bytes=MAX_COUNTER*2,download_bytes=MAX_COUNTER*2))

    def test_direction_write_failure_rolls_back_all_accounting(self):
        r=self.setup_total(40); self.report()
        with r.connect() as db:
            db.execute("CREATE TRIGGER fail_direction BEFORE UPDATE ON usage_cycle_confirmed_directions BEGIN SELECT RAISE(ABORT,'fixture'); END")
        self.item['upload_bytes']=5
        with self.assertRaises(sqlite3.IntegrityError): self.report(2)
        self.assertEqual(self.known(r),(4,3,0)); self.assertEqual(self.total(r),7)
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],4)
        with r.connect() as db: db.execute('DROP TRIGGER fail_direction')
        self.report(2); self.report(2)
        self.assertEqual(self.known(r),(5,3,0))

    def test_shared_cleanup_preserves_directions(self):
        self.share_delete(True)
        r=Registry(self.ledger.registry.path)
        with self.assertRaises(Error): self.known(r)
        with r.connect() as db:
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_cycle_accounting_events').fetchone()[0],1)

    def test_owner_deletion_and_helper_guards(self):
        r=self.setup_total(40); self.report()
        with r.connect() as db:
            with self.assertRaises(RuntimeError): directions(db,self.period)
            db.execute('BEGIN IMMEDIATE')
            for value in (True,-1,1.5):
                with self.assertRaises(ValueError): add_directions(db,self.period,value,0)
        with self.assertRaises(Error) as caught: r.delete('users',self.uid)
        self.assertEqual(caught.exception.status,409)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_cycle_confirmed_directions').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT count(*) FROM usage_cycle_accounting_events').fetchone()[0],1)
            self.assertIsNotNone(db.execute('SELECT 1 FROM users WHERE id=?',(self.uid,)).fetchone())
