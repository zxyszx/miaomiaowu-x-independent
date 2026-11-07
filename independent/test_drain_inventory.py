"""Drain frontier must survive cleanup without inventing final usage evidence."""
import sqlite3
import unittest
from core import Registry,Error
from usage_migration import UsageMigration
import test_usage_migration as base

class InventoryTests(unittest.TestCase):
    setUp=base.MigrationTests.setUp
    tearDown=base.MigrationTests.tearDown
    fixtures=base.MigrationTests.fixtures
    fixture=base.MigrationTests.fixture
    setup_total=base.MigrationTests.setup_total
    applied=base.MigrationTests.applied
    data=base.MigrationTests.data
    report=base.MigrationTests.report

    def rows(self,r):
        with r.connect() as db:
            return [tuple(row) for row in db.execute('SELECT * FROM usage_drain_inventory')]

    def test_frontier_survives_node_and_server_cleanup_and_restart(self):
        r=self.setup_total(); snap=self.applied(r); self.report()
        migration=UsageMigration(r); migration.begin_drain(self.period)
        before=self.rows(r)
        self.assertEqual(len(before),1)
        self.assertEqual(before[0][-2:],(4,3))
        r.delete('nodes',self.nid); r.delete('servers',snap['server_id'])
        reopened=Registry(r.path)
        self.assertEqual(self.rows(reopened),before)
        status=UsageMigration(reopened).inventory(self.period)
        self.assertTrue(status['frontier_captured'])
        self.assertEqual(status['execution_identities'],1)
        self.assertFalse(status['ready_for_execution'])

    def test_unreported_execution_is_unknown_and_late_report_cannot_rewrite_frontier(self):
        r=self.setup_total(); self.applied(r)
        migration=UsageMigration(r); migration.begin_drain(self.period)
        before=self.rows(r)
        self.assertEqual(before[0][-2:],(None,None))
        self.report()
        migration.begin_drain(self.period)
        self.assertEqual(self.rows(r),before)
        self.assertFalse(migration.inventory(self.period)['ready_for_execution'])

    def test_empty_new_frontier_is_distinct_from_unrecoverable_old_drain(self):
        r=self.setup_total(); migration=UsageMigration(r)
        migration.begin_drain(self.period)
        self.assertTrue(migration.inventory(self.period)['frontier_captured'])
        with r.connect() as db:
            db.execute('DELETE FROM usage_drain_inventory_captures')
        self.applied(r) # Empty configuration supplies no missing old evidence.
        migration.begin_drain(self.period)
        status=UsageMigration(Registry(r.path)).inventory(self.period)
        self.assertFalse(status['frontier_captured'])
        self.assertEqual(status['execution_identities'],0)
        self.assertFalse(status['ready_for_execution'])

    def test_inventory_and_marker_roll_back_with_failed_config_bump(self):
        r=self.setup_total(); self.applied(r)
        with r.connect() as db:
            db.execute("CREATE TRIGGER reject_inventory BEFORE UPDATE ON server_configs BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): UsageMigration(r).begin_drain(self.period)
        with r.connect() as db:
            for table in ('usage_cycle_drains','usage_drain_inventory','usage_drain_inventory_captures'):
                self.assertEqual(db.execute('SELECT count(*) FROM '+table).fetchone()[0],0)

    def test_undrained_cycle_has_no_invented_inventory(self):
        r=self.setup_total()
        with self.assertRaises(Error): UsageMigration(r).inventory(self.period)
        self.assertEqual(self.rows(r),[])

if __name__=='__main__': unittest.main()
