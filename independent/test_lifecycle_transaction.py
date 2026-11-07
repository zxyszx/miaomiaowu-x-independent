"""Caller-owned transactions for plan assignment and lifecycle management."""
from contextlib import contextmanager
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest
from unittest import mock

from core import Error, Registry
from plan_lifecycle import Lifecycle
from test_plan_transaction import BorrowedConnection, snapshot, seed


class DetailedConnection(BorrowedConnection):
    def execute(self, sql, parameters=()):
        normalized = ' '.join(sql.strip().split()).upper()
        if normalized.startswith('BEGIN'):
            self.transaction_sql.append(normalized)
        return self.db.execute(sql, parameters)


class LifecycleTransactionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'base.db'
        self.registry, self.plan, self.node = seed(self.path)
        self.user = self.registry.save('users', {'name': 'lifecycle'})['id']
        self.other = self.registry.save_plan({'name': 'other', 'bandwidth': {'upload_bps': 8192, 'download_bps': None}})['id']
        self.registry.assign_plan(self.user, {'plan_id': self.plan})
        self.registry.authorize_node_user(self.node, self.user)

    def tearDown(self):
        self.temp.cleanup()

    def test_owned_and_borrowed_assignment_and_lifecycle_match(self):
        owned_path = Path(self.temp.name) / 'owned.db'
        borrowed_path = Path(self.temp.name) / 'borrowed.db'
        shutil.copy2(self.path, owned_path)
        shutil.copy2(self.path, borrowed_path)
        owned = Registry(owned_path)
        borrowed = Registry(borrowed_path)
        owned_result = owned.assign_plan(self.user, {'plan_id': self.other})
        owned_life = Lifecycle(owned).manage(self.user, {'enabled': True, 'expires_at': 200.0})
        with borrowed.connect() as raw:
            raw.execute('BEGIN IMMEDIATE')
            proxy = BorrowedConnection(raw)
            borrowed_result = borrowed.assign_plan(self.user, {'plan_id': self.other}, db=proxy)
            borrowed_life = Lifecycle(borrowed).manage(
                self.user, {'enabled': True, 'expires_at': 200.0}, db=proxy)
            self.assertEqual((proxy.commit_calls, proxy.rollback_calls, proxy.close_calls), (0, 0, 0))
            self.assertEqual(proxy.transaction_sql, [])
        self.assertEqual(borrowed_result, owned_result)
        self.assertEqual(borrowed_life, owned_life)
        self.assertEqual(snapshot(borrowed), snapshot(owned))

    def test_caller_rollback_restores_assignment_lifecycle_and_maintenance(self):
        before = snapshot(self.registry)
        with self.registry.connect() as raw:
            raw.execute('BEGIN IMMEDIATE')
            proxy = BorrowedConnection(raw)
            raw.execute('INSERT INTO usage_cycles(id,user_id,starts_at,ends_at) VALUES(?,?,?,?)',
                        ('expired', self.user, 0, 50))
            raw.execute('INSERT INTO plan_lifecycle VALUES(?,?,?,?)',
                        (self.user, 1, 50, 1))
            self.registry.assign_plan(self.user, {'plan_id': self.other}, db=proxy)
            Lifecycle(self.registry).manage(self.user, db=proxy)
            self.assertNotEqual(snapshot_in_transaction(raw), before)
            raw.execute('ROLLBACK')
        self.assertEqual(snapshot(self.registry), before)

    def test_get_lifecycle_refreshes_expiry_and_grace_in_caller_transaction(self):
        with self.registry.connect() as raw:
            raw.execute('BEGIN IMMEDIATE')
            raw.execute('INSERT INTO usage_cycles(id,user_id,starts_at,ends_at) VALUES(?,?,?,?)',
                        ('expired', self.user, 0, 50))
            raw.execute('INSERT INTO plan_lifecycle VALUES(?,?,?,?)',
                        (self.user, 1, 50, 1))
            with mock.patch('plan_lifecycle.time.time', return_value=100.0):
                result = Lifecycle(self.registry).manage(self.user, db=BorrowedConnection(raw))
            self.assertEqual(result['status'], 'expired')
            self.assertEqual(raw.execute('SELECT expired FROM usage_cycles WHERE id=?', ('expired',)).fetchone()[0], 1)
            self.assertIsNotNone(raw.execute('SELECT 1 FROM plan_grace_observations WHERE user_id=?', (self.user,)).fetchone())
            raw.execute('ROLLBACK')

    def test_borrowed_calls_require_active_transaction_and_fk(self):
        raw = sqlite3.connect(self.path)
        raw.row_factory = sqlite3.Row
        raw.execute('PRAGMA foreign_keys=ON')
        try:
            with self.assertRaises(Error) as caught:
                self.registry.assign_plan(self.user, {'plan_id': self.plan}, db=raw)
            self.assertEqual(caught.exception.status, 503)
            with self.assertRaises(Error) as caught:
                Lifecycle(self.registry).manage(self.user, db=raw)
            self.assertEqual(caught.exception.status, 503)
        finally:
            raw.close()
        raw = sqlite3.connect(self.path)
        raw.row_factory = sqlite3.Row
        raw.execute('PRAGMA foreign_keys=OFF')
        raw.execute('BEGIN IMMEDIATE')
        try:
            for call in (
                lambda: self.registry.assign_plan(self.user, {'plan_id': self.plan}, db=raw),
                lambda: Lifecycle(self.registry).manage(self.user, db=raw),
            ):
                with self.assertRaises(Error) as caught:
                    call()
                self.assertEqual(caught.exception.status, 503)
            raw.execute('ROLLBACK')
        finally:
            raw.close()

    def test_default_paths_preserve_begin_immediate(self):
        original = self.registry.connect
        traces = []

        @contextmanager
        def traced_connect():
            with original() as raw:
                proxy = DetailedConnection(raw)
                traces.append(proxy)
                yield proxy

        self.registry.connect = traced_connect
        self.registry.assign_plan(self.user, {'plan_id': self.other})
        self.assertEqual(traces[-1].transaction_sql, ['BEGIN IMMEDIATE'])
        traces.clear()
        Lifecycle(self.registry).manage(self.user, {'enabled': True, 'expires_at': 200.0})
        self.assertEqual(traces[-1].transaction_sql, ['BEGIN IMMEDIATE'])

    def test_publication_failure_keeps_borrowed_transaction_owned_by_caller(self):
        with self.registry.connect() as raw:
            raw.execute('BEGIN IMMEDIATE')
            raw.execute("CREATE TEMP TRIGGER fail_publish BEFORE UPDATE ON server_configs BEGIN SELECT RAISE(ABORT,'fixture'); END")
            proxy = BorrowedConnection(raw)
            with self.assertRaises(sqlite3.IntegrityError):
                self.registry.assign_plan(self.user, {'plan_id': self.other}, db=proxy)
            self.assertTrue(raw.in_transaction)
            self.assertEqual((proxy.commit_calls, proxy.rollback_calls, proxy.close_calls), (0, 0, 0))
            raw.execute('ROLLBACK')


def snapshot_in_transaction(db):
    tables = [row[0] for row in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
    return {table: [tuple(row) for row in db.execute(
        'SELECT * FROM "' + table.replace('"', '""') + '" ORDER BY rowid')] for table in tables}


if __name__ == '__main__':
    unittest.main()
