"""Caller-owned transactions for user/server bandwidth and connection policies."""
from contextlib import contextmanager
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest
from unittest import mock

from core import Error, Registry
from test_plan_transaction import BorrowedConnection, snapshot, snapshot_in_transaction
import test_xray


RATES = {'upload_bps': 32768, 'download_bps': None}
AUTO = {
    'enabled': True,
    'threshold_bps': 65536,
    'sustain_seconds': 2,
    'download_bps': 16384,
    'duration_seconds': 5,
}


class DetailedConnection(BorrowedConnection):
    def execute(self, sql, parameters=()):
        normalized = ' '.join(sql.strip().split()).upper()
        if normalized.startswith('BEGIN'):
            self.transaction_sql.append(normalized)
            return self.db.execute(sql, parameters)
        return self.db.execute(sql, parameters)


def seed(path):
    registry = Registry(path)
    server = registry.save('servers', {'name': 'host'})['id']
    node = registry.save('nodes', {'name': 'node', 'server_id': server})['id']
    registry.configure_node(node, test_xray.VLESS)
    user = registry.save('users', {'name': 'user'})['id']
    registry.authorize_node_user(node, user)
    return registry, server, user


def exercise(registry, server, user, *, db=None):
    automatic = registry.automatic_bandwidth(user, AUTO, db=db)
    policy = registry.bandwidth_policy(user, db=db)
    bandwidth = registry.user_bandwidth(
        user, {'upload_bps': None, 'download_bps': 8192}, db=db)
    connections_policy = registry.connections_policy(user, db=db)
    connections = registry.user_connections(user, {'max_connections': None}, db=db)
    temporary = registry.temporary_bandwidth(
        user, {'bandwidth': RATES, 'expires_at': 200.0}, db=db)
    server_limits = registry.server_bandwidth(server, RATES, db=db)
    return (automatic, policy, bandwidth, connections_policy, connections,
            temporary, server_limits)


class PolicyTransactionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'base.db'
        self.registry, self.server, self.user = seed(self.path)

    def tearDown(self):
        self.temp.cleanup()

    def test_owned_and_borrowed_paths_have_identical_results_and_state(self):
        owned_path = Path(self.temp.name) / 'owned.db'
        borrowed_path = Path(self.temp.name) / 'borrowed.db'
        shutil.copy2(self.path, owned_path)
        shutil.copy2(self.path, borrowed_path)
        owned = Registry(owned_path)
        borrowed = Registry(borrowed_path)
        with mock.patch('core.time.time', return_value=100.0):
            owned_result = exercise(owned, self.server, self.user)
            with borrowed.connect() as raw:
                raw.execute('BEGIN IMMEDIATE')
                proxy = BorrowedConnection(raw)
                borrowed_result = exercise(
                    borrowed, self.server, self.user, db=proxy)
                self.assertTrue(raw.in_transaction)
                self.assertEqual(
                    (proxy.commit_calls, proxy.rollback_calls, proxy.close_calls),
                    (0, 0, 0))
                self.assertEqual(proxy.transaction_sql, [])
        self.assertEqual(borrowed_result, owned_result)
        self.assertEqual(snapshot(borrowed), snapshot(owned))

    def test_caller_rollback_restores_all_policy_rows_and_revisions(self):
        before = snapshot(self.registry)
        with mock.patch('core.time.time', return_value=100.0):
            with self.registry.connect() as raw:
                raw.execute('BEGIN IMMEDIATE')
                proxy = BorrowedConnection(raw)
                exercise(self.registry, self.server, self.user, db=proxy)
                self.assertTrue(raw.in_transaction)
                self.assertNotEqual(snapshot_in_transaction(raw), before)
                self.assertEqual(
                    (proxy.commit_calls, proxy.rollback_calls, proxy.close_calls),
                    (0, 0, 0))
                self.assertEqual(proxy.transaction_sql, [])
                raw.execute('ROLLBACK')
        self.assertEqual(snapshot(self.registry), before)

    def test_borrowed_calls_require_active_transaction_and_foreign_keys(self):
        operations = (
            lambda db: self.registry.automatic_bandwidth(self.user, db=db),
            lambda db: self.registry.bandwidth_policy(self.user, db=db),
            lambda db: self.registry.user_bandwidth(self.user, db=db),
            lambda db: self.registry.connections_policy(self.user, db=db),
            lambda db: self.registry.user_connections(self.user, db=db),
            lambda db: self.registry.temporary_bandwidth(self.user, db=db),
            lambda db: self.registry.server_bandwidth(self.server, db=db),
        )
        raw = sqlite3.connect(self.path)
        raw.row_factory = sqlite3.Row
        raw.execute('PRAGMA foreign_keys=ON')
        try:
            for operation in operations:
                with self.subTest(boundary='no_transaction', operation=operation), self.assertRaises(Error) as caught:
                    operation(raw)
                self.assertEqual(caught.exception.status, 503)
        finally:
            raw.close()
        raw = sqlite3.connect(self.path)
        raw.row_factory = sqlite3.Row
        raw.execute('PRAGMA foreign_keys=OFF')
        raw.execute('BEGIN IMMEDIATE')
        try:
            for operation in operations:
                with self.subTest(boundary='foreign_keys_off', operation=operation), self.assertRaises(Error) as caught:
                    operation(raw)
                self.assertEqual(caught.exception.status, 503)
            raw.execute('ROLLBACK')
        finally:
            raw.close()

    def test_default_paths_preserve_original_begin_modes(self):
        original = self.registry.connect
        traces = []

        @contextmanager
        def traced_connect():
            with original() as raw:
                proxy = DetailedConnection(raw)
                traces.append(proxy)
                yield proxy

        self.registry.connect = traced_connect
        cases = (
            (lambda: self.registry.automatic_bandwidth(self.user), ['BEGIN IMMEDIATE']),
            (lambda: self.registry.bandwidth_policy(self.user), ['BEGIN IMMEDIATE']),
            (lambda: self.registry.user_bandwidth(self.user), ['BEGIN IMMEDIATE']),
            (lambda: self.registry.connections_policy(self.user), []),
            (lambda: self.registry.user_connections(self.user), ['BEGIN IMMEDIATE']),
            (lambda: self.registry.temporary_bandwidth(self.user), ['BEGIN IMMEDIATE']),
            (lambda: self.registry.server_bandwidth(self.server), ['BEGIN']),
            (lambda: self.registry.server_bandwidth(self.server, RATES), ['BEGIN IMMEDIATE']),
        )
        for operation, expected in cases:
            traces.clear()
            operation()
            self.assertEqual(len(traces), 1)
            self.assertEqual(traces[0].transaction_sql, expected)

    def test_get_expiry_maintenance_and_publication_share_caller_transaction(self):
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('INSERT INTO temporary_bandwidth VALUES(?,?,?)',
                       (self.user, '{"download_bps":null,"upload_bps":8192}', 50.0))
        before = snapshot(self.registry)
        with mock.patch('core.time.time', return_value=100.0):
            with self.registry.connect() as raw:
                raw.execute('BEGIN IMMEDIATE')
                revision = raw.execute(
                    'SELECT revision FROM server_configs WHERE server_id=?',
                    (self.server,)).fetchone()[0]
                result = self.registry.bandwidth_policy(
                    self.user, db=BorrowedConnection(raw))
                self.assertEqual(result['source'], 'default')
                self.assertIsNone(raw.execute(
                    'SELECT 1 FROM temporary_bandwidth WHERE user_id=?',
                    (self.user,)).fetchone())
                self.assertEqual(raw.execute(
                    'SELECT revision FROM server_configs WHERE server_id=?',
                    (self.server,)).fetchone()[0], revision + 1)
                raw.execute('ROLLBACK')
        self.assertEqual(snapshot(self.registry), before)

    def test_publication_failure_does_not_take_ownership_of_borrowed_transaction(self):
        before = snapshot(self.registry)
        with self.registry.connect() as raw:
            raw.execute('BEGIN IMMEDIATE')
            raw.execute("CREATE TEMP TRIGGER fail_policy_publish BEFORE UPDATE ON server_configs "
                        "BEGIN SELECT RAISE(ABORT,'fixture'); END")
            proxy = BorrowedConnection(raw)
            with self.assertRaises(sqlite3.IntegrityError):
                self.registry.user_bandwidth(self.user, RATES, db=proxy)
            self.assertTrue(raw.in_transaction)
            self.assertEqual(
                (proxy.commit_calls, proxy.rollback_calls, proxy.close_calls),
                (0, 0, 0))
            self.assertEqual(proxy.transaction_sql, [])
            raw.execute('ROLLBACK')
        self.assertEqual(snapshot(self.registry), before)
