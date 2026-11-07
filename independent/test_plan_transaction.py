"""Caller-owned transaction contract for plan CRUD, scopes, and node policies."""
import json
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest

from core import Error, Registry
from plan_node_bandwidth import PlanNodeBandwidth
from plan_node_connections import PlanNodeConnections
from plan_nodes import PlanNodes
from test_plan_node_bandwidth import rates
from test_plan_node_connections import limit
import test_xray


class BorrowedConnection:
    """Expose transaction state while detecting ownership violations."""
    def __init__(self, db):
        self.db = db
        self.commit_calls = self.rollback_calls = self.close_calls = 0
        self.transaction_sql = []

    @property
    def in_transaction(self):
        return self.db.in_transaction

    def execute(self, sql, parameters=()):
        command = sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ''
        if command in ('BEGIN', 'COMMIT', 'ROLLBACK'):
            self.transaction_sql.append(command)
        return self.db.execute(sql, parameters)

    def executemany(self, sql, parameters):
        return self.db.executemany(sql, parameters)

    def commit(self):
        self.commit_calls += 1
        return self.db.commit()

    def rollback(self):
        self.rollback_calls += 1
        return self.db.rollback()

    def close(self):
        self.close_calls += 1
        return self.db.close()


def snapshot(registry):
    with registry.connect() as db:
        tables = [row[0] for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
        return {table: [tuple(row) for row in db.execute(
            'SELECT * FROM "' + table.replace('"', '""') + '" ORDER BY rowid')]
                for table in tables}


def seed(path):
    registry = Registry(path)
    server = registry.save('servers', {'name': 'host'})['id']
    node = registry.save('nodes', {'name': 'node', 'server_id': server})['id']
    registry.configure_node(node, test_xray.VLESS)
    user = registry.save('users', {'name': 'user'})['id']
    plan = registry.save_plan({
        'name': 'base',
        'bandwidth': rates(32768),
        'connections': {'max_connections': 3, 'max_ips': 2},
    })['id']
    registry.assign_plan(user, {'plan_id': plan})
    registry.authorize_node_user(node, user)
    return registry, plan, node


def exercise(registry, plan, node, *, db=None):
    current = registry.plans(versioned=True, db=db)[0]
    updated = registry.save_plan({
        'name': 'updated',
        'bandwidth': rates(16384),
        'connections': {'max_connections': 2, 'max_ips': 1},
    }, plan, if_match=current['version'], versioned=True, db=db)
    scope = PlanNodes(registry)
    restricted = scope.save(plan, {'node_ids': [node]}, db=db)
    read_scope = scope.get(plan, db=db)
    connections = PlanNodeConnections(registry).manage(plan, node, limit(None), db=db)
    bandwidth = PlanNodeBandwidth(registry)
    initial = bandwidth.manage(plan, node, versioned=True, db=db)
    limited = bandwidth.manage(
        plan, node, rates(8192), if_match=initial['version'], versioned=True, db=db)
    cleared = scope.clear(plan, db=db)
    disposable = registry.save_plan({'name': 'disposable', 'bandwidth': rates()},
                                    versioned=True, db=db)
    registry.delete_plan(disposable['id'], if_match=disposable['version'], db=db)
    return updated, restricted, read_scope, connections, limited, cleared


class PlanTransactionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'base.db'
        self.registry, self.plan, self.node = seed(self.path)

    def tearDown(self):
        self.temp.cleanup()

    def test_owned_and_borrowed_paths_have_identical_results_and_state(self):
        own_path = Path(self.temp.name) / 'owned.db'
        borrowed_path = Path(self.temp.name) / 'borrowed.db'
        shutil.copy2(self.path, own_path)
        shutil.copy2(self.path, borrowed_path)
        owned = Registry(own_path)
        borrowed = Registry(borrowed_path)

        owned_result = exercise(owned, self.plan, self.node)
        with borrowed.connect() as raw:
            raw.execute('BEGIN IMMEDIATE')
            proxy = BorrowedConnection(raw)
            borrowed_result = exercise(borrowed, self.plan, self.node, db=proxy)
            self.assertTrue(raw.in_transaction)
            self.assertEqual((proxy.commit_calls, proxy.rollback_calls, proxy.close_calls), (0, 0, 0))
            self.assertEqual(proxy.transaction_sql, [])

        self.assertEqual(borrowed_result, owned_result)
        self.assertEqual(snapshot(borrowed), snapshot(owned))

    def test_caller_rollback_restores_every_table_and_revision(self):
        before = snapshot(self.registry)
        with self.registry.connect() as raw:
            raw.execute('BEGIN IMMEDIATE')
            proxy = BorrowedConnection(raw)
            exercise(self.registry, self.plan, self.node, db=proxy)
            self.assertTrue(raw.in_transaction)
            self.assertNotEqual(snapshot_in_transaction(raw), before)
            self.assertEqual((proxy.commit_calls, proxy.rollback_calls, proxy.close_calls), (0, 0, 0))
            self.assertEqual(proxy.transaction_sql, [])
            raw.execute('ROLLBACK')
        self.assertEqual(snapshot(self.registry), before)

    def test_borrowed_calls_reject_missing_transaction_or_foreign_keys(self):
        operations = (
            lambda db: self.registry.plans(db=db),
            lambda db: self.registry.save_plan({'name': 'new', 'bandwidth': rates()}, db=db),
            lambda db: self.registry.delete_plan(self.plan, db=db),
            lambda db: PlanNodes(self.registry).get(self.plan, db=db),
            lambda db: PlanNodes(self.registry).save(self.plan, {'node_ids': [self.node]}, db=db),
            lambda db: PlanNodes(self.registry).clear(self.plan, db=db),
            lambda db: PlanNodeConnections(self.registry).manage(self.plan, self.node, db=db),
            lambda db: PlanNodeBandwidth(self.registry).manage(self.plan, self.node, db=db),
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

    def test_publication_failure_leaves_borrowed_transaction_owned_by_caller(self):
        before = snapshot(self.registry)
        with self.registry.connect() as raw:
            raw.execute('BEGIN IMMEDIATE')
            raw.execute("CREATE TEMP TRIGGER fail_plan_publish BEFORE UPDATE ON server_configs "
                        "BEGIN SELECT RAISE(ABORT,'fixture'); END")
            proxy = BorrowedConnection(raw)
            with self.assertRaises(sqlite3.IntegrityError):
                PlanNodeBandwidth(self.registry).manage(
                    self.plan, self.node, rates(1), db=proxy)
            self.assertTrue(raw.in_transaction)
            self.assertEqual((proxy.commit_calls, proxy.rollback_calls, proxy.close_calls), (0, 0, 0))
            self.assertEqual(proxy.transaction_sql, [])
            raw.execute('ROLLBACK')
        self.assertEqual(snapshot(self.registry), before)


def snapshot_in_transaction(db):
    tables = [row[0] for row in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
    return {table: [tuple(row) for row in db.execute(
        'SELECT * FROM "' + table.replace('"', '""') + '" ORDER BY rowid')]
            for table in tables}

