from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

from allocation_quota import AllocationQuota
from payload_meter import MeteredWriter, PayloadMeter, QuotaExceeded
import test_total_allocation as allocation_base


class AllocationQuotaTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)/'allocation.sqlite'
        self.scope = dict(allocation_id='allocation', policy_id=1, cycle_id='cycle',
                          server_id=1, session_id='session', node_id=2, user_id=3,
                          credential='identity', reserved_bytes=7,
                          issued_at=time.time()-10, valid_until=time.time()+3600)
        self.key = (2, 3, 'identity')

    def quota(self, **change):
        return AllocationQuota(self.path, {**self.scope, **change})

    def meter(self):
        meter = PayloadMeter(); meter.bind(1)
        return meter

    def test_real_tcp_duplex_uses_one_persistent_allocation(self):
        class Destination:
            def __init__(self, connection): self.connection = connection
            def write(self, data): self.connection.sendall(data)
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0)); listener.listen()
            with socket.create_connection(listener.getsockname(), timeout=2) as client:
                peer, _ = listener.accept()
                with peer:
                    peer.settimeout(2)
                    meter = self.meter()
                    MeteredWriter(Destination(client), meter, self.key, 0, self.quota()).write(b'hello')
                    self.assertEqual(peer.recv(20), b'hello')
                    with self.assertRaises(QuotaExceeded):
                        MeteredWriter(Destination(peer), meter, self.key, 1, self.quota()).write(b'world')
                    self.assertEqual(client.recv(20), b'wo')
                    self.assertEqual(meter.snapshot()['items'][0]['upload_bytes'], 5)
                    self.assertEqual(meter.snapshot()['items'][0]['download_bytes'], 2)
        recovered = self.quota().snapshot()
        self.assertEqual((recovered['reserved_upload_bytes'], recovered['reserved_download_bytes'], recovered['remaining_bytes']), (5, 2, 0))
        self.assertFalse(recovered['ready_for_execution'])

    def test_immutable_scope_and_writer_identity(self):
        quota = self.quota(); self.scope['reserved_bytes'] = 100
        self.assertEqual(quota.reserve(0, 100), 7)
        for change in (dict(policy_id=2), dict(session_id='next'), dict(node_id=9),
                       dict(cycle_id='next'), dict(credential='next'), dict(reserved_bytes=8),
                       dict(valid_until=self.scope['valid_until']+1), dict(server_id=2)):
            with self.subTest(change=change), self.assertRaises(ValueError): self.quota(**change)
        for key in ((9, 3, 'identity'), (2, 4, 'identity'), (2, 3, 'other')):
            with self.subTest(key=key), self.assertRaises(ValueError):
                MeteredWriter(Mock(), self.meter(), key, 0, quota)
        meter = PayloadMeter(); meter.bind(2)
        with self.assertRaises(ValueError): MeteredWriter(Mock(), meter, self.key, 0, quota)

    def test_expiry_and_future_issue_never_refund_high_water(self):
        quota = self.quota(); quota.reserve(1, 3)
        for now in (self.scope['issued_at']-1, self.scope['valid_until'], self.scope['valid_until']+1):
            with patch('allocation_quota.time.time', return_value=now):
                self.assertEqual(self.quota().reserve(0, 7), 0)
        self.assertEqual(self.quota().reserve(0, 7), 4)

    def test_failed_write_keeps_uncertain_reservation_separate(self):
        meter = self.meter(); target = Mock(); target.write.side_effect = OSError('partial write')
        with self.assertRaises(OSError):
            MeteredWriter(target, meter, self.key, 0, self.quota()).write(b'hello')
        self.assertEqual(meter.snapshot()['items'], [])
        self.assertEqual(self.quota().snapshot()['remaining_bytes'], 2)

    def test_storage_lock_prevents_payload_and_rolls_back(self):
        quota = self.quota(); target = Mock(); meter = self.meter()
        with sqlite3.connect(self.path) as lock:
            lock.execute('BEGIN IMMEDIATE')
            with self.assertRaises(OSError):
                MeteredWriter(target, meter, self.key, 0, quota).write(b'hello')
            lock.rollback()
        target.write.assert_not_called()
        self.assertEqual(self.quota().snapshot()['remaining_bytes'], 7)
        with sqlite3.connect(self.path) as db:
            db.execute("CREATE TRIGGER fail_reserve BEFORE INSERT ON allocation_quota BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(OSError): quota.reserve(0, 5)
        with sqlite3.connect(self.path) as db:
            db.execute('DROP TRIGGER fail_reserve')
        self.assertEqual(self.quota().snapshot()['remaining_bytes'], 7)

    def test_concurrent_adapters_share_both_direction_high_waters(self):
        self.quota()
        def reserve(direction):
            try: return self.quota().reserve(direction, 2)
            except OSError as error:
                # Contention is fail-closed; only a real SQLite lock is allowed.
                self.assertIsInstance(error.__cause__, sqlite3.OperationalError)
                self.assertIn('locked', str(error.__cause__))
                return 0
        with ThreadPoolExecutor(4) as pool:
            total = sum(pool.map(reserve, [0, 1]*10))
        state = self.quota().snapshot()
        self.assertEqual(total, 7)
        self.assertEqual(state['reserved_upload_bytes']+state['reserved_download_bytes'], 7)

    def test_sigkill_after_commit_does_not_reissue_bytes(self):
        script = '''import json,os,signal,sys
from allocation_quota import AllocationQuota
q=AllocationQuota(sys.argv[1],json.loads(sys.argv[2]))
assert q.reserve(0,5)==5
os.kill(os.getpid(),signal.SIGKILL)
'''
        proc = subprocess.run([sys.executable, '-c', script, str(self.path), json.dumps(self.scope)],
                              cwd=Path(__file__).parent, capture_output=True, text=True, timeout=10)
        self.assertEqual(proc.returncode, -signal.SIGKILL, proc.stderr)
        self.assertEqual(self.quota().reserve(1, 7), 2)

    def test_invalid_scope_and_counts_fail_without_storage(self):
        for change in (dict(reserved_bytes=True), dict(policy_id=0), dict(valid_until=float('nan')),
                       dict(valid_until=10**1000), dict(issued_at=True),
                       dict(issued_at=self.scope['valid_until']), dict(session_id=''), dict(extra=1)):
            with self.subTest(change=change), self.assertRaises(ValueError): self.quota(**change)
        self.assertFalse(self.path.exists())
        quota = self.quota()
        for direction, count in ((True, 1), (2, 1), (0, -1), (0, True), (0, 2**63)):
            with self.subTest(direction=direction, count=count), self.assertRaises(ValueError):
                quota.reserve(direction, count)


class MasterAllocationAdapterTests(unittest.TestCase):
    setUp = allocation_base.AllocationTests.setUp
    tearDown = allocation_base.AllocationTests.tearDown
    fixtures = allocation_base.AllocationTests.fixtures
    fixture = allocation_base.AllocationTests.fixture
    setup_total = allocation_base.AllocationTests.setup_total
    second_host = allocation_base.AllocationTests.second_host
    request = allocation_base.AllocationTests.request

    def test_two_master_chunks_keep_separate_local_limits_without_confirming_usage(self):
        from allocation_quota import FIELDS
        from total_allocation import TotalAllocations, held
        from total_usage import current
        r = self.setup_total(7); api = TotalAllocations(r)
        first = api.reserve(self.token, self.request(6))
        token, other = self.second_host(r)
        request = {**self.request(6), **{k:other['items'][0][k] for k in ('node_id', 'credential')},
                   'session_id':other['session_id']}
        second = api.reserve(token, request)
        with tempfile.TemporaryDirectory() as directory:
            for allocation, expected in ((first, 6), (second, 1)):
                path = Path(directory)/str(allocation['server_id'])
                quota = AllocationQuota(path, {k:allocation[k] for k in FIELDS})
                # Both directions spend only this master's finite chunk.
                self.assertEqual(quota.reserve(0, expected-1), expected-1)
                self.assertEqual(quota.reserve(1, 10), 1)
                self.assertEqual(AllocationQuota(path, {k:allocation[k] for k in FIELDS}).reserve(0, 1), 0)
        with r.connect() as db:
            db.execute('BEGIN')
            self.assertEqual(held(db, self.period), 7)
            self.assertEqual(current(db, self.period), 0)
            self.assertEqual(db.execute('SELECT count(*) FROM usage_total_allocation_reports').fetchone()[0], 0)
        self.assertFalse(first['ready_for_execution'])


if __name__ == '__main__': unittest.main()
