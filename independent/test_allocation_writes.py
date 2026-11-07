"""Allocation-local transport evidence; never master-confirmed traffic."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import sys
import unittest
from unittest.mock import Mock, patch

from allocation_quota import AllocationQuota
from payload_meter import MeteredWriter, QuotaExceeded
import test_allocation_quota as allocation_base


class AllocationWriteTests(unittest.TestCase):
    setUp = allocation_base.AllocationQuotaTests.setUp
    quota = allocation_base.AllocationQuotaTests.quota
    meter = allocation_base.AllocationQuotaTests.meter

    def test_real_duplex_success_matches_meter_and_preserves_cap(self):
        class Destination:
            def __init__(self, sock): self.sock = sock
            def write(self, data): self.sock.sendall(data)
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0)); listener.listen()
            with socket.create_connection(listener.getsockname(), timeout=2) as client:
                peer, _ = listener.accept()
                with peer:
                    peer.settimeout(2); meter = self.meter()
                    MeteredWriter(Destination(client), meter, self.key, 0, self.quota()).write(b'hello')
                    self.assertEqual(peer.recv(10), b'hello')
                    with self.assertRaises(QuotaExceeded):
                        MeteredWriter(Destination(peer), meter, self.key, 1, self.quota()).write(b'world')
                    self.assertEqual(client.recv(10), b'wo')
        snap = self.quota().snapshot()
        self.assertEqual((snap['successful_upload_bytes'], snap['successful_download_bytes']), (5, 2))
        self.assertEqual((snap['unknown_upload_bytes'], snap['unknown_download_bytes']), (0, 0))
        self.assertEqual(snap['remaining_bytes'], 0)
        self.assertEqual(meter.snapshot()['items'][0]['upload_bytes'], 5)
        self.assertFalse(snap['ready_for_execution'])

    def test_partial_transport_error_is_unknown_and_fences_writer(self):
        target = Mock(); wire = bytearray()
        def partial(data):
            wire.extend(data[:2]); raise OSError('partial queue')
        target.write.side_effect = partial
        meter = self.meter(); quota = self.quota()
        writer = MeteredWriter(target, meter, self.key, 0, quota)
        with self.assertRaises(OSError): writer.write(b'hello')
        with self.assertRaises(OSError): writer.write(b'x')
        self.assertEqual(wire, b'he'); self.assertEqual(target.write.call_count, 1)
        self.assertEqual(meter.snapshot()['items'], [])
        with sqlite3.connect(self.path) as db:
            ticket, = db.execute('SELECT ticket FROM allocation_writes').fetchone()
        with self.assertRaises(ValueError): quota.complete_write(ticket)
        snap = self.quota().snapshot()
        self.assertEqual((snap['successful_upload_bytes'], snap['unknown_upload_bytes'], snap['remaining_bytes']), (0, 5, 2))

    def test_meter_intent_failure_keeps_allocation_pending_without_transport(self):
        meter = self.meter(); target = Mock()
        def fail(*args): raise OSError('intent disk failure')
        meter.write_intent = fail
        with self.assertRaises(OSError):
            MeteredWriter(target, meter, self.key, 0, self.quota()).write(b'hello')
        target.write.assert_not_called()
        self.assertTrue(meter.failed)
        self.assertEqual(self.quota().snapshot()['unknown_upload_bytes'], 5)

    def test_completion_disk_failure_keeps_unknown_and_cannot_recover_as_success(self):
        quota = self.quota(); target = Mock(); meter = self.meter()
        with sqlite3.connect(self.path) as db:
            db.execute("CREATE TRIGGER reject_complete BEFORE UPDATE ON allocation_writes BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(OSError):
            MeteredWriter(target, meter, self.key, 0, quota).write(b'hello')
        target.write.assert_called_once_with(b'hello'); self.assertTrue(meter.failed)
        self.assertEqual(meter.snapshot()['items'], [])
        with sqlite3.connect(self.path) as db:
            ticket, = db.execute('SELECT ticket FROM allocation_writes').fetchone()
            db.execute('DROP TRIGGER reject_complete')
        recovered = self.quota()
        with self.assertRaises(ValueError): recovered.complete_write(ticket)
        with self.assertRaises(ValueError): quota.complete_write(ticket)
        self.assertEqual(recovered.snapshot()['unknown_upload_bytes'], 5)

    def test_expiry_stops_new_intents_but_not_existing_success_commit(self):
        quota = self.quota(); allowed, ticket = quota.reserve_write(0, 5)
        self.assertEqual(allowed, 5)
        with patch('allocation_quota.time.time', return_value=self.scope['valid_until']+10):
            self.assertEqual(quota.reserve_write(1, 2), (0, None))
            quota.complete_write(ticket)
        snap = quota.snapshot()
        self.assertEqual((snap['successful_upload_bytes'], snap['unknown_upload_bytes'], snap['remaining_bytes']), (5, 0, 2))

    def test_completion_is_idempotent_bound_and_legacy_reservations_stay_unknown(self):
        quota = self.quota(); quota.reserve(0, 2)
        allowed, ticket = quota.reserve_write(1, 3)
        self.assertEqual(allowed, 3)
        with self.assertRaises(ValueError): self.quota().complete_write(ticket)
        other = self.quota(allocation_id='other')
        with self.assertRaises(ValueError): other.complete_write(ticket)
        with self.assertRaises(ValueError): quota.complete_write('0'*32)
        quota.complete_write(ticket); quota.complete_write(ticket)
        self.quota().complete_write(ticket)  # Only already committed success is idempotent.
        snap = self.quota().snapshot()
        self.assertEqual((snap['successful_download_bytes'], snap['unknown_upload_bytes'], snap['unattributed_upload_bytes']), (3, 2, 2))

    def test_intent_insert_failure_rolls_back_reservation(self):
        quota = self.quota(); target = Mock()
        with sqlite3.connect(self.path) as db:
            db.execute("CREATE TRIGGER reject_intent BEFORE INSERT ON allocation_writes BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(OSError):
            MeteredWriter(target, self.meter(), self.key, 0, quota).write(b'hello')
        target.write.assert_not_called()
        self.assertEqual(self.quota().snapshot()['remaining_bytes'], 7)

    def test_concurrent_write_intents_are_atomic_and_share_cap(self):
        self.quota()
        def run(direction):
            try:
                quota = self.quota(); allowed, ticket = quota.reserve_write(direction, 7)
                if ticket: quota.complete_write(ticket)
                return allowed
            except OSError as error:
                self.assertIsInstance(error.__cause__, sqlite3.OperationalError)
                self.assertIn('locked', str(error.__cause__))
                return 0
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(run, (0, 1)))
        snap = self.quota().snapshot()
        self.assertEqual(sum(results), 7)
        self.assertEqual(snap['successful_upload_bytes']+snap['successful_download_bytes'], 7)
        self.assertEqual(snap['unknown_upload_bytes']+snap['unknown_download_bytes'], 0)

    def test_actual_sigkill_before_transport_after_transport_and_after_commit(self):
        script = '''
import json, os, signal, socket, sys
from allocation_quota import AllocationQuota
from payload_meter import MeteredWriter, PayloadMeter
path, scope, port, stage = sys.argv[1:]
quota = AllocationQuota(path, json.loads(scope))
meter = PayloadMeter(); meter.bind(1)
def die(): os.kill(os.getpid(), signal.SIGKILL)
if stage == 'reserved':
    original = quota.reserve_write
    def reserve(*args):
        result = original(*args); die(); return result
    quota.reserve_write = reserve
if stage == 'committed':
    original = quota.complete_write
    def complete(*args):
        original(*args); die()
    quota.complete_write = complete
with socket.create_connection(('127.0.0.1', int(port)), timeout=2) as sock:
    class Destination:
        def write(self, data):
            sock.sendall(data)
            if stage == 'transport': die()
    MeteredWriter(Destination(), meter, (2,3,'identity'), 0, quota).write(b'hello')
'''
        for stage in ('reserved', 'transport', 'committed'):
            with self.subTest(stage=stage), socket.socket() as listener:
                listener.bind(('127.0.0.1', 0)); listener.listen(); listener.settimeout(5)
                path = Path(self.temp.name)/(stage+'.sqlite')
                proc = subprocess.Popen([sys.executable, '-c', script, str(path), json.dumps(self.scope),
                                         str(listener.getsockname()[1]), stage],
                                        cwd=Path(__file__).resolve().parent, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    out, err = proc.communicate(timeout=5)
                finally:
                    if proc.poll() is None: proc.kill(); proc.communicate()
                self.assertEqual(proc.returncode, -signal.SIGKILL, (out, err))
                peer, _ = listener.accept()
                with peer:
                    peer.settimeout(2); wire = bytearray()
                    while True:
                        data = peer.recv(10)
                        if not data: break
                        wire.extend(data)
                self.assertEqual(wire, b'' if stage == 'reserved' else b'hello')
                recovered = AllocationQuota(path, self.scope)
                snap = recovered.snapshot()
                self.assertEqual(snap['remaining_bytes'], 2)
                self.assertEqual(snap['successful_upload_bytes'], 5 if stage == 'committed' else 0)
                self.assertEqual(snap['unknown_upload_bytes'], 0 if stage == 'committed' else 5)
                with sqlite3.connect(path) as db:
                    ticket, = db.execute('SELECT ticket FROM allocation_writes').fetchone()
                if stage != 'committed':
                    with self.assertRaises(ValueError): recovered.complete_write(ticket)


if __name__ == '__main__': unittest.main()
