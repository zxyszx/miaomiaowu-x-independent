"""Real HTTP retry boundaries; delivery remains evidence-only."""
from concurrent.futures import ThreadPoolExecutor
import sqlite3
import unittest

from agent import request as send
from allocation_delivery import AllocationDelivery
from allocation_outbox import acknowledge
from allocation_quota import AllocationQuota
from total_usage import current
import test_allocation_http as http


class AllocationDeliveryTests(unittest.TestCase):
    setUp = http.EvidenceHTTPTests.setUp
    tearDown = http.EvidenceHTTPTests.tearDown
    fixtures = http.EvidenceHTTPTests.fixtures
    fixture = http.EvidenceHTTPTests.fixture
    setup_total = http.EvidenceHTTPTests.setup_total
    request = http.EvidenceHTTPTests.request
    reserved = http.EvidenceHTTPTests.reserved
    prepare = http.EvidenceHTTPTests.prepare
    count = http.EvidenceHTTPTests.count
    start = http.EvidenceHTTPTests.start

    def worker(self):
        return AllocationDelivery(AllocationQuota(self.local, self.scope))

    def unchanged(self):
        with self.r.connect() as db:
            db.execute('BEGIN')
            self.assertEqual(current(db, self.period), 0)
            self.assertEqual(db.execute('SELECT count(*) FROM usage_deltas').fetchone()[0], 0)
        self.assertEqual(self.reserved(self.r), 7)

    def test_lost_response_restart_keeps_original_body_and_backoff(self):
        self.start()
        _, ticket = self.quota.reserve_write(0, 5)
        frozen = self.quota.freeze_evidence()
        worker = self.worker()
        calls = []
        def lose(*args):
            calls.append(args[3]); send(*args)
            raise OSError('response lost after master commit')
        self.assertEqual(worker.step(self.url, self.token, 100, transport=lose), 'retry')
        self.quota.complete_write(ticket)
        worker = self.worker()
        self.assertEqual(worker.step(self.url, self.token, 114, transport=lose), 'deferred')
        self.assertEqual(len(calls), 1)
        self.assertEqual(worker.step(self.url, self.token, 115), 'accepted')
        self.assertEqual(calls, [frozen]); self.assertEqual(self.count(), 1)
        self.assertEqual(worker.step(self.url, self.token, 116), 'idle')
        latest = self.quota.freeze_evidence()
        self.assertEqual(latest['sequence'], 2)
        self.assertEqual(latest['successful_upload_bytes'], 5)
        self.assertEqual(worker.step(self.url, self.token, 116), 'accepted')
        self.assertEqual(self.count(), 2); self.unchanged()

    def test_auth_revocation_stays_blocked_after_reopen(self):
        self.start(); frozen = self.quota.freeze_evidence()
        worker = self.worker()
        self.assertEqual(worker.step(self.url, 'invalid', 100), 'blocked')
        self.assertEqual(self.worker().step(self.url, self.token, 1000), 'blocked')
        self.assertEqual(self.quota.freeze_evidence(), frozen)
        self.assertEqual(self.count(), 0); self.unchanged()

    def test_bad_receipt_preserves_pending_and_backoff(self):
        self.start(); frozen = self.quota.freeze_evidence()
        def bad(*args):
            return dict(send(*args), sequence=True)
        self.assertEqual(self.worker().step(self.url, self.token, 100, transport=bad), 'retry')
        self.assertEqual(self.quota.freeze_evidence(), frozen)
        self.assertEqual(self.worker().step(self.url, self.token, 114), 'deferred')
        self.assertEqual(self.worker().step(self.url, self.token, 115), 'accepted')
        self.assertEqual(self.count(), 1); self.unchanged()

    def test_crash_after_claim_retains_backoff_before_network(self):
        self.start(); frozen = self.quota.freeze_evidence()
        def crash(*args):
            raise SystemExit('simulate interruption before request')
        with self.assertRaises(SystemExit):
            self.worker().step(self.url, self.token, 100, transport=crash)
        self.assertEqual(self.worker().step(self.url, self.token, 114), 'deferred')
        self.assertEqual(self.quota.freeze_evidence(), frozen)
        self.assertEqual(self.count(), 0)
        self.assertEqual(self.worker().step(self.url, self.token, 115), 'accepted')
        self.unchanged()

    def test_claim_storage_failure_makes_no_request(self):
        self.start(); self.quota.freeze_evidence(); worker = self.worker()
        with sqlite3.connect(self.local) as db:
            db.execute("CREATE TRIGGER reject_delivery BEFORE INSERT ON allocation_evidence_delivery BEGIN SELECT RAISE(ABORT,'disk failure'); END")
        calls = []
        with self.assertRaises(OSError):
            worker.step(self.url, self.token, 100, transport=lambda *args: calls.append(args))
        self.assertEqual(calls, []); self.assertEqual(self.count(), 0)
        with sqlite3.connect(self.local) as db: db.execute('DROP TRIGGER reject_delivery')
        self.assertEqual(worker.step(self.url, self.token, 100), 'accepted'); self.unchanged()

    def test_concurrent_steps_share_claim_and_do_not_duplicate_network(self):
        self.start(); self.quota.freeze_evidence(); worker = self.worker()
        calls = []
        def transport(*args):
            calls.append(args[3]); return send(*args)
        with ThreadPoolExecutor(2) as pool:
            results = list(pool.map(lambda _: worker.step(self.url, self.token, 100, transport=transport), range(2)))
        self.assertEqual(results.count('accepted'), 1)
        self.assertEqual(len(calls), 1)
        self.assertTrue(set(results) <= {'accepted', 'deferred', 'idle'})
        self.assertEqual(self.count(), 1); self.unchanged()

    def test_local_ack_failure_retries_exact_committed_http_evidence(self):
        self.start(); frozen = self.quota.freeze_evidence(); worker = self.worker()
        with sqlite3.connect(self.local) as db:
            db.execute("CREATE TRIGGER reject_ack BEFORE UPDATE ON allocation_evidence_outbox BEGIN SELECT RAISE(ABORT,'disk failure'); END")
        self.assertEqual(worker.step(self.url, self.token, 100), 'retry')
        self.assertEqual(self.count(), 1)
        self.assertEqual(self.quota.freeze_evidence(), frozen)
        with sqlite3.connect(self.local) as db: db.execute('DROP TRIGGER reject_ack')
        self.assertEqual(self.worker().step(self.url, self.token, 114), 'deferred')
        self.assertEqual(self.worker().step(self.url, self.token, 115), 'accepted')
        self.assertEqual(self.count(), 1); self.unchanged()

    def test_no_frozen_work_or_invalid_time_cannot_send(self):
        self.start(); worker = self.worker(); calls = []
        self.assertEqual(worker.step(self.url, self.token, 100, transport=lambda *a: calls.append(a)), 'idle')
        for now in (True, -1, float('nan'), float('inf')):
            with self.assertRaises(ValueError): worker.step(self.url, self.token, now)
        self.assertEqual(calls, []); self.assertEqual(self.count(), 0)

    def test_successor_receipt_cannot_ack_a_request_it_did_not_answer(self):
        self.start(); self.quota.freeze_evidence(); worker = self.worker()
        def successor(*args):
            acknowledge(self.quota, send(*args))
            next_body = self.quota.freeze_evidence()
            return send(self.url, self.token, http.PATH, next_body)
        self.assertEqual(worker.step(self.url, self.token, 100, transport=successor), 'retry')
        with sqlite3.connect(self.local) as db:
            row = db.execute('SELECT sequence,acknowledged FROM allocation_evidence_outbox').fetchone()
        self.assertEqual(row, (2, 0))
        self.assertEqual(worker.step(self.url, self.token, 100), 'accepted')
        self.assertEqual(self.count(), 2); self.unchanged()
