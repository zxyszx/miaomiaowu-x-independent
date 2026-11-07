"""Final evidence transport cannot overtake unacknowledged usage or invent closure."""
import json
import sqlite3
import unittest
import urllib.error
from unittest import mock
from closed_delivery import ClosedDelivery
from usage_outbox import UsageOutbox
import test_usage_outbox as base
import test_usage_delivery as delivery

class ClosedDeliveryTests(unittest.TestCase):
    setUp=base.OutboxTests.setUp
    tearDown=base.OutboxTests.tearDown
    snapshot=base.OutboxTests.snapshot
    receipt=base.OutboxTests.receipt

    def closed(self):
        self.box.capture(self.session,self.snapshot(self.item))
        self.box.close_epoch(self.session)
        self.queue=ClosedDelivery(self.box); self.queue.enqueue('cycle',self.session)

    def confirm(self):
        report=self.box.next_report(self.session)
        self.box.acknowledge(report,self.receipt(report))

    def ack(self,job):
        return dict(accepted=True,server_id=self.box.server_id,cycle_id=job['cycle_id'],
                    session_id=self.session,digest=job['digest'])

    def test_final_cannot_overtake_frozen_tail_then_retirement_preserves_delivery(self):
        self.closed(); self.assertIsNone(self.queue.ready(1))
        self.box.next_report(self.session); self.assertIsNone(self.queue.ready(1))
        self.confirm(); job=self.queue.ready(1); self.assertIsNotNone(job)
        import uuid
        self.box.retire_confirmed(str(uuid.uuid4()))
        self.assertEqual(self.queue.ready(1),job)
        self.queue.acknowledge(job,self.ack(job)); self.assertIsNone(self.queue.ready(1))
        self.assertIsNotNone(self.box.closed_epoch(self.session))

    def test_missing_closure_unknown_and_missing_ack_cannot_be_scheduled_as_zero(self):
        queue=ClosedDelivery(self.box)
        with self.assertRaises(ValueError): queue.enqueue('cycle',self.session)
        self.box.capture(self.session,self.snapshot(self.item))
        self.box.prepare_write(self.session,dict(self.item,upload_bytes=self.item['upload_bytes']+1))
        self.box.close_epoch(self.session)
        with self.assertRaisesRegex(ValueError,'未知写入'): queue.enqueue('cycle',self.session)
        self.assertIsNone(queue.ready(1))

    def test_lost_response_restart_retries_exact_original_digest_with_backoff(self):
        self.closed(); self.confirm(); job=self.queue.ready(10)
        self.queue.defer(job,10)
        self.queue=ClosedDelivery(UsageOutbox(self.path,self.box.server_id))
        self.assertIsNone(self.queue.ready(14)); self.assertEqual(self.queue.ready(15),job)
        self.queue.acknowledge(job,self.ack(job)); self.assertIsNone(self.queue.ready(100))

    def test_wrong_receipt_or_disk_failure_never_clears_pending_evidence(self):
        self.closed(); self.confirm(); job=self.queue.ready(1)
        for receipt in (dict(self.ack(job),digest='bad'),dict(self.ack(job),server_id=True),dict(self.ack(job),cycle_id='other')):
            with self.assertRaises(ValueError): self.queue.acknowledge(job,receipt)
        with self.box.connect() as db:
            db.execute("CREATE TRIGGER fail_closed_ack BEFORE UPDATE ON closed_deliveries BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.queue.acknowledge(job,self.ack(job))
        self.assertEqual(self.queue.ready(1),job)

    def test_revocation_pauses_exact_job_without_discarding_or_reenqueue_bypass(self):
        self.closed(); self.confirm(); job=self.queue.ready(1)
        self.queue.defer(job,1,revoked=True); self.queue.enqueue('cycle',self.session)
        self.assertIsNone(self.queue.ready(10000))
        self.assertIsNotNone(self.box.closed_epoch(self.session))
        with self.box.connect() as db:
            self.assertEqual(db.execute('SELECT status FROM closed_deliveries').fetchone()[0],'blocked')

class DeliverySchedulingTests(unittest.IsolatedAsyncioTestCase):
    setUp=delivery.DeliveryTests.setUp
    tearDown=delivery.DeliveryTests.tearDown
    receipt=delivery.DeliveryTests.receipt

    async def scheduled(self):
        await self.worker.capture()
        import uuid
        await self.worker.rotate(str(uuid.uuid4()))
        self.queue=ClosedDelivery(self.worker.box); self.queue.enqueue('old',self.session)

    async def test_single_attempt_per_round_and_receipts_before_final(self):
        await self.scheduled(); paths=[]
        def send(*args):
            paths.append(args[2]); data=args[3]
            if args[2].endswith('/closed'):
                return dict(accepted=True,server_id=1,session_id=self.session,cycle_id='old',digest=data['digest'])
            return self.receipt(*args)
        with mock.patch('usage_delivery.request',side_effect=send):
            await self.worker.step(); self.assertEqual(paths,['/agent/usage/settle'])
            await self.worker.step(); self.assertEqual(paths,['/agent/usage/settle','/agent/usage/closed'])
            await self.worker.step(); self.assertEqual(len(paths),2)

    async def test_http_revocation_retains_closed_evidence_and_pauses_retry(self):
        await self.scheduled()
        with mock.patch('usage_delivery.request',side_effect=self.receipt): await self.worker.step()
        with mock.patch('usage_delivery.request',side_effect=urllib.error.HTTPError('local',403,'expired',{},None)) as send:
            await self.worker.step(); await self.worker.step(); self.assertEqual(send.call_count,1)
        self.assertIsNotNone(self.worker.box.closed_epoch(self.session))
        self.assertIsNone(self.queue.ready(10000000000))

if __name__=='__main__': unittest.main()
