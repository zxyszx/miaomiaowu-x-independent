"""Final local checkpoint fences writes, never converts unknowns to usage."""
import copy
import sqlite3
import unittest
import uuid
from unittest import mock
from usage_outbox import UsageOutbox
from usage_delivery import UsageDelivery
import test_usage_outbox as base

class ClosedEpochTests(unittest.TestCase):
    setUp=base.OutboxTests.setUp
    snapshot=base.OutboxTests.snapshot
    receipt=base.OutboxTests.receipt

    def test_pending_delivery_retry_and_cleanup_preserve_final_manifest(self):
        self.box.capture(self.session,self.snapshot())
        pending=self.box.next_report(self.session)
        final=self.box.close_epoch(self.session)
        self.assertEqual(final['checkpoint']['items'],[self.item])
        self.assertEqual(final['checkpoint']['uncertain_writes'],[])
        reopened=UsageOutbox(self.path,1)
        self.assertEqual(reopened.close_epoch(self.session),final)
        self.assertEqual(reopened.next_report(self.session),pending)
        reopened.acknowledge(pending,self.receipt(pending))
        self.assertEqual(reopened.retire_confirmed(str(uuid.uuid4())),1)
        self.assertEqual(reopened.closed_epoch(self.session),final)
        self.assertEqual(reopened.close_epoch(self.session),final)

    def test_fence_rejects_new_intent_growth_and_identity_but_accepts_exact_capture(self):
        self.box.capture(self.session,self.snapshot()); final=self.box.close_epoch(self.session)
        self.box.capture(self.session,self.snapshot())
        for item in (dict(self.item,upload_bytes=6),dict(self.item,user_id=4)):
            with self.assertRaises(ValueError): self.box.capture(self.session,self.snapshot(item))
            with self.assertRaises(ValueError): self.box.prepare_write(self.session,item)
        self.assertEqual(self.box.closed_epoch(self.session),final)
        fresh=str(uuid.uuid4()); self.box.capture(fresh,self.snapshot())
        self.assertEqual(self.box.next_report(fresh)['items'],[self.item])

    def test_unknown_write_survives_restart_and_fence_without_becoming_billable(self):
        self.box.capture(self.session,self.snapshot())
        self.box.prepare_write(self.session,dict(self.item,upload_bytes=15))
        reopened=UsageOutbox(self.path,1)
        before=reopened.uncertain_writes(); final=reopened.close_epoch(self.session)
        self.assertEqual(final['checkpoint']['items'],[self.item])
        self.assertEqual(final['checkpoint']['uncertain_writes'][0]['possible_upload'],15)
        self.assertEqual(reopened.next_report(self.session)['items'],[self.item])
        self.assertEqual(reopened.uncertain_writes(),before)
        with self.assertRaises(ValueError): reopened.capture(self.session,self.snapshot(dict(self.item,upload_bytes=15)))
        self.assertEqual(UsageOutbox(self.path,1).closed_epoch(self.session),final)

    def test_failed_close_is_atomic_and_does_not_fence_uncommitted_session(self):
        self.box.capture(self.session,self.snapshot())
        with self.box.connect() as db:
            db.execute("CREATE TRIGGER fail_close BEFORE INSERT ON closed_epochs BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.box.close_epoch(self.session)
        self.assertIsNone(self.box.closed_epoch(self.session))
        self.box.capture(self.session,self.snapshot(dict(self.item,upload_bytes=6)))
        with self.box.connect() as db: db.execute('DROP TRIGGER fail_close')
        self.assertEqual(self.box.close_epoch(self.session)['checkpoint']['items'][0]['upload_bytes'],6)

    def test_empty_close_is_explicit_and_old_retirement_does_not_invent_evidence(self):
        self.assertIsNone(self.box.closed_epoch(self.session))
        final=self.box.close_epoch(self.session)
        self.assertEqual(final['checkpoint']['items'],[])
        with self.assertRaises(ValueError): self.box.capture(self.session,self.snapshot())
        old=str(uuid.uuid4())
        with self.box.connect() as db: db.execute('INSERT INTO retired VALUES(?)',(old,))
        with self.assertRaises(ValueError): self.box.close_epoch(old)
        self.assertIsNone(self.box.closed_epoch(old))

    def test_canonical_digest_and_return_values_cannot_mutate_stored_evidence(self):
        import hashlib,json
        self.box.capture(self.session,self.snapshot())
        final=self.box.close_epoch(self.session); original=copy.deepcopy(final)
        encoded=json.dumps(final['checkpoint'],sort_keys=True,separators=(',',':')).encode()
        self.assertEqual(final['digest'],hashlib.sha256(encoded).hexdigest())
        final['checkpoint']['items'][0]['upload_bytes']=999
        self.assertEqual(self.box.closed_epoch(self.session),original)

    def test_capture_racing_close_cannot_grow_beyond_committed_final_checkpoint(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier
        self.box.capture(self.session,self.snapshot()); gate=Barrier(2)
        def capture():
            gate.wait()
            try:
                self.box.capture(self.session,self.snapshot(dict(self.item,upload_bytes=6)))
                return True
            except ValueError: return False
        def close():
            gate.wait(); return self.box.close_epoch(self.session)
        with ThreadPoolExecutor(2) as pool:
            write=pool.submit(capture); closed=pool.submit(close)
            accepted=write.result(); final=closed.result()
        self.assertEqual(final['checkpoint']['items'][0]['upload_bytes'],6 if accepted else 5)
        self.assertEqual(self.box.closed_epoch(self.session),final)

class DeliveryCloseTests(unittest.IsolatedAsyncioTestCase):
    setUp=base.OutboxTests.setUp
    snapshot=base.OutboxTests.snapshot

    async def test_rotation_close_failure_keeps_epoch_and_baseline_then_retries(self):
        runtime=mock.Mock(); runtime.usage_snapshot.return_value=self.snapshot()
        delivery=UsageDelivery('unused','unused',self.path.parent,runtime,self.session)
        await delivery.capture(); old_baseline=copy.deepcopy(delivery.baseline)
        new=str(uuid.uuid4())
        with mock.patch.object(delivery.box,'close_epoch',side_effect=sqlite3.OperationalError('full')):
            with self.assertRaises(sqlite3.OperationalError): await delivery.rotate(new)
        self.assertEqual(delivery.session,self.session)
        self.assertEqual(delivery.baseline,old_baseline)
        await delivery.rotate(new)
        self.assertIsNotNone(delivery.box.closed_epoch(self.session))
        self.assertEqual(delivery.session,new)

    async def test_shutdown_preserves_frozen_pending_and_local_final_counters(self):
        runtime=mock.Mock(); runtime.usage_snapshot.return_value=self.snapshot()
        delivery=UsageDelivery('unused','unused',self.path.parent,runtime,self.session)
        await delivery.capture(); pending=delivery.box.next_report(self.session)
        await delivery.finish(None)
        final=delivery.box.closed_epoch(self.session)
        self.assertEqual(final['checkpoint']['items'],[self.item])
        self.assertEqual(delivery.box.next_report(self.session),pending)

    async def test_committed_close_lost_result_can_retry_without_resetting_final_counters(self):
        runtime=mock.Mock(); runtime.usage_snapshot.return_value=self.snapshot()
        delivery=UsageDelivery('unused','unused',self.path.parent,runtime,self.session)
        await delivery.capture(); original=delivery.box.close_epoch
        def committed_then_failed(session):
            original(session); raise OSError('lost completion')
        new=str(uuid.uuid4())
        with mock.patch.object(delivery.box,'close_epoch',side_effect=committed_then_failed):
            with self.assertRaises(OSError): await delivery.rotate(new)
        final=delivery.box.closed_epoch(self.session)
        self.assertEqual(delivery.session,self.session)
        await delivery.rotate(new)
        self.assertEqual(delivery.session,new)
        self.assertEqual(delivery.box.closed_epoch(self.session),final)

if __name__=='__main__': unittest.main()
