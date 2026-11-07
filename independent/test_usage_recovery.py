import asyncio
import copy
import json
import sqlite3
import unittest
import uuid
from unittest import mock
from usage_outbox import UsageOutbox
import test_usage_outbox
import test_usage_delivery


class RecoveryTests(unittest.TestCase):
    setUp=test_usage_outbox.OutboxTests.setUp
    snapshot=test_usage_outbox.OutboxTests.snapshot
    receipt=test_usage_outbox.OutboxTests.receipt

    def rejected(self):
        self.box.capture(self.session,self.snapshot())
        report=self.box.next_report(self.session)
        self.box.isolate(report,100)
        return report

    def test_explicit_retry_keeps_evidence_and_uses_latest_cumulative_totals(self):
        rejected=self.rejected()
        self.box.capture(self.session,self.snapshot(dict(self.item,upload_bytes=15)))
        evidence=self.box.isolation_status()['failures']
        self.box.retry_quarantined(rejected,101)
        restored=UsageOutbox(self.path,1)
        self.assertEqual(restored.isolation_status(),dict(failures=evidence,quarantined=[]))
        self.assertEqual(restored.recovery_status(),[dict(session=self.session,sequence=1,created_at=101)])
        report=restored.next_report(self.session)
        self.assertEqual((report['sequence'],report['items'][0]['upload_bytes']),(2,15))
        with restored.connect() as db:
            self.assertEqual(db.execute('SELECT acked_upload FROM totals').fetchone()[0],0)
        restored.acknowledge(report,self.receipt(report))
        self.assertEqual(restored.outstanding_sessions(),[])
        self.assertEqual(restored.retire_confirmed(str(uuid.uuid4())),0)

    def test_stale_or_repeated_action_cannot_release_a_later_rejection(self):
        old=self.rejected(); self.box.retry_quarantined(old,101)
        with self.assertRaises(ValueError): self.box.retry_quarantined(old,102)
        new=self.box.next_report(self.session); self.box.isolate(new,103)
        with self.assertRaises(ValueError): self.box.retry_quarantined(old,104)
        self.assertEqual(self.box.isolation_status()['quarantined'][0]['rejected_sequence'],2)
        self.box.retry_quarantined(new,105)
        self.assertEqual([r['sequence'] for r in self.box.recovery_status()],[1,2])

    def test_invalid_body_clock_and_mixed_batch_leave_quarantine_unchanged(self):
        report=self.rejected(); changed=copy.deepcopy(report); changed['items'][0]['upload_bytes']+=1
        mixed=copy.deepcopy(report); mixed['items'].append(dict(self.item,user_id=4))
        audit=self.box.isolation_status()
        for data,now in ((changed,101),(mixed,101),(report,True),(report,-1),(report,float('inf'))):
            with self.assertRaises(ValueError): self.box.retry_quarantined(data,now)
        self.assertEqual(self.box.isolation_status(),audit)
        self.assertEqual(self.box.recovery_status(),[])

    def test_delete_failure_rolls_back_recovery_audit_and_release(self):
        report=self.rejected(); audit=self.box.isolation_status()
        with self.box.connect() as db:
            db.execute("CREATE TRIGGER fail_recovery BEFORE DELETE ON quarantine BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.box.retry_quarantined(report,101)
        self.assertEqual(self.box.isolation_status(),audit)
        self.assertEqual(self.box.recovery_status(),[])
        with self.box.connect() as db: db.execute('DROP TRIGGER fail_recovery')
        self.box.retry_quarantined(report,102)

    def test_other_pending_report_and_backoff_are_not_changed(self):
        rejected=self.rejected()
        good=dict(self.item,user_id=4)
        self.box.capture(self.session,self.snapshot(good))
        pending=self.box.next_report(self.session); self.box.defer(pending,'network',101)
        retry=self.box.retry_status()
        self.box.retry_quarantined(rejected,102)
        self.assertEqual(self.box.next_report(self.session),pending)
        self.assertEqual(self.box.retry_status(),retry)
        self.box.acknowledge(pending,self.receipt(pending))
        next_report=self.box.next_report(self.session)
        self.assertEqual((next_report['sequence'],next_report['items']),(3,[self.item]))

    def test_additive_schema_upgrade_preserves_existing_quarantine(self):
        report=self.rejected()
        with self.box.connect() as db: db.execute('DROP TABLE recoveries')
        restored=UsageOutbox(self.path,1)
        self.assertEqual(restored.recovery_status(),[])
        restored.retry_quarantined(report,101)
        self.assertEqual(restored.next_report(self.session)['sequence'],2)


@unittest.skipUnless(test_usage_delivery.test_xray.BINARY.is_file(),'Set XRAY_BINARY')
class RecoveryRealTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_usage_delivery.DeliveryRealTests.setUp
    tearDown=test_usage_delivery.DeliveryRealTests.tearDown
    request=test_usage_delivery.DeliveryRealTests.request
    asyncSetUp=test_usage_delivery.DeliveryRealTests.asyncSetUp
    asyncTearDown=test_usage_delivery.DeliveryRealTests.asyncTearDown
    open=test_usage_delivery.DeliveryRealTests.open
    accepted=test_usage_delivery.DeliveryRealTests.accepted
    close_writer=test_usage_delivery.DeliveryRealTests.close_writer

    async def test_local_retry_cannot_restore_permission_then_valid_proof_settles_actual_payload(self):
        from execution_agent import cycle
        from usage_delivery import UsageDelivery
        session=str(uuid.uuid4())
        await cycle(self.base,self.token,self.state,self.runtime,1,session)
        reader,writer=await self.open(); await self.accepted(reader)
        registry=self.server.registry
        # Isolated test DB fault only: retain the real execution proof for repair.
        with registry.connect() as db:
            proof=dict(db.execute('SELECT * FROM usage_execution_grants WHERE user_id=?',(self.uid,)).fetchone())
            db.execute('DELETE FROM usage_execution_grants WHERE user_id=?',(self.uid,))
        worker=UsageDelivery(self.base,self.token,self.state,self.runtime,session)
        with mock.patch('builtins.print'): await worker.step()
        rejected=worker.box.isolation_status()['failures'][-1]['report']
        from usage_recovery_cli import ExistingOutbox,inspect,retry
        operations=ExistingOutbox(worker.box.path,1)
        retry(operations,session,rejected['sequence'],inspect(operations)[0][1]['report_digest'],'fixture proof repair pending')
        with mock.patch('builtins.print'): await worker.step()
        # Local operator action never bypasses the server's absent proof.
        self.assertEqual(worker.box.isolation_status()['quarantined'][0]['rejected_sequence'],2)
        with self.request(f'/api/users/{self.uid}/usage') as response: total=json.load(response)
        self.assertEqual(total['upload_bytes'],0)
        writer.write(b'more'); await writer.drain()
        self.assertEqual(await asyncio.wait_for(reader.readexactly(4),2),b'more')
        # Restore only the fixture's original proof, including its original expiry.
        with registry.connect() as db:
            columns=','.join(proof); marks=','.join('?' for _ in proof)
            db.execute(f'INSERT INTO usage_execution_grants({columns}) VALUES({marks})',tuple(proof.values()))
        rejected=worker.box.isolation_status()['failures'][-1]['report']
        retry(operations,session,rejected['sequence'],inspect(operations)[0][1]['report_digest'],'fixture original proof restored')
        await worker.step(); await worker.step()
        with self.request(f'/api/users/{self.uid}/usage') as response: total=json.load(response)
        self.assertEqual((total['upload_bytes'],total['download_bytes']),(9,9))
        self.assertEqual(worker.box.outstanding_sessions(),[])
        self.assertEqual(len(worker.box.recovery_status()),2)
        self.assertEqual(len(worker.box.isolation_status()['failures']),2)
        with worker.box.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM recovery_actions').fetchone()[0],2)
