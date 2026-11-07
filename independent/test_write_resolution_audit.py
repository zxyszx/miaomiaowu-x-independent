"""Reopening an uncertain intent preserves evidence through atomic resolution."""
import hashlib
import sqlite3
import unittest
import uuid

import test_usage_outbox
from usage_outbox import UsageOutbox


class ResolutionAuditTests(unittest.TestCase):
    setUp=test_usage_outbox.OutboxTests.setUp
    snapshot=test_usage_outbox.OutboxTests.snapshot
    receipt=test_usage_outbox.OutboxTests.receipt

    def recover(self):
        self.box.capture(self.session,self.snapshot())
        self.target=dict(self.item,upload_bytes=9)
        self.box.prepare_write(self.session,self.target)
        self.box=UsageOutbox(self.path,1)

    def test_covering_checkpoint_archives_bounds_without_exposing_credential(self):
        self.recover()
        self.box.capture(self.session,self.snapshot(self.target))
        rows=self.box.write_resolution_history()
        self.assertEqual(len(rows),1)
        row=rows[0]
        self.assertEqual((row['lower_upload'],row['lower_download'],row['target_upload'],
            row['target_download'],row['checkpoint_upload'],row['checkpoint_download']),(5,7,9,7,9,7))
        self.assertEqual((row['session'],row['node'],row['user']),(self.session,2,3))
        self.assertEqual(row['credential_fingerprint'],hashlib.sha256(self.item['credential'].encode()).hexdigest())
        self.assertNotIn(self.item['credential'],str(rows))
        self.assertEqual(row['source'],'successful_checkpoint')
        self.assertTrue(row['recorded_at'])
        self.assertEqual(self.box.uncertain_writes(),[])
        self.assertEqual(UsageOutbox(self.path,1).write_resolution_history(),rows)

    def test_partial_checkpoint_and_other_identity_cannot_resolve(self):
        self.recover()
        self.box.capture(self.session,self.snapshot(dict(self.item,upload_bytes=8)))
        self.box.capture(str(uuid.uuid4()),self.snapshot(self.target))
        self.box.capture(self.session,self.snapshot(dict(self.target,credential=str(uuid.uuid4()))))
        self.assertEqual(self.box.write_resolution_history(),[])
        self.assertEqual(len(self.box.uncertain_writes()),1)
        # Reopening after partial progress must not replace the original bounds.
        self.box=UsageOutbox(self.path,1)
        self.box.capture(self.session,self.snapshot(self.target))
        self.assertEqual(self.box.write_resolution_history()[0]['lower_upload'],5)

    def test_audit_failure_rolls_back_counters_and_intent_deletion(self):
        self.recover()
        with self.box.connect() as db:
            db.executescript("CREATE TRIGGER fixture_audit_abort BEFORE INSERT ON write_resolutions BEGIN SELECT RAISE(ABORT,'fixture'); END;")
        with self.assertRaises(sqlite3.Error):
            self.box.capture(self.session,self.snapshot(self.target))
        self.assertEqual(self.box.next_report(self.session)['items'][0]['upload_bytes'],5)
        self.assertEqual(len(self.box.uncertain_writes()),1)
        self.assertEqual(self.box.write_resolution_history(),[])
        with self.box.connect() as db: db.execute('DROP TRIGGER fixture_audit_abort')
        self.box.capture(self.session,self.snapshot(self.target))
        self.assertEqual(len(self.box.write_resolution_history()),1)

    def test_repeated_checkpoint_and_retirement_preserve_one_audit(self):
        self.recover()
        for _ in range(3):
            self.box=UsageOutbox(self.path,1)
            self.box.capture(self.session,self.snapshot(self.target))
        rows=self.box.write_resolution_history()
        self.assertEqual(len(rows),1)
        report=self.box.next_report(self.session)
        self.box.acknowledge(report,self.receipt(report))
        self.assertEqual(self.box.retire_confirmed(str(uuid.uuid4())),1)
        self.assertEqual(self.box.write_resolution_history(),rows)
        self.assertEqual(self.box.outstanding_sessions(),[])

    def test_failure_after_audit_insert_rolls_back_audit_with_counters(self):
        self.recover()
        with self.box.connect() as db:
            db.executescript("CREATE TRIGGER fixture_delete_abort BEFORE DELETE ON write_intents BEGIN SELECT RAISE(ABORT,'fixture'); END;")
        with self.assertRaises(sqlite3.Error):
            self.box.capture(self.session,self.snapshot(self.target))
        self.assertEqual(self.box.write_resolution_history(),[])
        self.assertEqual(len(self.box.uncertain_writes()),1)
        self.assertEqual(self.box.next_report(self.session)['items'][0]['upload_bytes'],5)
        with self.box.connect() as db: db.execute('DROP TRIGGER fixture_delete_abort')
        self.box.capture(self.session,self.snapshot(self.target))
        self.assertEqual(len(self.box.write_resolution_history()),1)

    def test_ordinary_successful_writes_do_not_accumulate_recovery_audits(self):
        for amount in range(5,30):
            item=dict(self.item,upload_bytes=amount)
            self.box.prepare_write(self.session,item)
            self.box.capture(self.session,self.snapshot(item))
        self.assertEqual(self.box.write_resolution_history(),[])
        with self.box.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM recovered_writes').fetchone()[0],0)

    def test_legacy_database_intent_is_observed_without_charging_or_refund(self):
        self.recover()
        with self.box.connect() as db:
            db.execute('DROP TABLE recovered_writes')
            db.execute('DROP TABLE write_resolutions')
        self.box=UsageOutbox(self.path,1)
        self.assertEqual(len(self.box.uncertain_writes()),1)
        self.assertEqual(self.box.write_resolution_history(),[])
        self.assertEqual(self.box.next_report(self.session)['items'][0]['upload_bytes'],5)
        self.box.capture(self.session,self.snapshot(self.target))
        self.assertEqual(len(self.box.write_resolution_history()),1)
