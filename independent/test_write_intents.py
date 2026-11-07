"""Durable uncertainty must remain separate from measured/reportable usage."""
import sqlite3
import unittest
from unittest import mock

import test_usage_outbox
from payload_meter import PayloadMeter, MeteredWriter


class WriteIntentTests(unittest.TestCase):
    setUp=test_usage_outbox.OutboxTests.setUp
    snapshot=test_usage_outbox.OutboxTests.snapshot
    receipt=test_usage_outbox.OutboxTests.receipt

    def test_prepare_is_not_reported_and_cannot_be_overwritten(self):
        self.box.prepare_write(self.session,self.item)
        restored=type(self.box)(self.path,1)
        self.assertIsNone(restored.next_report(self.session))
        self.assertEqual(restored.outstanding_sessions(),[])
        self.assertEqual(len(restored.uncertain_writes()),1)
        with self.assertRaises(ValueError): restored.prepare_write(self.session,dict(self.item,upload_bytes=9))
        self.assertEqual(restored.uncertain_writes()[0]['possible_upload'],5)


    def test_only_covering_capture_resolves_and_retirement_preserves_unknown(self):
        self.box.capture(self.session,self.snapshot())
        report=self.box.next_report(self.session); self.box.acknowledge(report,self.receipt(report))
        target=dict(self.item,upload_bytes=9)
        self.box.prepare_write(self.session,target)
        self.box.capture(self.session,self.snapshot())
        self.assertEqual(len(self.box.uncertain_writes()),1)
        import uuid
        self.assertEqual(self.box.retire_confirmed(str(uuid.uuid4())),0)
        self.box.capture(self.session,self.snapshot(target))
        self.assertEqual(self.box.uncertain_writes(),[])
        self.assertEqual(self.box.next_report(self.session)['items'][0]['upload_bytes'],9)


    def test_failed_capture_preserves_intent_and_old_totals_atomically(self):
        self.box.capture(self.session,self.snapshot())
        self.box.prepare_write(self.session,dict(self.item,upload_bytes=9))
        with self.box.connect() as db:
            db.executescript("CREATE TRIGGER fixture_abort BEFORE DELETE ON write_intents BEGIN SELECT RAISE(ABORT,'fixture'); END;")
        with self.assertRaises(sqlite3.Error):
            self.box.capture(self.session,self.snapshot(dict(self.item,upload_bytes=9)))
        self.assertEqual(self.box.next_report(self.session)['items'][0]['upload_bytes'],5)
        self.assertEqual(len(self.box.uncertain_writes()),1)


    def test_meter_stops_before_write_on_intent_storage_error(self):
        meter=PayloadMeter(); meter.bind(1)
        meter.set_write_intent(mock.Mock(side_effect=OSError('fixture')))
        target=mock.Mock(); writer=MeteredWriter(target,meter,(2,3,self.item['credential']),0)
        with self.assertRaises(OSError): writer.write(b'hello')
        target.write.assert_not_called()
        self.assertTrue(meter.failed); self.assertEqual(meter.snapshot()['items'],[])


    def test_failed_transport_keeps_intent_without_invented_usage(self):
        meter=PayloadMeter(); meter.bind(1)
        meter.set_write_intent(lambda s:self.box.prepare_write(self.session,s['items'][0]))
        meter.set_incremental_checkpoint(lambda s:self.box.capture(self.session,s))
        target=mock.Mock(); target.write.side_effect=OSError('partial queue unknown')
        writer=MeteredWriter(target,meter,(2,3,self.item['credential']),0)
        with self.assertRaises(OSError): writer.write(b'hello')
        self.assertTrue(meter.failed); self.assertIsNone(self.box.next_report(self.session))
        self.assertEqual(len(self.box.uncertain_writes()),1)
        with self.assertRaises(OSError): writer.write(b'again')
        target.write.assert_called_once()
