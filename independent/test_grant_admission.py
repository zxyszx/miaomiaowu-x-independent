"""First-grant admission is a live transaction check, never a traffic grant."""
import hashlib
import json
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from core import Registry,Error
from drain_seal import DrainSeal
from grant_admission import require_sealed_legacy
import test_drain_seal as base


class GrantAdmissionTests(unittest.TestCase):
    setUp=base.DrainSealTests.setUp
    tearDown=base.DrainSealTests.tearDown
    fixtures=base.DrainSealTests.fixtures
    fixture=base.DrainSealTests.fixture
    setup_total=base.DrainSealTests.setup_total
    setup_drain=base.DrainSealTests.setup_drain
    applied=base.DrainSealTests.applied
    report=base.DrainSealTests.report
    data=base.DrainSealTests.data
    encode=staticmethod(base.DrainSealTests.encode)
    envelope=base.DrainSealTests.envelope
    accept=base.DrainSealTests.accept

    def sealed(self):
        r=self.setup_drain(); self.accept(); self.seal=DrainSeal(r).seal(self.period)
        return r

    def check(self,r):
        with r.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            return require_sealed_legacy(db,self.period)

    def test_requires_caller_transaction_and_actual_seal(self):
        r=self.setup_total()
        with r.connect() as db:
            with self.assertRaises(RuntimeError): require_sealed_legacy(db,self.period)
        with self.assertRaises(Error): self.check(r)
        self.sid=self.applied(r)['server_id']
        from usage_migration import UsageMigration
        self.migration=UsageMigration(r); self.migration.begin_drain(self.period)
        self.report(); self.accept()
        with self.assertRaises(Error): self.check(r)
        DrainSeal(r).seal(self.period)
        self.assertEqual(self.check(r)['available_bytes'],0)

    def test_reopen_cleanup_preserves_uuid_usage_and_no_execution_or_reservation(self):
        r=self.sealed(); first=self.check(r)
        self.assertEqual((first['confirmed_bytes'],first['available_bytes']),(7,93))
        self.assertFalse(first['ready_for_execution']); self.assertTrue(first['prerequisite_only'])
        with r.connect() as db:
            uuid=db.execute('SELECT credential FROM node_users').fetchone()[0]
            self.assertEqual(uuid,self.item['credential'])
            before=db.execute('SELECT count(*) FROM usage_execution_grants').fetchone()[0]
            self.assertEqual(db.execute('SELECT count(*) FROM usage_total_allocations').fetchone()[0],0)
        self.assertTrue(self.report()['duplicate'])
        self.item['upload_bytes']+=1
        with self.assertRaises(Error): self.report(2)
        r.delete('nodes',self.nid); r.delete('servers',self.sid)
        reopened=Registry(r.path)
        self.assertEqual(self.check(reopened),first)
        with reopened.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_total_allocations').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT total_bytes FROM usage_cycle_confirmed_totals').fetchone()[0],'7')
        self.assertEqual(before,1)

    def test_current_policy_and_internal_zero_unlimited_conventions(self):
        r=self.sealed(); old=self.check(r)
        self.cycles.set_total_budget(self.period,0)
        zero=self.check(r)
        self.assertEqual(zero['available_bytes'],0)
        self.assertNotEqual(zero['policy_id'],old['policy_id'])
        self.cycles.set_total_budget(self.period,None)
        self.assertIsNone(self.check(r)['available_bytes'])
        self.cycles.set_total_budget(self.period,10)
        self.assertEqual(self.check(r)['available_bytes'],3)
        self.assertEqual(DrainSeal(r).seal(self.period)['digest'],old['seal_digest'])

    def test_missing_unknown_and_changed_accounting_never_repaired(self):
        r=self.sealed()
        statements=[
            "DELETE FROM usage_cycle_confirmed_directions",
            "UPDATE usage_cycle_confirmed_directions SET unknown_bytes='1'",
            "UPDATE usage_cycle_confirmed_totals SET total_bytes='8'",
            "DELETE FROM usage_drain_inventory_captures",
            "DELETE FROM usage_drain_inventory",
            "DELETE FROM usage_drain_checkpoints",
            "UPDATE usage_counters SET upload_bytes=5",
        ]
        for sql in statements:
            with self.subTest(sql=sql),r.connect() as db:
                db.execute('BEGIN IMMEDIATE'); db.execute(sql)
                with self.assertRaises(Error): require_sealed_legacy(db,self.period)
                db.rollback()
        self.assertEqual(self.check(r)['confirmed_bytes'],7)

    def test_corrupt_stored_seal_is_rejected_by_status_and_admission(self):
        r=self.sealed()
        for body in ('not-json','[]','{}'):
            with self.subTest(body=body),r.connect() as db:
                db.execute('BEGIN IMMEDIATE')
                db.execute('UPDATE usage_drain_seals SET body=?',(body,))
                with self.assertRaises(Error): require_sealed_legacy(db,self.period)
                db.rollback()
        with r.connect() as db: db.execute("UPDATE usage_drain_seals SET digest='wrong'")
        with self.assertRaises(Error): DrainSeal(r).status(self.period)
        with self.assertRaises(Error): self.check(r)

    def test_self_consistent_modified_seal_still_must_match_live_evidence(self):
        r=self.sealed()
        with r.connect() as db:
            body=json.loads(db.execute('SELECT body FROM usage_drain_seals').fetchone()[0])
            body['final_upload_bytes']+=1
            encoded=json.dumps(body,sort_keys=True,separators=(',',':'))
            db.execute('UPDATE usage_drain_seals SET body=?,digest=?',
                       (encoded,hashlib.sha256(encoded.encode()).hexdigest()))
        with self.assertRaises(Error): self.check(r)

    def test_expired_future_and_missing_policy_do_not_authorize(self):
        r=self.sealed()
        for when in (self.now-100,self.now+1000):
            with self.subTest(when=when),patch('grant_admission.time.time',return_value=when):
                with self.assertRaises(Error): self.check(r)
        with r.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('UPDATE usage_cycles SET expired=1')
            with self.assertRaises(Error): require_sealed_legacy(db,self.period)
            db.rollback()
            db.execute('BEGIN IMMEDIATE')
            db.execute('DELETE FROM usage_cycle_total_budgets')
            with self.assertRaises(Error): require_sealed_legacy(db,self.period)
            db.rollback()

    def test_failed_caller_transaction_does_not_create_policy_or_grant(self):
        r=self.sealed()
        # The issuer owns the entire transaction; even a lazily seeded current
        # policy must roll back if subsequent reservation persistence fails.
        with r.connect() as db:
            db.execute('DELETE FROM usage_execution_total_observations')
            db.execute('DELETE FROM usage_total_policy_versions')
        with self.assertRaises(RuntimeError),r.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            self.assertEqual(require_sealed_legacy(db,self.period)['available_bytes'],93)
            raise RuntimeError('simulated issuer persistence failure')
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_total_policy_versions').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM usage_total_allocations').fetchone()[0],0)

    def test_policy_writer_cannot_change_admission_snapshot_mid_transaction(self):
        r=self.sealed(); started=threading.Event(); finished=threading.Event()
        def lower():
            started.set()
            self.cycles.set_total_budget(self.period,0)
            finished.set()
        with ThreadPoolExecutor(1) as pool:
            with r.connect() as db:
                db.execute('BEGIN IMMEDIATE')
                first=require_sealed_legacy(db,self.period)
                task=pool.submit(lower); self.assertTrue(started.wait(2))
                self.assertFalse(finished.wait(.05))
                self.assertEqual(require_sealed_legacy(db,self.period),first)
            task.result(timeout=5)
        self.assertEqual(self.check(r)['available_bytes'],0)

    def test_malformed_retained_checkpoint_blocks_without_inventing_zero(self):
        r=self.sealed()
        with r.connect() as db:
            db.execute("UPDATE usage_drain_checkpoints SET body='{}'")
        with self.assertRaises(Error) as error: self.check(r)
        self.assertEqual(error.exception.status,409)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT total_bytes FROM usage_cycle_confirmed_totals').fetchone()[0],'7')


if __name__=='__main__': unittest.main()
