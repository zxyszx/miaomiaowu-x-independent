"""Authenticated final evidence never guesses unknown bytes or enables execution."""
import copy
import hashlib
import json
import sqlite3
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from core import Registry,Error
from usage_migration import UsageMigration
import test_drain_inventory as base

class DrainCheckpointTests(unittest.TestCase):
    setUp=base.InventoryTests.setUp
    tearDown=base.InventoryTests.tearDown
    fixtures=base.InventoryTests.fixtures
    fixture=base.InventoryTests.fixture
    setup_total=base.InventoryTests.setup_total
    applied=base.InventoryTests.applied
    data=base.InventoryTests.data
    report=base.InventoryTests.report

    def envelope(self):
        body=dict(format=1,server_id=self.sid,session_id=self.session,
                  items=[copy.deepcopy(self.item)],uncertain_writes=[])
        return self.encode(body)

    @staticmethod
    def encode(body):
        return dict(checkpoint=body,digest=hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',',':')).encode()).hexdigest())

    def setup_drain(self,report=True):
        r=self.setup_total(total=100); self.sid=self.applied(r)['server_id']
        self.migration=UsageMigration(r); self.migration.begin_drain(self.period)
        if report: self.report()
        return r

    def accept(self,envelope=None):
        return self.migration.accept_closed(self.period,self.token,envelope or self.envelope())

    def test_accept_restart_exact_retry_and_no_billing_or_readiness(self):
        r=self.setup_drain(); envelope=self.envelope()
        self.assertFalse(self.accept()['duplicate'])
        self.migration=UsageMigration(Registry(r.path))
        with patch('usage_migration.time.time',return_value=self.now+1000):
            self.assertTrue(self.accept(envelope)['duplicate'])
        self.assertFalse(self.accept()['ready_for_execution'])
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT total_bytes FROM usage_cycle_confirmed_totals').fetchone()[0],'7')
            self.assertEqual(db.execute('SELECT count(*) FROM usage_drain_checkpoints').fetchone()[0],1)

    def test_missing_or_not_yet_confirmed_counters_block(self):
        self.setup_drain(report=False)
        with self.assertRaises(Error): self.accept()
        self.report(); self.item['upload_bytes']=5
        with self.assertRaises(Error): self.accept()
        self.report(2); self.assertTrue(self.accept()['accepted'])

    def test_unknown_and_missing_identity_and_bad_digest_block(self):
        self.setup_drain()
        body=self.envelope()['checkpoint']; body['uncertain_writes']=[dict(possible_upload=5)]
        with self.assertRaises(Error): self.accept(self.encode(body))
        body=self.envelope()['checkpoint']; body['items'][0]['node_id']+=100
        with self.assertRaises(Error): self.accept(self.encode(body))
        envelope=self.envelope(); envelope['digest']='0'*64
        with self.assertRaises(Error): self.accept(envelope)

    def test_missing_old_frontier_and_empty_inventory_do_not_prove_zero(self):
        r=self.setup_drain()
        with r.connect() as db: db.execute('DELETE FROM usage_drain_inventory_captures')
        with self.assertRaises(Error): self.accept()
        with r.connect() as db:
            db.execute('INSERT INTO usage_drain_inventory_captures VALUES(?)',(self.period,))
            db.execute('DELETE FROM usage_drain_inventory')
        with self.assertRaises(Error): self.accept()

    def test_deadline_and_wrong_token_and_rotated_original_hash_block(self):
        r=self.setup_drain()
        with patch('usage_migration.time.time',return_value=self.now+1000):
            with self.assertRaises(Error): self.accept()
        with self.assertRaises(Error): self.migration.accept_closed(self.period,'x'*32,self.envelope())
        self.accept()
        self.token=r.provision_agent(self.sid)['token']
        with self.assertRaises(Error): self.accept()

    def test_concurrent_growth_and_final_acceptance_cannot_both_commit(self):
        r=self.setup_drain(); envelope=self.envelope()
        def seal():
            try: return self.accept(envelope)['accepted']
            except Error: return False
        def grow():
            try:
                self.ledger.settle(self.token,dict(session_id=self.session,sequence=2,
                    items=[dict(self.item,upload_bytes=5)]))
                return True
            except Error: return False
        with ThreadPoolExecutor(2) as pool:
            futures=[pool.submit(fn) for fn in (seal,grow)]
            results=[f.result() for f in futures]
        self.assertEqual(sum(results),1)
        with r.connect() as db:
            sealed=db.execute('SELECT count(*) FROM usage_drain_checkpoints').fetchone()[0]
            final=db.execute('SELECT upload_bytes FROM usage_counters').fetchone()[0]
        self.assertEqual((sealed,final),(1,4) if results[0] else (0,5))

    def test_deleted_counter_never_invents_zero_and_rollback_below_frontier_blocked(self):
        r=self.setup_total(total=100); self.sid=self.applied(r)['server_id']; self.report()
        self.migration=UsageMigration(r); self.migration.begin_drain(self.period)
        with r.connect() as db: db.execute('DELETE FROM usage_counters')
        with self.assertRaises(Error): self.accept()
        with r.connect() as db:
            db.execute('INSERT INTO usage_counters VALUES(?,?,?,?,?,?,?)',
                       (self.sid,self.session,self.nid,self.uid,self.item['credential'],0,0))
        body=self.envelope()['checkpoint']; body['items'][0].update(upload_bytes=0,download_bytes=0)
        with self.assertRaises(Error): self.accept(self.encode(body))

    def test_sealed_cannot_grow_report_or_settle_but_exact_retry_survives(self):
        r=self.setup_drain(); self.accept()
        self.assertTrue(self.report()['duplicate'])
        self.assertTrue(self.ledger.settle(self.token,self.data())['duplicate'])
        self.item['upload_bytes']=5
        for operation in (lambda:self.report(2),lambda:self.ledger.settle(self.token,self.data(2))):
            with self.assertRaises(Error): operation()
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT upload_bytes FROM usage_counters').fetchone()[0],4)

    def test_insert_failure_atomic_and_conflicting_checkpoint_rejected(self):
        r=self.setup_drain()
        with r.connect() as db:
            db.execute("CREATE TRIGGER fail_final BEFORE INSERT ON usage_drain_checkpoints BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.accept()
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_drain_checkpoints').fetchone()[0],0)
            db.execute('DROP TRIGGER fail_final')
        self.accept(); body=self.envelope()['checkpoint']; body['items'][0]['upload_bytes']=5
        with self.assertRaises(Error): self.accept(self.encode(body))

if __name__=='__main__': unittest.main()
