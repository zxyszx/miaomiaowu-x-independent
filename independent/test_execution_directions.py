"""Actual ACK transaction captures immutable versioned direction evidence."""
import copy
import json
import sqlite3
import unittest
from unittest.mock import patch
from core import Registry,Error
from execution_directions import encoded,record
from usage_migration import UsageMigration
import test_total_cycle_ledger as base


class DirectionReceiptTests(unittest.TestCase):
    setUp=base.TotalLedgerTests.setUp
    tearDown=base.TotalLedgerTests.tearDown
    fixtures=base.TotalLedgerTests.fixtures
    fixture=base.TotalLedgerTests.fixture
    setup_total=base.TotalLedgerTests.setup_total
    applied=base.TotalLedgerTests.applied
    data=base.TotalLedgerTests.data
    report=base.TotalLedgerTests.report

    def rows(self,r):
        with r.connect() as db:
            return [dict(row) for row in db.execute('SELECT * FROM usage_execution_direction_receipts ORDER BY revision')]

    def test_staging_is_not_execution_and_lowering_preserves_old_version(self):
        r=self.setup_total(); self.cycles.set_budget(self.period,dict(upload_bytes=9,download_bytes=8))
        snap=r.configuration(self.token)
        ack=dict(session_id=self.session,revision=snap['revision'],digest=snap['digest'])
        r.acknowledge_configuration(self.token,ack); self.assertEqual(self.rows(r),[])
        r.acknowledge_runtime(self.token,ack)
        first=self.rows(r)[0]
        self.assertEqual(json.loads(first['budget_json']),dict(upload_bytes=9,download_bytes=8))
        self.assertEqual(json.loads(first['baseline_json']),dict(upload_bytes=0,download_bytes=0))
        self.cycles.set_budget(self.period,dict(upload_bytes=1,download_bytes=1)); self.applied(r)
        rows=self.rows(r)
        self.assertEqual(len(rows),2); self.assertEqual(rows[0],first)
        self.assertEqual(json.loads(rows[1]['budget_json']),dict(upload_bytes=1,download_bytes=1))
        # Finite delivered directions support an accounting-only old tail.
        self.assertFalse(self.ledger.settle(self.token,self.data())['duplicate'])
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],4)
        self.assertEqual(self.rows(Registry(r.path)),rows)

    def test_same_current_version_renews_only_its_lease_and_drain_does_not(self):
        r=self.setup_total(); self.cycles.set_budget(self.period,dict(upload_bytes=9,download_bytes=8))
        with patch('usage_settlement.time.time',return_value=self.now): self.applied(r)
        first=self.rows(r)[0]
        with patch('usage_settlement.time.time',return_value=self.now+5): self.applied(r)
        second=self.rows(r)[0]
        self.assertEqual(first['issued_at'],second['issued_at'])
        self.assertEqual(second['execution_until'],first['execution_until']+5)
        UsageMigration(r).begin_drain(self.period)
        with patch('usage_settlement.time.time',return_value=self.now+10): self.applied(r)
        self.assertEqual(self.rows(r),[second])

    def test_total_observation_never_becomes_direction_execution_budget(self):
        r=self.setup_total(); self.applied(r)
        row=self.rows(r)[0]
        self.assertEqual(row['budget_json'],'null'); self.assertEqual(row['baseline_json'],'null')
        self.cycles.set_total_budget(self.period,0); self.applied(r)
        self.assertTrue(all(x['budget_json']=='null' for x in self.rows(r)))
        with self.assertRaises(Error): self.ledger.settle(self.token,self.data())
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],0)

    def test_receipt_insert_failure_rolls_back_runtime_ack_and_execution_grant(self):
        r=self.setup_total(); snap=r.configuration(self.token)
        ack=dict(session_id=self.session,revision=snap['revision'],digest=snap['digest'])
        r.acknowledge_configuration(self.token,ack)
        with r.connect() as db:
            db.execute("CREATE TRIGGER fail_directions BEFORE INSERT ON usage_execution_direction_receipts BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): r.acknowledge_runtime(self.token,ack)
        with r.connect() as db:
            for table in ('runtime_receipts','usage_execution_grants','usage_execution_direction_receipts','usage_execution_total_observations'):
                self.assertEqual(db.execute('SELECT count(*) FROM '+table).fetchone()[0],0)

    def test_old_database_grant_does_not_backfill_unproven_direction_history(self):
        r=self.setup_total(); self.applied(r)
        with r.connect() as db: db.execute('DROP TABLE usage_execution_direction_receipts')
        reopened=Registry(r.path)
        self.assertEqual(self.rows(reopened),[])
        with reopened.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_execution_grants').fetchone()[0],1)
        self.applied(reopened); self.assertEqual(len(self.rows(reopened)),1)
        with self.assertRaises(Error) as caught: reopened.delete('users',self.uid)
        self.assertEqual(caught.exception.status,409)
        self.assertEqual(len(self.rows(reopened)),1)

    def test_conflicting_version_cannot_rewrite_history_and_failed_renewal_is_atomic(self):
        r=self.setup_total(); snap=self.applied(r); before=self.rows(r)
        client=snap['nodes'][0]['clients'][0]
        changed=copy.deepcopy(client)
        changed['cycle_budget']=dict(upload_bytes=4,download_bytes=3)
        changed['cycle_usage']=dict(upload_bytes=0,download_bytes=0)
        with r.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            with self.assertRaises(Error):
                record(db,snap,self.session,snap['nodes'][0],changed,before[0]['token_hash'],self.now,self.now+45,self.now+345)
        self.assertEqual(self.rows(r),before)
        with r.connect() as db:
            db.execute("CREATE TRIGGER fail_renewal BEFORE UPDATE ON usage_execution_direction_receipts BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with r.connect() as db: old=tuple(db.execute('SELECT * FROM usage_execution_grants').fetchone())
        with patch('usage_settlement.time.time',return_value=self.now+10):
            with self.assertRaises(sqlite3.IntegrityError): self.applied(r)
        with r.connect() as db: self.assertEqual(tuple(db.execute('SELECT * FROM usage_execution_grants').fetchone()),old)
        self.assertEqual(self.rows(r),before)

    def test_arbitrary_precision_baseline_and_strict_input(self):
        value=dict(upload_bytes=2**65,download_bytes=2**66)
        self.assertEqual(json.loads(encoded(value)),value)
        for value in (dict(upload_bytes=True,download_bytes=1),dict(upload_bytes=-1,download_bytes=1),{},dict(upload_bytes=None,download_bytes=1)):
            with self.assertRaises(Error): encoded(value)
        self.assertEqual(json.loads(encoded(dict(upload_bytes=None,download_bytes=0),budget=True)),dict(upload_bytes=None,download_bytes=0))
        r=self.setup_total(); snap=self.applied(r); row=self.rows(r)[0]
        with r.connect() as db:
            with self.assertRaises(RuntimeError):
                record(db,snap,self.session,snap['nodes'][0],snap['nodes'][0]['clients'][0],row['token_hash'],self.now,self.now+45,self.now+345)


if __name__=='__main__': unittest.main()
