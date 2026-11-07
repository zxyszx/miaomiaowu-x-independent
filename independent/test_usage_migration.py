"""Internal drain barrier against mixed legacy/new quota execution."""
import sqlite3
import unittest
import uuid
from unittest.mock import patch
from core import Registry,Error
from usage_migration import UsageMigration
from usage_ledger import UsageLedger
from total_allocation import TotalAllocations
import test_total_cycle_ledger as base
import test_total_allocation as allocations


class MigrationTests(unittest.TestCase):
    setUp=base.TotalLedgerTests.setUp
    tearDown=base.TotalLedgerTests.tearDown
    fixtures=base.TotalLedgerTests.fixtures
    fixture=base.TotalLedgerTests.fixture
    setup_total=base.TotalLedgerTests.setup_total
    second_host=base.TotalLedgerTests.second_host
    applied=base.TotalLedgerTests.applied
    data=base.TotalLedgerTests.data
    report=base.TotalLedgerTests.report
    request=allocations.AllocationTests.request
    reserved=allocations.AllocationTests.reserved

    def test_drain_all_hosts_preserves_identity_and_is_persistent_idempotent(self):
        r=self.setup_total(); other,_=self.second_host(r)
        before=[r.configuration(t) for t in (self.token,other)]
        result=UsageMigration(r).begin_drain(self.period)
        self.assertFalse(result['ready_for_execution'])
        after=[r.configuration(t) for t in (self.token,other)]
        for a,b in zip(before,after):
            self.assertEqual(b['revision'],a['revision']+1)
            self.assertEqual(b['nodes'][0]['clients'],[])
        self.assertEqual(UsageMigration(Registry(r.path)).begin_drain(self.period),result)
        self.assertEqual([r.configuration(t) for t in (self.token,other)],after)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT credential FROM node_users WHERE node_id=? AND user_id=?',(self.nid,self.uid)).fetchone()[0],self.item['credential'])

    def test_stale_ack_and_post_drain_ack_cannot_grant_execution(self):
        r=self.setup_total(); snap=r.configuration(self.token)
        data=dict(revision=snap['revision'],digest=snap['digest'],session_id=self.session)
        r.acknowledge_configuration(self.token,data)
        UsageMigration(r).begin_drain(self.period)
        with self.assertRaises(Error): r.acknowledge_runtime(self.token,data)
        self.assertEqual(self.applied(r)['nodes'][0]['clients'],[])
        with self.assertRaises(Error): self.report()
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_execution_grants').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM usage_deltas').fetchone()[0],0)

    def test_authorized_tail_is_bounded_and_retry_does_not_charge_twice(self):
        r=self.setup_total(); self.applied(r); UsageMigration(r).begin_drain(self.period)
        self.report()
        with patch('usage_ledger.time.time',return_value=self.now+400):
            self.assertTrue(self.report()['duplicate'])
            self.item['upload_bytes']=5
            with self.assertRaises(Error): self.report(2)
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],4)

    def test_drained_old_tail_cannot_borrow_new_receipt_period(self):
        r=self.setup_total(); self.applied(r); UsageMigration(r).begin_drain(self.period)
        later=self.cycles.create(self.uid,dict(starts_at=self.now+100,ends_at=self.now+200))['id']
        self.cycles.set_total_budget(later,0)
        with patch('usage_ledger.time.time',return_value=self.now+101):
            with self.assertRaises(Error) as error: self.report()
            self.assertEqual(error.exception.status,409)
            self.ledger.settle(self.token,self.data())
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT cycle_id FROM usage_deltas').fetchone()[0],self.period)
            self.assertEqual(db.execute('SELECT total_bytes FROM usage_cycle_confirmed_totals WHERE cycle_id=?',(self.period,)).fetchone()[0],'7')

    def test_retired_session_must_settle_and_rotated_token_remains_revoked(self):
        r=self.setup_total(); snap=self.applied(r); UsageMigration(r).begin_drain(self.period)
        r.begin_session(self.token,dict(session_id=str(uuid.uuid4())))
        with self.assertRaises(Error): self.report()
        self.ledger.settle(self.token,self.data())
        old=self.token; self.token=r.provision_agent(snap['server_id'])['token']
        for token in (old,self.token):
            with self.assertRaises(Error): self.ledger.settle(token,self.data())

    def test_held_reservation_survives_drain_without_reissue_or_timeout_refund(self):
        r=self.setup_total(); request=self.request(); a=TotalAllocations(r).reserve(self.token,request)
        UsageMigration(r).begin_drain(self.period)
        with self.assertRaises(Error): TotalAllocations(r).reserve(self.token,self.request(1))
        with patch('usage_migration.time.time',return_value=self.now+400):
            reopened=Registry(r.path)
            self.assertFalse(UsageMigration(reopened).begin_drain(self.period)['ready_for_execution'])
            self.assertEqual(TotalAllocations(reopened).reserve(self.token,request),a)
        self.assertEqual(self.reserved(r),6)

    def test_drain_and_history_seed_rollback_on_configuration_failure(self):
        r=self.setup_total(); before=r.configuration(self.token)
        with r.connect() as db:
            db.execute("CREATE TRIGGER reject_drain BEFORE UPDATE ON server_configs BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): UsageMigration(r).begin_drain(self.period)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_cycle_drains').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM usage_cycle_confirmed_totals').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT count(*) FROM usage_cycle_accounting_anchors').fetchone()[0],1)
            db.execute('DROP TRIGGER reject_drain')
        self.assertEqual(r.configuration(self.token),before)

    def test_invalid_cycle_and_zero_delta_do_not_create_execution(self):
        r=self.setup_total(); migration=UsageMigration(r)
        for value in ('',None,True):
            with self.assertRaises(Error): migration.begin_drain(value)
        with self.assertRaises(Error): migration.begin_drain('missing')
        migration.begin_drain(self.period)
        self.item.update(upload_bytes=0,download_bytes=0)
        # A zero report must not introduce an uncaptured identity after drain.
        with self.assertRaises(Error) as caught: self.report()
        self.assertEqual(caught.exception.status,403)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_counters').fetchone()[0],0)
        self.item.update(upload_bytes=1)
        with self.assertRaises(Error): self.report(2)
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],0)
        with self.assertRaises(Error) as caught: r.delete('users',self.uid)
        self.assertEqual(caught.exception.status,409)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_cycle_drains').fetchone()[0],1)
            self.assertIsNotNone(db.execute('SELECT 1 FROM users WHERE id=?',(self.uid,)).fetchone())


if __name__=='__main__': unittest.main()
