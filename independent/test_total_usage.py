"""Resource cleanup must not mint confirmed aggregate cycle balance."""
import sqlite3
import unittest
from core import Registry,Error
from total_allocation import TotalAllocations
from total_usage import current,add
from usage_ledger import MAX_COUNTER
import test_total_allocation as base


class ConfirmedTests(unittest.TestCase):
    setUp=base.AllocationTests.setUp
    tearDown=base.AllocationTests.tearDown
    fixtures=base.AllocationTests.fixtures
    fixture=base.AllocationTests.fixture
    setup_total=base.AllocationTests.setup_total
    second_host=base.AllocationTests.second_host
    report=base.AllocationTests.report
    request=base.AllocationTests.request

    def total(self,r):
        with r.connect() as db:
            db.execute('BEGIN IMMEDIATE'); return current(db,self.period)

    def test_deleted_node_and_server_do_not_restore_allocation_balance(self):
        r=self.setup_total(20); self.report()
        token,data=self.second_host(r)
        sid=r.configuration(self.token)['server_id']
        r.delete('nodes',self.nid); r.delete('servers',sid)
        self.assertEqual(self.total(Registry(r.path)),7)
        req={**self.request(20),'node_id':data['items'][0]['node_id'],
             'credential':data['items'][0]['credential'],'session_id':data['session_id']}
        self.assertEqual(TotalAllocations(r).reserve(token,req)['reserved_bytes'],13)

    def test_deleted_detail_still_counts_against_legacy_reports(self):
        r=self.setup_total(7); self.report(); token,data=self.second_host(r)
        r.delete('nodes',self.nid)
        data['items'][0].update(upload_bytes=1,download_bytes=0)
        with self.assertRaises(Error): self.ledger.report(token,data)
        self.assertEqual(self.total(r),7)

    def test_retry_and_increment_are_counted_once(self):
        r=self.setup_total(30); self.report(); self.report()
        self.item['upload_bytes']=6; self.report(2); self.report(2)
        self.assertEqual(self.total(r),9)
        self.assertEqual(self.total(r),9)
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],6)

    def test_missing_confirmed_after_marker_blocks_without_lazy_repair(self):
        r=self.setup_total(20); self.report()
        with r.connect() as db: db.execute('DELETE FROM usage_cycle_confirmed_totals')
        r=Registry(r.path)
        with self.assertRaises(Error) as caught: r.delete('nodes',self.nid)
        self.assertEqual(caught.exception.status,409)
        with self.assertRaises(Error): self.total(r)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_cycle_confirmed_totals').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM usage_cycle_accounting_anchors').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT count(*) FROM usage_cycle_accounting_events').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT count(*) FROM usage_deltas').fetchone()[0],1)

    def test_atomic_failure_does_not_advance_confirmed_usage(self):
        r=self.setup_total(20); self.report()
        with r.connect() as db:
            db.execute("CREATE TRIGGER fail_confirmed BEFORE UPDATE ON usage_cycle_confirmed_totals BEGIN SELECT RAISE(ABORT,'fixture'); END")
        self.item['upload_bytes']=5
        with self.assertRaises(sqlite3.IntegrityError): self.report(2)
        self.assertEqual(self.total(r),7)
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],4)
        with r.connect() as db: db.execute('DROP TRIGGER fail_confirmed')
        self.report(2); self.assertEqual(self.total(r),8)

    def test_arbitrary_precision_and_owner_delete_preserves_liability(self):
        r=self.setup_total(None)
        self.item.update(upload_bytes=MAX_COUNTER,download_bytes=MAX_COUNTER)
        self.report(); self.assertEqual(self.total(r),MAX_COUNTER*2)
        r.delete('nodes',self.nid)
        self.assertEqual(self.total(Registry(r.path)),MAX_COUNTER*2)
        with self.assertRaises(Error) as caught: r.delete('users',self.uid)
        self.assertEqual(caught.exception.status,409)
        self.assertEqual(self.total(r),MAX_COUNTER*2)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_cycle_confirmed_totals').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT count(*) FROM usage_cycle_accounting_events').fetchone()[0],1)

    def test_tagged_claims_do_not_increment_confirmed_totals(self):
        r=self.setup_total(20); api=TotalAllocations(r); a=api.reserve(self.token,self.request(10))
        api.report(self.token,dict(allocation_id=a['allocation_id'],policy_id=a['policy_id'],
            session_id=self.session,sequence=1,upload_bytes=2,download_bytes=3))
        self.assertEqual(self.total(r),0)
        self.report(); self.assertEqual(self.total(r),7)

    def test_helpers_require_transaction_and_valid_increment(self):
        r=self.setup_total()
        with r.connect() as db:
            with self.assertRaises(RuntimeError): current(db,self.period)
            db.execute('BEGIN IMMEDIATE')
            for value in (-1,True,1.5):
                with self.assertRaises(ValueError): add(db,self.period,value)

    def share_delete(self,clean):
        from sharing import Shares
        from test_sharing import settings
        r=self.setup_total(20); self.report(); shares=Shares(r)
        sid=r.configuration(self.token)['server_id']
        invite=shares.create(sid,settings()); access=shares.claim(invite['invite_token'])['access_token']
        with r.connect() as db:
            db.execute('INSERT INTO shared_nodes VALUES(?,?)',(self.nid,invite['id']))
            db.execute('DELETE FROM usage_cycle_confirmed_totals') # upgrade baseline
        with self.assertRaises(Error) as caught:
            if clean:
                with r.connect() as db:
                    db.execute('BEGIN IMMEDIATE')
                    row=db.execute('SELECT * FROM server_shares WHERE id=?',(invite['id'],)).fetchone()
                    Shares.clean(db,row,'revoked')
            else: shares.guest(access,'delete',self.nid)
        self.assertEqual(caught.exception.status,409)
        with self.assertRaises(Error): self.total(r)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_cycle_confirmed_totals').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM usage_cycle_accounting_anchors').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT count(*) FROM usage_cycle_accounting_events').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT count(*) FROM usage_deltas').fetchone()[0],1)

    def test_guest_node_delete_blocks_on_damaged_accounting(self): self.share_delete(False)

    def test_share_cleanup_blocks_on_damaged_accounting(self): self.share_delete(True)
