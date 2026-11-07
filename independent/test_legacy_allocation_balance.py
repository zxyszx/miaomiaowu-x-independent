"""Conservative untagged/held coexistence; not a migration activation test."""
from concurrent.futures import ThreadPoolExecutor
import unittest
from unittest.mock import patch
from core import Registry,Error
from total_allocation import TotalAllocations,held
from usage_ledger import UsageLedger
import test_total_allocation as base
import test_total_cycle_ledger as ledger_base


class LegacyBalanceTests(unittest.TestCase):
    setUp=base.AllocationTests.setUp
    tearDown=base.AllocationTests.tearDown
    fixtures=base.AllocationTests.fixtures
    fixture=base.AllocationTests.fixture
    setup_total=base.AllocationTests.setup_total
    second_host=base.AllocationTests.second_host
    report=base.AllocationTests.report
    request=base.AllocationTests.request
    reserved=base.AllocationTests.reserved
    data=ledger_base.TotalLedgerTests.data
    applied=ledger_base.TotalLedgerTests.applied

    def test_untagged_growth_cannot_spend_reserved_balance_restart_and_retry(self):
        r=self.setup_total(20);api=TotalAllocations(r)
        allocation=api.reserve(self.token,self.request(10))
        self.report() # 7 confirmed, 10 held, 3 free
        self.item.update(upload_bytes=7,download_bytes=3);self.report(2)
        self.item['upload_bytes']=8;before=r.configuration(self.token)
        with self.assertRaises(Error): self.report(3)
        self.assertEqual(r.configuration(self.token),before)
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],7)
        self.assertEqual(self.reserved(r),10)
        self.ledger=UsageLedger(Registry(r.path))
        self.item['upload_bytes']=7
        self.assertTrue(self.report(2)['duplicate'])
        self.cycles.set_total_budget(self.period,0)
        self.assertTrue(self.report(2)['duplicate'])
        self.assertEqual(allocation['reserved_bytes'],10)
        self.assertEqual(self.reserved(r),10)

    def test_reserve_and_legacy_ingest_race_share_atomic_balance(self):
        r=self.setup_total(10);self.item.update(upload_bytes=4,download_bytes=3)
        def reserve():
            return TotalAllocations(Registry(r.path)).reserve(self.token,self.request(6))['reserved_bytes']
        def report():
            try: UsageLedger(Registry(r.path)).report(self.token,self.data());return 7
            except Error as e:
                self.assertEqual(e.status,409);return 0
        with ThreadPoolExecutor(2) as pool:
            a=pool.submit(reserve);b=pool.submit(report);reserved,confirmed=a.result(),b.result()
        self.assertIn((reserved,confirmed),((6,0),(3,7)))
        self.assertLessEqual(reserved+confirmed,10)

    def test_expiry_policy_change_claim_and_server_deletion_do_not_refund_hold(self):
        r=self.setup_total(10);api=TotalAllocations(r);a=api.reserve(self.token,self.request(6))
        api.report(self.token,dict(allocation_id=a['allocation_id'],policy_id=a['policy_id'],session_id=self.session,sequence=1,upload_bytes=2,download_bytes=3))
        self.cycles.set_total_budget(self.period,None);self.cycles.set_total_budget(self.period,10)
        with patch('usage_ledger.time.time',return_value=a['valid_until']+1):
            with self.assertRaises(Error): self.report()
        self.assertEqual(self.reserved(r),6)
        token,other=self.second_host(r)
        sid=r.configuration(self.token)['server_id'];r.delete('nodes',self.nid);r.delete('servers',sid)
        self.assertEqual(self.reserved(r),6)
        with self.assertRaises(Error): self.ledger.report(token,other)

    def test_batch_failure_rolls_back_first_counter_confirmed_and_revision(self):
        r=self.setup_total(10);TotalAllocations(r).reserve(self.token,self.request(5))
        import test_accounts
        nid=r.save('nodes',dict(name='extra',server_id=r.configuration(self.token)['server_id']))['id']
        r.configure_node(nid,dict(test_accounts.test_xray.VLESS,listen_port=24501))
        first=dict(self.item,upload_bytes=3,download_bytes=0)
        second=dict(first,node_id=nid,credential=r.authorize_node_user(nid,self.uid)['uuid'])
        before=r.configuration(self.token)
        with self.assertRaises(Error): self.report(items=[first,second])
        self.assertEqual(r.configuration(self.token),before)
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],0)
        with r.connect() as db:
            for table in ('usage_counters','usage_reports','usage_deltas'):
                self.assertEqual(db.execute('SELECT count(*) FROM '+table).fetchone()[0],0)

    def test_unlimited_and_cycle_isolation_do_not_borrow_or_release(self):
        r=self.setup_total(None);api=TotalAllocations(r);api.reserve(self.token,self.request(2**63-1));api.reserve(self.token,self.request(2**63-1))
        self.report()
        other=self.cycles.create(self.uid,dict(starts_at=self.now+200,ends_at=self.now+300))
        with r.connect() as db:
            db.execute('BEGIN')
            self.assertEqual(held(db,self.period),2*(2**63-1))
            self.assertEqual(held(db,other['id']),0)
        self.cycles.set_total_budget(self.period,2**63-1)
        self.item['upload_bytes']=5
        with self.assertRaises(Error): self.report(2)
        self.assertEqual(self.reserved(r),2*(2**63-1))

    def test_original_bounded_settlement_not_misattributed_to_hold(self):
        r=self.setup_total(10);self.applied(r)
        a=TotalAllocations(r).reserve(self.token,self.request(6))
        with self.assertRaises(Error): self.report()
        self.assertFalse(self.ledger.settle(self.token,self.data())['duplicate'])
        self.assertEqual(self.reserved(r),a['reserved_bytes'])
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],4)
        # Old tail plus held is now over the limit: no further allocation,
        # no invented release or activation of this unresolved migration state.
        with self.assertRaises(Error): TotalAllocations(r).reserve(self.token,self.request(1))
        with r.connect() as db:
            db.execute('BEGIN')
            self.assertEqual(held(db,self.period),6)


if __name__=='__main__': unittest.main()
