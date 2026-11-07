"""Internal aggregate ingestion guard; not a live distributed quota policy."""
from concurrent.futures import ThreadPoolExecutor
import sqlite3
import time
import unittest
import uuid
from unittest.mock import patch
from core import Registry, Error
from usage_cycles import UsageCycles
from usage_ledger import UsageLedger, MAX_COUNTER
import test_usage_ledger as base
import test_usage_settlement as settlement
import test_accounts


class TotalLedgerTests(unittest.TestCase):
    setUp=base.UsageTests.setUp
    tearDown=base.UsageTests.tearDown
    fixtures=base.UsageTests.fixtures
    fixture=base.UsageTests.fixture
    report=base.UsageTests.report
    applied=settlement.SettlementTests.applied
    data=settlement.SettlementTests.data

    def setup_total(self,total=7):
        r=self.fixture(); r.assign_plan(self.uid,{'plan_id':test_accounts.plan(r)})
        self.cycles=UsageCycles(r); self.now=time.time()
        self.period=self.cycles.create(self.uid,dict(starts_at=self.now-10,ends_at=self.now+100))['id']
        self.cycles.set_total_budget(self.period,total)
        self.item.update(upload_bytes=4,download_bytes=3)
        return r

    def test_duplex_limit_retry_restart_lowering_and_raising(self):
        r=self.setup_total(); self.report()
        self.ledger=UsageLedger(Registry(r.path)); self.assertTrue(self.report()['duplicate'])
        self.cycles.set_total_budget(self.period,0)
        self.assertTrue(self.report()['duplicate'])
        self.item['download_bytes']=4
        with self.assertRaises(Error) as caught: self.report(2)
        self.assertEqual(caught.exception.status,409)
        self.assertEqual(self.ledger.user(self.uid)['download_bytes'],3)
        self.cycles.set_total_budget(self.period,8); self.report(2)
        self.assertEqual(self.ledger.user(self.uid)['download_bytes'],4)
        self.cycles.set_total_budget(self.period,None)
        self.item['upload_bytes']=100; self.report(3)
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],100)

    def test_directional_cap_still_intersects_aggregate(self):
        self.setup_total(20)
        self.cycles.set_budget(self.period,dict(upload_bytes=3,download_bytes=20))
        with self.assertRaises(Error): self.report()
        self.item.update(upload_bytes=3,download_bytes=17); self.report()
        self.item['download_bytes']=18
        with self.assertRaises(Error): self.report(2)
        self.assertEqual(self.ledger.user(self.uid)['download_bytes'],17)

    def second_host(self,r):
        sid=r.save('servers',dict(name='second'))['id']
        nid=r.save('nodes',dict(name='second',server_id=sid))['id']
        r.configure_node(nid,test_accounts.test_xray.VLESS)
        token=r.provision_agent(sid)['token']; session=str(uuid.uuid4())
        r.begin_session(token,dict(session_id=session))
        item=dict(self.item,node_id=nid,credential=r.authorize_node_user(nid,self.uid)['uuid'])
        return token,dict(session_id=session,sequence=1,items=[item])

    def test_concurrent_servers_share_master_cap_without_partial_acceptance(self):
        r=self.setup_total(); other_token,other=self.second_host(r)
        first=self.data()
        def submit(args):
            try: self.ledger.report(*args); return True
            except Error as e:
                self.assertEqual(e.status,409); return False
        with ThreadPoolExecutor(2) as pool:
            self.assertEqual(sum(pool.map(submit,[(self.token,first),(other_token,other)])),1)
        total=self.ledger.user(self.uid)
        self.assertEqual(total['upload_bytes']+total['download_bytes'],7)
        with r.connect() as db: self.assertEqual(db.execute('SELECT count(*) FROM usage_reports').fetchone()[0],1)

    def test_over_cap_batch_rolls_back_all_items_and_revision(self):
        r=self.setup_total(); node=r.save('nodes',dict(name='extra',server_id=r.configuration(self.token)['server_id']))['id']
        r.configure_node(node,dict(test_accounts.test_xray.VLESS,listen_port=24501))
        extra=dict(self.item,node_id=node,credential=r.authorize_node_user(node,self.uid)['uuid'],upload_bytes=1,download_bytes=0)
        before=r.configuration(self.token)
        with self.assertRaises(Error): self.report(items=[self.item,extra])
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],0)
        self.assertEqual(r.configuration(self.token),before)
        self.assertFalse(self.report()['duplicate'])

    def test_validation_migration_and_policy_revision_rollback(self):
        r=self.setup_total(); before=r.configuration(self.token)
        self.cycles.set_total_budget(self.period,7)
        self.assertEqual(r.configuration(self.token),before)
        for bad in (True,-1,1.5,'7',MAX_COUNTER+1):
            with self.assertRaises(Error): self.cycles.set_total_budget(self.period,bad)
        with self.assertRaises(Error): self.cycles.set_total_budget('missing',1)
        with r.connect() as db:
            db.execute("CREATE TRIGGER reject_total BEFORE UPDATE ON server_configs BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.cycles.set_total_budget(self.period,8)
        self.assertEqual(self.cycles.total_budget(self.period)['total_bytes'],7)
        with r.connect() as db:
            db.execute('DROP TRIGGER reject_total'); db.execute('DROP TABLE usage_cycle_total_budgets')
        self.assertIsNone(UsageCycles(Registry(r.path)).total_budget(self.period))
        self.assertEqual(r.configuration(self.token)['nodes'][0]['clients'][0]['uuid'],self.item['credential'])

    def test_settlement_stays_in_executed_old_cycle_not_current_receipt_cycle(self):
        r=self.setup_total(); self.applied(r)
        later=self.cycles.create(self.uid,dict(starts_at=self.now+100,ends_at=self.now+200))['id']
        self.cycles.set_total_budget(later,0)
        with patch('usage_ledger.time.time',return_value=self.now+101):
            self.assertFalse(self.ledger.settle(self.token,self.data())['duplicate'])
            self.assertTrue(self.ledger.settle(self.token,self.data())['duplicate'])
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT cycle_id FROM usage_deltas').fetchone()[0],self.period)
        self.item['download_bytes']=4
        with self.assertRaises(Error): self.ledger.settle(self.token,self.data(2))
        self.assertEqual(self.ledger.user(self.uid)['download_bytes'],3)

    def test_zero_denies_positive_delta_and_unlimited_large_sum_is_exact(self):
        r=self.setup_total(0)
        with self.assertRaises(Error): self.report()
        self.item.update(upload_bytes=0,download_bytes=0); self.report()
        self.cycles.set_total_budget(self.period,None)
        self.item.update(upload_bytes=MAX_COUNTER,download_bytes=MAX_COUNTER); self.report(2)
        # Lowering after confirmed use must reject via Python arithmetic, not
        # overflow SQLite SUM when another host contributes a valid counter.
        token,data=self.second_host(r)
        data['items'][0].update(upload_bytes=1,download_bytes=1)
        self.ledger.report(token,data)
        self.cycles.set_total_budget(self.period,MAX_COUNTER)
        data['sequence']=2; data['items'][0]['upload_bytes']=2
        with self.assertRaises(Error) as caught: self.ledger.report(token,data)
        self.assertEqual(caught.exception.status,409)
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],MAX_COUNTER+1)

    def test_receipt_and_usage_rollback_if_revision_commit_fails(self):
        r=self.setup_total(); before=r.configuration(self.token)
        with r.connect() as db:
            db.execute("CREATE TRIGGER reject_usage_total BEFORE UPDATE ON server_configs BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.report()
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],0)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_reports').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM usage_deltas').fetchone()[0],0)
            db.execute('DROP TRIGGER reject_usage_total')
        self.assertEqual(r.configuration(self.token),before)
        self.assertFalse(self.report()['duplicate'])
        after=r.configuration(self.token)
        self.assertEqual(after['revision'],before['revision']+1)
        self.assertTrue(self.report()['duplicate']); self.assertEqual(r.configuration(self.token),after)
