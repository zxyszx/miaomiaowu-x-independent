"""Finite ACK direction history supports bounded accounting, never traffic."""
import json
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from core import Error,Registry
from usage_ledger import UsageLedger
import test_total_cycle_ledger as base


class DirectionTailTests(unittest.TestCase):
    setUp=base.TotalLedgerTests.setUp
    tearDown=base.TotalLedgerTests.tearDown
    fixtures=base.TotalLedgerTests.fixtures
    fixture=base.TotalLedgerTests.fixture
    setup_total=base.TotalLedgerTests.setup_total
    applied=base.TotalLedgerTests.applied
    data=base.TotalLedgerTests.data
    report=base.TotalLedgerTests.report
    second_host=base.TotalLedgerTests.second_host

    def old_budget(self,up=10,down=10):
        r=self.setup_total(None)
        with r.connect() as db: db.execute('UPDATE usage_cycles SET ends_at=? WHERE id=?',(self.now+1000,self.period))
        self.cycles.set_budget(self.period,dict(upload_bytes=up,download_bytes=down))
        self.applied(r)
        return r

    def lower(self):
        self.cycles.set_budget(self.period,dict(upload_bytes=0,download_bytes=0))

    def test_lowered_tail_retry_restart_and_global_ceiling(self):
        r=self.old_budget(); self.lower()
        with self.assertRaises(Error): self.report()
        self.assertFalse(self.ledger.settle(self.token,self.data())['duplicate'])
        self.ledger=UsageLedger(Registry(r.path))
        self.assertTrue(self.ledger.settle(self.token,self.data())['duplicate'])
        self.item.update(upload_bytes=10,download_bytes=10)
        self.ledger.settle(self.token,self.data(2))
        self.item['upload_bytes']=11
        with self.assertRaises(Error): self.ledger.settle(self.token,self.data(3))
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],10)

    def test_later_ack_does_not_extend_old_version_exception(self):
        r=self.old_budget()
        with r.connect() as db:
            old_end=db.execute('SELECT settle_until FROM usage_execution_direction_receipts').fetchone()[0]
        self.lower()
        with patch('usage_settlement.time.time',return_value=old_end-1): self.applied(r)
        with patch('usage_ledger.time.time',return_value=old_end+1):
            with self.assertRaises(Error) as caught: self.ledger.settle(self.token,self.data())
        self.assertEqual(caught.exception.status,409)
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],0)

    def test_missing_null_or_inconsistent_baseline_cannot_create_allowance(self):
        for mode in ('missing','null','baseline'):
            with self.subTest(mode=mode):
                r=self.old_budget(); self.lower()
                with r.connect() as db:
                    if mode=='missing': db.execute('DELETE FROM usage_execution_direction_receipts')
                    elif mode=='null': db.execute("UPDATE usage_execution_direction_receipts SET budget_json='null',baseline_json='null'")
                    else: db.execute('UPDATE usage_execution_direction_receipts SET baseline_json=?',(json.dumps(dict(upload_bytes=1,download_bytes=0)),))
                with self.assertRaises(Error): self.ledger.settle(self.token,self.data())
                self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],0)

    def test_one_intact_version_not_directional_mix(self):
        r=self.old_budget(10,1)
        self.cycles.set_budget(self.period,dict(upload_bytes=1,download_bytes=10)); self.applied(r)
        self.lower(); self.item.update(upload_bytes=5,download_bytes=5)
        with self.assertRaises(Error): self.ledger.settle(self.token,self.data())
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],0)

    def test_servers_share_ceiling_not_sum_of_historical_caps(self):
        r=self.old_budget(); other_token,other=self.second_host(r)
        snap=r.configuration(other_token)
        ack=dict(session_id=other['session_id'],revision=snap['revision'],digest=snap['digest'])
        r.acknowledge_configuration(other_token,ack);r.acknowledge_runtime(other_token,ack)
        self.lower()
        self.item.update(upload_bytes=6,download_bytes=6)
        other['items'][0].update(upload_bytes=6,download_bytes=6)
        def submit(args):
            try: self.ledger.settle(*args);return True
            except Error as e:
                self.assertEqual(e.status,409);return False
        with ThreadPoolExecutor(2) as pool:
            self.assertEqual(sum(pool.map(submit,[(self.token,self.data()),(other_token,other)])),1)
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],6)
        with r.connect() as db: self.assertEqual(db.execute('SELECT count(*) FROM usage_reports').fetchone()[0],1)

    def test_direction_proof_never_relaxes_aggregate_and_batch_rolls_back(self):
        r=self.old_budget();self.lower();self.cycles.set_total_budget(self.period,3)
        before=r.configuration(self.token)
        with self.assertRaises(Error): self.ledger.settle(self.token,self.data())
        self.assertEqual(r.configuration(self.token),before)
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],0)
        self.cycles.set_total_budget(self.period,None)
        # Two authorized nodes in a single batch must share the original cap.
        import test_accounts
        nid=r.save('nodes',dict(name='extra',server_id=before['server_id']))['id']
        r.configure_node(nid,dict(test_accounts.test_xray.VLESS,listen_port=24501))
        extra=dict(self.item,node_id=nid,credential=r.authorize_node_user(nid,self.uid)['uuid'],upload_bytes=8,download_bytes=8)
        self.cycles.set_budget(self.period,dict(upload_bytes=10,download_bytes=10)); self.applied(r); self.lower()
        before=r.configuration(self.token)
        data=self.data();data['items'].append(extra)
        with self.assertRaises(Error): self.ledger.settle(self.token,data)
        self.assertEqual(r.configuration(self.token),before)
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],0)
        with r.connect() as db: self.assertEqual(db.execute('SELECT count(*) FROM usage_reports').fetchone()[0],0)


    def test_unknown_direction_history_stays_blocked(self):
        r=self.old_budget();self.lower()
        with r.connect() as db:
            db.execute('UPDATE usage_cycle_confirmed_totals SET total_bytes=? WHERE cycle_id=?',('1',self.period))
            db.execute('UPDATE usage_cycle_confirmed_directions SET unknown_bytes=? WHERE cycle_id=?',('1',self.period))
        with self.assertRaises(Error): self.ledger.settle(self.token,self.data())
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT total_bytes FROM usage_cycle_confirmed_totals WHERE cycle_id=?',(self.period,)).fetchone()[0],'1')
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],0)


if __name__=='__main__': unittest.main()
