"""Whole captured frontier accounting with real transactional SQLite state."""
import sqlite3
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from core import Registry,Error
from drain_seal import DrainSeal
from total_allocation import TotalAllocations
import test_drain_checkpoints as base
import test_total_cycle_ledger as ledger_base
import test_total_allocation as allocations


class DrainSealTests(unittest.TestCase):
    setUp=base.DrainCheckpointTests.setUp
    tearDown=base.DrainCheckpointTests.tearDown
    fixtures=base.DrainCheckpointTests.fixtures
    fixture=base.DrainCheckpointTests.fixture
    setup_total=base.DrainCheckpointTests.setup_total
    setup_drain=base.DrainCheckpointTests.setup_drain
    applied=base.DrainCheckpointTests.applied
    report=base.DrainCheckpointTests.report
    data=base.DrainCheckpointTests.data
    encode=staticmethod(base.DrainCheckpointTests.encode)
    envelope=base.DrainCheckpointTests.envelope
    accept=base.DrainCheckpointTests.accept
    second_host=ledger_base.TotalLedgerTests.second_host
    request=allocations.AllocationTests.request

    def test_missing_evidence_never_completes_by_timeout_or_empty_inventory(self):
        r=self.setup_drain(); gate=DrainSeal(r)
        with patch('drain_seal.time.time',return_value=self.now+10000):
            self.assertIn('missing_or_extra_checkpoints',gate.status(self.period)['blockers'])
            with self.assertRaises(Error): gate.seal(self.period)
        with r.connect() as db:
            db.execute('DELETE FROM usage_drain_inventory_captures')
            db.execute('DELETE FROM usage_drain_inventory')
        status=gate.status(self.period)
        self.assertIn('empty_frontier',status['blockers'])
        self.assertIn('frontier_not_captured',status['blockers'])
        self.assertFalse(status['ready_for_execution'])

    def test_two_hosts_need_every_checkpoint_and_exact_sum(self):
        r=self.setup_total(100); self.sid=self.applied(r)['server_id']; self.report()
        token,other=self.second_host(r)
        snap=r.configuration(token); ack=dict(revision=snap['revision'],digest=snap['digest'],session_id=other['session_id'])
        r.acknowledge_configuration(token,ack); r.acknowledge_runtime(token,ack)
        self.ledger.report(token,other)
        from usage_migration import UsageMigration
        self.migration=UsageMigration(r); self.migration.begin_drain(self.period)
        self.accept(); gate=DrainSeal(r)
        self.assertEqual(gate.status(self.period)['accepted_sessions'],1)
        with self.assertRaises(Error): gate.seal(self.period)
        body=dict(format=1,server_id=snap['server_id'],session_id=other['session_id'],items=other['items'],uncertain_writes=[])
        self.migration.accept_closed(self.period,token,self.encode(body))
        result=gate.seal(self.period)
        self.assertEqual((result['final_upload_bytes'],result['final_download_bytes']),(8,6))
        self.assertFalse(result['ready_for_execution'])

    def test_persistent_idempotent_seal_survives_supported_cleanup_and_budget_change(self):
        r=self.setup_drain(); self.accept(); gate=DrainSeal(r)
        self.assertTrue(gate.status(self.period)['evidence_complete'])
        first=gate.seal(self.period)
        self.assertTrue(self.report()['duplicate'])
        self.cycles.set_total_budget(self.period,0)
        r.delete('nodes',self.nid); r.delete('servers',self.sid)
        gate=DrainSeal(Registry(r.path)); retry=gate.seal(self.period)
        self.assertTrue(retry['duplicate']); self.assertEqual(first['digest'],retry['digest'])
        self.assertEqual(first['state'],'captured_legacy_evidence_sealed')
        self.assertFalse(gate.status(self.period)['ready_for_execution'])

    def test_unknown_or_missing_or_unattributed_baseline_blocks_without_repairing(self):
        r=self.setup_drain(); self.accept(); gate=DrainSeal(r)
        with r.connect() as db:
            db.execute("UPDATE usage_cycle_confirmed_totals SET total_bytes='8'")
            db.execute("UPDATE usage_cycle_confirmed_directions SET unknown_bytes='1'")
        with self.assertRaises(Error) as caught: gate.status(self.period)
        self.assertEqual(caught.exception.status,409)
        with self.assertRaises(Error): gate.seal(self.period)
        with r.connect() as db: db.execute('DELETE FROM usage_cycle_confirmed_directions')
        with self.assertRaises(Error) as caught: gate.status(self.period)
        self.assertEqual(caught.exception.status,409)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_cycle_confirmed_directions').fetchone()[0],0)

    def test_unknown_reservation_not_released_by_expiry_or_evidence(self):
        r=self.setup_total(100); self.sid=self.applied(r)['server_id']; self.report()
        TotalAllocations(r).reserve(self.token,self.request(6))
        from usage_migration import UsageMigration
        self.migration=UsageMigration(r); self.migration.begin_drain(self.period); self.accept()
        gate=DrainSeal(r)
        with patch('drain_seal.time.time',return_value=self.now+10000):
            self.assertIn('unresolved_allocations',gate.status(self.period)['blockers'])
            with self.assertRaises(Error): gate.seal(self.period)
        self.assertEqual(gate.status(self.period)['held_bytes'],6)

    def test_missing_current_counter_after_authenticated_acceptance_is_preserved(self):
        r=self.setup_drain(); self.accept()
        r.delete('nodes',self.nid); r.delete('servers',self.sid)
        self.assertEqual(DrainSeal(r).seal(self.period)['final_upload_bytes'],4)

    def test_insert_failure_leaves_no_seal_or_usage_mutation(self):
        r=self.setup_drain(); self.accept(); gate=DrainSeal(r)
        with r.connect() as db:
            db.execute("CREATE TRIGGER reject_seal BEFORE INSERT ON usage_drain_seals BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): gate.seal(self.period)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_drain_seals').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT total_bytes FROM usage_cycle_confirmed_totals').fetchone()[0],'7')
            db.execute('DROP TRIGGER reject_seal')
        self.assertFalse(gate.seal(self.period)['duplicate'])

    def test_uncaptured_execution_blocks_even_with_zero_usage(self):
        r=self.setup_drain(); self.accept()
        # Emulate an inconsistent legacy database with execution omitted from
        # its capture. Real current snapshots withhold all drained identities.
        with r.connect() as db:
            db.execute("UPDATE usage_drain_inventory SET session_id='missing'")
        self.assertIn('uncaptured_execution',DrainSeal(r).status(self.period)['blockers'])

    def test_concurrent_exact_sealing_creates_one_durable_record(self):
        r=self.setup_drain(); self.accept()
        with ThreadPoolExecutor(2) as pool:
            rows=list(pool.map(lambda _:DrainSeal(Registry(r.path)).seal(self.period),range(2)))
        self.assertEqual(sum(row['duplicate'] for row in rows),1)
        self.assertEqual(len({row['digest'] for row in rows}),1)

    def test_sealed_cycle_rejects_growth_even_for_uncaptured_identity(self):
        r=self.setup_drain(); self.accept(); DrainSeal(r).seal(self.period)
        nid=r.save('nodes',dict(name='legacy-untracked',server_id=self.sid))['id']
        import test_xray
        r.configure_node(nid,dict(test_xray.VLESS,listen_port=24502))
        credential=r.authorize_node_user(nid,self.uid)['uuid']
        # Fault-injected old database: an uncaptured grant appears after seal.
        # No new runtime ACK is issued and no such grant is publicly enabled.
        with r.connect() as db:
            db.execute('''INSERT INTO usage_execution_grants
                SELECT server_id,session_id,?,user_id,?,cycle_id,token_hash,issued_at,settle_until
                FROM usage_execution_grants WHERE node_id=?''',(nid,credential,self.nid))
        item=dict(self.item,node_id=nid,credential=credential,upload_bytes=1,download_bytes=0)
        for method in (self.ledger.report,self.ledger.settle):
            with self.assertRaises(Error) as caught:
                method(self.token,dict(session_id=self.session,sequence=2,items=[item]))
            self.assertEqual(caught.exception.status,409)
            self.assertIn('已封存',str(caught.exception))
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT total_bytes FROM usage_cycle_confirmed_totals').fetchone()[0],'7')
            self.assertEqual(db.execute('SELECT count(*) FROM usage_counters WHERE node_id=?',(nid,)).fetchone()[0],0)
        self.assertTrue(self.ledger.settle(self.token,self.data())['duplicate'])

    def test_duplex_total_beyond_int64_keeps_exact_confirmation(self):
        r=self.setup_total(None); self.sid=self.applied(r)['server_id']
        from usage_ledger import MAX_COUNTER
        self.item.update(upload_bytes=MAX_COUNTER,download_bytes=MAX_COUNTER)
        self.report()
        from usage_migration import UsageMigration
        self.migration=UsageMigration(r); self.migration.begin_drain(self.period); self.accept()
        result=DrainSeal(r).seal(self.period)
        self.assertEqual(result['final_upload_bytes']+result['final_download_bytes'],2*MAX_COUNTER)
        self.assertEqual(DrainSeal(Registry(r.path)).status(self.period)['digest'],result['digest'])


if __name__=='__main__': unittest.main()
