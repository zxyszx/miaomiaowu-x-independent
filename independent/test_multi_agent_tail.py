"""E1 fixture-only authorities, tickets/fences and never-delivered final bodies."""
import asyncio
import copy
import json
import sqlite3
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from core import Registry,Error
from grant_agent import GrantStageStore
from grant_capabilities import GrantCapabilities,STAGING,STAGE_IDENTITY
from multi_agent_allocation import MultiAgentAllocations
from multi_agent_configuration import MultiAgentConfiguration
from multi_agent_lifecycle import MultiAgentLifecycle,MULTI_EXECUTION
from multi_agent_meter import MultiAgentMeter
from multi_agent_tail import MultiAgentTail,TAIL,unpack,sealed
from multi_agent_tail_store import MultiTailStore
from multi_agent_test_runtime import MultiAgentTestHarness
from node_config import digest
from test_xray import VLESS,BINARY
from test_runtime import free_port
import test_multi_agent_allocation as base


@unittest.skipUnless(BINARY.is_file(),'real Xray required')
class TailTests(unittest.TestCase):
    def setUp(self):
        self.h=MultiAgentTestHarness();self.f=base.MultiAgentAllocationTests();self.f.setUp()
        self.f.registry=self.h.registry;self.r=self.f.start(total=100,first_bytes=6)
        self.token,self.host=self.f.host(self.r)
        self.r.configure_node(self.host['node_id'],dict(VLESS,listen_host='127.0.0.1',listen_port=free_port()))
        GrantCapabilities(self.r).negotiate(self.token,dict(session_id=self.host['session_id'],
            protocols=[STAGING,STAGE_IDENTITY,MULTI_EXECUTION,TAIL]))
        self.a=MultiAgentAllocations(self.r).allocate(self.token,self.f.request(self.host,10))
        self.api=MultiAgentTail(self.r)
        self.auth=self.api.enroll(self.token,self.a['allocation_id'],self.host['session_id'])
        self.value=self.auth['base_binding']
        self.local=MultiTailStore(self.h,self.auth)
        self.h.tail_stores[self.a['allocation_id']]=self.local
        stage=GrantStageStore(self.h.path.parent/'tail-stage.db',self.value['server_id'],self.host['session_id'])
        stage.stage(self.value,dict(accepted=True,state='ledger_resolved',ledger_kind='multi_allocation',
            ledger_id=self.a['allocation_id'],binding_digest=self.value['digest'],ready_for_execution=False))
        self.runtime,self.receipt=asyncio.run(self.h.install(self.token,stage,self.value,BINARY))
        self.process=self.runtime.fixture_process

    def tearDown(self):
        asyncio.run(self.h.close());self.f.tearDown()

    def success(self,up=2,down=1):
        for direction,count in enumerate((up,down)):
            if count:
                ticket=self.local.reserve(direction,count);self.local.complete(ticket)

    def test_deadline_rechecked_after_waiting_for_local_transaction(self):
        now=time.time()
        with patch('multi_agent_tail_store.time.time',side_effect=[now,self.auth['execute_until']]):
            with self.assertRaises(Error):self.local.reserve(0,1)
        self.assertIsNotNone(self.process.returncode)
        with self.local.db() as db:self.assertEqual(db.execute('SELECT count(*) FROM tail_tickets').fetchone()[0],0)

    def test_reservation_storage_failure_closes_process_without_new_ticket(self):
        with self.local.db() as db:
            db.execute("CREATE TRIGGER no_ticket BEFORE INSERT ON tail_tickets BEGIN SELECT RAISE(ABORT,'fault'); END")
        with self.assertRaises(sqlite3.DatabaseError):self.local.reserve(0,1)
        self.assertIsNotNone(self.process.returncode)
        with self.assertRaises(Error):self.local.freeze()

    def test_reopen_checks_final_against_ticket_and_reconciliation_evidence(self):
        self.success();self.local.stop();self.local.reconcile(self.token);self.local.freeze()
        with self.local.db() as db:db.execute('UPDATE tail_tickets SET bytes=bytes+1 WHERE direction=0')
        reopened=MultiTailStore(self.h,self.auth)
        with self.assertRaises(Error):reopened.freeze()

    def test_typed_history_rejects_rehashed_wrong_session_without_growth(self):
        self.success();body=self.local.ordinary();MultiAgentMeter(self.r).report(self.token,body)
        before=self.accounting()
        forged=copy.deepcopy(body);forged['session_id']=self.auth['execution_id'];forged.pop('body_digest');forged=sealed(forged)
        from multi_agent_lifecycle import encoded
        with self.r.connect() as db:
            db.execute('UPDATE usage_multi_tail_ordinary SET body=?,digest=? WHERE allocation_id=?',
                       (encoded(forged),forged['body_digest'],self.a['allocation_id']))
        self.local.stop()
        with self.assertRaises(Error):self.local.reconcile(self.token)
        self.assertEqual(self.accounting(),before)

    def test_master_rehashed_fence_conflict_rejected_on_retry_and_query(self):
        self.success();body=self.local.ordinary();self.local.stop();proof=self.local.reconcile(self.token)
        from multi_agent_lifecycle import encoded
        forged=copy.deepcopy(proof['fence']);forged['sequence']=7;forged.pop('body_digest');forged=sealed(forged)
        with self.r.connect() as db:
            db.execute('UPDATE usage_multi_tail_fences SET receipt=? WHERE allocation_id=?',(encoded(forged),self.a['allocation_id']))
        with self.assertRaises(Error):self.api.prepare(self.token,proof['fence']['request'])
        query={k:body[k] for k in ('ledger_kind','ledger_id','execution_id','session_id','authority_digest','operation','sequence','body_digest')}
        with self.assertRaises(Error):self.api.query(self.token,query)

    def test_rehashed_reconciliation_cannot_hide_unknown_ordinary(self):
        self.success();self.local.ordinary();self.local.stop();proof=self.local.reconcile(self.token)
        proof['results']=[];proof.pop('body_digest');proof=sealed(proof)
        from multi_agent_lifecycle import encoded
        with self.local.db() as db:db.execute('UPDATE local_tail SET reconciliation=?',(encoded(proof),))
        with self.assertRaises(Error):self.local.freeze()

    def test_iss008_locally_self_consistent_rehash_cannot_replace_master_proof(self):
        from multi_agent_lifecycle import encoded
        self.success();body=self.local.ordinary();self.local.stop();proof=self.local.reconcile(self.token)
        original=copy.deepcopy(proof['fence']);before=self.accounting()
        fence=copy.deepcopy(original);fence.pop('body_digest')
        fence.update(sequence=1,upload_bytes=2,download_bytes=1,
                     ordinary=[dict(sequence=1,body_digest=body['body_digest'])]);fence=sealed(fence)
        response=copy.deepcopy(proof['results'][0]);response.pop('body_digest')
        response.update(accepted=True,ordinary=body,fence_digest=fence['body_digest']);response=sealed(response)
        forged=sealed(dict(fence=fence,results=[response],requests=[body]))
        with self.local.db() as db:
            db.execute("UPDATE tail_ordinary SET state='acked'")
            db.execute('UPDATE local_tail SET reconciliation=?',(encoded(forged),))
        for local in (self.local,MultiTailStore(self.h,self.auth)):
            with self.assertRaises(Error):local.freeze()
        with self.local.db() as db:self.assertIsNone(db.execute('SELECT final FROM local_tail').fetchone()[0])
        with self.r.connect() as db:
            self.assertEqual(json.loads(db.execute('SELECT receipt FROM usage_multi_tail_fences').fetchone()[0]),original)
        self.assertEqual(self.accounting(),before)

    def test_iss008_first_freeze_rejects_post_fence_ticket(self):
        self.local.stop();self.local.reconcile(self.token)
        with self.local.db() as db:db.execute('INSERT INTO tail_tickets(direction,bytes,successful) VALUES(0,2,1)')
        for local in (self.local,MultiTailStore(self.h,self.auth)):
            with self.assertRaises(Error):local.freeze()
        with self.local.db() as db:self.assertIsNone(db.execute('SELECT final FROM local_tail').fetchone()[0])

    def test_stop_ticket_manifest_rejects_prefence_reservation_change(self):
        self.success();self.local.stop();self.local.reconcile(self.token)
        with self.local.db() as db:db.execute('UPDATE tail_tickets SET bytes=bytes+1 WHERE direction=0')
        with self.assertRaises(Error):self.local.freeze()

    def test_ticket_stop_complete_race_has_one_persistent_fence(self):
        import threading
        barrier=threading.Barrier(3)
        def reserve():
            barrier.wait()
            try:return self.local.reserve(0,1)
            except Error:return None
        def stop():barrier.wait();return self.local.stop()
        with ThreadPoolExecutor(2) as pool:
            r=pool.submit(reserve);s=pool.submit(stop);barrier.wait();ticket=r.result();intent=s.result()
        if ticket is not None:
            self.assertLessEqual(ticket,intent['fence_sequence']);self.local.complete(ticket)
        with self.assertRaises(Error):self.local.reserve(0,1)
        self.local.reconcile(self.token);final=self.local.freeze()
        self.assertEqual(final['upload_bytes'],int(ticket is not None))

    def balances(self):
        from total_usage import directions
        from total_allocation import held
        from usage_accounting import liability
        with self.r.connect() as db:
            db.execute('BEGIN')
            return directions(db,self.f.period),held(db,self.f.period),liability(db,self.f.period)

    def accounting(self):
        with self.r.connect() as db:
            return tuple(tuple(tuple(r) for r in db.execute('SELECT * FROM '+table)) for table in
                ('usage_multi_meter','usage_attributed_sources','usage_cycle_accounting_events',
                 'usage_cycle_confirmed_totals','usage_cycle_confirmed_directions'))

    def test_new_authority_and_facts_fixed_before_install_no_production_gate(self):
        self.assertEqual(self.api.enroll(self.token,self.a['allocation_id'],self.host['session_id']),self.auth)
        self.assertEqual(self.auth['settle_until'],self.auth['execute_until']+300)
        fact=self.receipt['execution']
        self.assertEqual(fact['tail_protocol'],TAIL)
        self.assertEqual(fact['authority_digest'],self.auth['body_digest'])
        self.assertEqual(fact['settle_until'],self.auth['settle_until'])
        before=self.accounting()
        with self.assertRaises(Error):MultiAgentTail(Registry(self.r.path)).enroll(self.token,self.a['allocation_id'],self.host['session_id'])
        self.assertEqual(self.accounting(),before)
        old=MultiAgentAllocations(self.r).allocate(self.token,self.f.request(self.host,1))
        MultiAgentConfiguration(self.r).bind(self.token,old['allocation_id'],session_id=self.host['session_id'])
        with self.assertRaises(Error):self.api.enroll(self.token,old['allocation_id'],self.host['session_id'])

    def test_committed_ordinary_lost_ack_stop_reconcile_and_freeze_once(self):
        self.success();ordinary=self.local.ordinary()
        MultiAgentMeter(self.r).report(self.token,ordinary) # simulated lost response
        before=self.balances();self.local.stop()
        self.assertIsNotNone(self.process.returncode)
        proof=self.local.reconcile(self.token)
        self.assertTrue(proof['results'][0]['accepted'])
        final=self.local.freeze();self.assertEqual(final['last_ordinary_sequence'],1)
        self.assertEqual((final['upload_bytes'],final['download_bytes']),(2,1))
        self.assertEqual(self.local.freeze(),final);self.assertEqual(self.balances(),before)
        reopened=MultiTailStore(self.h,self.auth)
        self.assertEqual(reopened.freeze(),final)
        with self.assertRaises(Error):reopened.reserve(0,1)
        self.assertFalse(final.get('ready_for_execution',False))

    def test_uncommitted_pending_is_not_resent_and_final_covers_known_bytes(self):
        self.success();ordinary=self.local.ordinary();before=self.accounting()
        self.local.stop();proof=self.local.reconcile(self.token)
        self.assertFalse(proof['results'][0]['accepted'])
        with self.assertRaises(Error):MultiAgentMeter(self.r).report(self.token,ordinary)
        final=self.local.freeze()
        self.assertEqual(final['last_ordinary_sequence'],0)
        self.assertEqual(final['final_sequence'],2)
        self.assertEqual(final['upload_bytes']+final['download_bytes'],3)
        self.assertEqual(self.accounting(),before) # E1 final is not delivered

    def test_ordinary_and_prepare_race_has_one_persistent_snapshot(self):
        self.success();body=self.local.ordinary();intent=self.local.stop()
        request=self.local._prepare_request(intent)
        def report():
            try:return MultiAgentMeter(self.r).report(self.token,body)['accepted']
            except Error:return False
        with ThreadPoolExecutor(2) as pool:
            a=pool.submit(report);b=pool.submit(self.api.prepare,self.token,request)
            accepted=a.result();fence=b.result()
        self.assertEqual(fence['sequence'],int(accepted))
        self.assertEqual(self.api.prepare(self.token,request),fence)
        self.local.reconcile(self.token);self.local.freeze()

    def test_stop_is_independent_of_rotated_token_and_new_session(self):
        self.success();self.r.provision_agent(self.value['server_id'])
        self.local.stop();self.assertIsNotNone(self.process.returncode)
        before=self.accounting()
        with self.assertRaises(Error):self.local.reconcile(self.token)
        with self.assertRaises(Error):self.local.freeze()
        self.assertEqual(self.accounting(),before)

    def test_stop_fence_races_reservation_and_unknown_is_never_confirmed(self):
        ticket=self.local.reserve(0,3)
        self.local.stop()
        with self.assertRaises(Error):self.local.reserve(1,1)
        self.local.abandon(ticket)
        self.local.reconcile(self.token);body=self.local.freeze()
        self.assertEqual((body['upload_bytes'],body['download_bytes']),(0,0))
        with self.assertRaises(Error):self.local.complete(ticket)
        self.assertEqual(self.balances()[0],(4,3,0))

    def test_stop_storage_failure_still_stops_real_process(self):
        with self.local.db() as db:
            db.execute("CREATE TRIGGER fail BEFORE UPDATE OF stop ON local_tail BEGIN SELECT RAISE(ABORT,'disk'); END")
        with self.assertRaises(sqlite3.IntegrityError):self.local.stop()
        self.assertIsNotNone(self.process.returncode)
        with self.assertRaises(Error):self.local.freeze()

    def test_freeze_failure_rolls_back_and_reopen_keeps_first_body(self):
        self.success();self.local.stop();self.local.reconcile(self.token)
        with self.local.db() as db:
            db.execute("CREATE TRIGGER fail BEFORE UPDATE OF final ON local_tail BEGIN SELECT RAISE(ABORT,'disk'); END")
        with self.assertRaises(sqlite3.IntegrityError):self.local.freeze()
        with self.local.db() as db:
            self.assertIsNone(self.local._row(db)['final']);db.execute('DROP TRIGGER fail')
        body=self.local.freeze()
        self.assertEqual(MultiTailStore(self.h,self.auth).freeze(),body)

    def test_master_prepare_failure_rolls_back_and_queries_are_pure(self):
        self.success();body=self.local.ordinary();MultiAgentMeter(self.r).report(self.token,body)
        intent=self.local.stop();req=self.local._prepare_request(intent)
        with self.r.connect() as db:
            db.execute("CREATE TRIGGER fail BEFORE UPDATE OF phase ON usage_multi_tail_authorities BEGIN SELECT RAISE(ABORT,'disk'); END")
        with self.assertRaises(sqlite3.IntegrityError):self.api.prepare(self.token,req)
        with self.r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_multi_tail_fences').fetchone()[0],0)
            db.execute('DROP TRIGGER fail')
        self.api.prepare(self.token,req)
        query={k:body[k] for k in ('ledger_kind','ledger_id','execution_id','session_id','authority_digest','operation','sequence','body_digest')}
        before=self.accounting()
        self.assertTrue(self.api.query(self.token,query)['accepted'])
        self.assertEqual(before,self.accounting())

    def test_expired_execute_can_reconcile_but_expired_settle_cannot_prepare(self):
        self.local.stop();req=self.local._prepare_request(json.loads(self.local_stop()))
        with patch('multi_agent_tail.time.time',return_value=self.auth['execute_until']+1):
            self.api.prepare(self.token,req)
        self.local.reconcile(self.token)
        with patch('multi_agent_tail_store.time.time',return_value=self.auth['settle_until']+1):
            with self.assertRaises(Error):self.local.freeze()

    def local_stop(self):
        with self.local.db() as db:return self.local._row(db)['stop']

    def test_local_reopen_never_recreates_ticket_success_or_execution(self):
        ticket=self.local.reserve(0,2);self.local.stop()
        other=MultiTailStore(self.h,self.auth)
        with self.assertRaises(Error):other.complete(ticket)
        with self.assertRaises(Error):other.reserve(0,1)
        other.reconcile(self.token)
        body=other.freeze();self.assertEqual(body['upload_bytes'],0)

    def test_conflicting_prepare_and_forged_ordinary_reject_without_growth(self):
        self.success();body=self.local.ordinary()
        for changed in ({'operation':'final'},{'authority_digest':'0'*64},{'body_digest':'0'*64}):
            with self.subTest(changed=changed),self.assertRaises(Error):MultiAgentMeter(self.r).report(self.token,dict(body,**changed))
        self.local.stop();proof=self.local.reconcile(self.token)
        req=proof['fence']['request'];before=self.accounting()
        with self.assertRaises(Error):self.api.prepare(self.token,dict(req,stop_intent_digest='0'*64))
        self.assertEqual(before,self.accounting())
