"""E2 isolated terminal ledger, delivery faults and listener-free recovery."""
import copy
import json
import sqlite3
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from core import Registry,Error
from multi_agent_final import MultiAgentFinal,final_receipt
from multi_agent_tail import sealed
from multi_agent_tail_store import MultiTailStore
from multi_agent_meter import MultiAgentMeter
from multi_agent_lifecycle import encoded
import test_multi_agent_tail as base


@unittest.skipUnless(base.BINARY.is_file(),'real Xray required')
class FinalTests(unittest.TestCase):
    setUp=base.TailTests.setUp
    tearDown=base.TailTests.tearDown
    success=base.TailTests.success
    balances=base.TailTests.balances
    accounting=base.TailTests.accounting

    def frozen(self):
        self.local.stop();self.local.reconcile(self.token);self.local.freeze()
        return self.local.frozen_bundle()

    def all_master(self):
        with self.r.connect() as db:
            db.execute('BEGIN')
            tables=[r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
            return [(t,[tuple(r) for r in db.execute('SELECT * FROM "'+t+'"')]) for t in tables]

    def test_final_converts_only_delta_and_close_preserves_remaining_hold(self):
        original=self.balances();self.success();ordinary=self.local.send_ordinary(self.token)
        before=self.balances();self.success(1,2);bundle=self.frozen()
        response=self.local.deliver(self.token)
        self.assertFalse(response['duplicate']);self.assertEqual(response['state'],'accepted')
        after=self.balances()
        self.assertEqual(after[0][0:2],(original[0][0]+3,original[0][1]+3))
        self.assertEqual(after[1],before[1]-3);self.assertEqual(after[2],original[2])
        self.assertEqual(self.local.deliver(self.token)['final_receipt'],response['final_receipt'])
        closed=self.local.finish(self.token);self.assertEqual(closed['state'],'closed')
        self.assertEqual(self.local.finish(self.token),closed);self.assertEqual(self.balances(),after)
        retry=self.local.deliver(self.token);self.assertTrue(retry['duplicate']);self.assertEqual(retry['state'],'closed')
        old_body=bundle['evidence']['reconciliation']['requests'][0]
        self.assertEqual(MultiAgentMeter(self.r).report(self.token,old_body)['confirmed_bytes'],6)
        ordinary_final=dict(old_body,sequence=bundle['final']['final_sequence'],upload_bytes=3,download_bytes=3)
        ordinary_final.pop('body_digest');ordinary_final=sealed(ordinary_final)
        with self.assertRaises(Error):MultiAgentMeter(self.r).report(self.token,ordinary_final)

    def test_zero_delta_has_distinct_terminal_event(self):
        self.success();self.local.send_ordinary(self.token);bundle=self.frozen();before=self.balances()
        self.local.deliver(self.token);self.assertEqual(self.balances(),before)
        with self.r.connect() as db:
            event=db.execute('SELECT * FROM usage_cycle_accounting_events WHERE ledger_id=? AND sequence=?',
                (self.a['allocation_id'],bundle['final']['final_sequence'])).fetchone()
            self.assertEqual((event['upload_delta'],event['download_delta']),('0','0'))

    def test_lost_final_ack_reopen_resends_exact_body_without_runtime_or_session(self):
        self.success();bundle=self.frozen();endpoint=MultiAgentFinal(self.r)
        class Lost:
            def submit(inner,token,data):endpoint.submit(token,data);raise OSError('lost response')
        with self.assertRaises(OSError):self.local.deliver(self.token,Lost())
        after=self.balances()
        self.r=self.h.reopen();recovered=MultiTailStore(self.h,self.auth)
        with patch.object(self.h,'install',side_effect=AssertionError('install')),patch.object(self.r,'begin_session',side_effect=AssertionError('new session')):
            result=recovered.deliver(self.token);recovered.finish(self.token)
        self.assertTrue(result['duplicate']);self.assertEqual(recovered.frozen_bundle(),bundle)
        self.assertEqual(self.balances(),after);self.assertIsNotNone(self.process.returncode)
        with self.assertRaises(Error):recovered.reserve(0,1)
        with self.assertRaises(Error):recovered.ordinary()

    def test_unfrozen_recovery_never_creates_final_or_executes(self):
        self.success();self.local.stop();self.local.reconcile(self.token)
        recovered=MultiTailStore(self.h,self.auth);before=self.all_master()
        with self.assertRaises(Error):recovered.deliver(self.token)
        with self.local.db() as db:self.assertIsNone(db.execute('SELECT final FROM local_tail').fetchone()[0])
        self.assertEqual(before,self.all_master())

    def test_deadline_exact_then_late_duplicate_and_identity_revocation(self):
        self.success();bundle=self.frozen();api=MultiAgentFinal(self.r)
        with patch('multi_agent_final.time.time',return_value=self.auth['settle_until']):first=api.submit(self.token,bundle)
        snapshot=self.all_master()
        with patch('multi_agent_final.time.time',return_value=self.auth['settle_until']+1):
            retry=api.submit(self.token,bundle)
            altered=copy.deepcopy(bundle);altered['final']['freeze_completed_at']+=0.01
            raw=altered['final'];raw.pop('body_digest');altered['final']=sealed(raw)
            with self.assertRaises(Error):api.submit(self.token,altered)
        self.assertEqual(retry['final_receipt'],first['final_receipt']);self.assertEqual(snapshot,self.all_master())
        self.r.provision_agent(self.value['server_id'])
        with self.assertRaises(Error):api.submit(self.token,bundle)

    def test_new_expired_future_clock_and_forged_fields_reject_without_writes(self):
        self.success();bundle=self.frozen();api=MultiAgentFinal(self.r);before=self.all_master()
        for now in (self.auth['settle_until']+0.001,bundle['final']['freeze_completed_at']-1,float('nan')):
            with patch('multi_agent_final.time.time',return_value=now):
                with self.assertRaises(Error):api.submit(self.token,bundle)
        for key,value in (('upload_bytes',True),('final_sequence',True),('version',True),('ticket_manifest_digest','0'*64),('stop_at',float('inf'))):
            forged=copy.deepcopy(bundle);raw=forged['final'];raw.pop('body_digest');raw[key]=value
            if key=='stop_at':
                raw['body_digest']='0'*64
            else:forged['final']=sealed(raw)
            with self.assertRaises((Error,ValueError)):api.submit(self.token,forged)
        self.assertEqual(self.all_master(),before)

    def test_same_and_conflicting_final_races_only_one_terminal(self):
        self.success();bundle=self.frozen();api=MultiAgentFinal(self.r)
        with ThreadPoolExecutor(2) as pool:results=list(pool.map(lambda _:api.submit(self.token,bundle),range(2)))
        self.assertEqual(sum(not r['duplicate'] for r in results),1)
        snapshot=self.all_master();altered=copy.deepcopy(bundle);body=altered['final'];body.pop('body_digest');body['final_sequence']+=1;altered['final']=sealed(body)
        with self.assertRaises(Error):api.submit(self.token,altered)
        self.assertEqual(snapshot,self.all_master())

    def test_each_terminal_write_failure_rolls_back_all_tables(self):
        self.success();bundle=self.frozen();api=MultiAgentFinal(self.r)
        points=[('usage_cycle_accounting_events','INSERT'),('usage_attributed_sources','UPDATE'),
                ('usage_cycle_confirmed_totals','UPDATE'),('usage_cycle_confirmed_directions','UPDATE'),
                ('usage_multi_meter','INSERT'),('usage_multi_finals','INSERT'),('usage_multi_terminals','INSERT')]
        for table,operation in points:
            with self.subTest(table=table):
                with self.r.connect() as db:db.execute('CREATE TRIGGER fail_final BEFORE '+operation+' ON '+table+" BEGIN SELECT RAISE(ABORT,'fault'); END")
                before=self.all_master()
                with self.assertRaises(sqlite3.DatabaseError):api.submit(self.token,bundle)
                self.assertEqual(before,self.all_master())
                with self.r.connect() as db:db.execute('DROP TRIGGER fail_final')
        self.assertTrue(api.submit(self.token,bundle)['accepted'])

    def test_local_final_ack_commit_failure_does_not_repeat_charge(self):
        self.success();self.frozen()
        with self.local.db() as db:db.execute("CREATE TRIGGER fail_ack BEFORE INSERT ON tail_delivery BEGIN SELECT RAISE(ABORT,'fault'); END")
        with self.assertRaises(sqlite3.DatabaseError):self.local.deliver(self.token)
        after=self.all_master()
        with self.local.db() as db:db.execute('DROP TRIGGER fail_ack')
        self.assertTrue(MultiTailStore(self.h,self.auth).deliver(self.token)['duplicate']);self.assertEqual(after,self.all_master())

    def test_close_requires_final_and_lost_close_ack_retry_is_exact(self):
        self.success();bundle=self.frozen();api=MultiAgentFinal(self.r)
        request=dict(ledger_kind='multi_allocation',ledger_id=self.a['allocation_id'],execution_id=self.auth['execution_id'],
            session_id=self.value['session_id'],authority_digest=self.auth['body_digest'],final_digest=bundle['final']['body_digest'])
        before=self.all_master()
        with self.assertRaises(Error):api.close(self.token,request)
        with self.assertRaises(Error):self.local.finish(self.token)
        self.assertEqual(self.all_master(),before);self.local.deliver(self.token)
        class Lost:
            def close(inner,token,data):api.close(token,data);raise OSError('lost close ACK')
        with self.assertRaises(OSError):self.local.finish(self.token,Lost())
        after=self.all_master();closed=MultiTailStore(self.h,self.auth).finish(self.token)
        self.assertEqual(closed['state'],'closed');self.assertEqual(self.all_master(),after)

    def test_damaged_terminal_or_final_receipt_blocks_retry_and_balance_reads(self):
        self.success();bundle=self.frozen();api=MultiAgentFinal(self.r);api.submit(self.token,bundle)
        for table in ('usage_multi_terminals','usage_multi_finals'):
            with self.subTest(table=table):
                with self.r.connect() as db:
                    db.execute('PRAGMA foreign_keys=OFF')  # Deliberate damaged fixture, not normal deletion.
                    saved=dict(db.execute('SELECT * FROM '+table).fetchone());db.execute('DELETE FROM '+table)
                before=self.all_master()
                with self.assertRaises(Error):api.submit(self.token,bundle)
                with self.assertRaises(Error):self.balances()
                self.assertEqual(before,self.all_master())
                with self.r.connect() as db:db.execute('INSERT INTO '+table+' VALUES('+','.join('?' for _ in saved)+')',tuple(saved.values()))

    def test_expired_cycle_and_current_configuration_do_not_revoke_original_tail(self):
        self.success();bundle=self.frozen()
        with self.r.connect() as db:
            db.execute('UPDATE usage_cycles SET expired=1 WHERE id=?',(self.value['cycle_id'],))
            db.execute('DELETE FROM node_users WHERE node_id=?',(self.value['node_id'],))
        self.assertTrue(MultiAgentFinal(self.r).submit(self.token,bundle)['accepted'])
        self.assertEqual(self.local.deliver(self.token)['state'],'accepted')

    def test_production_gate_and_cross_agent_cannot_submit(self):
        self.success();bundle=self.frozen();before=self.all_master()
        with self.assertRaises(Error):MultiAgentFinal(Registry(self.r.path)).submit(self.token,bundle)
        other,_=self.f.host(self.r)
        snapshot=self.all_master()
        with self.assertRaises(Error):MultiAgentFinal(self.r).submit(other,bundle)
        self.assertEqual(snapshot,self.all_master())

    def test_legacy_lifecycle_close_and_block_never_downgrade_terminal(self):
        from multi_agent_lifecycle import MultiAgentLifecycle
        self.success();self.frozen();self.local.deliver(self.token)
        data=dict(ledger_kind='multi_allocation',ledger_id=self.a['allocation_id'],session_id=self.value['session_id'],
                  binding_digest=self.value['digest'],action='close')
        endpoint=MultiAgentLifecycle(self.r)
        self.assertEqual(endpoint.advance(self.token,data)['state'],'closed')
        before=self.all_master()
        for action in ('close','block'):
            self.assertEqual(endpoint.advance(self.token,dict(data,action=action))['state'],'closed')
            self.assertEqual(before,self.all_master())

    def test_receipt_only_fake_response_is_not_persisted_as_acceptance(self):
        self.success();bundle=self.frozen();body=bundle['final'];api=MultiAgentFinal(self.r)
        class Fake:
            def submit(inner,token,data):return api._result(final_receipt(body,time.time()),'accepted',False)
        before=self.all_master()
        with self.assertRaises(Error):self.local.deliver(self.token,Fake())
        with self.local.db() as db:self.assertIsNone(db.execute('SELECT * FROM tail_delivery').fetchone())
        self.assertEqual(before,self.all_master())

    def test_close_write_faults_roll_back_terminal_and_lifecycle_together(self):
        self.success();bundle=self.frozen();self.local.deliver(self.token)
        for table in ('usage_multi_terminals','usage_multi_lifecycle'):
            with self.subTest(table=table):
                with self.r.connect() as db:db.execute('CREATE TRIGGER fail_close BEFORE UPDATE ON '+table+" BEGIN SELECT RAISE(ABORT,'fault'); END")
                before=self.all_master()
                with self.assertRaises(sqlite3.DatabaseError):self.local.finish(self.token)
                self.assertEqual(before,self.all_master())
                with self.r.connect() as db:db.execute('DROP TRIGGER fail_close')
        self.assertEqual(self.local.finish(self.token)['state'],'closed')

    def test_policy_lowered_allows_existing_hold_conversion(self):
        self.success();bundle=self.frozen();before=self.balances()
        self.f.cycles.set_total_budget(self.f.period,0)
        self.assertTrue(MultiAgentFinal(self.r).submit(self.token,bundle)['accepted'])
        self.assertEqual(self.balances()[2],before[2])

    def test_retired_session_rejects_new_and_accepted_final_without_takeover(self):
        import uuid
        self.success();bundle=self.frozen();MultiAgentFinal(self.r).submit(self.token,bundle)
        session=str(uuid.uuid4());self.r.begin_session(self.token,dict(session_id=session))
        before=self.all_master()
        with self.assertRaises(Error):self.local.deliver(self.token)
        changed=copy.deepcopy(bundle);raw=changed['final'];raw.pop('body_digest');raw['session_id']=session;changed['final']=sealed(raw)
        with self.assertRaises(Error):MultiAgentFinal(self.r).submit(self.token,changed)
        self.assertEqual(before,self.all_master())

    def test_unknown_ticket_zero_final_keeps_entire_hold(self):
        ticket=self.local.reserve(0,3);self.local.stop();self.local.abandon(ticket)
        self.local.reconcile(self.token);body=self.local.freeze();before=self.balances()
        self.assertEqual(body['upload_bytes'],0)
        self.local.deliver(self.token);self.local.finish(self.token)
        self.assertEqual(before,self.balances())

    def test_final_requires_complete_original_evidence_and_actual_stopped_instance(self):
        self.success();bundle=self.frozen();api=MultiAgentFinal(self.r);before=self.all_master()
        for key in ('stop','stop_receipt','tickets','reconciliation'):
            changed=copy.deepcopy(bundle);changed['evidence'].pop(key)
            with self.assertRaises(Error):api.submit(self.token,changed)
        original=self.runtime.fixture_instance_id;self.runtime.fixture_instance_id=self.auth['execution_id']
        try:
            with self.assertRaises(Error):api.submit(self.token,bundle)
        finally:self.runtime.fixture_instance_id=original
        self.assertEqual(before,self.all_master())

    def test_final_and_explicit_close_compete_without_partial_terminal(self):
        self.success();bundle=self.frozen();api=MultiAgentFinal(self.r);body=bundle['final']
        request={k:body[k] for k in ('ledger_kind','ledger_id','execution_id','session_id','authority_digest')}
        request['final_digest']=body['body_digest']
        def close():
            try:return api.close(self.token,request)
            except Error:return None
        with ThreadPoolExecutor(2) as pool:
            a=pool.submit(api.submit,self.token,bundle);b=pool.submit(close);a.result();b.result()
        self.assertEqual(api.close(self.token,request)['state'],'closed')
        self.assertEqual(self.balances()[2],sum(self.balances()[0])+self.balances()[1])

    def test_final_transaction_second_connection_cannot_observe_half_conversion(self):
        self.success();bundle=self.frozen();before=self.all_master()
        import multi_agent_final as module
        original=module.final_receipt;observed=[]
        def observing(body,at):
            # Called after event/source/confirmed/meter writes, before terminal evidence.
            observed.append(self.all_master());return original(body,at)
        with patch.object(module,'final_receipt',side_effect=observing):
            MultiAgentFinal(self.r).submit(self.token,bundle)
        self.assertTrue(observed);self.assertTrue(all(snapshot==before for snapshot in observed))

    def test_close_ack_local_commit_failure_retries_without_master_rewrite(self):
        self.success();self.frozen();self.local.deliver(self.token)
        with self.local.db() as db:db.execute("CREATE TRIGGER fail_closed_ack BEFORE UPDATE ON tail_delivery BEGIN SELECT RAISE(ABORT,'fault'); END")
        with self.assertRaises(sqlite3.DatabaseError):self.local.finish(self.token)
        snapshot=self.all_master()
        with self.local.db() as db:db.execute('DROP TRIGGER fail_closed_ack')
        self.assertEqual(MultiTailStore(self.h,self.auth).finish(self.token)['state'],'closed')
        self.assertEqual(snapshot,self.all_master())
