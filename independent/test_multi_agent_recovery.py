"""E3 isolated administrator permissions, historic evidence and atomic recovery."""
import copy
import json
from pathlib import Path
import sqlite3
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from core import Registry,Error
from multi_agent_final import MultiAgentFinal
from multi_agent_recovery import MultiAgentRecovery,fixture_admin,PERMISSIONS,READ,PROPOSE,COMMIT,CAPTURE,OBSERVE
from multi_agent_tail_store import MultiTailStore
import test_multi_agent_final as base


@unittest.skipUnless(base.base.BINARY.is_file(),'real Xray required')
class RecoveryTests(unittest.TestCase):
    setUp=base.FinalTests.setUp
    tearDown=base.FinalTests.tearDown
    success=base.FinalTests.success
    balances=base.FinalTests.balances
    all_master=base.FinalTests.all_master
    frozen=base.FinalTests.frozen

    def setup_recovery(self):
        self.actor=fixture_admin(self.h,PERMISSIONS);self.recovery=MultiAgentRecovery(self.r)
        self.success();self.bundle=self.frozen();self.recovery.capture(self.actor['token'],self.a['allocation_id'])

    def proposal(self):
        response=self.recovery.dry_run(self.actor['token'],self.a['allocation_id'],'fixture explicitly reviewed')
        data=response['proposal']
        return dict(proposal_id=data['proposal_id'],challenge=data['challenge'],confirmation_digest=data['confirmation_digest'],confirmed=True)

    def test_late_recovery_with_original_commitment_once_and_no_listener_or_refund(self):
        self.setup_recovery();before=self.balances();self.r.provision_agent(self.value['server_id'])
        with patch('multi_agent_recovery.time.time',return_value=self.auth['settle_until']+1):
            data=self.proposal()
            with patch.object(self.h,'install',side_effect=AssertionError('install')):
                first=self.recovery.commit(self.actor['token'],data)
            after=self.balances();snapshot=self.all_master()
            self.assertEqual(first['operation'],'manual_recovery')
            self.assertEqual(self.recovery.commit(self.actor['token'],data),first)
            self.assertEqual(self.recovery.result(self.actor['token'],data),first)
            self.assertEqual(self.all_master(),snapshot)
        self.assertEqual(after[0][:2],(before[0][0]+2,before[0][1]+1));self.assertEqual(after[1],before[1]-3)
        self.assertEqual(after[2],before[2]);self.assertIsNotNone(self.process.returncode)
        with self.r.connect() as db:
            row=db.execute('SELECT accepted_at,receipt FROM usage_multi_finals').fetchone()
            self.assertGreater(row['accepted_at'],self.auth['settle_until'])
            self.assertEqual(json.loads(row['receipt'])['acceptance_mode'],'manual_recovery')
            self.assertNotEqual(db.execute('SELECT state FROM usage_multi_lifecycle').fetchone()[0],'closed')

    def test_inspect_sanitized_and_read_only_no_session_credential_or_payload(self):
        self.setup_recovery();before=self.all_master();result=self.recovery.inspect(self.actor['token'],self.a['allocation_id'])
        raw=json.dumps(result)
        for sensitive in (self.token,self.actor['token'],self.value['session_id'],self.value['client']['uuid'],self.actor['session_id']):
            self.assertNotIn(sensitive,raw)
        for word in ('token_hash','node_config','challenge','password','session_id','credential'):self.assertNotIn(word,raw)
        self.assertEqual(self.all_master(),before);self.assertEqual(result['recovery_state'],'eligible_for_dry_run')

    def test_production_and_agent_tokens_do_not_authorize_admin_methods(self):
        self.setup_recovery();before=self.all_master()
        with self.assertRaises(Error):MultiAgentRecovery(Registry(self.r.path)).inspect(self.actor['token'],self.a['allocation_id'])
        for call in (lambda:self.recovery.inspect(self.token,self.a['allocation_id']),
                     lambda:self.recovery.capture(self.token,self.a['allocation_id']),
                     lambda:self.recovery.dry_run(self.token,self.a['allocation_id'],'reason')):
            with self.assertRaises(Error):call()
        self.assertEqual(before,self.all_master())

    def test_capture_needs_both_independent_permissions_and_original_window(self):
        self.success();self.frozen();actor=fixture_admin(self.h,[CAPTURE]);api=MultiAgentRecovery(self.r)
        before=self.all_master()
        with self.assertRaises(Error):api.capture(actor['token'],self.a['allocation_id'])
        self.assertEqual(before,self.all_master())
        actor=fixture_admin(self.h,[CAPTURE,OBSERVE])
        with patch('multi_agent_recovery.time.time',return_value=self.auth['settle_until']+1):
            with self.assertRaises(Error):api.capture(actor['token'],self.a['allocation_id'])
        with self.r.connect() as db:self.assertEqual(db.execute('SELECT count(*) FROM usage_multi_recovery_commitments').fetchone()[0],0)

    def test_no_historic_anchor_cannot_be_reconstructed_from_local_timestamps(self):
        self.success();self.frozen();actor=fixture_admin(self.h,PERMISSIONS);api=MultiAgentRecovery(self.r)
        before=self.all_master()
        with patch('multi_agent_recovery.time.time',return_value=self.auth['settle_until']+1):
            self.assertEqual(api.inspect(actor['token'],self.a['allocation_id'])['recovery_state'],'blocked_missing_or_unknown_evidence')
            with self.assertRaises(Error):api.dry_run(actor['token'],self.a['allocation_id'],'preserved local file')
        self.assertEqual(self.all_master(),before)

    def test_host_boot_start_instance_and_listener_observation_must_match(self):
        self.setup_recovery();before=self.all_master()
        for obj,key in ((self.h,'fixture_host_id'),(self.h,'fixture_boot_id'),(self.runtime,'fixture_start_id'),(self.runtime,'fixture_instance_id')):
            original=getattr(obj,key);setattr(obj,key,'changed')
            try:
                with self.assertRaises(Error):self.proposal()
            finally:setattr(obj,key,original)
        self.assertEqual(before,self.all_master())

    def test_unknown_ticket_remains_blocked_even_with_trusted_anchor(self):
        self.actor=fixture_admin(self.h,PERMISSIONS);self.recovery=MultiAgentRecovery(self.r)
        ticket=self.local.reserve(0,2);self.local.stop();self.local.abandon(ticket);self.local.reconcile(self.token);self.local.freeze()
        self.recovery.capture(self.actor['token'],self.a['allocation_id']);before=self.all_master()
        with self.assertRaises(Error):self.proposal()
        self.assertEqual(before,self.all_master())

    def test_unconfirmed_wrong_challenge_digest_or_actor_and_new_session_reject(self):
        self.setup_recovery();other=fixture_admin(self.h,PERMISSIONS);data=self.proposal();before=self.all_master()
        for changes in ({'confirmed':False},{'challenge':'wrong'},{'confirmation_digest':'0'*64},{'proposal_id':self.auth['execution_id']}):
            with self.assertRaises(Error):self.recovery.commit(self.actor['token'],dict(data,**changes))
        with self.assertRaises(Error):self.recovery.commit(other['token'],data)
        self.assertEqual(before,self.all_master())
        renewed=fixture_admin(self.h,PERMISSIONS,self.actor['actor_id']);before=self.all_master()
        with self.assertRaises(Error):self.recovery.commit(renewed['token'],data)
        with self.assertRaises(Error):self.recovery.result(renewed['token'],data)
        self.assertEqual(before,self.all_master())

    def test_current_permissions_and_active_flag_rechecked_for_commit_and_consumed_reads(self):
        self.setup_recovery();data=self.proposal();self.recovery.commit(self.actor['token'],data)
        for field,value in (('permissions',json.dumps([READ,PROPOSE])),('active',0)):
            with self.r.connect() as db:
                original=db.execute('SELECT '+field+' FROM usage_multi_recovery_admins WHERE actor_id=?',(self.actor['actor_id'],)).fetchone()[0]
                db.execute('UPDATE usage_multi_recovery_admins SET '+field+'=? WHERE actor_id=?',(value,self.actor['actor_id']))
            before=self.all_master()
            with self.assertRaises(Error):self.recovery.commit(self.actor['token'],data)
            with self.assertRaises(Error):self.recovery.result(self.actor['token'],data)
            self.assertEqual(before,self.all_master());self.balances()
            with self.r.connect() as db:db.execute('UPDATE usage_multi_recovery_admins SET '+field+'=? WHERE actor_id=?',(original,self.actor['actor_id']))

    def test_proposal_exact_ttl_and_clock_rollback_reject_unused_but_consumed_retry_is_readonly(self):
        self.setup_recovery();now=time.time();data=self.proposal();before=self.all_master()
        with self.r.connect() as db:p=json.loads(db.execute('SELECT record FROM usage_multi_recovery_proposals').fetchone()[0])
        for at in (p['created_at']-0.001,p['expires_at'],float('nan')):
            with patch('multi_agent_recovery.time.time',return_value=at):
                with self.assertRaises(Error):self.recovery.commit(self.actor['token'],data)
        self.assertEqual(before,self.all_master())
        result=self.recovery.commit(self.actor['token'],data);after=self.all_master()
        with patch('multi_agent_recovery.time.time',return_value=p['expires_at']+1):
            self.assertEqual(self.recovery.commit(self.actor['token'],data),result)
        self.assertEqual(after,self.all_master())

    def test_policy_or_other_business_change_invalidates_whole_snapshot(self):
        self.setup_recovery();data=self.proposal();before_balance=self.balances()
        self.f.cycles.set_total_budget(self.f.period,0);changed=self.all_master()
        with self.assertRaises(Error):self.recovery.commit(self.actor['token'],data)
        self.assertEqual(changed,self.all_master());self.assertEqual(before_balance,self.balances())
        fresh=self.proposal();self.assertTrue(self.recovery.commit(self.actor['token'],fresh)['accepted'])

    def test_private_backup_required_consistent_and_restricted(self):
        self.setup_recovery();data=self.proposal()
        root=Path(self.h.temp.name)/'recovery-private';path=root/(data['proposal_id']+'.sqlite')
        self.assertEqual(root.stat().st_mode&0o777,0o700);self.assertEqual(path.stat().st_mode&0o777,0o600)
        path.write_bytes(path.read_bytes()+b'damaged');before=self.all_master()
        with self.assertRaises(Error):self.recovery.commit(self.actor['token'],data)
        self.assertEqual(before,self.all_master())

    def test_auto_manual_mutual_exclusion_and_already_accepted_is_readonly(self):
        self.setup_recovery();data=self.proposal();before=self.balances();auto=MultiAgentFinal(self.r)
        def manual():
            try:return self.recovery.commit(self.actor['token'],data)
            except Error:return None
        with ThreadPoolExecutor(2) as pool:
            a=pool.submit(auto.submit,self.token,self.bundle);b=pool.submit(manual);a.result();b.result()
        after=self.balances();self.assertEqual(before[2],after[2]);self.assertEqual(after[1],before[1]-3)
        snapshot=self.all_master();result=self.recovery.dry_run(self.actor['token'],self.a['allocation_id'],'already accepted')
        self.assertIsNone(result['proposal']);self.assertTrue(result['summary']['already_accepted']);self.assertEqual(snapshot,self.all_master())

    def test_every_manual_write_point_rolls_back_audit_consumption_and_accounting(self):
        self.setup_recovery();data=self.proposal()
        for table,op in [('usage_cycle_accounting_events','INSERT'),('usage_attributed_sources','UPDATE'),
                ('usage_cycle_confirmed_totals','UPDATE'),('usage_cycle_confirmed_directions','UPDATE'),
                ('usage_multi_meter','INSERT'),('usage_multi_finals','INSERT'),('usage_multi_terminals','INSERT'),
                ('usage_multi_recovery_audits','INSERT'),('usage_multi_recovery_consumed','INSERT')]:
            # Trigger schema is part of the snapshot: create before a fresh dry-run.
            with self.r.connect() as db:db.execute('CREATE TRIGGER manual_fault BEFORE '+op+' ON '+table+" BEGIN SELECT RAISE(ABORT,'fault'); END")
            proposal=self.proposal();before=self.all_master()
            with self.assertRaises(sqlite3.DatabaseError):self.recovery.commit(self.actor['token'],proposal)
            self.assertEqual(before,self.all_master())
            with self.r.connect() as db:db.execute('DROP TRIGGER manual_fault')
        self.assertTrue(self.recovery.commit(self.actor['token'],self.proposal())['accepted'])

    def test_concurrent_same_proposal_one_audit_and_immutable_success_records(self):
        self.setup_recovery();data=self.proposal()
        with ThreadPoolExecutor(2) as pool:results=list(pool.map(lambda _:self.recovery.commit(self.actor['token'],data),range(2)))
        self.assertEqual(results[0],results[1]);before=self.all_master()
        for table in ('usage_multi_recovery_audits','usage_multi_recovery_consumed','usage_multi_recovery_proposals','usage_multi_recovery_commitments','usage_multi_fixture_instances'):
            for op in ('DELETE','UPDATE'):
                with self.assertRaises(sqlite3.DatabaseError):
                    with self.r.connect() as db:
                        if op=='DELETE':db.execute('DELETE FROM '+table)
                        else:
                            key=db.execute('PRAGMA table_info('+table+')').fetchone()[1]
                            db.execute('UPDATE '+table+' SET '+key+'='+key)
        self.assertEqual(before,self.all_master())

    def test_reopen_preserves_proposal_and_consumed_result_without_new_session(self):
        self.setup_recovery();data=self.proposal();self.r=self.h.reopen();api=MultiAgentRecovery(self.r)
        with patch.object(self.r,'begin_session',side_effect=AssertionError('new agent session')):
            result=api.commit(self.actor['token'],data)
        snapshot=self.all_master();self.assertEqual(api.result(self.actor['token'],data),result);self.assertEqual(snapshot,self.all_master())

    def test_unconsumed_permission_revocation_and_readonly_actor_cannot_propose(self):
        self.setup_recovery();reader=fixture_admin(self.h,[READ]);data=self.proposal()
        with self.assertRaises(Error):self.recovery.dry_run(reader['token'],self.a['allocation_id'],'read only')
        with self.r.connect() as db:db.execute('UPDATE usage_multi_recovery_admins SET permissions=? WHERE actor_id=?',
            (json.dumps([READ,PROPOSE]),self.actor['actor_id']))
        before=self.all_master()
        with self.assertRaises(Error):self.recovery.commit(self.actor['token'],data)
        self.assertEqual(before,self.all_master())
        self.assertEqual(self.recovery.inspect(reader['token'],self.a['allocation_id'])['recovery_state'],'eligible_for_dry_run')

    def test_two_distinct_proposals_for_same_execution_only_one_can_commit(self):
        self.setup_recovery();first=self.proposal();second=self.proposal()
        def submit(data):
            try:return self.recovery.commit(self.actor['token'],data)
            except Error:return None
        with ThreadPoolExecutor(2) as pool:results=list(pool.map(submit,(first,second)))
        self.assertEqual(sum(r is not None for r in results),1)
        with self.r.connect() as db:self.assertEqual(db.execute('SELECT count(*) FROM usage_multi_recovery_audits').fetchone()[0],1)

    def test_failure_to_create_private_backup_never_creates_proposal(self):
        self.setup_recovery();before=self.all_master()
        with patch('multi_agent_recovery._backup',side_effect=OSError('backup fault')):
            with self.assertRaises(OSError):self.proposal()
        self.assertEqual(before,self.all_master())

    def test_missing_historical_manual_audit_blocks_balance_and_consumed_query(self):
        self.setup_recovery();data=self.proposal();self.recovery.commit(self.actor['token'],data)
        with self.r.connect() as db:
            db.execute('PRAGMA foreign_keys=OFF');db.execute('DROP TRIGGER usage_multi_recovery_audits_delete')
            db.execute('DELETE FROM usage_multi_recovery_audits')
        before=self.all_master()
        with self.assertRaises(Error):self.balances()
        with self.assertRaises(Error):self.recovery.result(self.actor['token'],data)
        self.assertEqual(before,self.all_master())
