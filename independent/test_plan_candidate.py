"""Candidate inspection must never be mistaken for an execution grant."""
import json
import unittest
from unittest.mock import patch
from core import Error
from plan_policy import candidate_snapshot
from plan_lifecycle import Lifecycle
from plan_nodes import PlanNodes
from plan_node_bandwidth import PlanNodeBandwidth
from plan_node_connections import PlanNodeConnections
from usage_cycles import UsageCycles
from test_plan_node_bandwidth import rates
from test_connection_plans import limit
import plan_grace
import test_plan_grace
import test_core


class CandidateTests(unittest.TestCase):
    setUp=test_core.RegistryTests.setUp
    tearDown=test_core.RegistryTests.tearDown
    fixtures=test_plan_grace.GracePreviewTests.fixtures

    def inspect(self,uid,now):
        with self.registry.connect() as db:
            db.execute('BEGIN')
            return candidate_snapshot(db,uid,now)

    def test_grace_bundle_uses_target_policies_preserves_database_and_live_snapshot(self):
        r,m,uid,nid,token,a,b,end,auth=self.fixtures()
        r.save_plan(dict(name='target',bandwidth=rates(40,50),connections=limit(3)),b)
        PlanNodeBandwidth(r).manage(b,nid,rates(10,20))
        PlanNodeConnections(r).manage(b,nid,limit(2))
        cycle=UsageCycles(r).create(uid,dict(starts_at=end-2000,ends_at=end+5000))['id']
        UsageCycles(r).set_budget(cycle,dict(upload_bytes=100,download_bytes=200))
        before=r.configuration(token)
        with r.connect() as db: contents=list(db.iterdump())
        result=self.inspect(uid,end)
        self.assertEqual(result['candidate_plan_id'],b)
        self.assertEqual(result['policies'],dict(bandwidth=rates(40,50),connections=limit(3),
                         nodes=[dict(node_id=nid,bandwidth=rates(10,20),connections=limit(2))]))
        self.assertEqual(result['cycle'],dict(id=cycle,ends_at=end+5000,
                         budget=dict(upload_bytes=100,download_bytes=200)))
        self.assertFalse(result['execution_ready'])
        self.assertIn('plan_aggregate_quota_unresolved',result['blockers'])
        self.assertNotIn(auth['uuid'],str(result))
        with r.connect() as db: self.assertEqual(list(db.iterdump()),contents)
        self.assertEqual(r.configuration(token),before)

    def test_ended_temporary_rule_does_not_hide_target_node_policy(self):
        r,m,uid,nid,token,a,b,end,auth=self.fixtures()
        PlanNodeBandwidth(r).manage(b,nid,rates(10,20))
        r.temporary_bandwidth(uid,dict(bandwidth=rates(90,100),expires_at=end+10))
        self.assertEqual(self.inspect(uid,end)['policies']['bandwidth'],rates(90,100))
        self.assertIsNone(self.inspect(uid,end)['policies']['nodes'][0]['bandwidth'])
        after=self.inspect(uid,end+10)['policies']
        self.assertEqual(after['bandwidth'],rates(32768))
        self.assertEqual(after['nodes'][0]['bandwidth'],rates(10,20))
        # Read-only evaluation must not delete the expired row; the normal
        # configuration path owns its expiration and revision bump.
        with r.connect() as db:
            self.assertIsNotNone(db.execute('SELECT 1 FROM temporary_bandwidth WHERE user_id=?',(uid,)).fetchone())
        r.user_bandwidth(uid,rates())
        after=self.inspect(uid,end+10)['policies']
        self.assertEqual(after['bandwidth'],rates())
        self.assertIsNone(after['nodes'][0]['bandwidth'])

    def test_empty_scope_disabled_expired_and_cycle_gap_cannot_fall_back(self):
        r,m,uid,nid,token,a,b,end,auth=self.fixtures()
        PlanNodes(r).save(b,dict(node_ids=[]))
        self.assertEqual(self.inspect(uid,end)['policies']['nodes'],[])
        UsageCycles(r).create(uid,dict(starts_at=end+20,ends_at=end+50))
        result=self.inspect(uid,end)
        self.assertEqual(result['cycle'],dict(id=None,ends_at=None,budget=None))
        self.assertIn('no_open_usage_cycle',result['blockers'])
        for moment in (end+86400,end+100000):
            result=self.inspect(uid,moment)
            self.assertEqual(result['state'],'expired')
            self.assertIsNone(result['policies']); self.assertEqual(result['node_ids'],[])
        Lifecycle(r).manage(uid,dict(enabled=False,expires_at=end))
        result=self.inspect(uid,end)
        self.assertEqual(result['state'],'disabled'); self.assertIsNone(result['policies'])

    def test_renewal_returns_original_policy_without_replacing_identity_or_cycle(self):
        r,m,uid,nid,token,a,b,end,auth=self.fixtures()
        r.save_plan(dict(name='target',bandwidth=rates(5,6),connections=limit(3)),b)
        cycle=UsageCycles(r).create(uid,dict(starts_at=end-2000,ends_at=end+300000))['id']
        self.assertEqual(self.inspect(uid,end)['policies']['bandwidth'],rates(5,6))
        Lifecycle(r).manage(uid,dict(enabled=True,expires_at=end+200000))
        result=self.inspect(uid,end+1)
        self.assertEqual((result['state'],result['candidate_plan_id']),('active',a))
        self.assertEqual(result['policies']['bandwidth'],rates(32768))
        self.assertEqual(result['cycle']['id'],cycle)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT credential FROM node_users WHERE user_id=?',(uid,)).fetchone()[0],auth['uuid'])

    def test_selection_scope_policies_and_budget_share_one_sqlite_snapshot(self):
        r,m,uid,nid,token,a,b,end,auth=self.fixtures()
        cycle=UsageCycles(r).create(uid,dict(starts_at=end-2000,ends_at=end+5000))['id']
        UsageCycles(r).set_budget(cycle,dict(upload_bytes=100,download_bytes=200))
        PlanNodes(r).save(b,dict(node_ids=[nid]))
        # WAL allows an actual writer commit between selection and remaining
        # reads. The caller transaction must still see the complete old state.
        with r.connect() as db: db.execute('PRAGMA journal_mode=WAL')
        original=plan_grace.candidate
        def select_then_commit(db,ident,now):
            choice=original(db,ident,now)
            with r.connect() as writer:
                writer.execute('BEGIN IMMEDIATE')
                writer.execute('UPDATE plans SET bandwidth=? WHERE id=?',(json.dumps(rates(7,8)),b))
                writer.execute('UPDATE plan_connections SET payload=? WHERE plan_id=?',(json.dumps(limit(4)),b))
                writer.execute('DELETE FROM plan_nodes WHERE plan_id=?',(b,))
                writer.execute('UPDATE usage_cycle_budgets SET upload_bytes=1,download_bytes=2 WHERE cycle_id=?',(cycle,))
            return choice
        with patch('plan_grace.candidate',side_effect=select_then_commit):
            result=self.inspect(uid,end)
        self.assertEqual(result['policies']['bandwidth'],rates(32768))
        self.assertEqual(result['policies']['connections'],limit(1))
        self.assertEqual(result['node_ids'],[nid])
        self.assertEqual(result['cycle']['budget'],dict(upload_bytes=100,download_bytes=200))
        fresh=self.inspect(uid,end)
        self.assertEqual(fresh['policies']['bandwidth'],rates(7,8))
        self.assertEqual(fresh['policies']['connections'],limit(4))
        self.assertEqual(fresh['node_ids'],[])
        self.assertEqual(fresh['cycle']['budget'],dict(upload_bytes=1,download_bytes=2))

    def test_caller_transaction_is_required_and_invalid_identity_rejected(self):
        r,m,uid,nid,token,a,b,end,auth=self.fixtures()
        with r.connect() as db:
            with self.assertRaises(RuntimeError): candidate_snapshot(db,uid,end)
            db.execute('BEGIN')
            for invalid in (True,None,0,-1,'1',999):
                with self.assertRaises(Error): candidate_snapshot(db,invalid,end)
