"""Internal plan selection must not mutate bindings or activate grace access."""
import time
import unittest
from core import Error
from plan_policy import bandwidth,connections,node_override,selected
from plan_node_bandwidth import PlanNodeBandwidth
from plan_node_connections import PlanNodeConnections
import test_plan_grace
from test_plan_node_bandwidth import rates
from test_connection_plans import limit
import test_core


class SelectionTests(unittest.TestCase):
    setUp=test_core.RegistryTests.setUp
    tearDown=test_core.RegistryTests.tearDown
    fixtures=test_plan_grace.GracePreviewTests.fixtures

    def test_explicit_target_general_policy_preserves_original_snapshot(self):
        r,m,uid,nid,token,a,b,end,auth=self.fixtures()
        r.save_plan(dict(name='target',bandwidth=rates(1234,5678),connections=limit(3)),b)
        before=r.configuration(token)
        with r.connect() as db:
            db.execute('BEGIN')
            self.assertEqual(selected(db,uid),a)
            self.assertEqual(bandwidth(db,uid,plan_id=b),rates(1234,5678))
            self.assertEqual(connections(db,uid,plan_id=b),limit(3))
            self.assertEqual(bandwidth(db,uid),rates(32768))
            self.assertEqual(connections(db,uid),limit(1))
        self.assertEqual(r.configuration(token),before)

    def test_node_policy_selection_and_explicit_unlimited_do_not_fall_back(self):
        r,m,uid,nid,token,a,b,end,auth=self.fixtures()
        PlanNodeBandwidth(r).manage(a,nid,rates(512,1024))
        PlanNodeConnections(r).manage(a,nid,limit(2))
        PlanNodeBandwidth(r).manage(b,nid,rates())
        PlanNodeConnections(r).manage(b,nid,limit(None))
        before=r.configuration(token)
        with r.connect() as db:
            self.assertEqual(node_override(db,uid,nid,'bandwidth'),rates(512,1024))
            self.assertEqual(node_override(db,uid,nid,'bandwidth',plan_id=b),rates())
            self.assertEqual(node_override(db,uid,nid,'connections',plan_id=b),limit(None))
        PlanNodeBandwidth(r).manage(b,nid,inherit=True)
        PlanNodeConnections(r).manage(b,nid,inherit=True)
        with r.connect() as db:
            self.assertIsNone(node_override(db,uid,nid,'bandwidth',plan_id=b))
            self.assertIsNone(node_override(db,uid,nid,'connections',plan_id=b))
        self.assertEqual(r.configuration(token),before)

    def test_user_and_temporary_overrides_keep_existing_priority_for_target(self):
        r,m,uid,nid,token,a,b,end,auth=self.fixtures()
        PlanNodeBandwidth(r).manage(b,nid,rates(4,5))
        PlanNodeConnections(r).manage(b,nid,limit(2))
        r.user_bandwidth(uid,rates()); r.user_connections(uid,limit(None))
        with r.connect() as db:
            self.assertEqual(bandwidth(db,uid,plan_id=b),rates())
            self.assertEqual(connections(db,uid,plan_id=b),limit(None))
            self.assertIsNone(node_override(db,uid,nid,'bandwidth',plan_id=b))
            self.assertIsNone(node_override(db,uid,nid,'connections',plan_id=b))
        r.temporary_bandwidth(uid,dict(bandwidth=rates(20,30),expires_at=time.time()+100))
        with r.connect() as db:
            self.assertEqual(bandwidth(db,uid,plan_id=b),rates(20,30))
        r.temporary_bandwidth(uid,remove=True)
        with r.connect() as db: self.assertEqual(bandwidth(db,uid,plan_id=b),rates())

    def test_invalid_explicit_selector_rejected_even_with_user_override(self):
        r,m,uid,nid,token,a,b,end,auth=self.fixtures()
        r.user_bandwidth(uid,rates(20));r.user_connections(uid,limit(4))
        with r.connect() as db:
            for pid in (None,True,0,-1,'1',999):
                for call in (lambda:bandwidth(db,uid,plan_id=pid),
                             lambda:connections(db,uid,plan_id=pid),
                             lambda:node_override(db,uid,nid,'bandwidth',plan_id=pid)):
                    with self.assertRaises(Error): call()
            with self.assertRaises(ValueError): node_override(db,uid,nid,'invalid')

    def test_unbound_defaults_remain_unchanged(self):
        r,m,uid,nid,token,a,b,end,auth=self.fixtures()
        r.assign_plan(uid,dict(plan_id=None))
        with r.connect() as db:
            self.assertIsNone(selected(db,uid)); self.assertIsNone(bandwidth(db,uid))
            self.assertEqual(connections(db,uid),limit(None))
            self.assertIsNone(node_override(db,uid,nid,'connections'))
