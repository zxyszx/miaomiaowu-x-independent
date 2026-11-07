"""Policy history is evidence for future allocation work, never a tail bypass."""
import sqlite3
import unittest
from core import Registry, Error
from usage_cycles import UsageCycles
from total_policy import current
import test_total_cycle_ledger as base


class TotalPolicyTests(unittest.TestCase):
    setUp=base.TotalLedgerTests.setUp
    tearDown=base.TotalLedgerTests.tearDown
    fixtures=base.TotalLedgerTests.fixtures
    fixture=base.TotalLedgerTests.fixture
    setup_total=base.TotalLedgerTests.setup_total
    applied=base.TotalLedgerTests.applied
    data=base.TotalLedgerTests.data

    def rows(self,r):
        with r.connect() as db:
            return [dict(x) for x in db.execute('SELECT * FROM usage_total_policy_versions ORDER BY id')]

    def test_change_history_is_monotonic_and_never_reuses_same_cap_epoch(self):
        r=self.setup_total(); first=self.rows(r)
        self.cycles.set_total_budget(self.period,7); self.assertEqual(self.rows(r),first)
        for cap in (0,None,7): self.cycles.set_total_budget(self.period,cap)
        rows=self.rows(Registry(r.path))
        self.assertEqual([x['total_bytes'] for x in rows],[7,0,None,7])
        self.assertEqual(len({x['id'] for x in rows}),4)
        self.assertEqual(rows[0],first[0])

    def test_upgrade_baseline_does_not_invent_past_versions(self):
        r=self.setup_total()
        with r.connect() as db: db.execute('DELETE FROM usage_total_policy_versions')
        self.cycles=UsageCycles(Registry(r.path)); self.cycles.set_total_budget(self.period,3)
        rows=self.rows(r)
        self.assertEqual([(x['total_bytes'],x['origin']) for x in rows],[(7,'baseline'),(3,'change')])

    def test_ack_observations_preserve_old_version_but_do_not_relax_settlement(self):
        r=self.setup_total(); first=self.applied(r); self.applied(r)
        self.cycles.set_total_budget(self.period,0); second=self.applied(r)
        with r.connect() as db:
            rows=db.execute('''SELECT o.revision,p.total_bytes FROM usage_execution_total_observations o
                JOIN usage_total_policy_versions p ON p.id=o.policy_id ORDER BY o.revision''').fetchall()
            self.assertEqual([tuple(x) for x in rows],[(first['revision'],7),(second['revision'],0)])
        # An observed old master cap is not proof of delivered/enforced bytes.
        with self.assertRaises(Error): self.ledger.settle(self.token,self.data())
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],0)
        self.assertEqual(first['nodes'][0]['clients'][0]['uuid'],second['nodes'][0]['clients'][0]['uuid'])
        self.assertNotIn('total_limit',second['nodes'][0]['clients'][0])
        reopened=Registry(r.path)
        with reopened.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_execution_total_observations').fetchone()[0],2)
        with self.assertRaises(Error) as caught: r.delete('users',self.uid)
        self.assertEqual(caught.exception.status,409)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_total_policy_versions').fetchone()[0],2)
            self.assertEqual(db.execute('SELECT count(*) FROM usage_execution_total_observations').fetchone()[0],2)

    def test_history_failure_rolls_back_cap_and_configuration(self):
        r=self.setup_total(); before=r.configuration(self.token); rows=self.rows(r)
        with r.connect() as db:
            db.execute("CREATE TRIGGER fail_policy BEFORE INSERT ON usage_total_policy_versions BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.cycles.set_total_budget(self.period,0)
        self.assertEqual(self.cycles.total_budget(self.period)['total_bytes'],7)
        self.assertEqual(self.rows(r),rows); self.assertEqual(r.configuration(self.token),before)

    def test_observation_failure_rolls_back_execution_receipt_and_grant(self):
        r=self.setup_total(); snap=r.configuration(self.token)
        data=dict(revision=snap['revision'],digest=snap['digest'],session_id=self.session)
        r.acknowledge_configuration(self.token,data)
        with r.connect() as db:
            db.execute("CREATE TRIGGER fail_observation BEFORE INSERT ON usage_execution_total_observations BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): r.acknowledge_runtime(self.token,data)
        with r.connect() as db:
            for table in ('usage_execution_grants','runtime_receipts','usage_execution_total_observations'):
                self.assertEqual(db.execute('SELECT count(*) FROM '+table).fetchone()[0],0)

    def test_untracked_mutation_and_no_transaction_are_rejected(self):
        r=self.setup_total()
        with r.connect() as db:
            with self.assertRaises(RuntimeError): current(db,self.period)
            db.execute('UPDATE usage_cycle_total_budgets SET total_bytes=1')
        with self.assertRaises(Error): self.cycles.set_total_budget(self.period,2)
        with self.assertRaises(Error): self.applied(r)
        self.assertEqual([x['total_bytes'] for x in self.rows(r)],[7])
