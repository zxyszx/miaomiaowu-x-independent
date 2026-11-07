"""First intent reserves once under SQLite, without enabling Agent execution."""
import sqlite3
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from core import Registry,Error
from first_grant import FirstGrant
from total_allocation import TotalAllocations,held
import test_grant_admission as base
import test_total_allocation as allocation_base


class FirstGrantTests(unittest.TestCase):
    setUp=base.GrantAdmissionTests.setUp
    tearDown=base.GrantAdmissionTests.tearDown
    fixtures=base.GrantAdmissionTests.fixtures
    fixture=base.GrantAdmissionTests.fixture
    setup_total=base.GrantAdmissionTests.setup_total
    setup_drain=base.GrantAdmissionTests.setup_drain
    applied=base.GrantAdmissionTests.applied
    report=base.GrantAdmissionTests.report
    data=base.GrantAdmissionTests.data
    encode=staticmethod(base.GrantAdmissionTests.encode)
    envelope=base.GrantAdmissionTests.envelope
    accept=base.GrantAdmissionTests.accept
    sealed=base.GrantAdmissionTests.sealed
    request=allocation_base.AllocationTests.request

    def prepare(self,r,data=None):
        return FirstGrant(r).prepare(self.token,data or self.request())

    def test_atomic_baseline_partial_balance_and_original_uuid(self):
        r=self.sealed(); self.cycles.set_total_budget(self.period,10)
        result=self.prepare(r,self.request(20))
        self.assertEqual((result['baseline_bytes'],result['reserved_bytes']),(7,3))
        self.assertFalse(result['ready_for_execution'])
        self.assertEqual(result['delivery_state'],'not_delivered')
        self.assertEqual(result['credential'],self.item['credential'])
        with r.connect() as db:
            db.execute('BEGIN')
            self.assertEqual(held(db,self.period),3)
            self.assertEqual(db.execute('SELECT total_bytes FROM usage_cycle_confirmed_totals').fetchone()[0],'7')
        self.assertEqual(r.configuration(self.token)['nodes'][0]['clients'],[])

    def test_exact_retry_reopen_expiry_lowering_and_cleanup_do_not_refund_or_extend(self):
        r=self.sealed(); data=self.request(6); first=self.prepare(r,data)
        self.cycles.set_total_budget(self.period,0)
        r.delete('nodes',self.nid)
        with patch('first_grant.time.time',return_value=self.now+10000):
            self.assertEqual(self.prepare(Registry(r.path),data),first)
            with self.assertRaises(Error): self.prepare(r)
        r.delete('servers',first['server_id'])
        with r.connect() as db:
            db.execute('BEGIN')
            self.assertEqual(held(db,self.period),6)

    def test_two_concurrent_requests_cannot_each_get_full_cycle_balance(self):
        r=self.sealed(); data=[self.request(100),self.request(100)]
        def attempt(item):
            try: return self.prepare(Registry(r.path),item)['reserved_bytes']
            except Error as exc: return exc.status
        with ThreadPoolExecutor(2) as pool: results=list(pool.map(attempt,data))
        self.assertEqual(sorted(results),[93,409])
        with r.connect() as db:
            db.execute('BEGIN'); self.assertEqual(held(db,self.period),93)

    def test_same_request_concurrently_is_immutable_one_row(self):
        r=self.sealed(); data=self.request(6)
        with ThreadPoolExecutor(2) as pool:
            rows=list(pool.map(lambda _:self.prepare(Registry(r.path),data),range(2)))
        self.assertEqual(rows[0],rows[1])
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_first_total_grants').fetchone()[0],1)

    def test_missing_seal_unknown_and_existing_internal_hold_block(self):
        r=self.setup_total(100); TotalAllocations(r).reserve(self.token,self.request())
        self.sid=self.applied(r)['server_id']
        from usage_migration import UsageMigration
        self.migration=UsageMigration(r); self.migration.begin_drain(self.period)
        self.report(); self.accept()
        with self.assertRaises(Error): self.prepare(r)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_first_total_grants').fetchone()[0],0)

    def test_invalid_identity_bool_unknown_fields_and_changed_request_rejected(self):
        r=self.sealed(); data=self.request()
        for key,value in [('node_id',999),('user_id',999),('credential',str(uuid.uuid4())),
                          ('requested_bytes',True),('requested_bytes',0),('extra',1)]:
            with self.subTest(key=key),self.assertRaises(Error): self.prepare(r,{**data,key:value})
        first=self.prepare(r,data)
        with self.assertRaises(Error): self.prepare(r,{**data,'requested_bytes':7})
        self.assertEqual(self.prepare(r,data),first)

    def test_token_rotation_or_retired_session_cannot_recover_receipt(self):
        r=self.sealed(); data=self.request(); self.prepare(r,data)
        r.begin_session(self.token,dict(session_id=str(uuid.uuid4())))
        with self.assertRaises(Error): self.prepare(r,data)
        self.session=r.configuration(self.token).get('session_id',self.session)
        with r.connect() as db: db.execute("UPDATE agents SET token_hash='revoked'")
        with self.assertRaises(Error): self.prepare(r,data)

    def test_zero_denies_unlimited_still_reserves_finite_requested_count(self):
        r=self.sealed(); self.cycles.set_total_budget(self.period,0)
        with self.assertRaises(Error): self.prepare(r)
        self.cycles.set_total_budget(self.period,None)
        self.assertEqual(self.prepare(r,self.request(123))['reserved_bytes'],123)

    def test_insert_failure_rolls_back_admission_and_leaves_baseline_unchanged(self):
        r=self.sealed()
        with r.connect() as db:
            db.execute('''CREATE TRIGGER fail_first_grant BEFORE INSERT ON usage_first_total_grants
                BEGIN SELECT RAISE(ABORT,'simulated grant disk failure'); END''')
        with self.assertRaises(sqlite3.IntegrityError): self.prepare(r)
        with r.connect() as db:
            db.execute('BEGIN'); self.assertEqual(held(db,self.period),0)
            self.assertEqual(db.execute('SELECT total_bytes FROM usage_cycle_confirmed_totals').fetchone()[0],'7')
            db.execute('DROP TRIGGER fail_first_grant')
        self.assertEqual(self.prepare(r)['reserved_bytes'],6)


if __name__=='__main__': unittest.main()
