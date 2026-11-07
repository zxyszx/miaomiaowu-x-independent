"""C7-A additive accounting migration, evidence and transaction boundaries."""
import sqlite3
import time
import unittest

from core import Error,Registry
from first_grant import FirstGrant
from multi_agent_allocation import MultiAgentAllocations
from total_allocation import held
from total_usage import directions
from usage_accounting import VERSION,FIRST,MULTI,initialize_cycle,liability
from usage_cycles import UsageCycles
import test_grant_meter as meter_base
import test_multi_agent_allocation as multi_base


def reset_to_legacy(registry):
    with registry.connect() as db:
        db.execute('DELETE FROM usage_cycle_accounting_events')
        db.execute('DELETE FROM usage_attributed_sources')
        db.execute('DELETE FROM usage_cycle_accounting_anchors')
        db.execute('DELETE FROM schema_migrations WHERE name=?',(VERSION,))


class AccountingMigrationTests(unittest.TestCase):
    setUp=multi_base.MultiAgentAllocationTests.setUp
    tearDown=multi_base.MultiAgentAllocationTests.tearDown
    fixtures=multi_base.MultiAgentAllocationTests.fixtures
    fixture=multi_base.MultiAgentAllocationTests.fixture
    setup_total=multi_base.MultiAgentAllocationTests.setup_total
    setup_drain=multi_base.MultiAgentAllocationTests.setup_drain
    applied=multi_base.MultiAgentAllocationTests.applied
    report=multi_base.MultiAgentAllocationTests.report
    data=multi_base.MultiAgentAllocationTests.data
    encode=staticmethod(multi_base.MultiAgentAllocationTests.encode)
    envelope=multi_base.MultiAgentAllocationTests.envelope
    accept=multi_base.MultiAgentAllocationTests.accept
    sealed=multi_base.MultiAgentAllocationTests.sealed
    first_request=multi_base.MultiAgentAllocationTests.first_request
    second_host=multi_base.MultiAgentAllocationTests.second_host
    start=multi_base.MultiAgentAllocationTests.start
    host=multi_base.MultiAgentAllocationTests.host
    request=multi_base.MultiAgentAllocationTests.request
    balances=multi_base.MultiAgentAllocationTests.balances

    def test_existing_cycles_first_and_multi_migrate_once_without_rebilling(self):
        r=self.start();token,host=self.host(r)
        allocation=MultiAgentAllocations(r).allocate(token,self.request(host,4))
        with r.connect() as db:
            db.execute('BEGIN');before=(directions(db,self.period),held(db,self.period))
        reset_to_legacy(r)
        reopened=Registry(r.path)
        with reopened.connect() as db:
            db.execute('BEGIN')
            self.assertIsNotNone(db.execute(
                'SELECT 1 FROM schema_migrations WHERE name=?',(VERSION,)).fetchone())
            anchor=tuple(db.execute('''SELECT upload_bytes,download_bytes,unknown_bytes
                FROM usage_cycle_accounting_anchors WHERE cycle_id=?''',(self.period,)).fetchone())
            sources=[tuple(row) for row in db.execute('''SELECT ledger_kind,ledger_id,
                initial_sequence,accepted_sequence,initial_upload_bytes,initial_download_bytes,
                upload_bytes,download_bytes FROM usage_attributed_sources ORDER BY ledger_kind''')]
            self.assertEqual(anchor,tuple(str(v) for v in before[0]))
            self.assertEqual(sources,[(FIRST,self.first['grant_id'],0,0,'0','0','0','0'),
                                      (MULTI,allocation['allocation_id'],0,0,'0','0','0','0')])
            self.assertEqual((directions(db,self.period),held(db,self.period)),before)
        Registry(r.path)
        with r.connect() as db:
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_cycle_accounting_anchors').fetchone()[0],1)
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_attributed_sources').fetchone()[0],2)

    def test_marker_insert_failure_rolls_back_all_seed_rows_and_retries(self):
        r=self.start();token,host=self.host(r)
        MultiAgentAllocations(r).allocate(token,self.request(host,4))
        reset_to_legacy(r)
        with r.connect() as db:
            db.execute("""CREATE TRIGGER fail_accounting_marker BEFORE INSERT ON schema_migrations
                WHEN NEW.name='attributed-accounting-v1'
                BEGIN SELECT RAISE(ABORT,'fixture'); END""")
        with self.assertRaises(sqlite3.IntegrityError): Registry(r.path)
        with r.connect() as db:
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_cycle_accounting_anchors').fetchone()[0],0)
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_attributed_sources').fetchone()[0],0)
            self.assertIsNone(db.execute(
                'SELECT 1 FROM schema_migrations WHERE name=?',(VERSION,)).fetchone())
            db.execute('DROP TRIGGER fail_accounting_marker')
        Registry(r.path)
        with r.connect() as db:
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_cycle_accounting_anchors').fetchone()[0],1)
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_attributed_sources').fetchone()[0],2)

    def test_new_cycle_is_invisible_until_zero_accounting_commits(self):
        r=self.setup_total(20);future='future-accounting-cycle'
        first=r.connect();db=first.__enter__()
        try:
            db.execute('BEGIN IMMEDIATE')
            db.execute('INSERT INTO usage_cycles(id,user_id,starts_at,ends_at) VALUES(?,?,?,?)',
                       (future,self.uid,self.now+200,self.now+300))
            initialize_cycle(db,future)
            with r.connect() as observer:
                self.assertIsNone(observer.execute(
                    'SELECT 1 FROM usage_cycles WHERE id=?',(future,)).fetchone())
            first.__exit__(None,None,None)
            first=None
        finally:
            if first is not None:first.__exit__(Exception,Exception('rollback'),None)
        with r.connect() as db:
            self.assertEqual(tuple(db.execute('''SELECT t.total_bytes,d.upload_bytes,
                d.download_bytes,d.unknown_bytes,a.upload_bytes,a.download_bytes,a.unknown_bytes
                FROM usage_cycle_confirmed_totals t
                JOIN usage_cycle_confirmed_directions d ON d.cycle_id=t.cycle_id
                JOIN usage_cycle_accounting_anchors a ON a.cycle_id=t.cycle_id
                WHERE t.cycle_id=?''',(future,)).fetchone()),('0',)*7)

    def test_cycle_init_and_issuance_source_failures_leave_no_half_rows(self):
        r=self.setup_total(20);before=len(UsageCycles(r).list(self.uid))
        with r.connect() as db:
            db.execute("CREATE TRIGGER fail_anchor BEFORE INSERT ON usage_cycle_accounting_anchors BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            UsageCycles(r).create(self.uid,dict(starts_at=self.now+200,ends_at=self.now+300))
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_cycles').fetchone()[0],before)
            db.execute('DROP TRIGGER fail_anchor')

        r=self.sealed()
        with r.connect() as db:
            db.execute("CREATE TRIGGER fail_source BEFORE INSERT ON usage_attributed_sources BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            FirstGrant(r).prepare(self.token,self.first_request(2))
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_first_total_grants').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM usage_attributed_sources').fetchone()[0],0)

    def test_multi_source_failure_rolls_back_allocation_and_policy_write(self):
        r=self.start();token,host=self.host(r)
        with r.connect() as db:
            before_policy=db.execute(
                'SELECT count(*) FROM usage_total_policy_versions').fetchone()[0]
            db.execute("""CREATE TRIGGER fail_multi_source BEFORE INSERT
                ON usage_attributed_sources WHEN NEW.ledger_kind='multi_allocation'
                BEGIN SELECT RAISE(ABORT,'fixture'); END""")
        with self.assertRaises(sqlite3.IntegrityError):
            MultiAgentAllocations(r).allocate(token,self.request(host,4))
        with r.connect() as db:
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_multi_agent_allocations').fetchone()[0],0)
            self.assertEqual(db.execute('''SELECT count(*) FROM usage_attributed_sources
                WHERE ledger_kind=?''',(MULTI,)).fetchone()[0],0)
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_total_policy_versions').fetchone()[0],before_policy)

    def test_missing_source_blocks_retry_and_admission_without_recreation(self):
        r=self.start();data=self.first_request(2)
        with r.connect() as db:
            db.execute('DELETE FROM usage_attributed_sources WHERE ledger_kind=?',(FIRST,))
        with self.assertRaises(Error): FirstGrant(r).prepare(self.token,data)
        with self.assertRaises(Error):
            with r.connect() as db:
                db.execute('BEGIN');liability(db,self.period)
        with r.connect() as db:
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_attributed_sources').fetchone()[0],0)

    def test_raised_first_source_blocks_hold_multi_and_legacy_admission(self):
        r=self.start();token,host=self.host(r)
        with r.connect() as db:
            db.execute('''UPDATE usage_attributed_sources SET upload_bytes='1'
                WHERE ledger_kind=?''',(FIRST,))
        with r.connect() as db:
            db.execute('BEGIN')
            with self.assertRaises(Error): held(db,self.period)
            with self.assertRaises(Error): liability(db,self.period)
        with self.assertRaises(Error):
            MultiAgentAllocations(r).allocate(token,self.request(host,4))
        self.item['upload_bytes']+=1
        with self.assertRaises(Error): self.report(2)
        with r.connect() as db:
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_multi_agent_allocations').fetchone()[0],0)
            self.assertEqual(db.execute('''SELECT count(*) FROM usage_cycle_accounting_events
                WHERE event_kind=?''',(FIRST,)).fetchone()[0],0)

    def test_multi_source_baseline_and_current_must_remain_zero(self):
        r=self.start();token,host=self.host(r)
        allocation=MultiAgentAllocations(r).allocate(token,self.request(host,4))
        with r.connect() as db:
            db.execute('''UPDATE usage_attributed_sources
                SET initial_upload_bytes='1',upload_bytes='1'
                WHERE ledger_kind=? AND ledger_id=?''',
                (MULTI,allocation['allocation_id']))
        with r.connect() as db:
            db.execute('BEGIN')
            with self.assertRaises(Error): held(db,self.period)
            with self.assertRaises(Error): liability(db,self.period)
        with self.assertRaises(Error):
            MultiAgentAllocations(r).allocate(token,self.request(host,1))

    def test_cycle_parent_delete_is_restricted_and_evidence_remains(self):
        r=self.setup_total(20);self.report()
        with r.connect() as db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute('DELETE FROM usage_cycles WHERE id=?',(self.period,))
        with r.connect() as db:
            self.assertIsNotNone(db.execute(
                'SELECT 1 FROM usage_cycles WHERE id=?',(self.period,)).fetchone())
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_cycle_accounting_anchors').fetchone()[0],1)
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_cycle_accounting_events').fetchone()[0],1)

    def test_legacy_event_failure_rolls_back_counter_delta_and_confirmed(self):
        r=self.setup_total(20)
        with r.connect() as db:
            db.execute("CREATE TRIGGER fail_event BEFORE INSERT ON usage_cycle_accounting_events BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError):self.report()
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_cycle_accounting_events').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM usage_deltas').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM usage_counters').fetchone()[0],0)
            self.assertEqual(db.execute(
                'SELECT total_bytes FROM usage_cycle_confirmed_totals').fetchone()[0],'0')


class AccountingFirstMigrationTests(unittest.TestCase):
    setUp=meter_base.GrantMeterTests.setUp
    tearDown=meter_base.GrantMeterTests.tearDown
    fixtures=meter_base.GrantMeterTests.fixtures
    fixture=meter_base.GrantMeterTests.fixture
    setup_total=meter_base.GrantMeterTests.setup_total
    setup_drain=meter_base.GrantMeterTests.setup_drain
    applied=meter_base.GrantMeterTests.applied
    report=meter_base.GrantMeterTests.report
    data=meter_base.GrantMeterTests.data
    encode=staticmethod(meter_base.GrantMeterTests.encode)
    envelope=meter_base.GrantMeterTests.envelope
    accept=meter_base.GrantMeterTests.accept
    sealed=meter_base.GrantMeterTests.sealed
    request=meter_base.GrantMeterTests.request
    prepared=meter_base.GrantMeterTests.prepared
    event=meter_base.GrantMeterTests.event
    meter=meter_base.GrantMeterTests.meter
    balance=meter_base.GrantMeterTests.balance

    def test_existing_first_high_water_seeds_source_without_second_charge(self):
        r=self.prepared();self.meter(r,self.event(1,2,1))
        before=self.balance(r)
        reset_to_legacy(r)
        reopened=Registry(r.path)
        with reopened.connect() as db:
            source=db.execute('''SELECT initial_sequence,accepted_sequence,initial_upload_bytes,
                initial_download_bytes,upload_bytes,download_bytes,execution_id
                FROM usage_attributed_sources WHERE ledger_kind=? AND ledger_id=?''',
                (FIRST,self.g['grant_id'])).fetchone()
            self.assertEqual(tuple(source)[:6],(1,1,'2','1','2','1'))
            self.assertIsNotNone(source['execution_id'])
        self.assertEqual(self.balance(reopened),before)
        self.assertTrue(self.meter(reopened,self.event(1,2,1))['duplicate'])

    def assert_damage_blocked(self,r):
        with r.connect() as db:
            db.execute('BEGIN')
            with self.assertRaises(Error): held(db,self.period)
            with self.assertRaises(Error): liability(db,self.period)
        with self.assertRaises(Error): self.meter(r)

    def test_lowered_source_high_water_blocks_all_balance_reads(self):
        r=self.prepared();self.meter(r,self.event(1,2,1))
        with r.connect() as db:
            db.execute('''UPDATE usage_attributed_sources SET upload_bytes='1'
                WHERE ledger_kind=? AND ledger_id=?''',(FIRST,self.g['grant_id']))
        self.assert_damage_blocked(r)

    def test_meter_high_water_damage_blocks_source_and_balance_reads(self):
        r=self.prepared();self.meter(r,self.event(1,2,1))
        with r.connect() as db:
            db.execute('''UPDATE usage_first_total_meter SET upload_bytes=1
                WHERE grant_id=? AND sequence=1''',(self.g['grant_id'],))
        self.assert_damage_blocked(r)

    def test_deleted_zero_event_is_detected_by_meter_event_correspondence(self):
        r=self.prepared();self.meter(r,self.event(1,0,0))
        with r.connect() as db:
            db.execute('''DELETE FROM usage_cycle_accounting_events
                WHERE event_kind=? AND ledger_id=?''',(FIRST,self.g['grant_id']))
        self.assert_damage_blocked(r)

    def test_deleted_zero_event_and_meter_are_detected_by_source_sequence(self):
        r=self.prepared();self.meter(r,self.event(1,0,0))
        with r.connect() as db:
            db.execute('''DELETE FROM usage_cycle_accounting_events
                WHERE event_kind=? AND ledger_id=?''',(FIRST,self.g['grant_id']))
            db.execute('DELETE FROM usage_first_total_meter WHERE grant_id=?',
                       (self.g['grant_id'],))
        self.assert_damage_blocked(r)

    def test_source_assigned_to_wrong_cycle_blocks_both_cycles(self):
        r=self.prepared();future=UsageCycles(r).create(
            self.uid,dict(starts_at=self.now+200,ends_at=self.now+300))
        with r.connect() as db:
            db.execute('''UPDATE usage_attributed_sources SET cycle_id=?
                WHERE ledger_kind=? AND ledger_id=?''',
                (future['id'],FIRST,self.g['grant_id']))
        self.assert_damage_blocked(r)
        with r.connect() as db:
            db.execute('BEGIN')
            with self.assertRaises(Error): held(db,future['id'])

    def test_source_with_accepted_event_is_delete_restricted(self):
        r=self.prepared();self.meter(r,self.event(1,2,1))
        with r.connect() as db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute('''DELETE FROM usage_attributed_sources
                    WHERE ledger_kind=? AND ledger_id=?''',(FIRST,self.g['grant_id']))
        self.assertEqual(self.balance(r),((6,4,0),3))


if __name__=='__main__':unittest.main()
