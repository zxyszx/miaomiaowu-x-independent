"""Attributed first-grant accounting requires the complete execution chain."""
import json
import sqlite3
import unittest
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from core import Registry,Error
from first_grant import FirstGrant
from grant_meter import GrantMeter
from total_allocation import held
from total_usage import directions
import test_first_grant as base

class GrantMeterTests(unittest.TestCase):
    setUp=base.FirstGrantTests.setUp
    tearDown=base.FirstGrantTests.tearDown
    fixtures=base.FirstGrantTests.fixtures
    fixture=base.FirstGrantTests.fixture
    setup_total=base.FirstGrantTests.setup_total
    setup_drain=base.FirstGrantTests.setup_drain
    applied=base.FirstGrantTests.applied
    report=base.FirstGrantTests.report
    data=base.FirstGrantTests.data
    encode=staticmethod(base.FirstGrantTests.encode)
    envelope=base.FirstGrantTests.envelope
    accept=base.FirstGrantTests.accept
    sealed=base.FirstGrantTests.sealed
    request=base.FirstGrantTests.request

    def prepared(self,apply=True,count=6):
        r=self.sealed(); self.g=FirstGrant(r).prepare(self.token,self.request(count))
        if apply:
            fact=dict(grant_id=self.g['grant_id'],server_id=self.g['server_id'],
                session_id=self.session,revision=1,digest='a'*64,
                runtime_digest='b'*64,protocol='vless',
                valid_until=self.g['valid_until'])
            execution=dict(fact,execution_id=str(uuid.uuid4()))
            with r.connect() as db:
                db.execute('INSERT INTO usage_first_total_configurations VALUES(?,?,?,?)',
                    (self.g['grant_id'],'{}','a'*64,time.time()))
                db.execute('INSERT INTO usage_first_total_installations VALUES(?,?)',
                    (self.g['grant_id'],json.dumps(fact,sort_keys=True,separators=(',',':'))))
                db.execute('INSERT INTO usage_first_total_applied VALUES(?,?,?,?)',
                    (self.g['grant_id'],1,'a'*64,self.g['valid_until']))
                db.execute('INSERT INTO usage_first_total_lifecycle VALUES(?,?,?,?,?,?)',
                    (self.g['grant_id'],self.g['server_id'],self.session,'a'*64,
                     'installed',time.time()))
                db.execute('INSERT INTO usage_first_total_execution VALUES(?,?,?)',
                    (self.g['grant_id'],json.dumps(execution,sort_keys=True,separators=(',',':')),
                     time.time()))
        return r

    def event(self,seq=1,up=2,down=1):
        return dict(grant_id=self.g['grant_id'],policy_id=self.g['policy_id'],session_id=self.session,
                    sequence=seq,upload_bytes=up,download_bytes=down)

    def meter(self,r,data=None): return GrantMeter(r).report(self.token,data or self.event())

    def balance(self,r):
        with r.connect() as db:
            db.execute('BEGIN')
            return directions(db,self.period),held(db,self.period)

    def test_undelivered_intent_never_enters_confirmed_usage(self):
        r=self.prepared(False)
        with self.assertRaises(Error): self.meter(r)
        self.assertEqual(self.balance(r),((4,3,0),6))
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_first_total_meter').fetchone()[0],0)

    def test_attributed_delta_moves_hold_to_confirmed_without_refund(self):
        r=self.prepared(); result=self.meter(r)
        self.assertEqual((result['confirmed_bytes'],result['held_bytes']),(3,3))
        self.assertFalse(result['ready_for_execution'])
        self.assertEqual(self.balance(r),((6,4,0),3))
        self.assertEqual(self.meter(r,self.event(2,3,3))['held_bytes'],0)
        self.assertEqual(self.balance(r),((7,6,0),0))
        with self.assertRaises(Error): self.meter(r,self.event(3,4,3))
        self.assertEqual(self.balance(r),((7,6,0),0))

    def test_exact_old_retry_after_new_high_water_reopen_and_expiry(self):
        r=self.prepared(); self.meter(r); self.meter(r,self.event(2,3,2))
        with patch('grant_meter.time.time',return_value=self.g['valid_until']+100):
            retry=self.meter(Registry(r.path))
            self.assertTrue(retry['duplicate']); self.assertEqual(retry['confirmed_bytes'],5)
            with self.assertRaises(Error): self.meter(r,self.event(3,3,3))
        self.assertEqual(self.balance(r),((7,5,0),1))

    def test_clock_rollback_before_issue_rejects_new_usage_without_spending_hold(self):
        r=self.prepared()
        with patch('grant_meter.time.time',return_value=self.g['issued_at']-1):
            with self.assertRaises(Error) as caught: self.meter(r)
        self.assertEqual(caught.exception.status,409)
        self.assertEqual(self.balance(r),((4,3,0),6))
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_first_total_meter').fetchone()[0],0)
        with patch('grant_meter.time.time',return_value=self.g['issued_at']):
            self.assertTrue(self.meter(r)['accepted'])
        self.assertEqual(self.balance(r),((6,4,0),3))

    def test_clock_rollback_allows_exact_retry_but_not_new_sequence(self):
        r=self.prepared(); self.meter(r)
        with patch('grant_meter.time.time',return_value=self.g['issued_at']-1):
            self.assertTrue(self.meter(Registry(r.path))['duplicate'])
            with self.assertRaises(Error): self.meter(r,self.event(2,3,2))
        self.assertEqual(self.balance(r),((6,4,0),3))
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_first_total_meter').fetchone()[0],1)

    def test_execution_deadline_is_exclusive_for_growth_but_not_exact_retry(self):
        r=self.prepared(); self.meter(r)
        with patch('grant_meter.time.time',return_value=self.g['valid_until']):
            self.assertTrue(self.meter(r)['duplicate'])
            with self.assertRaises(Error): self.meter(r,self.event(2,3,2))
        self.assertEqual(self.balance(r),((6,4,0),3))

    def test_conflicting_sequence_rollback_and_monotonic_directions(self):
        r=self.prepared(); self.meter(r)
        for data in [self.event(1,1,2),self.event(2,1,3),self.event(0),self.event(2,True,1),{**self.event(),'other':1}]:
            with self.assertRaises(Error): self.meter(r,data)
        self.assertEqual(self.balance(r),((6,4,0),3))

    def test_two_conflicting_concurrent_reports_commit_once(self):
        r=self.prepared()
        def send(data):
            try: return self.meter(Registry(r.path),data)['accepted']
            except Error as exc: return exc.status
        with ThreadPoolExecutor(2) as pool: out=list(pool.map(send,[self.event(),self.event(1,1,3)]))
        self.assertEqual(out.count(True),1); self.assertEqual(out.count(409),1)
        dirs,pending=self.balance(r)
        self.assertEqual(sum(dirs)+pending,13)
        with r.connect() as db: self.assertEqual(db.execute('SELECT count(*) FROM usage_first_total_meter').fetchone()[0],1)

    def test_failed_insert_rolls_back_confirmed_and_hold_in_same_transaction(self):
        r=self.prepared()
        with r.connect() as db:
            db.execute("CREATE TRIGGER fail_meter BEFORE INSERT ON usage_first_total_meter BEGIN SELECT RAISE(ABORT,'disk failure'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.meter(r)
        self.assertEqual(self.balance(r),((4,3,0),6))
        with r.connect() as db: db.execute('DROP TRIGGER fail_meter')
        self.meter(r); self.assertEqual(self.balance(r),((6,4,0),3))

    def test_failed_source_update_rolls_back_event_total_and_meter(self):
        r=self.prepared()
        with r.connect() as db:
            db.execute("""CREATE TRIGGER fail_source_update BEFORE UPDATE
                ON usage_attributed_sources BEGIN SELECT RAISE(ABORT,'disk failure'); END""")
        with self.assertRaises(sqlite3.IntegrityError): self.meter(r)
        self.assertEqual(self.balance(r),((4,3,0),6))
        with r.connect() as db:
            self.assertEqual(db.execute(
                """SELECT count(*) FROM usage_cycle_accounting_events
                WHERE event_kind='first_grant'""").fetchone()[0],0)
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_first_total_meter').fetchone()[0],0)
            self.assertEqual(tuple(db.execute('''SELECT upload_bytes,download_bytes
                FROM usage_attributed_sources''').fetchone()),('0','0'))

    def test_legacy_growth_fenced_but_exact_retry_and_original_policy_survive(self):
        r=self.prepared(); self.cycles.set_total_budget(self.period,0)
        self.assertTrue(self.report()['duplicate'])
        self.item['upload_bytes']+=1
        with self.assertRaises(Error): self.report(2)
        self.meter(r)
        self.assertEqual(self.balance(r),((6,4,0),3))
        with self.assertRaises(Error): self.meter(r,{**self.event(2,3,1),'policy_id':self.g['policy_id']+1})
        r.delete('nodes',self.nid)
        self.assertEqual(self.balance(Registry(r.path)),((6,4,0),3))

    def test_bad_applied_record_or_baseline_rejected_not_repaired(self):
        r=self.prepared()
        with r.connect() as db: db.execute("UPDATE usage_first_total_applied SET config_digest='bad'")
        with self.assertRaises(Error): self.meter(r)
        with r.connect() as db:
            db.execute('UPDATE usage_first_total_applied SET config_digest=?',('a'*64,))
            db.execute("UPDATE usage_cycle_confirmed_totals SET total_bytes='8'")
        with self.assertRaises(Error): self.meter(r)
        with r.connect() as db: self.assertEqual(db.execute('SELECT count(*) FROM usage_first_total_meter').fetchone()[0],0)

    def test_retired_session_and_revoked_credentials_cannot_meter_or_retry(self):
        import uuid
        r=self.prepared(); self.meter(r)
        r.begin_session(self.token,dict(session_id=str(uuid.uuid4())))
        with self.assertRaises(Error): self.meter(r)
        with r.connect() as db: db.execute("UPDATE agents SET token_hash='revoked'")
        with self.assertRaises(Error): self.meter(r)
        self.assertEqual(self.balance(r),((6,4,0),3))

    def test_direction_damage_is_blocked_not_repaired_or_refunded(self):
        r=self.prepared()
        with r.connect() as db:
            db.execute("UPDATE usage_cycle_confirmed_directions SET upload_bytes='2',unknown_bytes='2'")
        with self.assertRaises(Error): self.meter(r)
        with self.assertRaises(Error): self.balance(r)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_first_total_meter').fetchone()[0],0)
            self.assertEqual(tuple(db.execute('''SELECT upload_bytes,download_bytes,reserved_bytes
                FROM usage_attributed_sources''').fetchone()),('0','0','6'))

if __name__=='__main__': unittest.main()
