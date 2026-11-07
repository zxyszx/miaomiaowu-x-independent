"""Exact first/multi stage identity and fail-closed legacy migration."""
import copy
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from grant_agent import FIELDS,GrantStageStore
from node_config import digest
import test_grant_configuration as binding


class MultiAgentStageTests(unittest.TestCase):
    def setUp(self):
        self.fixture=binding.GrantConfigurationTests()
        self.fixture.setUp(); self.addCleanup(self.fixture.tearDown)
        registry=self.fixture.start(); self.row=self.fixture.bind(registry)
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.path=Path(self.temp.name)/'multi-stage.db'
        self.store=GrantStageStore(self.path,self.row['server_id'],self.row['session_id'])

    @staticmethod
    def identity(row,kind):
        return {'accepted':True,'state':'ledger_resolved','ledger_kind':kind,
                'ledger_id':row['grant_id'],'binding_digest':row['digest'],
                'ready_for_execution':False}

    @staticmethod
    def sign(row):
        row['digest']=digest({key:value for key,value in row.items()
                              if key in FIELDS|{'server_bandwidth'}})
        return row

    def legacy(self,rows):
        with sqlite3.connect(self.path) as db:
            db.execute('CREATE TABLE grant_owner(server_id INTEGER NOT NULL)')
            db.execute('INSERT INTO grant_owner VALUES(?)',(self.row['server_id'],))
            db.execute('''CREATE TABLE staged_grants(
                grant_id TEXT PRIMARY KEY,cycle_id TEXT NOT NULL UNIQUE,
                payload TEXT NOT NULL,digest TEXT NOT NULL)''')
            db.executemany('INSERT INTO staged_grants VALUES(?,?,?,?)',rows)

    def test_same_cycle_accepts_multiple_exact_first_and_multi_identities(self):
        first=copy.deepcopy(self.row)
        later=copy.deepcopy(self.row); later['grant_id']=str(uuid.uuid4()); self.sign(later)
        one=self.store.stage(first,self.identity(first,'first_grant'))
        two=self.store.stage(later,self.identity(later,'multi_allocation'))
        self.assertEqual((one['ledger_kind'],two['ledger_kind']),
                         ('first_grant','multi_allocation'))
        with sqlite3.connect(self.path) as db:
            rows=db.execute('''SELECT ledger_kind,grant_id,cycle_id FROM staged_grants
                ORDER BY ledger_kind''').fetchall()
        self.assertEqual(len(rows),2)
        self.assertEqual({row[2] for row in rows},{self.row['cycle_id']})

    def test_legacy_rows_resolve_or_quarantine_without_payload_changes(self):
        ids=[str(uuid.uuid4()) for _ in range(3)]
        rows=[(ids[0],str(uuid.uuid4()),' {"first":1}\n','1'*64),
              (ids[1],str(uuid.uuid4()),'multi-bytes','2'*64),
              (ids[2],str(uuid.uuid4()),'unresolved\u96c6','3'*64)]
        self.legacy(rows)
        kinds={ids[0]:'first_grant',ids[1]:'multi_allocation'}
        def resolver(ledger_id,binding_digest):
            if ledger_id not in kinds: raise OSError('offline')
            return {'accepted':True,'state':'ledger_resolved',
                    'ledger_kind':kinds[ledger_id],'ledger_id':ledger_id,
                    'binding_digest':binding_digest,'ready_for_execution':False}
        self.store.migrate(resolver)
        with sqlite3.connect(self.path) as db:
            active=db.execute('SELECT grant_id,cycle_id,payload,digest FROM staged_grants').fetchall()
            quarantine=db.execute('''SELECT grant_id,cycle_id,payload,digest
                FROM quarantined_grants''').fetchall()
            marker=db.execute('SELECT version FROM grant_stage_schema').fetchall()
        self.assertEqual(sorted(active+quarantine),sorted(rows))
        self.assertEqual({row[0] for row in active},set(ids[:2]))
        self.assertEqual(quarantine,[rows[2]])
        self.assertEqual(marker,[(2,)])
        self.store.migrate(lambda *_:self.fail('idempotent reopen must not resolve again'))

    def test_no_identity_never_defaults_new_or_legacy_row_to_first(self):
        with self.assertRaisesRegex(ValueError,'精确账本身份'):
            self.store.stage(self.row)
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM staged_grants').fetchone()[0],0)
        other=Path(self.temp.name)/'legacy.db'
        old=GrantStageStore(other,self.row['server_id'],self.row['session_id'])
        self.path=other; self.legacy([(self.row['grant_id'],self.row['cycle_id'],
                                      json.dumps(self.row),self.row['digest'])])
        old.migrate()
        with sqlite3.connect(other) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM staged_grants').fetchone()[0],0)
            saved=db.execute('''SELECT grant_id,cycle_id,payload,digest
                FROM quarantined_grants''').fetchone()
        self.assertEqual(saved,(self.row['grant_id'],self.row['cycle_id'],
                                json.dumps(self.row),self.row['digest']))

    def test_mid_migration_failure_rolls_back_and_concurrent_retry_is_complete(self):
        rows=[(str(uuid.uuid4()),str(uuid.uuid4()),'payload-'+str(i),str(i)*64)
              for i in range(1,3)]
        self.legacy(rows)
        def resolver(ledger_id,binding_digest):
            return {'accepted':True,'state':'ledger_resolved','ledger_kind':'first_grant',
                    'ledger_id':ledger_id,'binding_digest':binding_digest,
                    'ready_for_execution':False}
        original=GrantStageStore._create_quarantine
        with patch.object(GrantStageStore,'_create_quarantine',side_effect=sqlite3.OperationalError('disk')):
            with self.assertRaises(sqlite3.OperationalError): self.store.migrate(resolver)
        with sqlite3.connect(self.path) as db:
            self.assertEqual(tuple(row[1] for row in db.execute('PRAGMA table_info(staged_grants)')),
                             ('grant_id','cycle_id','payload','digest'))
            self.assertEqual(db.execute('SELECT count(*) FROM staged_grants').fetchone()[0],2)
            self.assertNotIn('grant_stage_schema',{
                row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")})
        with ThreadPoolExecutor(2) as pool:
            list(pool.map(lambda _:GrantStageStore(
                self.path,self.row['server_id'],self.row['session_id']).migrate(resolver),range(2)))
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM staged_grants').fetchone()[0],2)
            self.assertEqual(db.execute('SELECT count(*) FROM quarantined_grants').fetchone()[0],0)

    def test_second_connection_observes_only_complete_old_or_new_schema(self):
        row=(str(uuid.uuid4()),str(uuid.uuid4()),'exact-payload','4'*64)
        self.legacy([row])
        entered,release=threading.Event(),threading.Event()
        original=GrantStageStore._create_quarantine
        def paused(db,name='quarantined_grants'):
            original(db,name); entered.set(); release.wait(2)
        def resolver(ledger_id,binding_digest):
            return {'accepted':True,'state':'ledger_resolved','ledger_kind':'first_grant',
                    'ledger_id':ledger_id,'binding_digest':binding_digest,
                    'ready_for_execution':False}
        with patch.object(GrantStageStore,'_create_quarantine',side_effect=paused):
            with ThreadPoolExecutor(1) as pool:
                future=pool.submit(self.store.migrate,resolver)
                self.assertTrue(entered.wait(1))
                with sqlite3.connect(self.path) as observer:
                    self.assertEqual(tuple(r[1] for r in observer.execute(
                        'PRAGMA table_info(staged_grants)')),
                        ('grant_id','cycle_id','payload','digest'))
                    self.assertEqual(observer.execute(
                        'SELECT grant_id,cycle_id,payload,digest FROM staged_grants').fetchone(),row)
                release.set(); future.result()
        with sqlite3.connect(self.path) as observer:
            self.assertIn('ledger_kind',tuple(r[1] for r in observer.execute(
                'PRAGMA table_info(staged_grants)')))
            self.assertEqual(observer.execute(
                'SELECT grant_id,cycle_id,payload,digest FROM staged_grants').fetchone(),row)


if __name__=='__main__': unittest.main()
