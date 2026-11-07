"""Durable staging is deliberately not runtime installation or execution proof."""
import copy
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from grant_agent import GrantStageStore,validate,FIELDS
from node_config import digest
import test_grant_configuration as binding


class GrantAgentTests(unittest.TestCase):
    def setUp(self):
        self.fixture=binding.GrantConfigurationTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        registry=self.fixture.start()
        self.row=self.fixture.bind(registry)
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.path=Path(self.temp.name)/'stage.db'
        self.store=GrantStageStore(self.path,self.row['server_id'],self.row['session_id'])

    def sign(self,row):
        row['digest']=digest({k:v for k,v in row.items() if k in FIELDS|{'server_bandwidth'}})
        return row

    def identity(self,row=None,kind='first_grant'):
        row=self.row if row is None else row
        return {'accepted':True,'state':'ledger_resolved','ledger_kind':kind,
                'ledger_id':row['grant_id'],'binding_digest':row['digest'],
                'ready_for_execution':False}

    def stage(self,row=None,kind='first_grant'):
        row=self.row if row is None else row
        return self.store.stage(row,self.identity(row,kind))

    def rows(self):
        with sqlite3.connect(self.path) as db:
            return db.execute('SELECT payload,digest FROM staged_grants').fetchall()

    def test_real_binding_reopen_and_no_execution_receipt(self):
        receipt=self.stage()
        self.assertEqual(receipt['state'],'staged'); self.assertFalse(receipt['ready_for_execution'])
        self.assertEqual(GrantStageStore(self.path,self.row['server_id'],self.row['session_id']).stage(self.row),receipt)
        self.assertEqual(len(self.rows()),1)
        with sqlite3.connect(self.path) as db:
            self.assertEqual({r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")},
                             {'grant_owner','staged_grants','quarantined_grants','grant_stage_schema'})

    def test_wrong_identity_protocol_digest_and_unknown_fields_rejected(self):
        for changes in ({'server_id':999},{'session_id':str(uuid.uuid4())},{'schema':'other'},
                        {'digest':'0'*64},{'extra':1},{'ready_for_execution':True},
                        {'revision':True},{'reserved_bytes':True}):
            row=copy.deepcopy(self.row); row.update(changes)
            with self.subTest(changes=changes),self.assertRaises(ValueError): self.stage(row)
        self.assertFalse(self.path.exists())

    def test_policy_validation_before_cross_field_checks(self):
        for changes in ({'cycle_usage':'bad'},{'valid_until':'bad'},{'uuid':'bad'},
                        {'connections':{'max_connections':-1}}):
            row=copy.deepcopy(self.row); row['client'].update(changes); self.sign(row)
            with self.subTest(changes=changes),self.assertRaises(ValueError): self.stage(row)

    def test_expiry_and_clock_reversal_do_not_remove_saved_grant(self):
        self.stage(); saved=self.rows()
        for now in (self.row['issued_at']-1,self.row['valid_until']):
            with patch('grant_agent.time.time',return_value=now),self.assertRaises(ValueError): self.stage()
        self.assertEqual(self.rows(),saved)

    def test_same_cycle_second_grant_and_changed_payload_cannot_overwrite(self):
        self.stage(); saved=self.rows()
        for changes in ({'reserved_bytes':self.row['reserved_bytes']+1},):
            row=copy.deepcopy(self.row); row.update(changes); self.sign(row)
            with self.assertRaises((ValueError,sqlite3.IntegrityError)): self.stage(row)
        self.assertEqual(self.rows(),saved)

    def test_concurrent_retry_one_immutable_row(self):
        self.stage()
        with ThreadPoolExecutor(2) as pool:
            receipts=list(pool.map(lambda _:self.store.stage(self.row),range(2)))
        self.assertEqual(receipts[0],receipts[1]); self.assertEqual(len(self.rows()),1)

    def test_insert_failure_rolls_back_then_retry(self):
        self.stage()
        with sqlite3.connect(self.path) as db:
            db.execute('DELETE FROM staged_grants')
            db.execute("CREATE TRIGGER failed BEFORE INSERT ON staged_grants BEGIN SELECT RAISE(ABORT,'disk'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.stage()
        self.assertEqual(self.rows(),[])
        with sqlite3.connect(self.path) as db: db.execute('DROP TRIGGER failed')
        self.stage(); self.assertEqual(len(self.rows()),1)

    def test_large_baseline_exact_json_and_cross_field_rejection(self):
        row=copy.deepcopy(self.row); row['baseline_bytes']=2**70
        row['client'].update(cycle_id=row['cycle_id'],cycle_usage={'upload_bytes':2**70,'download_bytes':0},
                             cycle_budget={'upload_bytes':2**71,'download_bytes':1})
        self.sign(row); self.stage(row)
        self.assertEqual(json.loads(self.rows()[0][0])['baseline_bytes'],2**70)
        row['client']['cycle_usage']['download_bytes']=1; self.sign(row)
        with self.assertRaises(ValueError): self.stage(row)

    def test_deadline_rechecked_inside_transaction(self):
        with patch('grant_agent.time.time',side_effect=[self.row['issued_at'],self.row['valid_until']]),self.assertRaises(ValueError):
            self.stage()
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM staged_grants').fetchone()[0],0)

    def test_committed_stage_survives_real_sigkill(self):
        source=Path(self.temp.name)/'binding.json'; source.write_text(json.dumps(self.row))
        script="""import json,sys,signal
from grant_agent import GrantStageStore
v=json.load(open(sys.argv[2])); identity={'accepted':True,'state':'ledger_resolved','ledger_kind':'first_grant','ledger_id':v['grant_id'],'binding_digest':v['digest'],'ready_for_execution':False}; GrantStageStore(sys.argv[1],v['server_id'],v['session_id']).stage(v,identity)
print('committed',flush=True)
signal.pause()
"""
        child=subprocess.Popen([sys.executable,'-c',script,str(self.path),str(source)],stdout=subprocess.PIPE,text=True,cwd=Path(__file__).parent)
        try:
            import select
            self.assertTrue(select.select([child.stdout],[],[],10)[0])
            self.assertEqual(child.stdout.readline().strip(),'committed')
            os.kill(child.pid,signal.SIGKILL); child.wait(timeout=10)
            saved=self.rows(); self.store.stage(self.row); self.assertEqual(saved,self.rows())
        finally:
            if child.poll() is None: child.kill(); child.wait(timeout=10)
            child.stdout.close()
