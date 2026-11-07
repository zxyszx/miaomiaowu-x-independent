"""Real SQLite reservations; no claim of Agent delivery or payload enforcement."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
import signal
import sqlite3
import subprocess
import sys
import unittest
import uuid
from unittest.mock import patch
from core import Registry,Error
from total_allocation import TotalAllocations
import test_total_cycle_ledger as base


class AllocationTests(unittest.TestCase):
    setUp=base.TotalLedgerTests.setUp
    tearDown=base.TotalLedgerTests.tearDown
    fixtures=base.TotalLedgerTests.fixtures
    fixture=base.TotalLedgerTests.fixture
    setup_total=base.TotalLedgerTests.setup_total
    second_host=base.TotalLedgerTests.second_host
    report=base.TotalLedgerTests.report

    def request(self,count=6):
        return dict(request_id=str(uuid.uuid4()),session_id=self.session,cycle_id=self.period,
                    node_id=self.nid,user_id=self.uid,credential=self.item['credential'],requested_bytes=count)

    def reserved(self,r):
        with r.connect() as db:
            return sum(x[0] for x in db.execute('SELECT reserved_bytes FROM usage_total_allocations'))

    def test_concurrent_hosts_share_one_cycle_balance(self):
        r=self.setup_total(); other_token,other=self.second_host(r)
        a=self.request(); b={**self.request(),**{k:other['items'][0][k] for k in ('node_id','credential')},'session_id':other['session_id']}
        def reserve(args): return TotalAllocations(Registry(r.path)).reserve(*args)
        with ThreadPoolExecutor(2) as pool: rows=list(pool.map(reserve,[(self.token,a),(other_token,b)]))
        self.assertEqual(sorted(x['reserved_bytes'] for x in rows),[1,6])
        self.assertEqual(self.reserved(r),7)
        self.assertNotEqual(rows[0]['allocation_id'],rows[1]['allocation_id'])
        with self.assertRaises(Error): TotalAllocations(r).reserve(self.token,self.request(1))

    def test_idempotent_retry_survives_restart_policy_change_and_expiry(self):
        r=self.setup_total(); data=self.request(); first=TotalAllocations(r).reserve(self.token,data)
        self.cycles.set_total_budget(self.period,0)
        with patch('total_allocation.time.time',return_value=self.now+60):
            self.assertEqual(TotalAllocations(Registry(r.path)).reserve(self.token,data),first)
            with self.assertRaises(Error): TotalAllocations(r).reserve(self.token,self.request(1))
        self.cycles.set_total_budget(self.period,7)
        with patch('total_allocation.time.time',return_value=self.now+60):
            second=TotalAllocations(r).reserve(self.token,self.request(6))
        self.assertEqual(second['reserved_bytes'],1)
        self.assertNotEqual(first['policy_id'],second['policy_id'])
        self.assertEqual(self.reserved(r),7)
        self.assertLess(first['valid_until'],second['issued_at'])
        self.assertFalse(first['ready_for_execution'])

    def test_confirmed_legacy_usage_is_also_charged_without_guessing_attribution(self):
        r=self.setup_total(20); self.report()
        a=TotalAllocations(r).reserve(self.token,self.request(10))
        self.assertEqual(a['reserved_bytes'],10)
        self.item['upload_bytes']=5; self.report(2)
        b=TotalAllocations(r).reserve(self.token,self.request(10))
        self.assertEqual(b['reserved_bytes'],2) # 20 - confirmed 8 - unresolved 10
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],5)
        with r.connect() as db: self.assertEqual(db.execute('SELECT count(*) FROM usage_execution_grants').fetchone()[0],0)

    def test_replay_conflicts_authentication_and_token_rotation_do_not_refund(self):
        r=self.setup_total(); data=self.request(); a=TotalAllocations(r); a.reserve(self.token,data)
        with self.assertRaises(Error) as e: a.reserve(self.token,dict(data,requested_bytes=5))
        self.assertEqual(e.exception.status,409)
        token,other=self.second_host(r)
        foreign=dict(data,session_id=other['session_id'])
        with self.assertRaises(Error) as e: a.reserve(token,foreign)
        self.assertEqual(e.exception.status,403)
        sid=r.configuration(self.token)['server_id']; old=self.token
        self.token=r.provision_agent(sid)['token']; self.session=str(uuid.uuid4())
        r.begin_session(self.token,dict(session_id=self.session))
        for token in (old,self.token):
            with self.assertRaises(Error): a.reserve(token,data)
        self.assertEqual(a.reserve(self.token,self.request(6))['reserved_bytes'],1)
        self.assertEqual(self.reserved(r),7)

    def test_identity_cycle_enabled_state_and_validation(self):
        r=self.setup_total(); a=TotalAllocations(r); data=self.request()
        for field,value in (('node_id',True),('user_id',0),('requested_bytes',False),('requested_bytes',0),('requested_bytes',2**63),('session_id','bad'),('request_id',None),('cycle_id',None)):
            with self.subTest(field=field,value=value),self.assertRaises(Error): a.reserve(self.token,dict(data,**{field:value}))
        for bad in (dict(data,credential=str(uuid.uuid4())),dict(data,cycle_id='other')):
            with self.assertRaises(Error): a.reserve(self.token,bad)
        from test_xray import VLESS
        r.configure_node(self.nid,dict(VLESS,enabled=False))
        with self.assertRaises(Error): a.reserve(self.token,data)
        self.assertEqual(self.reserved(r),0)

    def test_commit_failure_rolls_back_baseline_and_reservation(self):
        r=self.setup_total()
        with r.connect() as db:
            db.execute('DELETE FROM usage_total_policy_versions')
            db.execute("CREATE TRIGGER fail_allocation BEFORE INSERT ON usage_total_allocations BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): TotalAllocations(r).reserve(self.token,self.request())
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_total_policy_versions').fetchone()[0],0)
        self.assertEqual(self.reserved(r),0)

    def test_sigkill_after_commit_before_reply_preserves_exact_reservation(self):
        r=self.setup_total(); data=self.request()
        script='''import json,os,signal,sys
from core import Registry
from total_allocation import TotalAllocations
row=TotalAllocations(Registry(sys.argv[1])).reserve(sys.argv[2],json.loads(sys.argv[3]))
print(json.dumps(row),flush=True)
os.kill(os.getpid(),signal.SIGKILL)
'''
        proc=subprocess.run([sys.executable,'-c',script,str(r.path),self.token,json.dumps(data)],
                            env={**os.environ,'PYTHONPATH':str(__import__('pathlib').Path(__file__).parent)},capture_output=True,text=True,timeout=10)
        self.assertEqual(proc.returncode,-signal.SIGKILL,proc.stderr)
        original=json.loads(proc.stdout)
        self.assertEqual(TotalAllocations(Registry(r.path)).reserve(self.token,data),original)
        self.assertEqual(self.reserved(r),6)

    def test_unlimited_policy_still_issues_finite_chunks_and_host_deletion_keeps_hold(self):
        r=self.setup_total(None); a=TotalAllocations(r)
        self.assertEqual(a.reserve(self.token,self.request(2**63-1))['reserved_bytes'],2**63-1)
        sid=r.configuration(self.token)['server_id']; r.delete('nodes',self.nid); r.delete('servers',sid)
        self.assertEqual(self.reserved(r),2**63-1)
        self.cycles.set_total_budget(self.period,10)
        token,data=self.second_host(r)
        request={**self.request(1),'session_id':data['session_id'],'node_id':data['items'][0]['node_id'],'credential':data['items'][0]['credential']}
        with self.assertRaises(Error): a.reserve(token,request)
