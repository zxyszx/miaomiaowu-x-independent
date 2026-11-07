"""Tagged claims do not release holds, authorize payloads or rebill legacy usage."""
from concurrent.futures import ThreadPoolExecutor
import sqlite3
import json
import os
import signal
import subprocess
import sys
from pathlib import Path
import unittest
from unittest.mock import patch
from core import Registry,Error
from total_allocation import TotalAllocations
import test_total_allocation as base


class ReportTests(unittest.TestCase):
    setUp=base.AllocationTests.setUp
    tearDown=base.AllocationTests.tearDown
    fixtures=base.AllocationTests.fixtures
    fixture=base.AllocationTests.fixture
    setup_total=base.AllocationTests.setup_total
    second_host=base.AllocationTests.second_host
    report=base.AllocationTests.report
    request=base.AllocationTests.request
    reserved=base.AllocationTests.reserved

    def prepare(self):
        r=self.setup_total(20); self.api=TotalAllocations(r)
        a=self.api.reserve(self.token,self.request(10))
        self.data=dict(allocation_id=a['allocation_id'],policy_id=a['policy_id'],
            session_id=self.session,sequence=1,upload_bytes=2,download_bytes=3)
        return r,a

    def test_claims_monotonic_duplex_and_historical_retry(self):
        r,a=self.prepare(); first=self.api.report(self.token,self.data)
        self.assertEqual((first['reported_bytes'],first['unresolved_bytes'],first['held_bytes']),(5,5,10))
        second=self.api.report(self.token,dict(self.data,sequence=3,upload_bytes=7))
        retry=self.api.report(self.token,self.data)
        self.assertTrue(retry['duplicate']); self.assertEqual(retry['reported_bytes'],10)
        self.assertFalse(second['ready_for_execution'])
        for change in (dict(sequence=2),dict(sequence=3,upload_bytes=6),dict(sequence=4,upload_bytes=6),dict(sequence=4,download_bytes=4)):
            with self.subTest(change=change),self.assertRaises(Error):
                self.api.report(self.token,{**self.data,'upload_bytes':7,**change})
        self.assertEqual(self.reserved(r),10)

    def test_policy_lowering_expiry_and_restart_keep_original_identity_and_hold(self):
        r,a=self.prepare(); self.cycles.set_total_budget(self.period,0)
        with patch('total_allocation.time.time',return_value=a['valid_until']+100):
            accepted=TotalAllocations(Registry(r.path)).report(self.token,self.data)
        self.assertEqual(accepted['policy_id'],a['policy_id'])
        with self.assertRaises(Error): self.api.report(self.token,dict(self.data,policy_id=a['policy_id']+1))
        with self.assertRaises(Error): self.api.reserve(self.token,self.request(1))
        self.assertEqual(self.reserved(r),10)

    def test_claims_do_not_rebill_legacy_or_release_uncertain_bytes(self):
        r,a=self.prepare(); self.api.report(self.token,self.data)
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],0)
        self.report() # legacy confirmed 4+3 is still unassociated
        self.assertEqual(self.api.reserve(self.token,self.request(10))['reserved_bytes'],3)
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],4)
        self.assertEqual(self.reserved(r),13)

    def test_auth_session_and_strict_validation(self):
        r,a=self.prepare(); token,other=self.second_host(r)
        with self.assertRaises(Error): self.api.report(token,dict(self.data,session_id=other['session_id']))
        for key,value in (('sequence',True),('sequence',0),('policy_id',False),('upload_bytes',-1),('download_bytes',2**63),('allocation_id','bad')):
            with self.subTest(key=key),self.assertRaises(Error): self.api.report(self.token,dict(self.data,**{key:value}))
        with self.assertRaises(Error): self.api.report(self.token,dict(self.data,extra=1))
        sid=r.configuration(self.token)['server_id']; new=r.provision_agent(sid)['token']
        with self.assertRaises(Error): self.api.report(self.token,self.data)
        with self.assertRaises(Error): self.api.report(new,self.data)

    def test_insert_failure_is_atomic_and_retry_is_durable(self):
        r,a=self.prepare()
        with r.connect() as db:
            db.execute("CREATE TRIGGER fail_claim BEFORE INSERT ON usage_total_allocation_reports BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.api.report(self.token,self.data)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_total_allocation_reports').fetchone()[0],0)
            db.execute('DROP TRIGGER fail_claim')
        self.api.report(self.token,self.data)
        retry=TotalAllocations(Registry(r.path)).report(self.token,self.data)
        self.assertTrue(retry['duplicate']); self.assertEqual(retry['held_bytes'],10)

    def test_concurrent_conflicting_sequence_only_one_commits(self):
        r,a=self.prepare()
        def submit(up):
            try:
                return TotalAllocations(Registry(r.path)).report(self.token,dict(self.data,upload_bytes=up))['reported_bytes']
            except Error as e:
                self.assertEqual(e.status,409); return None
        with ThreadPoolExecutor(2) as pool: result=list(pool.map(submit,[2,3]))
        self.assertEqual(sum(x is not None for x in result),1)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_total_allocation_reports').fetchone()[0],1)
        self.assertEqual(self.reserved(r),10)

    def test_real_sigkill_after_claim_commit_and_user_cleanup(self):
        r,a=self.prepare()
        script='''import json,os,signal,sys
from core import Registry
from total_allocation import TotalAllocations
print(json.dumps(TotalAllocations(Registry(sys.argv[1])).report(sys.argv[2],json.loads(sys.argv[3]))),flush=True)
os.kill(os.getpid(),signal.SIGKILL)
'''
        proc=subprocess.run([sys.executable,'-c',script,str(r.path),self.token,json.dumps(self.data)],
            env={**os.environ,'PYTHONPATH':str(Path(__file__).parent)},capture_output=True,text=True,timeout=10)
        self.assertEqual(proc.returncode,-signal.SIGKILL,proc.stderr)
        first=json.loads(proc.stdout)
        retry=TotalAllocations(Registry(r.path)).report(self.token,self.data)
        self.assertEqual(retry,{**first,'duplicate':True})
        self.assertEqual(self.reserved(r),10)
        with self.assertRaises(Error) as caught: r.delete('users',self.uid)
        self.assertEqual(caught.exception.status,409)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_total_allocation_reports').fetchone()[0],1)
            self.assertIsNotNone(db.execute('SELECT 1 FROM users WHERE id=?',(self.uid,)).fetchone())
