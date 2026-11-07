"""Durable exact evidence retries; no network worker or billable usage."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
import signal
import sqlite3
import subprocess
import sys
import unittest

from allocation_evidence import AllocationEvidence
from allocation_outbox import acknowledge
from allocation_quota import AllocationQuota
from core import Registry
import test_allocation_evidence as base


class OutboxTests(unittest.TestCase):
    setUp=base.EvidenceTests.setUp
    tearDown=base.EvidenceTests.tearDown
    fixtures=base.EvidenceTests.fixtures
    fixture=base.EvidenceTests.fixture
    setup_total=base.EvidenceTests.setup_total
    request=base.EvidenceTests.request
    reserved=base.EvidenceTests.reserved
    prepare=base.EvidenceTests.prepare

    def reopen(self): return AllocationQuota(self.local,self.scope)

    def test_lost_response_freezes_original_then_advances_after_exact_ack(self):
        self.prepare(); _,ticket=self.quota.reserve_write(0,5)
        first=self.quota.freeze_evidence()
        receipt=self.api.receive(self.token,first) # Server accepted; response lost.
        self.quota.complete_write(ticket)
        self.assertEqual(self.quota.freeze_evidence(),first) # Do not replace pending unknown.
        reopened=self.reopen(); retry=reopened.freeze_evidence()
        self.assertEqual(retry,first)
        recovered=AllocationEvidence(Registry(self.r.path)).receive(self.token,retry)
        self.assertEqual(recovered,dict(receipt,duplicate=True))
        self.assertTrue(acknowledge(reopened,recovered))
        self.assertFalse(acknowledge(reopened,recovered))
        second=reopened.freeze_evidence()
        self.assertEqual(second['sequence'],2)
        self.assertEqual(second['successful_upload_bytes'],5)
        self.assertEqual(second['unknown_upload_bytes'],0)
        with self.assertRaises(ValueError): acknowledge(reopened,receipt)
        self.assertEqual(reopened.freeze_evidence(),second)
        acknowledge(reopened,self.api.receive(self.token,second))
        from total_usage import current
        with self.r.connect() as db:
            db.execute('BEGIN'); self.assertEqual(current(db,self.period),0)
        self.assertEqual(self.reserved(self.r),7)

    def test_concurrent_freezes_share_one_sequence_and_snapshot(self):
        self.prepare(); self.quota.reserve_write(1,2)
        with ThreadPoolExecutor(4) as pool:
            requests=list(pool.map(lambda _:self.reopen().freeze_evidence(),range(4)))
        self.assertTrue(all(item==requests[0] for item in requests))
        self.assertEqual(requests[0]['sequence'],1)
        self.assertEqual(requests[0]['unknown_download_bytes'],2)
        receipt=self.api.receive(self.token,requests[0])
        with ThreadPoolExecutor(4) as pool:
            results=list(pool.map(lambda _:acknowledge(self.reopen(),receipt),range(4)))
        self.assertEqual(results.count(True),1)

    def test_wrong_receipts_never_acknowledge_or_replace_pending(self):
        self.prepare(); first=self.quota.freeze_evidence(); receipt=self.api.receive(self.token,first)
        for key,value in (('accepted',1),('duplicate',1),('evidence_only',1),
                          ('ready_for_execution',True),('sequence',True),('sequence',2),
                          ('server_id',True),('held_bytes',8),('digest','other'),
                          ('cycle_id','other'),('session_id','other'),('policy_id',999),
                          ('allocation_id','other')):
            with self.subTest(key=key),self.assertRaises(ValueError):
                acknowledge(self.quota,dict(receipt,**{key:value}))
            self.assertEqual(self.reopen().freeze_evidence(),first)
        for data in (dict(receipt,extra=1),{k:v for k,v in receipt.items() if k!='duplicate'}):
            with self.assertRaises(ValueError): acknowledge(self.quota,data)
        self.assertTrue(acknowledge(self.quota,receipt))

    def test_failed_ack_commit_keeps_exact_request_for_master_retry(self):
        self.prepare(); first=self.quota.freeze_evidence(); receipt=self.api.receive(self.token,first)
        with sqlite3.connect(self.local) as db:
            db.execute("CREATE TRIGGER fail_ack BEFORE UPDATE ON allocation_evidence_outbox BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(OSError): acknowledge(self.quota,receipt)
        self.assertEqual(self.reopen().freeze_evidence(),first)
        with sqlite3.connect(self.local) as db: db.execute('DROP TRIGGER fail_ack')
        duplicate=self.api.receive(self.token,first)
        self.assertTrue(duplicate['duplicate']); acknowledge(self.reopen(),duplicate)
        self.assertEqual(self.reopen().freeze_evidence()['sequence'],2)

    def test_failed_freeze_rolls_back_sequence_and_snapshot(self):
        self.prepare(); first=self.quota.freeze_evidence()
        acknowledge(self.quota,self.api.receive(self.token,first))
        self.quota.reserve_write(1,2)
        with sqlite3.connect(self.local) as db:
            db.execute("CREATE TRIGGER fail_freeze BEFORE INSERT ON allocation_evidence_outbox BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(OSError): self.quota.freeze_evidence()
        with sqlite3.connect(self.local) as db:
            self.assertEqual(db.execute('SELECT sequence,acknowledged FROM allocation_evidence_outbox').fetchone(),(1,1))
            db.execute('DROP TRIGGER fail_freeze')
        second=self.reopen().freeze_evidence()
        self.assertEqual(second['sequence'],2); self.assertEqual(second['unknown_download_bytes'],2)

    def test_sigkill_after_freeze_recovers_exact_request_and_unknown(self):
        self.prepare()
        script='''import json,sys,time
from allocation_quota import AllocationQuota
q=AllocationQuota(sys.argv[1],json.loads(sys.argv[2]))
q.reserve_write(0,5)
print(json.dumps(q.freeze_evidence(),sort_keys=True),flush=True)
time.sleep(60)
'''
        process=subprocess.Popen([sys.executable,'-c',script,str(self.local),json.dumps(self.scope)],
                                 stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,
                                 cwd=os.path.dirname(__file__))
        try:
            import selectors
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout,selectors.EVENT_READ)
                self.assertTrue(selector.select(10),'child did not freeze evidence')
            line=process.stdout.readline(); self.assertTrue(line)
            first=json.loads(line)
            process.send_signal(signal.SIGKILL); process.wait(timeout=5)
            self.assertEqual(process.returncode,-signal.SIGKILL)
            reopened=self.reopen(); self.assertEqual(reopened.freeze_evidence(),first)
            self.assertEqual(first['unknown_upload_bytes'],5)
            self.assertEqual(first['successful_upload_bytes'],0)
            self.assertEqual(first['remaining_bytes'],2)
            acknowledge(reopened,self.api.receive(self.token,first))
            self.assertEqual(reopened.snapshot()['unknown_upload_bytes'],5)
            self.assertEqual(self.reserved(self.r),7)
        finally:
            if process.poll() is None: process.kill(); process.wait(timeout=5)
            process.stdout.close(); process.stderr.close()


if __name__=='__main__': unittest.main()
