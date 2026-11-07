"""Internal authenticated declarations are retained, never billed or refunded."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import socket
import sqlite3
import unittest
from unittest.mock import patch
from allocation_evidence import AllocationEvidence
from allocation_quota import AllocationQuota, FIELDS
from core import Registry, Error
from payload_meter import PayloadMeter, MeteredWriter
from total_allocation import TotalAllocations
import test_total_allocation as base


class EvidenceTests(unittest.TestCase):
    setUp=base.AllocationTests.setUp
    tearDown=base.AllocationTests.tearDown
    fixtures=base.AllocationTests.fixtures
    fixture=base.AllocationTests.fixture
    setup_total=base.AllocationTests.setup_total
    second_host=base.AllocationTests.second_host
    request=base.AllocationTests.request
    reserved=base.AllocationTests.reserved

    def prepare(self):
        self.r=self.setup_total(7)
        self.allocation=TotalAllocations(self.r).reserve(self.token,self.request(7))
        self.scope={k:self.allocation[k] for k in FIELDS}
        self.local=Path(self.r.path).with_name('local-allocation.sqlite')
        self.quota=AllocationQuota(self.local,self.scope)
        self.api=AllocationEvidence(self.r)
        return self.r

    def evidence(self,sequence=1): return dict(self.quota.snapshot(),sequence=sequence)

    def count(self):
        with self.r.connect() as db:
            return db.execute('SELECT count(*) FROM usage_allocation_evidence').fetchone()[0]

    def test_real_tcp_success_and_unknown_are_authenticated_but_not_confirmed(self):
        r=self.prepare(); meter=PayloadMeter(); meter.bind(self.scope['server_id'])
        key=tuple(self.scope[k] for k in ('node_id','user_id','credential'))
        class Destination:
            def __init__(self,sock): self.sock=sock
            def write(self,data): self.sock.sendall(data)
        with socket.socket() as listener:
            listener.bind(('127.0.0.1',0)); listener.listen()
            with socket.create_connection(listener.getsockname(),timeout=2) as sock:
                peer,_=listener.accept()
                with peer:
                    peer.settimeout(2)
                    MeteredWriter(Destination(sock),meter,key,0,self.quota).write(b'hello')
                    self.assertEqual(peer.recv(8),b'hello')
        self.quota.reserve_write(1,2) # No transport: unresolved intent, never success.
        data=self.evidence(); receipt=self.api.receive(self.token,data)
        self.assertEqual(data['successful_upload_bytes'],5)
        self.assertEqual(data['unknown_download_bytes'],2)
        self.assertEqual(receipt['held_bytes'],7)
        self.assertTrue(receipt['evidence_only']); self.assertFalse(receipt['ready_for_execution'])
        encoded=json.dumps(data,sort_keys=True,separators=(',',':'))
        self.assertEqual(receipt['digest'],hashlib.sha256(encoded.encode()).hexdigest())
        from total_usage import current
        with r.connect() as db:
            db.execute('BEGIN')
            self.assertEqual(current(db,self.period),0)
            self.assertEqual(db.execute('SELECT count(*) FROM usage_deltas').fetchone()[0],0)
        self.assertEqual(self.reserved(r),7)
        with self.assertRaises(Error): TotalAllocations(r).reserve(self.token,self.request(1))

    def test_immutable_retry_highwaters_and_pending_completion(self):
        self.prepare(); allowed,ticket=self.quota.reserve_write(0,5)
        self.assertEqual(allowed,5)
        first=self.evidence(); original=self.api.receive(self.token,first)
        self.quota.complete_write(ticket) # Controlled local completion, not remote proof.
        latest=self.evidence(3); self.api.receive(self.token,latest)
        reopened=AllocationEvidence(Registry(self.r.path))
        self.assertEqual(reopened.receive(self.token,first),dict(original,duplicate=True))
        for data in (dict(first,sequence=2),dict(first,sequence=4),dict(latest,sequence=1)):
            with self.subTest(data=data),self.assertRaises(Error): reopened.receive(self.token,data)
        self.assertEqual(self.count(),2); self.assertEqual(self.reserved(self.r),7)

    def test_scope_counter_types_and_cross_agent_are_rejected(self):
        self.prepare(); data=self.evidence()
        for key,value in (('policy_id',True),('issued_at',float('nan')),('valid_until',False),
                          ('sequence',True),('sequence',0),('reserved_bytes',8),
                          ('remaining_bytes',-1),('ready_for_execution',True),
                          ('successful_upload_bytes',1),('unknown_upload_bytes',1),
                          ('unattributed_upload_bytes',1),('server_id',self.scope['server_id']+1),
                          ('cycle_id','other'),('credential','other'),('node_id',999)):
            with self.subTest(key=key,value=value),self.assertRaises(Error):
                self.api.receive(self.token,dict(data,**{key:value}))
        for malformed in (dict(data,extra=0),{k:v for k,v in data.items() if k!='sequence'}):
            with self.assertRaises(Error): self.api.receive(self.token,malformed)
        foreign,other=self.second_host(self.r)
        with self.assertRaises(Error): self.api.receive(foreign,dict(data,session_id=other['session_id']))
        self.assertEqual(self.count(),0)

    def test_expiry_and_lowering_never_rewrite_original_evidence_or_hold(self):
        self.prepare(); self.quota.reserve(0,2); first=self.evidence()
        self.api.receive(self.token,first)
        self.cycles.set_total_budget(self.period,0)
        with patch('allocation_evidence.time.time',return_value=self.scope['valid_until']+100):
            receipt=self.api.receive(self.token,dict(first,sequence=2))
        self.assertFalse(receipt['ready_for_execution']); self.assertEqual(receipt['held_bytes'],7)
        # Unattributed reservations cannot become attributed merely by a claim.
        with self.assertRaises(Error):
            self.api.receive(self.token,dict(first,sequence=3,unattributed_upload_bytes=0))
        self.assertEqual(self.count(),2)

    def test_rotation_revokes_even_duplicate_intake(self):
        self.prepare(); first=self.evidence(); self.api.receive(self.token,first)
        sid=self.scope['server_id']; new=self.r.provision_agent(sid)['token']
        for token in (self.token,new):
            with self.assertRaises(Error): self.api.receive(token,first)
        self.assertEqual(self.count(),1); self.assertEqual(self.reserved(self.r),7)

    def test_session_retirement_rejects_new_and_duplicate_evidence(self):
        import uuid
        self.prepare(); first=self.evidence(); self.api.receive(self.token,first)
        self.r.begin_session(self.token,dict(session_id=str(uuid.uuid4())))
        for data in (first,dict(first,sequence=2)):
            with self.assertRaises(Error): self.api.receive(self.token,data)
        self.assertEqual(self.count(),1); self.assertEqual(self.reserved(self.r),7)

    def test_storage_failure_rolls_back_and_restart_retries_exactly(self):
        self.prepare(); first=self.evidence()
        with self.r.connect() as db:
            db.execute("CREATE TRIGGER fail_evidence BEFORE INSERT ON usage_allocation_evidence BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.api.receive(self.token,first)
        self.assertEqual(self.count(),0)
        with self.r.connect() as db: db.execute('DROP TRIGGER fail_evidence')
        receipt=self.api.receive(self.token,first)
        self.assertEqual(AllocationEvidence(Registry(self.r.path)).receive(self.token,first),dict(receipt,duplicate=True))
        self.assertEqual(self.count(),1)

    def test_conflicting_concurrent_sequence_only_one_commits(self):
        self.prepare(); self.quota.reserve_write(0,5); pending=self.evidence()
        successful=dict(pending,successful_upload_bytes=5,unknown_upload_bytes=0)
        def run(data):
            try: return AllocationEvidence(Registry(self.r.path)).receive(self.token,data)
            except Error as error:
                self.assertEqual(error.status,409); return None
        with ThreadPoolExecutor(2) as pool: receipts=list(pool.map(run,(pending,successful)))
        self.assertEqual(sum(r is not None for r in receipts),1)
        self.assertEqual(self.count(),1); self.assertEqual(self.reserved(self.r),7)


if __name__=='__main__': unittest.main()
