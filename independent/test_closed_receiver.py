"""Internal collector authentication and atomic completion, not remote stop proof."""
from pathlib import Path
import copy
import sqlite3
import uuid
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
import unittest
from core import Registry,Error
from closed_fragments import ClosedFragments,digest,encoded
from closed_receiver import ClosedReceiver
from usage_outbox import UsageOutbox
import test_drain_checkpoints as base

class ClosedReceiverTests(unittest.TestCase):
    setUp=base.DrainCheckpointTests.setUp
    tearDown=base.DrainCheckpointTests.tearDown
    fixtures=base.DrainCheckpointTests.fixtures
    fixture=base.DrainCheckpointTests.fixture
    setup_total=base.DrainCheckpointTests.setup_total
    applied=base.DrainCheckpointTests.applied
    data=base.DrainCheckpointTests.data
    report=base.DrainCheckpointTests.report
    setup_drain=base.DrainCheckpointTests.setup_drain

    def setup_fragments(self,count=129):
        r=self.setup_drain(); items=[copy.deepcopy(self.item)]
        # Isolated persistence fixture for historical execution identities. No
        # remote/runtime-stop claim is inferred from these seeded rows.
        with r.connect() as db:
            row=db.execute('SELECT * FROM usage_drain_inventory').fetchone()
            for i in range(count-1):
                item=dict(self.item,credential=str(uuid.UUID(int=i+1)))
                items.append(item)
                fields=list(row); fields[5]=item['credential']; fields[-2:]=[4,3]
                db.execute('INSERT INTO usage_drain_inventory VALUES(?,?,?,?,?,?,?,?,?,?,?)',fields)
                db.execute('INSERT INTO usage_counters VALUES(?,?,?,?,?,?,?)',
                           (self.sid,self.session,item['node_id'],item['user_id'],item['credential'],4,3))
        box=UsageOutbox(Path(r.path).parent/'fragments-outbox.db',self.sid)
        box.capture(self.session,dict(server_id=self.sid,items=items)); self.final=box.close_epoch(self.session)
        f=ClosedFragments(box); self.manifest=f.prepare(self.session)
        self.parts=[f.part(self.session,i) for i in range(len(self.manifest['manifest']['parts']))]
        self.r=r; self.receiver=ClosedReceiver(r)
        return r

    def accept(self,index,manifest=None,part=None):
        return self.receiver.accept(self.period,self.token,manifest or self.manifest,part or self.parts[index])

    def counts(self):
        with self.r.connect() as db:
            return tuple(db.execute('SELECT count(*) FROM '+t).fetchone()[0]
                         for t in ('usage_closed_manifests','usage_closed_parts','usage_drain_checkpoints'))

    def test_out_of_order_restart_and_lost_completion_response_no_billing(self):
        self.setup_fragments()
        self.assertFalse(self.accept(2)['complete']); self.assertEqual(self.counts(),(1,1,0))
        self.assertFalse(self.accept(0)['complete']); self.assertFalse(self.accept(0)['complete'])
        self.receiver=ClosedReceiver(Registry(self.r.path))
        result=self.accept(1); self.assertTrue(result['complete']); self.assertFalse(result['ready_for_execution'])
        self.assertEqual(result['digest'],self.final['digest']); self.assertEqual(self.counts(),(1,3,1))
        with patch('closed_receiver.time.time',return_value=self.now+1000),patch('usage_migration.time.time',return_value=self.now+1000):
            self.assertTrue(self.accept(1)['duplicate'])
        with self.r.connect() as db:
            self.assertEqual(db.execute('SELECT total_bytes FROM usage_cycle_confirmed_totals').fetchone()[0],'7')
            import json
            stored=json.loads(db.execute('SELECT body FROM usage_drain_checkpoints').fetchone()[0])
            self.assertEqual(len(stored['items']),129)

    def test_bad_token_wrong_server_and_rotated_original_authority(self):
        self.setup_fragments(); token=self.token; self.token='x'*32
        with self.assertRaises(Error): self.accept(0)
        self.assertEqual(self.counts(),(0,0,0)); self.token=token
        self.accept(0); self.token=self.r.provision_agent(self.sid)['token']
        with self.assertRaises(Error): self.accept(1)
        self.assertEqual(self.counts(),(1,1,0))

    def test_expired_partial_does_not_get_retry_exception(self):
        self.setup_fragments(); self.accept(0)
        with patch('closed_receiver.time.time',return_value=self.now+1000):
            with self.assertRaises(Error): self.accept(0)
            with self.assertRaises(Error): self.accept(1)
        self.assertEqual(self.counts(),(1,1,0))

    def test_missing_counter_final_fragment_rolls_back_until_confirmed(self):
        self.setup_fragments(); self.accept(0); self.accept(1)
        item=self.parts[2]['body']['items'][0]
        key=(self.sid,self.session,item['node_id'],item['user_id'],item['credential'])
        with self.r.connect() as db:
            db.execute('DELETE FROM usage_counters WHERE server_id=? AND session_id=? AND node_id=? AND user_id=? AND credential=?',key)
        with self.assertRaises(Error): self.accept(2)
        self.assertEqual(self.counts(),(1,2,0))
        with self.r.connect() as db: db.execute('INSERT INTO usage_counters VALUES(?,?,?,?,?,?,?)',(*key,4,3))
        self.assertTrue(self.accept(2)['complete'])

    def test_manifest_and_part_conflict_never_replace_staged_evidence(self):
        self.setup_fragments(); self.accept(0)
        part=copy.deepcopy(self.parts[1]); part['body']['items'][0]['upload_bytes']=5; part['digest']=digest(part['body'])
        with self.assertRaises(Error): self.accept(1,part=part)
        m=copy.deepcopy(self.manifest); m['manifest']['parts'][1]['digest']=part['digest']; m['digest']=digest(m['manifest'])
        with self.assertRaises(Error): self.accept(1,manifest=m,part=part)
        self.assertEqual(self.counts(),(1,1,0))

    def test_cross_part_duplicate_rejected_before_completion(self):
        self.setup_fragments(); m=copy.deepcopy(self.manifest); p=copy.deepcopy(self.parts[1])
        p['body']['items'][0]=copy.deepcopy(self.parts[0]['body']['items'][0]); p['digest']=digest(p['body'])
        m['manifest']['parts'][1]['digest']=p['digest']; m['digest']=digest(m['manifest'])
        self.accept(0,manifest=m)
        with self.assertRaises(Error): self.accept(1,manifest=m,part=p)
        self.assertEqual(self.counts(),(1,1,0))

    def test_complete_global_digest_failure_does_not_seal_or_commit_last_part(self):
        self.setup_fragments(); m=copy.deepcopy(self.manifest); m['manifest']['checkpoint_digest']='0'*64
        parts=copy.deepcopy(self.parts)
        for index,p in enumerate(parts):
            p['body']['checkpoint_digest']='0'*64; p['digest']=digest(p['body'])
            m['manifest']['parts'][index]['digest']=p['digest']
        m['digest']=digest(m['manifest'])
        self.accept(0,manifest=m,part=parts[0]); self.accept(1,manifest=m,part=parts[1])
        with self.assertRaises(Error): self.accept(2,manifest=m,part=parts[2])
        self.assertEqual(self.counts(),(1,2,0))

    def test_seal_insert_failure_rolls_back_last_part_and_concurrent_retry_is_single(self):
        self.setup_fragments(); self.accept(0); self.accept(1)
        with self.r.connect() as db:
            db.execute("CREATE TRIGGER fail_fragment_seal BEFORE INSERT ON usage_drain_checkpoints BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.accept(2)
        self.assertEqual(self.counts(),(1,2,0))
        with self.r.connect() as db: db.execute('DROP TRIGGER fail_fragment_seal')
        with ThreadPoolExecutor(2) as pool:
            results=list(pool.map(lambda _:self.accept(2),range(2)))
        self.assertEqual(sorted(r['duplicate'] for r in results),[False,True])
        self.assertEqual(self.counts(),(1,3,1))

    def test_wrong_server_and_manifest_shape_rejected_without_staging(self):
        self.setup_fragments()
        for change in ('wrong_server','boolean_index','bad_count','bad_hash'):
            m=copy.deepcopy(self.manifest); p=copy.deepcopy(self.parts[0])
            if change=='wrong_server':
                m['manifest']['server_id']+=1; p['body']['server_id']+=1
                p['digest']=digest(p['body']); m['manifest']['parts'][0]['digest']=p['digest']
            elif change=='boolean_index': p['body']['index']=False; p['digest']=digest(p['body'])
            elif change=='bad_count': m['manifest']['item_count']+=1
            elif change=='bad_hash': p['digest']='0'*64
            m['digest']=digest(m['manifest'])
            with self.subTest(change=change),self.assertRaises(Error): self.accept(0,manifest=m,part=p)
        self.assertEqual(self.counts(),(0,0,0))

    def test_missing_capture_and_frontier_are_not_implicit_zero(self):
        self.setup_fragments()
        with self.r.connect() as db: db.execute('DELETE FROM usage_drain_inventory_captures')
        with self.assertRaises(Error): self.accept(0)
        with self.r.connect() as db:
            db.execute('INSERT INTO usage_drain_inventory_captures VALUES(?)',(self.period,))
            db.execute('DELETE FROM usage_drain_inventory')
        with self.assertRaises(Error): self.accept(0)
        self.assertEqual(self.counts(),(0,0,0))

    def test_stored_part_corruption_does_not_allow_progress_or_silent_repair(self):
        self.setup_fragments(); self.accept(0)
        with self.r.connect() as db: db.execute("UPDATE usage_closed_parts SET digest=?",('0'*64,))
        with self.assertRaises(Error): self.accept(1)
        self.assertEqual(self.counts(),(1,1,0))

    def test_completed_fragment_checkpoint_fences_future_usage_growth(self):
        self.setup_fragments()
        for index in range(3): self.accept(index)
        self.item['upload_bytes']=5
        with self.assertRaises(Error): self.report(2)
        self.token=self.r.provision_agent(self.sid)['token']
        with self.assertRaises(Error): self.accept(2)
        self.assertEqual(self.counts(),(1,3,1))
