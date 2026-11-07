"""Internal fragmentation preserves evidence; it does not enable remote closure."""
import copy
import sqlite3
import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from closed_fragments import ClosedFragments,digest,encoded
from usage_outbox import UsageOutbox
import test_usage_outbox as base

class ClosedFragmentTests(unittest.TestCase):
    setUp=base.OutboxTests.setUp
    snapshot=base.OutboxTests.snapshot
    receipt=base.OutboxTests.receipt

    def close(self,count=129):
        items=[dict(self.item,user_id=i+1,upload_bytes=2**63-1) for i in range(count)]
        self.box.capture(self.session,self.snapshot(*items))
        return self.box.close_epoch(self.session)

    def replace_source(self,body):
        with self.box.connect() as db:
            db.execute('UPDATE closed_epochs SET body=?,digest=? WHERE session=?',
                       (encoded(body),digest(body),self.session))

    def test_three_parts_reconstruct_exact_checkpoint_and_survive_cleanup(self):
        final=self.close(); fragments=ClosedFragments(self.box)
        result=fragments.prepare(self.session)
        self.assertEqual([p['count'] for p in result['manifest']['parts']],[64,64,1])
        parts=[fragments.part(self.session,i) for i in range(3)]
        self.assertEqual([item for p in parts for item in p['body']['items']],final['checkpoint']['items'])
        self.assertEqual(result['manifest']['checkpoint_digest'],final['digest'])
        self.assertEqual(result['digest'],digest(result['manifest']))
        for p in parts: self.assertEqual(p['digest'],digest(p['body']))
        while report:=self.box.next_report(self.session):
            self.box.acknowledge(report,self.receipt(report))
        import uuid
        self.box.retire_confirmed(str(uuid.uuid4()))
        reopened=ClosedFragments(UsageOutbox(self.path,1))
        self.assertEqual(reopened.prepare(self.session),result)
        self.assertEqual(reopened.part(self.session,2),parts[2])
        self.assertEqual(self.box.closed_epoch(self.session),final)

    def test_boundary_sizes_and_index_validation(self):
        self.close(65); f=ClosedFragments(self.box)
        self.assertIsNone(f.manifest(self.session)); f.prepare(self.session)
        self.assertEqual(len(f.part(self.session,1)['body']['items']),1)
        for index in (True,-1,2,'0'):
            with self.subTest(index=index),self.assertRaises(ValueError): f.part(self.session,index)

    def test_cross_page_duplicate_and_disorder_rejected_even_with_valid_source_hash(self):
        final=self.close(); f=ClosedFragments(self.box)
        for mode in ('duplicate','disorder'):
            body=copy.deepcopy(final['checkpoint'])
            if mode=='duplicate': body['items'][64]=body['items'][0]
            else: body['items'][63],body['items'][64]=body['items'][64],body['items'][63]
            self.replace_source(body)
            with self.subTest(mode=mode),self.assertRaises(ValueError): f.prepare(self.session)
        with self.box.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM closed_fragment_manifests').fetchone()[0],0)

    def test_unknown_writes_and_missing_evidence_never_become_final_parts(self):
        f=ClosedFragments(self.box)
        with self.assertRaises(ValueError): f.prepare(self.session)
        self.box.capture(self.session,self.snapshot())
        self.box.prepare_write(self.session,dict(self.item,upload_bytes=15))
        final=self.box.close_epoch(self.session)
        with self.assertRaises(ValueError): f.prepare(self.session)
        self.assertEqual(self.box.closed_epoch(self.session),final)
        with self.box.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM closed_fragment_parts').fetchone()[0],0)

    def test_empty_and_wrong_server_source_rejected(self):
        final=self.box.close_epoch(self.session); f=ClosedFragments(self.box)
        with self.assertRaises(ValueError): f.prepare(self.session)
        body=copy.deepcopy(final['checkpoint']); body['server_id']=2; body['items']=[self.item]
        self.replace_source(body)
        with self.assertRaises(ValueError): f.prepare(self.session)

    def test_insert_failure_rolls_back_entire_manifest_and_retry_is_exact(self):
        final=self.close(); f=ClosedFragments(self.box)
        with self.box.connect() as db:
            db.execute("CREATE TRIGGER fail_part BEFORE INSERT ON closed_fragment_parts WHEN NEW.part=1 BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): f.prepare(self.session)
        with self.box.connect() as db:
            for table in ('closed_fragment_manifests','closed_fragment_parts'):
                self.assertEqual(db.execute('SELECT count(*) FROM '+table).fetchone()[0],0)
            db.execute('DROP TRIGGER fail_part')
        self.assertEqual(self.box.closed_epoch(self.session),final)
        self.assertEqual(f.prepare(self.session)['manifest']['item_count'],129)

    def test_tampering_or_missing_part_is_not_silently_repaired(self):
        self.close(); f=ClosedFragments(self.box); f.prepare(self.session)
        part=f.part(self.session,1); altered=copy.deepcopy(part['body'])
        altered['items'][0]['upload_bytes']-=1
        with self.box.connect() as db:
            db.execute('UPDATE closed_fragment_parts SET body=?,digest=? WHERE session=? AND part=1',
                       (encoded(altered),digest(altered),self.session))
        with self.assertRaises(ValueError): f.prepare(self.session)
        with self.assertRaises(ValueError): f.part(self.session,0)
        with self.box.connect() as db:
            db.execute('DELETE FROM closed_fragment_parts WHERE session=? AND part=1',(self.session,))
        with self.assertRaises(ValueError): f.manifest(self.session)
        with self.assertRaises(ValueError): f.prepare(self.session)

    def test_concurrent_prepare_commits_one_identical_set(self):
        self.close(); f=ClosedFragments(self.box); gate=Barrier(2)
        def prepare():
            gate.wait(); return f.prepare(self.session)
        with ThreadPoolExecutor(2) as pool:
            a=pool.submit(prepare); b=pool.submit(prepare)
            self.assertEqual(a.result(),b.result())
        with self.box.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM closed_fragment_manifests').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT count(*) FROM closed_fragment_parts').fetchone()[0],3)
