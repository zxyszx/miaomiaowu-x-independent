"""Durable fragment progression; HTTP uses isolated historical execution evidence."""
import copy
import json
from pathlib import Path
import sqlite3
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer
from agent import request
from server import Handler
from fragment_delivery import FragmentDelivery
from usage_outbox import UsageOutbox
import test_closed_receiver as base

class FragmentDeliveryTests(unittest.TestCase):
    setUp=base.ClosedReceiverTests.setUp
    tearDown=base.ClosedReceiverTests.tearDown
    fixtures=base.ClosedReceiverTests.fixtures
    fixture=base.ClosedReceiverTests.fixture
    setup_total=base.ClosedReceiverTests.setup_total
    applied=base.ClosedReceiverTests.applied
    data=base.ClosedReceiverTests.data
    report=base.ClosedReceiverTests.report
    setup_drain=base.ClosedReceiverTests.setup_drain
    setup_fragments=base.ClosedReceiverTests.setup_fragments
    counts=base.ClosedReceiverTests.counts

    def setup_queue(self):
        self.setup_fragments()
        self.box=UsageOutbox(Path(self.r.path).parent/'fragments-outbox.db',self.sid)
        self.queue=FragmentDelivery(self.box);self.queue.enqueue(self.period,self.session)

    def confirm(self):
        while report:=self.box.next_report(self.session):
            self.box.acknowledge(report,dict(accepted=True,server_id=self.sid,session_id=self.session,sequence=report['sequence']))

    def receipt(self,job,complete=False):
        result=dict(accepted=True,complete=complete,server_id=self.sid,cycle_id=self.period,
            session_id=self.session,manifest_digest=job['envelope']['digest'],
            part=job['fragment']['body']['index'],ready_for_execution=False)
        if complete:result.update(digest=self.final['digest'],duplicate=False)
        return result

    def state(self):
        with self.box.connect() as db:
            return tuple(db.execute('SELECT part,status,attempts,next_attempt FROM closed_fragment_deliveries').fetchone())

    def test_confirmed_gate_and_each_ack_advances_one_part_without_deleting_evidence(self):
        self.setup_queue();self.assertIsNone(self.queue.ready(1));self.confirm()
        for i in range(3):
            job=self.queue.ready(1);self.assertEqual(job['fragment']['body']['index'],i)
            self.queue.acknowledge(job,self.receipt(job,i==2))
        self.assertIsNone(self.queue.ready(1));self.queue.enqueue(self.period,self.session)
        self.assertIsNone(self.queue.ready(100));self.assertEqual(self.state(),(2,'accepted',0,0))
        self.assertEqual(self.box.closed_epoch(self.session),self.final)

    def test_restart_backoff_and_revocation_reenqueue_cannot_bypass(self):
        self.setup_queue();self.confirm();job=self.queue.ready(10);self.queue.defer(job,10)
        self.queue=FragmentDelivery(UsageOutbox(self.box.path,self.sid))
        self.assertIsNone(self.queue.ready(14));self.assertEqual(self.queue.ready(15),job)
        self.queue.defer(job,15,revoked=True);self.queue.enqueue(self.period,self.session)
        self.assertIsNone(self.queue.ready(1000));self.assertEqual(self.state()[1],'blocked')
        self.assertIsNotNone(self.box.closed_epoch(self.session))

    def test_wrong_receipt_early_complete_or_final_partial_never_advance(self):
        self.setup_queue();self.confirm();job=self.queue.ready(1)
        for bad in (dict(self.receipt(job),part=1),dict(self.receipt(job),server_id=True),self.receipt(job,True)):
            with self.assertRaises(ValueError):self.queue.acknowledge(job,bad)
        self.assertEqual(self.state()[0],0)
        self.queue.acknowledge(job,self.receipt(job));job=self.queue.ready(1)
        self.queue.acknowledge(job,self.receipt(job));job=self.queue.ready(1)
        with self.assertRaises(ValueError):self.queue.acknowledge(job,self.receipt(job))
        self.assertEqual(self.state()[0],2)

    def test_cursor_disk_failure_and_concurrent_duplicate_do_not_skip_part(self):
        self.setup_queue();self.confirm();job=self.queue.ready(1)
        with self.box.connect() as db:
            db.execute("CREATE TRIGGER fail_cursor BEFORE UPDATE ON closed_fragment_deliveries BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError):self.queue.acknowledge(job,self.receipt(job))
        self.assertEqual(self.queue.ready(1),job)
        with self.box.connect() as db:db.execute('DROP TRIGGER fail_cursor')
        def ack(_):
            try:self.queue.acknowledge(job,self.receipt(job));return True
            except ValueError:return False
        with ThreadPoolExecutor(2) as pool:self.assertEqual(sorted(pool.map(ack,range(2))),[False,True])
        self.assertEqual(self.state()[0],1)

    def test_unknown_missing_or_changed_source_cannot_be_enqueued_or_sent(self):
        self.setup_queue();self.confirm();job=self.queue.ready(1)
        changed=copy.deepcopy(job);changed['fragment']['body']['items'][0]['upload_bytes']+=1
        with self.assertRaises(ValueError):self.queue.acknowledge(changed,self.receipt(job))
        with self.box.connect() as db:db.execute("INSERT INTO isolation VALUES(?)",(self.session,))
        self.assertIsNone(self.queue.ready(1))
        with self.assertRaises(ValueError):self.queue.acknowledge(job,self.receipt(job))
        with self.assertRaises(ValueError):self.queue.enqueue('cycle','missing')
        self.assertEqual(self.state()[0],0)

    def test_http_lost_partial_and_final_response_reopen_preserves_exact_work(self):
        self.setup_queue();self.confirm()
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.registry=self.r
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            url='http://127.0.0.1:%s'%server.server_port
            for i in range(3):
                job=self.queue.ready(100)
                reply=request(url,self.token,'/agent/usage/closed/fragment',job)
                if i in (0,2):
                    # Master committed but the caller loses the completion.
                    self.queue.defer(job,100)
                    self.queue=FragmentDelivery(UsageOutbox(self.box.path,self.sid))
                    self.assertIsNone(self.queue.ready(104));self.assertEqual(self.queue.ready(105),job)
                    reply=request(url,self.token,'/agent/usage/closed/fragment',job)
                self.queue.acknowledge(job,reply)
            self.assertIsNone(self.queue.ready(1000));self.assertEqual(self.counts(),(1,3,1))
            with self.r.connect() as db:self.assertEqual(db.execute('SELECT total_bytes FROM usage_cycle_confirmed_totals').fetchone()[0],'7')
        finally:server.shutdown();thread.join();server.server_close()

if __name__=='__main__':unittest.main()
