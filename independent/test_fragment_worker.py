"""Real delivery step with isolated 129-identity historical closure evidence."""
import asyncio
import json
import time
import unittest
import urllib.error
import uuid
from pathlib import Path
from unittest import mock
from usage_delivery import UsageDelivery
from fragment_delivery import FragmentDelivery
from usage_outbox import UsageOutbox
import test_fragment_delivery as base
import test_closed_delivery as closed_base

class FragmentWorkerTests(unittest.IsolatedAsyncioTestCase):
    setUp=base.FragmentDeliveryTests.setUp
    tearDown=base.FragmentDeliveryTests.tearDown
    fixtures=base.FragmentDeliveryTests.fixtures
    fixture=base.FragmentDeliveryTests.fixture
    setup_total=base.FragmentDeliveryTests.setup_total
    applied=base.FragmentDeliveryTests.applied
    data=base.FragmentDeliveryTests.data
    report=base.FragmentDeliveryTests.report
    setup_drain=base.FragmentDeliveryTests.setup_drain
    setup_fragments=base.FragmentDeliveryTests.setup_fragments
    setup_queue=base.FragmentDeliveryTests.setup_queue
    confirm=base.FragmentDeliveryTests.confirm
    receipt=base.FragmentDeliveryTests.receipt
    counts=base.FragmentDeliveryTests.counts
    state=base.FragmentDeliveryTests.state
    snapshot=closed_base.ClosedDeliveryTests.snapshot

    def worker(self):
        runtime=mock.Mock()
        runtime.usage_snapshot.return_value=dict(server_id=self.sid,items=[])
        worker=UsageDelivery('http://fixture',self.token,self.box.path.parent,runtime,str(uuid.uuid4()))
        worker.box=self.box
        return worker

    async def test_step_sends_one_part_and_shares_slots_with_discovery(self):
        self.setup_queue();self.confirm();worker=self.worker();paths=[]
        def send(base,token,path,job):
            paths.append(path)
            if path.endswith('/fragment'):return self.receipt(job,job['fragment']['body']['index']==2)
            return dict(server_id=self.sid,sessions=job['sessions'],drains=[],next_after=None,ready_for_execution=False)
        with mock.patch('usage_delivery.request',side_effect=send):
            for _ in range(10):
                before=len(paths);await worker.step();self.assertLessEqual(len(paths)-before,1)
                if self.state()[1]=='accepted':break
        self.assertEqual(paths.count('/agent/usage/closed/fragment'),3)
        self.assertEqual(self.state()[1],'accepted')

    async def test_worker_lost_response_reopen_retries_same_fragment(self):
        self.setup_queue();self.confirm();worker=self.worker();job=self.queue.ready(time.time())
        with mock.patch('usage_delivery.request',side_effect=OSError('lost')) as send,mock.patch('builtins.print'):
            await worker.step();self.assertEqual(send.call_count,1)
        self.assertEqual(self.state()[0],0);self.assertEqual(self.state()[2],1)
        worker=self.worker()
        with mock.patch('usage_delivery.request',return_value=self.receipt(job)) as send,mock.patch('usage_delivery.time.time',return_value=time.time()+6):
            await worker.step();self.assertEqual(send.call_args.args[3],job)
        self.assertEqual(self.state()[0],1)

    async def test_bad_receipt_retains_part_and_auth_errors_block_reenqueue(self):
        self.setup_queue();self.confirm();worker=self.worker()
        with mock.patch('usage_delivery.request',return_value={}),mock.patch('builtins.print'):
            await worker.step()
        self.assertEqual(self.state()[0],0);self.assertEqual(self.state()[2],1)
        for code in (401,403):
            worker=self.worker()
            with mock.patch('usage_delivery.request',side_effect=urllib.error.HTTPError('local',code,'revoked',{},None)),mock.patch('usage_delivery.time.time',return_value=time.time()+100),mock.patch('builtins.print'):
                await worker.step()
            self.assertEqual(self.state()[1],'blocked')
            self.queue.enqueue(self.period,self.session)
            self.assertIsNone(self.queue.ready(time.time()+1000))
            if code==401:
                # New isolated fixture for the second authentication status.
                with self.box.connect() as db:db.execute("UPDATE closed_fragment_deliveries SET status='pending',next_attempt=0")

    async def test_current_unconfirmed_and_unacked_epochs_are_not_sent(self):
        self.setup_queue();worker=self.worker()
        with mock.patch('usage_delivery.request') as send:
            worker.unconfirmed_session=self.session
            await worker.step();send.assert_not_called()
            self.confirm();await worker.step();send.assert_not_called()
            worker.unconfirmed_session=None;worker.session=self.session
            # Empty runtime cannot capture into a closed current epoch; the
            # queue itself must also exclude it before any network request.
            with mock.patch.object(worker,'capture',new=mock.AsyncMock()):await worker.step()
            send.assert_not_called()

    async def test_growing_current_usage_is_not_starved_by_fragment_backlog(self):
        self.setup_queue();self.confirm();worker=self.worker()
        item=dict(node_id=999,user_id=999,credential=str(uuid.uuid4()),upload_bytes=1,download_bytes=1)
        worker.runtime.usage_snapshot.side_effect=lambda:dict(server_id=self.sid,items=[dict(item)])
        paths=[]
        def send(base,token,path,job):
            paths.append(path)
            if path.endswith('/fragment'):return self.receipt(job,job['fragment']['body']['index']==2)
            if path.endswith('/settle'):
                return dict(accepted=True,server_id=self.sid,session_id=job['session_id'],sequence=job['sequence'])
            return dict(server_id=self.sid,sessions=job['sessions'],drains=[],next_after=None,ready_for_execution=False)
        with mock.patch('usage_delivery.request',side_effect=send):
            for _ in range(12):
                item['upload_bytes']+=1
                before=len(paths);await worker.step();self.assertEqual(len(paths)-before,1)
        self.assertEqual(self.state()[1],'accepted')
        self.assertGreaterEqual(paths.count('/agent/usage/settle'),6)
        self.assertFalse(any(paths[i].endswith('/fragment') and paths[i+1].endswith('/fragment') for i in range(len(paths)-1)))

    async def test_real_http_worker_completes_three_parts_without_manual_send(self):
        from http.server import ThreadingHTTPServer
        import threading
        from server import Handler
        self.setup_queue();self.confirm();worker=self.worker()
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.registry=self.r
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        worker.base='http://127.0.0.1:%s'%server.server_port
        try:
            for _ in range(10):
                await worker.step()
                if self.state()[1]=='accepted':break
            self.assertEqual(self.state()[1],'accepted')
            self.assertEqual(self.counts(),(1,3,1))
            self.assertIsNotNone(self.box.closed_epoch(self.session))
        finally:
            await asyncio.to_thread(server.shutdown);thread.join();server.server_close()

    async def test_large_discovery_enqueues_fragments_and_reopens_lost_part(self):
        # This uses 65 historical identities to exercise the first page of a
        # large closed epoch; it is evidence-fixture scale, not 65 live cores.
        self.r=self.setup_drain()
        from usage_outbox import UsageOutbox
        self.box=UsageOutbox(Path(self.r.path).parent/'fragments-outbox.db',self.sid)
        session=str(uuid.uuid4())
        items=[dict(self.item,user_id=i+1) for i in range(65)]
        self.box.capture(session,self.snapshot(*items)); self.box.close_epoch(session)
        while report:=self.box.next_report(session):
            self.box.acknowledge(report,dict(accepted=True,server_id=self.sid,
                session_id=session,sequence=report['sequence']))
        worker=self.worker(); paths=[]; fragment_jobs=[]
        final=self.box.closed_epoch(session)
        def large_receipt(job,complete=False):
            body=job['fragment']['body']
            result=dict(accepted=True,complete=complete,server_id=self.sid,
                cycle_id='large',session_id=session,
                manifest_digest=job['envelope']['digest'],part=body['index'],
                ready_for_execution=False)
            if complete: result.update(digest=final['digest'],duplicate=False)
            return result
        def send(base_url,token,path,job):
            paths.append(path)
            if path.endswith('/drains'):
                return dict(server_id=self.sid,sessions=[session],drains=[dict(cycle_id='large',session_id=session)],next_after=None,ready_for_execution=False)
            if path.endswith('/fragment'):
                fragment_jobs.append(job)
                if len(fragment_jobs)==1: raise OSError('fixture: response lost')
                body=job['fragment']['body']
                return large_receipt(job,body['index']==body['total']-1)
            raise AssertionError(path)
        with mock.patch('usage_delivery.request',side_effect=send):
            await worker.step()  # discovery only
            self.assertEqual(paths,['/agent/usage/drains'])
            await worker.step()  # first fragment is lost after send
            self.assertEqual(paths[-1],'/agent/usage/closed/fragment')
            self.assertEqual(len(fragment_jobs),1)
            worker=self.worker()
            with mock.patch('usage_delivery.time.time',return_value=time.time()+6):
                worker.closed_turn=True
                await worker.step()  # same first fragment retries exactly
                worker.closed_turn=True
                await worker.step()  # second fragment completes the epoch
                worker.closed_turn=True
                await worker.step()  # fragment turn sends the primed part
        self.assertEqual(len(fragment_jobs),3)
        self.assertEqual([j['fragment']['body']['index'] for j in fragment_jobs],[0,0,1])
        with self.box.connect() as db:
            self.assertEqual(tuple(db.execute('SELECT status,part FROM closed_fragment_deliveries').fetchone()),('accepted',1))
            self.assertEqual(db.execute('SELECT count(*) FROM closed_deliveries').fetchone()[0],0)

    async def test_large_cycle_real_http_discovery_and_fragment_delivery(self):
        # The registry and fragment receiver are real HTTP endpoints here;
        # only the first client response is deliberately discarded to model a
        # committed response lost during a worker restart.
        from http.server import ThreadingHTTPServer
        import threading
        from agent import request as real_request
        from server import Handler
        self.setup_fragments(count=65)
        source_items=[item for part in self.parts for item in part['body']['items']]
        self.box=UsageOutbox(Path(self.r.path).parent/'large-http-outbox.db',self.sid)
        self.box.capture(self.session,dict(server_id=self.sid,items=source_items))
        self.box.close_epoch(self.session)
        while report:=self.box.next_report(self.session):
            self.box.acknowledge(report,dict(accepted=True,server_id=self.sid,
                session_id=self.session,sequence=report['sequence']))
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.registry=self.r
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        worker=self.worker();worker.base='http://127.0.0.1:%s'%server.server_port
        fragment_calls=[]
        lost=[True]
        def request_with_lost_response(base_url,token,path,job):
            reply=real_request(base_url,token,path,job)
            if path.endswith('/fragment'):
                fragment_calls.append(job)
                if lost[0]:
                    lost[0]=False
                    raise OSError('fixture: response lost after commit')
            return reply
        try:
            with mock.patch('usage_delivery.request',side_effect=request_with_lost_response):
                await worker.step()  # real discovery and atomic fragment enqueue
                await worker.step()  # first fragment commits remotely, response is lost
                self.assertEqual(len(fragment_calls),1)
                worker=self.worker();worker.base='http://127.0.0.1:%s'%server.server_port
                with mock.patch('usage_delivery.time.time',return_value=time.time()+6):
                    worker.closed_turn=True;await worker.step()  # retry part 0
                    worker.closed_turn=True;await worker.step()  # part 1
                    worker.closed_turn=True;await worker.step()  # primed fragment slot
            with self.box.connect() as db:
                self.assertEqual(tuple(db.execute('SELECT status,part FROM closed_fragment_deliveries').fetchone()),('accepted',1))
                self.assertEqual(db.execute('SELECT count(*) FROM closed_deliveries').fetchone()[0],0)
            with self.r.connect() as db:
                parts=db.execute('SELECT body FROM usage_closed_parts').fetchall()
                self.assertEqual(len(parts),2)
                self.assertEqual(sum(len(json.loads(row['body'])['items']) for row in parts),65)
                self.assertEqual(db.execute('SELECT count(*) FROM usage_drain_checkpoints').fetchone()[0],1)
        finally:
            server.shutdown();thread.join();server.server_close()

    async def test_large_cycle_real_http_throughput_completes_all_fragments(self):
        # Five immutable parts exercise the bounded large-cycle path without
        # pretending that fixture identities are live runtime processes.
        from http.server import ThreadingHTTPServer
        import threading
        from agent import request as real_request
        from server import Handler
        self.setup_fragments(count=257)
        source_items=[item for part in self.parts for item in part['body']['items']]
        self.box=UsageOutbox(Path(self.r.path).parent/'large-throughput-outbox.db',self.sid)
        self.box.capture(self.session,dict(server_id=self.sid,items=source_items))
        self.box.close_epoch(self.session)
        while report:=self.box.next_report(self.session):
            self.box.acknowledge(report,dict(accepted=True,server_id=self.sid,
                session_id=self.session,sequence=report['sequence']))
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.registry=self.r
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        worker=self.worker();worker.base='http://127.0.0.1:%s'%server.server_port
        calls=[];started=time.monotonic()
        try:
            with mock.patch('usage_delivery.request',side_effect=real_request):
                for _ in range(2*len(self.parts)+2):
                    before=len(calls)
                    worker.closed_turn=True
                    # Capture the actual path through a wrapper while keeping
                    # the network and receiver real.
                    async def one_step():
                        return await worker.step()
                    with mock.patch('usage_delivery.request',side_effect=lambda base_url,token,path,job: (calls.append(path) or real_request(base_url,token,path,job))):
                        await one_step()
                    self.assertLessEqual(len(calls)-before,1)
                    with self.box.connect() as db:
                        status=db.execute('SELECT status FROM closed_fragment_deliveries WHERE cycle=? AND session=?',(self.period,self.session)).fetchone()[0]
                    if status=='accepted':break
            with self.box.connect() as db:
                status=db.execute('SELECT status FROM closed_fragment_deliveries WHERE cycle=? AND session=?',(self.period,self.session)).fetchone()[0]
            self.assertEqual(status,'accepted')
            self.assertEqual(calls.count('/agent/usage/closed/fragment'),5)
            with self.r.connect() as db:
                rows=db.execute('SELECT body FROM usage_closed_parts').fetchall()
                self.assertEqual(len(rows),5)
                self.assertEqual(sum(len(json.loads(row['body'])['items']) for row in rows),257)
            self.assertLess(time.monotonic()-started,10)
        finally:
            server.shutdown();thread.join();server.server_close()

if __name__=='__main__':unittest.main()
