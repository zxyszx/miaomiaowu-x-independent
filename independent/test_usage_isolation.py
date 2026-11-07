import copy
import json
import sqlite3
import unittest
import urllib.error
import uuid
from unittest import mock
from usage_outbox import UsageOutbox
import test_usage_outbox
import test_usage_delivery


class IsolationTests(unittest.TestCase):
    setUp=test_usage_outbox.OutboxTests.setUp
    snapshot=test_usage_outbox.OutboxTests.snapshot
    receipt=test_usage_outbox.OutboxTests.receipt

    def report(self,*items):
        self.box.capture(self.session,self.snapshot(*items))
        return self.box.next_report(self.session)

    def test_split_archives_exact_batch_without_acknowledging_bytes(self):
        other=dict(self.item,user_id=4)
        report=self.report(self.item,other); self.box.defer(report,'rejected',100)
        self.box.isolate(report,101)
        restored=UsageOutbox(self.path,1)
        audit=restored.isolation_status()
        self.assertEqual(audit['failures'][0]['report'],report)
        self.assertEqual(audit['failures'][0]['reason'],'split_after_403')
        self.assertEqual(audit['quarantined'],[])
        self.assertEqual(restored.retry_status(),[])
        single=restored.next_report(self.session)
        self.assertEqual((single['sequence'],len(single['items'])),(2,1))
        with restored.connect() as db:
            self.assertEqual([r[0] for r in db.execute('SELECT acked_upload FROM totals')],[0,0])
        restored.acknowledge(single,self.receipt(single))
        next_report=restored.next_report(self.session)
        self.assertEqual(next_report['items'][0]['user_id'],4)

    def test_quarantined_growth_retained_and_other_identity_continues(self):
        report=self.report(); self.box.isolate(report,100)
        grown=dict(self.item,upload_bytes=15)
        self.box.capture(self.session,self.snapshot(grown))
        restored=UsageOutbox(self.path,1)
        self.assertEqual(restored.ready_sessions(101),[])
        self.assertEqual(restored.outstanding_sessions(),[])
        self.assertIsNone(restored.next_report(self.session))
        row=restored.isolation_status()['quarantined'][0]
        self.assertEqual((row['upload'],row['acked_upload'],row['rejected_sequence']),(15,0,1))
        good=dict(self.item,user_id=4); restored.capture(self.session,self.snapshot(good))
        next_report=restored.next_report(self.session)
        self.assertEqual(next_report['sequence'],2)
        self.assertEqual(next_report['items'],[good])
        restored.acknowledge(next_report,self.receipt(next_report))
        self.assertEqual(restored.outstanding_sessions(),[])

    def test_archive_quarantine_cursor_pending_retry_are_atomic(self):
        report=self.report(); self.box.defer(report,'rejected',100)
        with self.box.connect() as db:
            db.execute("CREATE TRIGGER fail_delete BEFORE DELETE ON pending BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.box.isolate(report,101)
        self.assertEqual(self.box.isolation_status(),dict(failures=[],quarantined=[]))
        self.assertEqual(self.box.next_report(self.session),report)
        self.assertEqual(len(self.box.retry_status()),1)
        with self.box.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM cursors').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM isolation').fetchone()[0],0)

    def test_invalid_stale_or_sequence_exhausted_report_is_not_discarded(self):
        report=self.report(); changed=copy.deepcopy(report); changed['items'][0]['upload_bytes']+=1
        for data,now in ((changed,100),(report,True),(report,-1),(report,float('nan'))):
            with self.assertRaises(ValueError): self.box.isolate(data,now)
        self.assertEqual(self.box.isolation_status()['failures'],[])
        self.box.isolate(report,100)
        with self.assertRaises(ValueError): self.box.isolate(report,101)
        other=str(uuid.uuid4()); self.box.capture(other,self.snapshot())
        with self.box.connect() as db: db.execute('INSERT INTO cursors VALUES(?,?)',(other,2**63-2))
        last=self.box.next_report(other)
        with self.assertRaises(ValueError): self.box.isolate(last,101)
        self.assertEqual(self.box.next_report(other),last)

    def test_schema_upgrade_preserves_pending_and_new_epoch_batches_normally(self):
        report=self.report()
        with self.box.connect() as db:
            for table in ('isolation','failures','quarantine'): db.execute('DROP TABLE '+table)
        restored=UsageOutbox(self.path,1)
        self.assertEqual(restored.next_report(self.session),report)
        restored.isolate(report,100)
        other=str(uuid.uuid4()); restored.capture(other,self.snapshot(self.item,dict(self.item,user_id=4)))
        self.assertEqual(len(restored.next_report(other)['items']),2)


class IsolationDeliveryTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_usage_delivery.DeliveryTests.setUp
    receipt=test_usage_delivery.DeliveryTests.receipt

    async def test_same_session_bad_identity_splits_then_good_acknowledges(self):
        bad=dict(self.item,user_id=1)
        self.runtime.usage_snapshot.side_effect=lambda:dict(server_id=1,items=[bad,dict(self.item)])
        sent=[]
        def send(*args):
            report=args[3]; sent.append(copy.deepcopy(report))
            if any(i['user_id']==1 for i in report['items']):
                raise urllib.error.HTTPError('local',403,'denied',{},None)
            return self.receipt(*args)
        with mock.patch('usage_delivery.request',side_effect=send),mock.patch('builtins.print') as output:
            for _ in range(4): await self.worker.step()
        self.assertEqual([len(r['items']) for r in sent],[2,1,1])
        self.assertEqual([r['sequence'] for r in sent],[1,2,3])
        self.assertEqual(self.worker.box.outstanding_sessions(),[])
        self.assertEqual(len(self.worker.box.isolation_status()['quarantined']),1)
        self.assertNotIn(bad['credential'],str(output.call_args_list))

    async def test_non_identity_failures_keep_exact_pending_without_isolation(self):
        for code in (401,409,500):
            with self.subTest(code=code),mock.patch('usage_delivery.request',side_effect=urllib.error.HTTPError('local',code,'error',{},None)),mock.patch('usage_delivery.time.time',return_value=1000+code),mock.patch('builtins.print'):
                if self.worker.box:
                    with self.worker.box.connect() as db: db.execute('DELETE FROM retries')
                await self.worker.step()
                self.assertEqual(self.worker.box.isolation_status(),dict(failures=[],quarantined=[]))
                self.assertEqual(self.worker.box.next_report(self.session)['sequence'],1)
                self.assertEqual(self.worker.box.retry_status()[0]['reason'],'rejected')


@unittest.skipUnless(test_usage_delivery.test_xray.BINARY.is_file(),'Set XRAY_BINARY')
class IsolationRealTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_usage_delivery.DeliveryRealTests.setUp
    tearDown=test_usage_delivery.DeliveryRealTests.tearDown
    request=test_usage_delivery.DeliveryRealTests.request
    asyncSetUp=test_usage_delivery.DeliveryRealTests.asyncSetUp
    asyncTearDown=test_usage_delivery.DeliveryRealTests.asyncTearDown
    open=test_usage_delivery.DeliveryRealTests.open
    accepted=test_usage_delivery.DeliveryRealTests.accepted
    close_writer=test_usage_delivery.DeliveryRealTests.close_writer

    async def test_real_payload_with_unexecuted_fixture_identity_isolates_and_settles_growth(self):
        import asyncio
        from agent import request
        from execution_agent import cycle
        from usage_delivery import UsageDelivery
        session=str(uuid.uuid4())
        await cycle(self.base,self.token,self.state,self.runtime,1,session)
        reader,writer=await self.open(); await self.accepted(reader)
        worker=UsageDelivery(self.base,self.token,self.state,self.runtime,session)
        def lost(*args): request(*args); raise OSError('lost after commit')
        with mock.patch('usage_delivery.request',side_effect=lost),mock.patch('builtins.print'):
            await worker.step()
        original=worker.box.next_report(session)
        # Only the good identity carries real VLESS payload. The invalid UUID
        # is an explicit fixture to provoke the atomic endpoint's HTTP 403.
        with worker.box.connect() as db: db.execute('UPDATE retries SET next_attempt=0')
        await worker.step()  # exact replay acknowledges the earlier real five bytes
        bad=dict(original['items'][0],credential=str(uuid.uuid4()))
        writer.write(b'more'); await writer.drain()
        self.assertEqual(await asyncio.wait_for(reader.readexactly(4),2),b'more')
        await worker.capture()
        worker.box.capture(session,dict(server_id=self.sid,items=[bad]))
        with mock.patch('builtins.print'):
            for _ in range(4): await worker.step()
        with self.request(f'/api/users/{self.uid}/usage') as response: total=json.load(response)
        self.assertEqual((total['upload_bytes'],total['download_bytes']),(9,9))
        self.assertEqual(worker.box.outstanding_sessions(),[])
        audit=worker.box.isolation_status()
        self.assertEqual(len(audit['quarantined']),1)
        self.assertEqual(audit['quarantined'][0]['acked_upload'],0)
        self.assertEqual(audit['failures'][0]['reason'],'split_after_403')

    async def test_committed_multi_identity_lost_ack_then_missing_proof_does_not_double_count(self):
        import asyncio
        from agent import request
        from execution_agent import cycle
        from usage_delivery import UsageDelivery
        registry=self.server.registry
        other=registry.save('users',dict(name='second-metered-user'))['id']
        credential=registry.authorize_node_user(self.nid,other)['uuid']
        session=str(uuid.uuid4())
        await cycle(self.base,self.token,self.state,self.runtime,1,session)
        reader,writer=await self.open(); await self.accepted(reader)
        second,second_writer=await self.open(credential); await self.accepted(second)
        worker=UsageDelivery(self.base,self.token,self.state,self.runtime,session)
        def lost(*args): request(*args); raise OSError('lost after commit')
        with mock.patch('usage_delivery.request',side_effect=lost),mock.patch('builtins.print'):
            await worker.step()
        self.assertEqual(len(worker.box.next_report(session)['items']),2)
        # Simulate loss of one historical proof, without deleting settled usage.
        with registry.connect() as db:
            db.execute('DELETE FROM usage_execution_grants WHERE user_id=?',(other,))
        writer.write(b'more'); await writer.drain()
        self.assertEqual(await asyncio.wait_for(reader.readexactly(4),2),b'more')
        with worker.box.connect() as db: db.execute('UPDATE retries SET next_attempt=0')
        with mock.patch('builtins.print'):
            for _ in range(4): await worker.step()
        for uid,expected in ((self.uid,9),(other,5)):
            with self.request(f'/api/users/{uid}/usage') as response: total=json.load(response)
            self.assertEqual((total['upload_bytes'],total['download_bytes']),(expected,expected))
        self.assertEqual(worker.box.outstanding_sessions(),[])
        audit=worker.box.isolation_status()
        self.assertEqual(audit['failures'][0]['report']['sequence'],1)
        row=audit['quarantined'][0]
        self.assertEqual((row['user'],row['upload'],row['acked_upload']),(other,5,0))
