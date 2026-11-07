import copy
import sqlite3
import unittest
import uuid
from unittest import mock
from usage_outbox import UsageOutbox,RETRY_MAX
import test_usage_outbox
import test_usage_delivery


class RetryTests(unittest.TestCase):
    setUp=test_usage_outbox.OutboxTests.setUp
    snapshot=test_usage_outbox.OutboxTests.snapshot
    receipt=test_usage_outbox.OutboxTests.receipt

    def report(self):
        self.box.capture(self.session,self.snapshot())
        return self.box.next_report(self.session)

    def test_durable_backoff_caps_delay_and_preserves_exact_report(self):
        report=self.report(); now=1000
        for delay in (5,10,20,40,60,60):
            self.box.defer(report,'network',now)
            restored=UsageOutbox(self.path,1)
            self.assertEqual(restored.ready_sessions(now),[])
            self.assertEqual(restored.ready_sessions(now+delay-0.1),[])
            self.assertEqual(restored.ready_sessions(now+delay),[self.session])
            self.assertEqual(restored.next_report(self.session),report)
            self.assertEqual(restored.retry_status()[0]['next_attempt'],now+delay)
            now+=delay
        self.assertEqual(restored.retry_status()[0]['attempts'],5)
        self.assertEqual(set(restored.retry_status()[0]),{'session','sequence','attempts','next_attempt','reason'})

    def test_bad_report_reason_clock_and_stale_confirmation_cannot_change_schedule(self):
        report=self.report(); self.box.defer(report,'receipt',100)
        original=self.box.retry_status()
        changed=copy.deepcopy(report); changed['items'][0]['upload_bytes']+=1
        for data,reason,now in ((changed,'receipt',100),(report,'URL-and-secret',100),(report,'receipt',True),(report,'receipt',float('inf')),(report,'receipt',-1)):
            with self.subTest(reason=reason,now=now),self.assertRaises(ValueError): self.box.defer(data,reason,now)
            self.assertEqual(self.box.retry_status(),original)
        self.box.acknowledge(report,self.receipt(report))
        with self.assertRaises(ValueError): self.box.defer(report,'rejected',200)
        self.assertEqual(self.box.retry_status(),[])

    def test_ack_and_retry_clear_are_one_transaction(self):
        report=self.report(); self.box.defer(report,'receipt',100)
        with self.box.connect() as db:
            db.execute("CREATE TRIGGER fail_retry_delete BEFORE DELETE ON retries BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.box.acknowledge(report,self.receipt(report))
        self.assertEqual(self.box.next_report(self.session),report)
        self.assertEqual(len(self.box.retry_status()),1)
        with self.box.connect() as db:
            self.assertEqual(db.execute('SELECT acked_upload FROM totals').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM cursors').fetchone()[0],0)
            db.execute('DROP TRIGGER fail_retry_delete')
        self.box.acknowledge(report,self.receipt(report)); self.assertEqual(self.box.retry_status(),[])
        grown=dict(self.item,upload_bytes=10); self.box.capture(self.session,self.snapshot(grown))
        next_report=self.box.next_report(self.session); self.box.defer(next_report,'network',100)
        self.assertEqual(self.box.retry_status()[0]['attempts'],1)

    def test_wall_clock_rollback_does_not_suspend_queue_forever(self):
        report=self.report(); self.box.defer(report,'network',1000)
        self.assertEqual(self.box.ready_sessions(1001),[])
        self.assertEqual(self.box.ready_sessions(900),[self.session])
        for now in (True,float('nan'),-1):
            with self.assertRaises(ValueError): self.box.ready_sessions(now)

    def test_schema_upgrade_preserves_preexisting_frozen_report(self):
        report=self.report()
        with self.box.connect() as db: db.execute('DROP TABLE retries')
        restored=UsageOutbox(self.path,1)
        self.assertEqual(restored.next_report(self.session),report)
        self.assertEqual(restored.ready_sessions(1000),[self.session])
        restored.defer(report,'rejected',1000)
        self.assertEqual(restored.retry_status()[0]['reason'],'rejected')


class RetryDeliveryTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_usage_delivery.DeliveryTests.setUp
    receipt=test_usage_delivery.DeliveryTests.receipt

    async def test_one_network_attempt_per_round_and_rotation_does_not_starve_sessions(self):
        await self.worker.capture()
        sessions=[self.session]+[str(uuid.uuid4()) for _ in range(4)]
        for session in sessions[1:]: self.worker.box.capture(session,dict(server_id=1,items=[dict(self.item)]))
        sent=[]
        def send(*args):
            sent.append(args[3]['session_id']); raise OSError('offline')
        with mock.patch('usage_delivery.request',side_effect=send),mock.patch('usage_delivery.time.time',return_value=1000),mock.patch('builtins.print'):
            for count in range(1,6):
                await self.worker.step(); self.assertEqual(len(sent),count)
            await self.worker.step(); self.assertEqual(len(sent),5)
        self.assertEqual(set(sent),set(sessions))
        self.assertEqual(set(self.worker.box.outstanding_sessions()),set(sessions))
        restarted=type(self.worker)(self.worker.base,'fixture',self.state,self.runtime,self.session)
        with mock.patch('usage_delivery.request',side_effect=self.receipt) as send,mock.patch('usage_delivery.time.time',return_value=1001):
            await restarted.step(); send.assert_not_called()
        with mock.patch('usage_delivery.request',side_effect=self.receipt) as send,mock.patch('usage_delivery.time.time',return_value=1005):
            await restarted.step(); send.assert_called_once()

    async def test_failed_session_does_not_suppress_growth_capture_or_other_session(self):
        with mock.patch('usage_delivery.request',side_effect=OSError('offline')),mock.patch('usage_delivery.time.time',return_value=1000),mock.patch('builtins.print'):
            await self.worker.step()
        frozen=self.worker.box.next_report(self.session); self.item['upload_bytes']=20
        other=str(uuid.uuid4()); self.worker.box.capture(other,dict(server_id=1,items=[dict(self.item)]))
        with mock.patch('usage_delivery.request',side_effect=self.receipt) as send,mock.patch('usage_delivery.time.time',return_value=1001):
            await self.worker.step(); self.assertEqual(send.call_args.args[3]['session_id'],other)
            await self.worker.step(); self.assertEqual(send.call_count,1)
        self.assertEqual(self.worker.box.next_report(self.session),frozen)
        with mock.patch('usage_delivery.request',side_effect=self.receipt),mock.patch('usage_delivery.time.time',return_value=1005):
            await self.worker.step()
        self.assertEqual(self.worker.box.next_report(self.session)['items'][0]['upload_bytes'],20)


@unittest.skipUnless(test_usage_delivery.test_xray.BINARY.is_file(),'Set XRAY_BINARY')
class RetryRealTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_usage_delivery.DeliveryRealTests.setUp
    tearDown=test_usage_delivery.DeliveryRealTests.tearDown
    request=test_usage_delivery.DeliveryRealTests.request
    asyncSetUp=test_usage_delivery.DeliveryRealTests.asyncSetUp
    asyncTearDown=test_usage_delivery.DeliveryRealTests.asyncTearDown
    open=test_usage_delivery.DeliveryRealTests.open
    accepted=test_usage_delivery.DeliveryRealTests.accepted
    close_writer=test_usage_delivery.DeliveryRealTests.close_writer

    async def test_real_http_lost_confirmation_defers_duplicate_then_growth_counts_once(self):
        import asyncio
        import json
        from execution_agent import cycle
        from agent import request
        from usage_delivery import UsageDelivery
        session=str(uuid.uuid4())
        await cycle(self.base,self.token,self.state,self.runtime,1,session)
        reader,writer=await self.open(); await self.accepted(reader)
        worker=UsageDelivery(self.base,self.token,self.state,self.runtime,session)
        def lost(*args): request(*args); raise OSError('response lost after commit')
        with mock.patch('usage_delivery.request',side_effect=lost) as send,mock.patch('builtins.print'):
            await worker.step(); await worker.step(); self.assertEqual(send.call_count,1)
        frozen=worker.box.next_report(session)
        writer.write(b'more'); await writer.drain()
        self.assertEqual(await asyncio.wait_for(reader.readexactly(4),2),b'more')
        with worker.box.connect() as db: db.execute('UPDATE retries SET next_attempt=0')
        with mock.patch('usage_delivery.request',wraps=request) as send:
            await worker.step(); self.assertEqual(send.call_args.args[3],frozen)
            await worker.step(); self.assertEqual(send.call_args.args[3]['sequence'],2)
        with self.request(f'/api/users/{self.uid}/usage') as response: total=json.load(response)
        self.assertEqual((total['upload_bytes'],total['download_bytes']),(9,9))
        self.assertEqual(worker.box.outstanding_sessions(),[])
        self.assertEqual(worker.box.retry_status(),[])
