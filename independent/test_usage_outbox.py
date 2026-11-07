import asyncio
from concurrent.futures import ThreadPoolExecutor
import copy
import json
from pathlib import Path
import tempfile
import unittest
import uuid

from core import Error
from usage_outbox import UsageOutbox
import test_payload_meter
import test_xray


class OutboxTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.path=Path(self.temp.name)/'usage.db'; self.box=UsageOutbox(self.path,1)
        self.session=str(uuid.uuid4())
        self.item=dict(node_id=2,user_id=3,credential=str(uuid.uuid4()),upload_bytes=5,download_bytes=7)

    def snapshot(self,*items): return dict(server_id=1,items=list(items or [self.item]))
    def receipt(self,report,**changes):
        return dict(dict(accepted=True,server_id=1,session_id=report['session_id'],sequence=report['sequence']),**changes)

    def test_restart_retry_immutable_pending_and_later_growth(self):
        self.box.capture(self.session,self.snapshot()); first=self.box.next_report(self.session)
        grown=dict(self.item,upload_bytes=15); self.box.capture(self.session,self.snapshot(grown))
        restored=UsageOutbox(self.path,1)
        self.assertEqual(restored.next_report(self.session),first)
        first['items'][0]['upload_bytes']=999
        first=restored.next_report(self.session)
        self.assertEqual(first['items'][0]['upload_bytes'],5)
        restored.acknowledge(first,self.receipt(first))
        second=restored.next_report(self.session)
        self.assertEqual(second['sequence'],2); self.assertEqual(second['items'][0]['upload_bytes'],15)
        restored.acknowledge(second,self.receipt(second))
        self.assertIsNone(UsageOutbox(self.path,1).next_report(self.session))
        self.assertEqual(self.path.stat().st_mode&0o777,0o600)

    def test_bad_receipt_or_mutated_report_cannot_advance(self):
        self.box.capture(self.session,self.snapshot()); first=self.box.next_report(self.session)
        for changes in (dict(accepted=False),dict(server_id=2),dict(server_id=True),dict(sequence=2),dict(sequence=True),dict(session_id=str(uuid.uuid4()))):
            with self.subTest(changes=changes),self.assertRaises(ValueError):
                self.box.acknowledge(first,self.receipt(first,**changes))
        modified=copy.deepcopy(first); modified['items'][0]['upload_bytes']=6
        with self.assertRaises(ValueError): self.box.acknowledge(modified,self.receipt(modified))
        self.assertEqual(self.box.next_report(self.session),first)

    def test_batch_limit_drains_all_omitted_entries(self):
        items=[dict(self.item,user_id=i+1,credential=str(uuid.uuid4())) for i in range(130)]
        self.box.capture(self.session,self.snapshot(*items)); sizes=[]; users=[]
        while report:=self.box.next_report(self.session):
            sizes.append(len(report['items'])); users.extend(i['user_id'] for i in report['items'])
            self.box.acknowledge(report,self.receipt(report))
        self.assertEqual(sizes,[64,64,2]); self.assertEqual(users,list(range(1,131)))

    def test_discover_recovered_sessions_and_empty_capture_validation(self):
        self.assertEqual(self.box.outstanding_sessions(),[])
        self.box.capture(self.session,dict(server_id=1,items=[]))
        self.assertIsNone(self.box.next_report(self.session))
        with self.assertRaises(ValueError): self.box.capture(self.session,dict(server_id=True,items=[]))
        with self.assertRaises(ValueError): self.box.capture('bad',dict(server_id=1,items=[]))
        self.box.capture(self.session,self.snapshot())
        restored=UsageOutbox(self.path,1)
        self.assertEqual(restored.outstanding_sessions(),[self.session])
        report=restored.next_report(self.session)
        restored.acknowledge(report,self.receipt(report))
        self.assertEqual(restored.outstanding_sessions(),[])

    def test_hot_first_batch_does_not_starve_later_identity(self):
        items=[dict(self.item,user_id=i+1,credential=str(uuid.uuid4())) for i in range(65)]
        self.box.capture(self.session,self.snapshot(*items))
        report=self.box.next_report(self.session); self.box.acknowledge(report,self.receipt(report))
        self.box.capture(self.session,self.snapshot(*(dict(i,upload_bytes=10) for i in items)))
        second=self.box.next_report(self.session)
        self.assertEqual(second['items'][0]['user_id'],65)

    def test_invalid_capture_and_late_regression_are_atomic(self):
        self.box.capture(self.session,self.snapshot())
        new=dict(self.item,user_id=4)
        for items in ([new,dict(self.item,upload_bytes=4)],[new,dict(self.item,download_bytes=True)],[new,self.item,self.item]):
            with self.assertRaises((ValueError,Error)): self.box.capture(self.session,self.snapshot(*items))
        report=self.box.next_report(self.session)
        self.assertEqual(report['items'],[self.item])
        with self.assertRaises(ValueError): UsageOutbox(self.path,2)
        with self.assertRaises(ValueError): self.box.capture(self.session,dict(server_id=2,items=[]))

    def test_new_process_session_does_not_overwrite_old_pending(self):
        self.box.capture(self.session,self.snapshot()); old=self.box.next_report(self.session)
        new=str(uuid.uuid4()); self.box.capture(new,self.snapshot(dict(self.item,upload_bytes=1,download_bytes=2)))
        self.assertEqual(self.box.next_report(new)['sequence'],1)
        self.assertEqual(self.box.next_report(self.session),old)
        with self.assertRaises(ValueError): self.box.capture(self.session,self.snapshot(dict(self.item,upload_bytes=0)))

    def test_concurrent_report_creation_is_same_persisted_batch(self):
        self.box.capture(self.session,self.snapshot())
        with ThreadPoolExecutor(max_workers=6) as pool:
            reports=list(pool.map(lambda _:UsageOutbox(self.path,1).next_report(self.session),range(12)))
        self.assertTrue(all(r==reports[0] for r in reports)); self.assertEqual(reports[0]['sequence'],1)

    def test_ack_transaction_failure_keeps_pending_and_watermark(self):
        self.box.capture(self.session,self.snapshot()); report=self.box.next_report(self.session)
        with self.box.connect() as db:
            db.execute("CREATE TRIGGER fail_cursor BEFORE INSERT ON cursors BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(Exception): self.box.acknowledge(report,self.receipt(report))
        self.assertEqual(self.box.next_report(self.session),report)
        with self.box.connect() as db:
            self.assertEqual(db.execute('SELECT acked_upload FROM totals').fetchone()[0],0)
            db.execute('DROP TRIGGER fail_cursor')
        self.box.acknowledge(report,self.receipt(report)); self.assertIsNone(self.box.next_report(self.session))


@unittest.skipUnless(test_xray.BINARY.is_file(),'Set XRAY_BINARY')
class OutboxRealTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_payload_meter.PayloadRealTests.setUp
    tearDown=test_payload_meter.PayloadRealTests.tearDown
    request=test_payload_meter.PayloadRealTests.request
    asyncSetUp=test_payload_meter.PayloadRealTests.asyncSetUp
    asyncTearDown=test_payload_meter.PayloadRealTests.asyncTearDown
    open=test_payload_meter.PayloadRealTests.open
    accepted=test_payload_meter.PayloadRealTests.accepted
    close_writer=test_payload_meter.PayloadRealTests.close_writer
    sync=test_payload_meter.PayloadRealTests.sync

    async def test_lost_http_ack_then_local_restart_retries_real_payload_once(self):
        from agent import request
        await self.sync(); reader,writer=await self.open(); await self.accepted(reader)
        session=str(uuid.uuid4()); self.server.registry.begin_session(self.token,dict(session_id=session))
        path=Path(self.temp.name)/'outbox.db'
        box=UsageOutbox(path,self.runtime.usage_snapshot()['server_id'])
        box.capture(session,self.runtime.usage_snapshot()); report=box.next_report(session)
        await asyncio.to_thread(request,self.base,self.token,'/agent/usage',report) # drop acknowledgement
        box=UsageOutbox(path,box.server_id); report=box.next_report(session)
        receipt=await asyncio.to_thread(request,self.base,self.token,'/agent/usage',report)
        self.assertTrue(receipt['duplicate']); box.acknowledge(report,receipt)
        writer.write(b'next'); await writer.drain()
        self.assertEqual(await asyncio.wait_for(reader.readexactly(4),2),b'next')
        box.capture(session,self.runtime.usage_snapshot()); report=box.next_report(session)
        receipt=await asyncio.to_thread(request,self.base,self.token,'/agent/usage',report)
        box.acknowledge(report,receipt); self.assertIsNone(box.next_report(session))
        with self.request(f'/api/users/{self.uid}/usage') as response: result=json.load(response)
        self.assertEqual((result['upload_bytes'],result['download_bytes']),(9,9))
