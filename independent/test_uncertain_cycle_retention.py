"""Uncertain tails survive real period changes; only measured tails settle."""
import asyncio
import sqlite3
import unittest
import uuid
from unittest import mock

from agent import request
from execution_agent import cycle
from execution_epochs import ExecutionEpochs
from quota_store import QuotaStore
from usage_cycles import UsageCycles
from usage_ledger import UsageLedger
from usage_outbox import UsageOutbox
from vless_runtime import VlessRuntime
import test_usage_delivery


@unittest.skipUnless(test_usage_delivery.test_xray.BINARY.is_file(),'Set XRAY_BINARY')
class UncertainCycleRetentionTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_usage_delivery.DeliveryRealTests.setUp
    tearDown=test_usage_delivery.DeliveryRealTests.tearDown
    request=test_usage_delivery.DeliveryRealTests.request
    asyncSetUp=test_usage_delivery.DeliveryRealTests.asyncSetUp
    asyncTearDown=test_usage_delivery.DeliveryRealTests.asyncTearDown
    open=test_usage_delivery.DeliveryRealTests.open
    accepted=test_usage_delivery.DeliveryRealTests.accepted
    close_writer=test_usage_delivery.DeliveryRealTests.close_writer

    async def test_unknown_old_tail_survives_restart_two_periods_and_cleanup(self):
        await self.retention(False)

    async def test_surviving_checkpoint_resolves_audit_but_tail_stays_in_old_period(self):
        await self.retention(True)

    async def retention(self,recover_checkpoint):
        clock=test_usage_delivery.scoped_period_clock(self); now=clock()
        registry=self.server.registry
        periods=['uncertain-old','uncertain-next','uncertain-last']
        with registry.connect() as db:
            for index,ident in enumerate(periods):
                db.execute('INSERT INTO usage_cycles(id,user_id,starts_at,ends_at) VALUES(?,?,?,?)',
                           (ident,self.uid,now-1+60*index,now-1+60*(index+1)))
                from usage_accounting import initialize_cycle
                initialize_cycle(db,ident)
        for ident in periods:
            UsageCycles(registry).set_budget(ident,dict(upload_bytes=20,download_bytes=20))
        old=str(uuid.uuid4())
        epochs=ExecutionEpochs(self.base,self.token,self.state,self.runtime,old)
        await cycle(self.base,self.token,self.state,self.runtime,1,old,epochs)
        reader,writer=await self.open(); await self.accepted(reader)
        await self.close_writer(writer)
        await epochs.delivery.capture()
        box=epochs.delivery.box
        frozen=box.next_report(old)
        self.assertEqual([(i['upload_bytes'],i['download_bytes']) for i in frozen['items']],[(5,5)])
        # The second real write succeeds; its checkpoint fails, so only the
        # earlier 5/5 snapshot is durable. Do not manufacture an unknown intent.
        with mock.patch.object(box,'capture',side_effect=sqlite3.OperationalError('fixture checkpoint failure')):
            reader,writer=await self.open()
            # Only the unmetered protocol response may precede the failed
            # asynchronous upload checkpoint; no application echo is allowed.
            self.assertIn(await asyncio.wait_for(reader.read(),3),(b'',b'\x00\x00'))
            await self.close_writer(writer)
        self.assertTrue(self.runtime.meter.failed)
        self.assertEqual([(i['upload_bytes'],i['download_bytes']) for i in self.runtime.usage_snapshot()['items']],[(10,5)])
        restored=UsageOutbox(self.state/'usage-outbox.sqlite',self.sid)
        unknown=restored.uncertain_writes()
        self.assertEqual(len(unknown),1)
        self.assertEqual((unknown[0]['committed_upload'],unknown[0]['possible_upload'],
                          unknown[0]['committed_download'],unknown[0]['possible_download']),(5,10,5,5))
        self.assertEqual(restored.write_resolution_history(),[])
        clock.return_value=now+60
        if recover_checkpoint:
            # The old executor is still alive here: its successful-write
            # counters, not a new process or the intent target, are evidence.
            await epochs.delivery.capture()
            self.assertEqual(restored.uncertain_writes(),[])
            audit=restored.write_resolution_history()
            self.assertEqual(len(audit),1)
            self.assertEqual((audit[0]['session'],audit[0]['lower_upload'],
                audit[0]['target_upload'],audit[0]['checkpoint_upload']),(old,5,10,10))
        # Real HTTP confirms the frozen lower bound after the period ended.
        receipt=await asyncio.to_thread(request,self.base,self.token,'/agent/usage/settle',frozen)
        restored.acknowledge(frozen,receipt)
        if recover_checkpoint:
            tail=restored.next_report(old)
            receipt=await asyncio.to_thread(request,self.base,self.token,'/agent/usage/settle',tail)
            restored.acknowledge(tail,receipt)
        await self.runtime.close()
        core_state=self.runtime.quota_path.parent
        self.runtime=VlessRuntime(test_usage_delivery.test_xray.BINARY,core_state,
                                 local_test_target=('127.0.0.1',self.target_port))
        epochs=ExecutionEpochs(self.base,self.token,self.state,self.runtime,str(uuid.uuid4()))
        for index in (1,2):
            clock.return_value=now+60*index
            await cycle(self.base,self.token,self.state,self.runtime,index+1,epochs.session,epochs)
            reader,writer=await self.open(); await self.accepted(reader)
            await self.close_writer(writer)
            await epochs.delivery.step()
            await epochs.delivery.step()
            restored=UsageOutbox(self.state/'usage-outbox.sqlite',self.sid)
            self.assertEqual(restored.outstanding_sessions(),[])
            if recover_checkpoint:
                self.assertEqual(restored.uncertain_writes(),[])
                self.assertEqual(restored.write_resolution_history(),audit)
            else:
                self.assertEqual(restored.uncertain_writes(),unknown)
                self.assertEqual(restored.write_resolution_history(),[])
        with registry.connect() as db:
            rows=[tuple(r) for r in db.execute('SELECT cycle_id,sum(upload_bytes),sum(download_bytes) FROM usage_deltas GROUP BY cycle_id ORDER BY cycle_id')]
        expected={ident:(5,5) for ident in periods}
        if recover_checkpoint: expected[periods[0]]=(10,5)
        self.assertEqual(rows,[(ident,*expected[ident]) for ident in sorted(periods)])
        total=UsageLedger(registry).user(self.uid)
        self.assertEqual((total['upload_bytes'],total['download_bytes']),(20 if recover_checkpoint else 15,15))
        quota=QuotaStore(self.runtime.quota_path,self.sid)
        for index,ident in enumerate(periods):
            _,used=quota.update((self.uid,ident),(0,0),(20,20))
            self.assertEqual(used,[10,5] if index==0 else [5,5])
        with restored.connect() as db:
            old_retired=db.execute('SELECT 1 FROM retired WHERE session=?',(old,)).fetchone()
            self.assertEqual(old_retired is not None,recover_checkpoint)
            self.assertEqual(db.execute('PRAGMA integrity_check').fetchone()[0],'ok')
        # A new queue cannot be rebound to another server to clear history.
        with self.assertRaises(ValueError): UsageOutbox(restored.path,self.sid+1)
