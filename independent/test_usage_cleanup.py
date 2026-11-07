import asyncio
import json
import sqlite3
import unittest
import uuid
from unittest import mock
from contextlib import contextmanager
from usage_outbox import UsageOutbox
import test_usage_outbox
import test_usage_delivery


class CleanupTests(unittest.TestCase):
    setUp=test_usage_outbox.OutboxTests.setUp
    snapshot=test_usage_outbox.OutboxTests.snapshot
    receipt=test_usage_outbox.OutboxTests.receipt

    def confirmed(self,session=None,*items):
        session=session or self.session
        self.box.capture(session,self.snapshot(*items))
        report=self.box.next_report(session)
        self.box.acknowledge(report,self.receipt(report))
        return report

    def test_retired_confirmed_totals_pruned_but_cursor_and_tombstone_survive_restart(self):
        report=self.confirmed(self.session,self.item,dict(self.item,user_id=4))
        self.assertEqual(self.box.retire_confirmed(str(uuid.uuid4())),2)
        restored=UsageOutbox(self.path,1)
        with restored.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM totals').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT sequence FROM cursors').fetchone()[0],report['sequence'])
            self.assertEqual(db.execute('SELECT session FROM retired').fetchone()[0],self.session)
        for snapshot in (self.snapshot(),dict(server_id=1,items=[])):
            with self.assertRaises(ValueError): restored.capture(self.session,snapshot)
        self.assertIsNone(restored.next_report(self.session))
        self.assertEqual(restored.outstanding_sessions(),[])
        with self.assertRaises(ValueError): restored.acknowledge(report,self.receipt(report))

    def test_current_epoch_preserved_and_later_growth_keeps_sequence(self):
        self.confirmed()
        self.assertEqual(self.box.retire_confirmed(self.session),0)
        grown=dict(self.item,upload_bytes=10)
        self.box.capture(self.session,self.snapshot(grown))
        self.assertEqual(self.box.next_report(self.session)['sequence'],2)
        with self.assertRaises(ValueError): self.box.retire_confirmed('not-a-session')

    def test_unconfirmed_partial_growth_and_zero_without_receipt_are_preserved(self):
        report=self.confirmed()
        self.box.capture(self.session,self.snapshot(self.item,dict(self.item,user_id=4)))
        current=str(uuid.uuid4())
        self.assertEqual(self.box.retire_confirmed(current),0)
        next_report=self.box.next_report(self.session)
        self.box.defer(next_report,'network',100)
        self.assertEqual(self.box.retire_confirmed(current),0)
        self.assertEqual(self.box.next_report(self.session),next_report)
        self.box.acknowledge(next_report,self.receipt(next_report))
        self.box.capture(self.session,self.snapshot(dict(self.item,download_bytes=11)))
        self.assertEqual(self.box.retire_confirmed(current),0)
        zero=str(uuid.uuid4()); self.box.capture(zero,self.snapshot(dict(self.item,upload_bytes=0,download_bytes=0)))
        self.assertEqual(self.box.retire_confirmed(current),0)
        with self.box.connect() as db: self.assertEqual(db.execute('SELECT count(*) FROM totals').fetchone()[0],3)

    def test_audited_session_is_never_pruned_even_when_surviving_identity_confirms(self):
        self.box.capture(self.session,self.snapshot(self.item,dict(self.item,user_id=4)))
        self.box.isolate(self.box.next_report(self.session),100)
        rejected=self.box.next_report(self.session); self.box.isolate(rejected,101)
        good=self.box.next_report(self.session); self.box.acknowledge(good,self.receipt(good))
        audit=self.box.isolation_status()
        self.assertEqual(self.box.retire_confirmed(str(uuid.uuid4())),0)
        self.assertEqual(self.box.isolation_status(),audit)
        with self.box.connect() as db: self.assertEqual(db.execute('SELECT count(*) FROM totals').fetchone()[0],2)

    def test_cleanup_failure_rolls_back_tombstone_and_all_rows(self):
        self.confirmed(self.session,self.item,dict(self.item,user_id=4))
        with self.box.connect() as db:
            db.execute("CREATE TRIGGER fail_cleanup BEFORE DELETE ON totals WHEN OLD.user=4 BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.box.retire_confirmed(str(uuid.uuid4()))
        with self.box.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM totals').fetchone()[0],2)
            self.assertEqual(db.execute('SELECT count(*) FROM retired').fetchone()[0],0)
            db.execute('DROP TRIGGER fail_cleanup')
        self.assertEqual(self.box.retire_confirmed(str(uuid.uuid4())),2)

    def test_one_old_epoch_per_round_and_additive_schema_upgrade(self):
        sessions=[self.session,str(uuid.uuid4()),str(uuid.uuid4())]
        for session in sessions: self.confirmed(session)
        with self.box.connect() as db: db.execute('DROP TABLE retired')
        restored=UsageOutbox(self.path,1)
        for remaining in (2,1,0):
            self.assertEqual(restored.retire_confirmed(str(uuid.uuid4())),1)
            with restored.connect() as db: self.assertEqual(db.execute('SELECT count(*) FROM totals').fetchone()[0],remaining)
        self.assertEqual(restored.retire_confirmed(str(uuid.uuid4())),0)

    def test_large_audited_epoch_does_not_rescan_every_identity_for_cleanup(self):
        audited='00000000-0000-4000-8000-000000000001'
        healthy='ffffffff-ffff-4fff-8fff-ffffffffffff'
        current=str(uuid.uuid4())
        # Fixture already-settled historical rows, with a retained audit marker.
        rows=[(audited,1,i,str(uuid.uuid4()),1,1,1,1,1) for i in range(1,2049)]
        rows.append((healthy,1,9999,str(uuid.uuid4()),1,1,1,1,1))
        with self.box.connect() as db:
            db.executemany('INSERT INTO totals VALUES(?,?,?,?,?,?,?,?,?)',rows)
            db.execute('INSERT INTO failures VALUES(?,?,?,?,?)',
                       (audited,1,'{}','fixture',1))
        original=self.box.connect; instructions=[0]
        @contextmanager
        def bounded(*args,**kwargs):
            with original(*args,**kwargs) as db:
                def progress():
                    instructions[0]+=1000
                    return instructions[0]>300000
                # Work budget, independent of wall-clock speed or fsync latency.
                db.set_progress_handler(progress,1000)
                yield db
        with mock.patch.object(self.box,'connect',bounded):
            self.assertEqual(self.box.retire_confirmed(current),1)
        with original() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM totals WHERE session=?',
                                        (audited,)).fetchone()[0],2048)
            self.assertEqual(db.execute('SELECT count(*) FROM failures').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT session FROM retired').fetchone()[0],healthy)


class CleanupDeliveryTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_usage_delivery.DeliveryTests.setUp
    receipt=test_usage_delivery.DeliveryTests.receipt

    async def test_cleanup_failure_does_not_suppress_capture_or_delivery(self):
        with mock.patch('usage_delivery.UsageOutbox.retire_confirmed',side_effect=sqlite3.OperationalError('fixture')),mock.patch('usage_delivery.request',side_effect=self.receipt) as send,mock.patch('builtins.print'):
            await self.worker.step(); send.assert_called_once()
        self.assertEqual(self.worker.box.outstanding_sessions(),[])


@unittest.skipUnless(test_usage_delivery.test_xray.BINARY.is_file(),'Set XRAY_BINARY')
class CleanupRealTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_usage_delivery.DeliveryRealTests.setUp
    tearDown=test_usage_delivery.DeliveryRealTests.tearDown
    request=test_usage_delivery.DeliveryRealTests.request
    asyncSetUp=test_usage_delivery.DeliveryRealTests.asyncSetUp
    asyncTearDown=test_usage_delivery.DeliveryRealTests.asyncTearDown
    open=test_usage_delivery.DeliveryRealTests.open
    accepted=test_usage_delivery.DeliveryRealTests.accepted
    close_writer=test_usage_delivery.DeliveryRealTests.close_writer

    async def test_repeated_cycles_settle_retire_and_restart_without_recounting(self):
        import time
        from execution_agent import cycle
        from execution_epochs import ExecutionEpochs
        from quota_store import QuotaStore
        from usage_cycles import UsageCycles
        from vless_runtime import VlessRuntime
        r=self.server.registry; now=time.time(); sessions=[]
        periods=[f'retention-{i:02}' for i in range(12)]
        with r.connect() as db:
            for index,ident in enumerate(periods):
                db.execute('INSERT INTO usage_cycles(id,user_id,starts_at,ends_at) VALUES(?,?,?,?)',
                           (ident,self.uid,now-1+60*index,now-1+60*(index+1)))
                from usage_accounting import initialize_cycle
                initialize_cycle(db,ident)
        for ident in periods:
            UsageCycles(r).set_budget(ident,dict(upload_bytes=10,download_bytes=100))
        epochs=ExecutionEpochs(self.base,self.token,self.state,self.runtime,str(uuid.uuid4()))
        with mock.patch('usage_cycles.time.time',return_value=now) as clock:
            for index,ident in enumerate(periods):
                clock.return_value=now+60*index
                await cycle(self.base,self.token,self.state,self.runtime,index+1,epochs.session,epochs)
                sessions.append(epochs.session)
                reader,writer=await self.open(); await self.accepted(reader)
                await self.close_writer(writer)
                await epochs.delivery.step()
                # Discovery may use one round; the alternating scheduler must
                # still confirm current usage by the next bounded round.
                await epochs.delivery.step()
                box=epochs.delivery.box
                self.assertEqual(box.outstanding_sessions(),[])
                self.assertEqual(set(self.runtime.cycle_quotas),{(self.uid,ident)})
                with box.connect() as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM totals').fetchone()[0],1)
                    self.assertEqual(db.execute('SELECT count(*) FROM retired').fetchone()[0],index)
                    self.assertEqual(db.execute('SELECT count(*) FROM cursors').fetchone()[0],index+1)
            self.assertEqual(len(set(sessions)),12)
            state=self.runtime.quota_path.parent
            await self.runtime.close()
            self.runtime=VlessRuntime(test_usage_delivery.test_xray.BINARY,state,
                local_test_target=('127.0.0.1',self.target_port))
            recovered=ExecutionEpochs(self.base,self.token,self.state,self.runtime,str(uuid.uuid4()))
            await cycle(self.base,self.token,self.state,self.runtime,13,recovered.session,recovered)
            reader,writer=await self.open(); await self.accepted(reader)
            await self.close_writer(writer)
            await recovered.delivery.step()
            reader,_=await self.open()
            self.assertEqual(await asyncio.wait_for(reader.read(),3),b'')
            self.assertEqual(recovered.delivery.box.outstanding_sessions(),[])
            with self.request(f'/api/users/{self.uid}/usage') as response: total=json.load(response)
            self.assertEqual((total['upload_bytes'],total['download_bytes']),(65,65))
            with r.connect() as db:
                rows=[tuple(row) for row in db.execute('SELECT cycle_id,sum(upload_bytes),sum(download_bytes) FROM usage_deltas GROUP BY cycle_id ORDER BY cycle_id')]
            self.assertEqual(rows,[(ident,5,5) for ident in periods[:-1]]+[(periods[-1],10,10)])
            restored=UsageOutbox(self.state/'usage-outbox.sqlite',self.sid)
            with restored.connect() as db:
                self.assertEqual(db.execute('SELECT count(*) FROM retired').fetchone()[0],12)
                self.assertEqual(db.execute('SELECT count(*) FROM totals').fetchone()[0],1)
                self.assertEqual(db.execute('SELECT count(*) FROM cursors').fetchone()[0],13)
            for session in sessions:
                with self.assertRaises(ValueError):
                    restored.capture(session,dict(server_id=self.sid,items=[]))
            # Historic reservations remain available even after all old reports retire.
            quota=QuotaStore(self.runtime.quota_path,self.sid)
            for ident in periods:
                _,used=quota.update((self.uid,ident),(0,0),(10,100))
                self.assertEqual(used,[10,10] if ident==periods[-1] else [5,5])

    async def test_actual_http_ack_old_epoch_prunes_without_changing_remote_usage(self):
        from execution_agent import cycle
        from usage_delivery import UsageDelivery
        old=str(uuid.uuid4())
        await cycle(self.base,self.token,self.state,self.runtime,1,old)
        reader,writer=await self.open(); await self.accepted(reader)
        first=UsageDelivery(self.base,self.token,self.state,self.runtime,old)
        await first.step(); self.assertEqual(first.box.retire_confirmed(old),0)
        await self.close_writer(writer); await self.runtime.close()
        # A new execution process owns a fresh epoch and starts counters at zero.
        from vless_runtime import VlessRuntime
        self.runtime=VlessRuntime(test_usage_delivery.test_xray.BINARY,self.state/'restarted-core',local_test_target=('127.0.0.1',self.target_port))
        current=str(uuid.uuid4())
        await cycle(self.base,self.token,self.state,self.runtime,1,current)
        second=UsageDelivery(self.base,self.token,self.state,self.runtime,current)
        await second.capture()
        with second.box.connect() as db:
            self.assertIsNotNone(db.execute('SELECT 1 FROM retired WHERE session=?',(old,)).fetchone())
            self.assertEqual(db.execute('SELECT count(*) FROM totals WHERE session=?',(old,)).fetchone()[0],0)
        reader,writer=await self.open(); await self.accepted(reader)
        await second.step()
        with self.request(f'/api/users/{self.uid}/usage') as response: total=json.load(response)
        self.assertEqual((total['upload_bytes'],total['download_bytes']),(10,10))
        self.assertEqual(second.box.outstanding_sessions(),[])
        self.assertIsNone(second.box.next_report(old))
        self.assertEqual(second.box.retire_confirmed(current),0)
