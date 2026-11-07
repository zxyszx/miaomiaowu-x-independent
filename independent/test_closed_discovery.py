"""Closed evidence discovery must never invent closure or bypass revocation."""
import copy
import sqlite3
import unittest
import uuid
import urllib.error
from unittest import mock
from closed_discovery import ClosedDiscovery
from closed_delivery import ClosedDelivery
from usage_outbox import UsageOutbox
import test_closed_delivery as base
import test_usage_delivery as delivery


class DiscoveryTests(unittest.TestCase):
    setUp=base.ClosedDeliveryTests.setUp
    tearDown=base.ClosedDeliveryTests.tearDown
    snapshot=base.ClosedDeliveryTests.snapshot
    receipt=base.ClosedDeliveryTests.receipt
    confirm=base.ClosedDeliveryTests.confirm

    def closed(self):
        self.box.capture(self.session,self.snapshot(self.item))
        self.box.close_epoch(self.session)
        self.queue=ClosedDiscovery(self.box)

    def reply(self,cycles=(),next_after=None):
        return dict(server_id=1,sessions=[self.session],
                    drains=[dict(cycle_id=c,session_id=self.session) for c in cycles],
                    next_after=next_after,ready_for_execution=False)

    def test_only_closed_confirmed_and_not_current_epochs_discovered(self):
        queue=ClosedDiscovery(self.box)
        self.box.capture(self.session,self.snapshot())
        self.assertIsNone(queue.ready(1))
        self.box.close_epoch(self.session)
        self.assertIsNone(queue.ready(1))
        self.confirm()
        self.assertIsNone(queue.ready(1,(self.session,)))
        job=queue.ready(1)
        self.assertEqual(job['request'],dict(sessions=[self.session],after=None))
        self.assertEqual(job['digest'],self.box.closed_epoch(self.session)['digest'])

    def test_unknown_write_is_blocked_and_large_epoch_is_fragment_queued(self):
        self.box.capture(self.session,self.snapshot())
        self.box.prepare_write(self.session,dict(self.item,upload_bytes=6))
        self.box.close_epoch(self.session); self.confirm()
        self.assertIsNone(ClosedDiscovery(self.box).ready(1))
        other=str(uuid.uuid4())
        items=[dict(self.item,user_id=i+1) for i in range(65)]
        self.box.capture(other,self.snapshot(*items)); self.box.close_epoch(other)
        while report:=self.box.next_report(other): self.box.acknowledge(report,self.receipt(report))
        large=ClosedDiscovery(self.box)
        job=large.ready(1)
        self.assertEqual(job['request'],dict(sessions=[other],after=None))
        large.accept(dict(job,session=other),dict(server_id=1,sessions=[other],drains=[dict(cycle_id='large',session_id=other)],next_after=None,ready_for_execution=False),1)
        with self.box.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM closed_deliveries').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM closed_fragment_deliveries').fetchone()[0],1)

    def test_strict_response_binding_order_and_pagination_are_atomic(self):
        self.closed(); self.confirm(); job=self.queue.ready(1)
        valid=self.reply(['a','b'])
        bad=[dict(valid,server_id=True),dict(valid,server_id=2),dict(valid,sessions=[str(uuid.uuid4())]),
             dict(valid,ready_for_execution=True),dict(valid,next_after=['b',self.session]),
             self.reply(['b','a']),self.reply(['a','a']),dict(valid,extra=True)]
        wrong=copy.deepcopy(valid); wrong['drains'][1]['session_id']=str(uuid.uuid4()); bad.append(wrong)
        for reply in bad:
            with self.subTest(reply=reply),self.assertRaises(ValueError): self.queue.accept(job,reply,1)
        with self.box.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM closed_deliveries').fetchone()[0],0)
            db.execute("CREATE TRIGGER fail_discovery BEFORE INSERT ON closed_deliveries WHEN NEW.cycle='b' BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.queue.accept(job,valid,1)
        with self.box.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM closed_deliveries').fetchone()[0],0)
        self.assertEqual(self.queue.ready(1),job)

    def test_page_cursor_reopens_and_empty_poll_restarts_from_beginning(self):
        self.closed(); self.confirm(); job=self.queue.ready(1)
        cycles=[f'c{i:03}' for i in range(64)]; cursor=[cycles[-1],self.session]
        self.queue.accept(job,self.reply(cycles,cursor),1)
        queue=ClosedDiscovery(UsageOutbox(self.path,1))
        self.assertIsNone(queue.ready(5))
        second=queue.ready(6); self.assertEqual(second['request']['after'],cursor)
        with self.assertRaises(ValueError): queue.accept(second,self.reply(['c000']),6)
        queue.accept(second,self.reply(['c064']),6)
        self.assertIsNone(queue.ready(65))
        third=queue.ready(66); self.assertIsNone(third['request']['after'])
        queue.accept(third,self.reply(),66)
        self.assertIsNone(queue.ready(125)); self.assertIsNotNone(queue.ready(126))
        with self.box.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM closed_deliveries').fetchone()[0],65)

    def test_backoff_and_revocation_survive_reopen_without_reset(self):
        self.closed(); self.confirm(); job=self.queue.ready(10)
        self.queue.defer(job,10)
        queue=ClosedDiscovery(UsageOutbox(self.path,1))
        self.assertIsNone(queue.ready(14)); self.assertEqual(queue.ready(15),job)
        queue.defer(job,15,revoked=True)
        self.assertIsNone(ClosedDiscovery(UsageOutbox(self.path,1)).ready(10000))
        with self.assertRaises(ValueError): queue.accept(job,self.reply(['c']),10000)
        self.assertIsNotNone(self.box.closed_epoch(self.session))

    def test_repeated_discovery_never_reopens_blocked_final_delivery(self):
        self.closed(); self.confirm(); job=self.queue.ready(1)
        self.queue.accept(job,self.reply(['c']),1)
        final=ClosedDelivery(self.box); final_job=final.ready(1)
        final.defer(final_job,1,revoked=True)
        again=self.queue.ready(61); self.queue.accept(again,self.reply(['c']),61)
        self.assertIsNone(final.ready(10000))
        with self.box.connect() as db:
            self.assertEqual(db.execute('SELECT status FROM closed_deliveries').fetchone()[0],'blocked')


class SchedulingTests(unittest.IsolatedAsyncioTestCase):
    setUp=delivery.DeliveryTests.setUp
    tearDown=delivery.DeliveryTests.tearDown
    receipt=delivery.DeliveryTests.receipt

    async def prepare(self):
        await self.worker.capture(); await self.worker.rotate(str(uuid.uuid4()))

    async def test_automatic_order_one_network_attempt_each_round(self):
        await self.prepare(); paths=[]
        def send(*args):
            path=args[2]; data=args[3]; paths.append(path)
            if path.endswith('/drains'):
                return dict(server_id=1,sessions=[self.session],drains=[dict(cycle_id='old',session_id=self.session)],next_after=None,ready_for_execution=False)
            if path.endswith('/closed'):
                return dict(accepted=True,server_id=1,session_id=self.session,cycle_id='old',digest=data['digest'])
            return self.receipt(*args)
        with mock.patch('usage_delivery.request',side_effect=send):
            for expected in ('/settle','/drains','/closed'):
                before=len(paths); await self.worker.step()
                self.assertEqual(len(paths),before+1); self.assertTrue(paths[-1].endswith(expected))
            await self.worker.step(); self.assertEqual(len(paths),3)

    async def test_discovery_auth_failure_does_not_loop_or_discard_evidence(self):
        await self.prepare()
        with mock.patch('usage_delivery.request',side_effect=self.receipt): await self.worker.step()
        with mock.patch('usage_delivery.request',side_effect=urllib.error.HTTPError('local',401,'revoked',{},None)) as send:
            await self.worker.step(); await self.worker.step(); self.assertEqual(send.call_count,1)
        self.assertIsNotNone(self.worker.box.closed_epoch(self.session))
        with self.worker.box.connect() as db:
            self.assertEqual(db.execute('SELECT status FROM closed_discoveries').fetchone()[0],'blocked')


if __name__=='__main__': unittest.main()
