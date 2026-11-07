import asyncio
import copy
import json
from pathlib import Path
import sqlite3
import unittest
import urllib.error
import uuid
from unittest import mock

from core import Registry,Error
from usage_ledger import UsageLedger
from usage_outbox import UsageOutbox
from usage_settlement import DELIVERY_WINDOW,EXECUTION_LEASE
import test_usage_ledger
import test_payload_meter
import test_xray


def initialized_cycle(registry,cycle_id,user_id,starts_at,ends_at):
    from usage_accounting import initialize_cycle
    with registry.connect() as db:
        db.execute('BEGIN IMMEDIATE')
        db.execute('INSERT INTO usage_cycles(id,user_id,starts_at,ends_at) VALUES(?,?,?,?)',
                   (cycle_id,user_id,starts_at,ends_at))
        initialize_cycle(db,cycle_id)


class SettlementTests(unittest.TestCase):
    setUp=test_usage_ledger.UsageTests.setUp
    tearDown=test_usage_ledger.UsageTests.tearDown
    fixtures=test_usage_ledger.UsageTests.fixtures
    fixture=test_usage_ledger.UsageTests.fixture

    def data(self,sequence=1): return dict(session_id=self.session,sequence=sequence,items=[dict(self.item)])
    def applied(self,r,session=None):
        snap=r.configuration(self.token)
        data=dict(revision=snap['revision'],digest=snap['digest'],session_id=session or self.session)
        r.acknowledge_configuration(self.token,data); r.acknowledge_runtime(self.token,data)
        return snap

    def test_budget_usage_revision_tracks_growth_not_retries_and_rolls_back_with_receipt(self):
        import time
        from usage_cycles import UsageCycles
        r=self.fixture(); now=time.time()
        initialized_cycle(r,'revision-period',self.uid,now-1,now+100)
        UsageCycles(r).set_budget('revision-period',dict(upload_bytes=100,download_bytes=100))
        before=self.applied(r)
        self.ledger.settle(self.token,self.data())
        after=r.configuration(self.token)
        self.assertEqual(after['revision'],before['revision']+1)
        self.assertEqual(after['nodes'][0]['clients'][0]['cycle_usage'],dict(upload_bytes=10,download_bytes=20))
        self.assertTrue(self.ledger.settle(self.token,self.data())['duplicate'])
        self.ledger.settle(self.token,self.data(2))
        self.assertEqual(r.configuration(self.token),after)
        with r.connect() as db:
            db.execute("CREATE TRIGGER reject_usage_revision BEFORE UPDATE ON server_configs BEGIN SELECT RAISE(ABORT,'fixture'); END")
        self.item['upload_bytes']=11
        with self.assertRaises(sqlite3.IntegrityError): self.ledger.settle(self.token,self.data(3))
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],10)
        self.assertEqual(r.configuration(self.token),after)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT sequence FROM usage_reports').fetchone()[0],2)

    def test_revoked_grant_settles_once_without_reauthorizing_or_changing_config(self):
        r=self.fixture(); self.applied(r)
        r.authorize_node_user(self.nid,self.uid,revoke=True)
        before=r.configuration(self.token)
        with self.assertRaises(Error): self.ledger.report(self.token,self.data())
        self.assertFalse(self.ledger.settle(self.token,self.data())['duplicate'])
        self.assertTrue(self.ledger.settle(self.token,self.data())['duplicate'])
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],10)
        self.assertEqual(r.configuration(self.token),before)
        self.assertEqual(before['nodes'][0]['clients'],[])

    def test_retired_session_can_only_settle_its_executed_identity(self):
        r=self.fixture(); self.applied(r); old=self.data()
        new=str(uuid.uuid4()); r.begin_session(self.token,dict(session_id=new))
        self.assertTrue(self.ledger.settle(self.token,old)['accepted'])
        with self.assertRaises(Error): r.begin_session(self.token,dict(session_id=self.session))
        wrong=copy.deepcopy(old); wrong['session_id']=new
        with self.assertRaises(Error): self.ledger.settle(self.token,wrong)
        wrong=copy.deepcopy(old); wrong['items'][0]['credential']=str(uuid.uuid4())
        with self.assertRaises(Error): self.ledger.settle(self.token,wrong)

    def test_configuration_staging_or_unexecuted_grant_is_insufficient(self):
        r=self.fixture(); snap=r.configuration(self.token)
        data=dict(revision=snap['revision'],digest=snap['digest'],session_id=self.session)
        r.acknowledge_configuration(self.token,data)
        with self.assertRaises(Error): self.ledger.settle(self.token,self.data())
        r.acknowledge_runtime(self.token,data)
        other=r.save('users',dict(name='later'))['id']; auth=r.authorize_node_user(self.nid,other)
        wrong=self.data(); wrong['items']=[dict(self.item,user_id=other,credential=auth['uuid'])]
        with self.assertRaises(Error): self.ledger.settle(self.token,wrong)

    def test_fixed_window_rejects_growth_but_exact_accepted_retry_recovers(self):
        r=self.fixture()
        with mock.patch('usage_settlement.time.time',return_value=1000): self.applied(r)
        with mock.patch('usage_settlement.time.time',return_value=1001): self.ledger.settle(self.token,self.data())
        with mock.patch('usage_settlement.time.time',return_value=1001+EXECUTION_LEASE+DELIVERY_WINDOW):
            self.assertTrue(self.ledger.settle(self.token,self.data())['duplicate'])
            with self.assertRaises(Error): self.ledger.settle(self.token,self.data(2))
        with mock.patch('usage_settlement.time.time',return_value=999):
            with self.assertRaises(Error): self.ledger.settle(self.token,self.data(2))
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],10)

    def test_token_rotation_revokes_old_settlement_for_both_tokens(self):
        r=self.fixture(); snap=self.applied(r); old=self.token
        self.token=r.provision_agent(snap['server_id'])['token']
        for token in (old,self.token):
            with self.assertRaises(Error): self.ledger.settle(token,self.data())
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],0)

    def test_server_isolation_and_deleted_resource_id_reuse(self):
        r=self.fixture(); self.applied(r)
        sid=r.save('servers',dict(name='other'))['id']; token=r.provision_agent(sid)['token']
        with self.assertRaises(Error): self.ledger.settle(token,self.data())
        r.delete('users',self.uid)
        recreated=r.save('users',dict(name='replacement'))['id']; self.assertEqual(recreated,self.uid)
        with self.assertRaises(Error): self.ledger.settle(self.token,self.data())
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],0)

    def test_history_persists_and_late_batch_failure_rolls_back(self):
        r=self.fixture(); self.applied(r); self.ledger=UsageLedger(Registry(r.path))
        data=self.data(); bad=dict(self.item,user_id=999)
        data['items'].append(bad)
        with self.assertRaises(Error): self.ledger.settle(self.token,data)
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],0)
        self.assertFalse(self.ledger.settle(self.token,self.data())['duplicate'])
        with self.assertRaises(Error): self.ledger.settle(self.token,dict(self.data(2),items=[dict(self.item,upload_bytes=1)]))

    def test_execution_history_failure_rolls_back_runtime_receipt(self):
        r=self.fixture(); snap=r.configuration(self.token)
        data=dict(revision=snap['revision'],digest=snap['digest'],session_id=self.session)
        r.acknowledge_configuration(self.token,data)
        with r.connect() as db:
            db.execute("CREATE TRIGGER fail_history BEFORE INSERT ON usage_execution_grants BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): r.acknowledge_runtime(self.token,data)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM runtime_receipts').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM usage_execution_grants').fetchone()[0],0)

    def cycle_fixture(self):
        r=self.fixture()
        initialized_cycle(r,'old',self.uid,990,1010)
        initialized_cycle(r,'new',self.uid,1010,1200)
        with mock.patch('usage_settlement.time.time',return_value=1000): self.applied(r)
        return r

    def test_late_tail_belongs_to_executed_cycle_not_receipt_cycle(self):
        r=self.cycle_fixture()
        with mock.patch('usage_settlement.time.time',return_value=1020):
            self.assertTrue(self.ledger.settle(self.token,self.data())['accepted'])
            self.assertTrue(self.ledger.settle(self.token,self.data())['duplicate'])
        with r.connect() as db:
            rows=db.execute('SELECT cycle_id,upload_bytes,download_bytes FROM usage_deltas').fetchall()
        self.assertEqual([tuple(x) for x in rows],[('old',10,20)])

    def test_next_cycle_needs_new_session_and_receipt_failure_is_atomic(self):
        r=self.cycle_fixture()
        with r.connect() as db: before=tuple(db.execute('SELECT * FROM runtime_receipts').fetchone())
        with mock.patch('usage_settlement.time.time',return_value=1020):
            with self.assertRaises(Error) as caught: self.applied(r)
            self.assertEqual(caught.exception.status,409)
            with r.connect() as db:
                self.assertEqual(tuple(db.execute('SELECT * FROM runtime_receipts').fetchone()),before)
                self.assertEqual(db.execute('SELECT cycle_id FROM usage_execution_grants').fetchone()[0],'old')
            old_data=self.data(); self.session=str(uuid.uuid4())
            r.begin_session(self.token,dict(session_id=self.session)); self.applied(r)
            self.ledger.settle(self.token,old_data)
            self.item.update(upload_bytes=3,download_bytes=4)
            self.ledger.settle(self.token,self.data())
        with r.connect() as db:
            rows=db.execute('SELECT cycle_id,upload_bytes FROM usage_deltas ORDER BY upload_bytes').fetchall()
        self.assertEqual([tuple(x) for x in rows],[('new',3),('old',10)])

    def test_old_cycle_budget_still_applies_to_late_growth(self):
        from usage_cycles import UsageCycles
        r=self.cycle_fixture()
        UsageCycles(r).set_budget('old',dict(upload_bytes=10,download_bytes=20))
        with mock.patch('usage_settlement.time.time',return_value=1020):
            self.ledger.settle(self.token,self.data())
            self.item['upload_bytes']=11
            with self.assertRaises(Error) as caught: self.ledger.settle(self.token,self.data(2))
            self.assertEqual(caught.exception.status,409)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_deltas').fetchone()[0],1)
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],10)

    def test_uncycled_execution_is_not_reassigned_to_later_cycle(self):
        r=self.fixture()
        with mock.patch('usage_settlement.time.time',return_value=1000): self.applied(r)
        initialized_cycle(r,'later',self.uid,1010,1200)
        with mock.patch('usage_settlement.time.time',return_value=1020):
            self.ledger.settle(self.token,self.data())
            with self.assertRaises(Error): self.applied(r)
        with r.connect() as db:
            self.assertIsNone(db.execute('SELECT cycle_id FROM usage_deltas').fetchone()[0])


@unittest.skipUnless(test_xray.BINARY.is_file(),'Set XRAY_BINARY')
class SettlementRealTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_payload_meter.PayloadRealTests.setUp
    tearDown=test_payload_meter.PayloadRealTests.tearDown
    request=test_payload_meter.PayloadRealTests.request
    asyncSetUp=test_payload_meter.PayloadRealTests.asyncSetUp
    asyncTearDown=test_payload_meter.PayloadRealTests.asyncTearDown
    open=test_payload_meter.PayloadRealTests.open
    accepted=test_payload_meter.PayloadRealTests.accepted
    rejected=test_payload_meter.PayloadRealTests.rejected
    close_writer=test_payload_meter.PayloadRealTests.close_writer
    sync=test_payload_meter.PayloadRealTests.sync

    async def test_actual_vless_tail_after_boundary_charges_old_cycle_via_http(self):
        from test_usage_delivery import scoped_period_clock
        from execution_agent import cycle
        from agent import request
        clock=scoped_period_clock(self); now=clock(); r=self.server.registry; session=str(uuid.uuid4())
        initialized_cycle(r,'real-old',self.uid,now-1,now+30)
        initialized_cycle(r,'real-new',self.uid,now+30,now+120)
        await cycle(self.base,self.token,self.state,self.runtime,1,session)
        reader,_=await self.open(); await self.accepted(reader)
        await self.runtime.close()
        box=UsageOutbox(Path(self.temp.name)/'cycle-tail.db',self.runtime.usage_snapshot()['server_id'])
        box.capture(session,self.runtime.usage_snapshot()); report=box.next_report(session)
        clock.return_value=now+30
        receipt=await asyncio.to_thread(request,self.base,self.token,'/agent/usage/settle',report)
        retry=await asyncio.to_thread(request,self.base,self.token,'/agent/usage/settle',report)
        box.acknowledge(report,receipt)
        self.assertTrue(retry['duplicate'])
        with r.connect() as db:
            rows=db.execute('SELECT cycle_id,upload_bytes,download_bytes FROM usage_deltas').fetchall()
        self.assertEqual([tuple(x) for x in rows],[('real-old',5,5)])

    async def test_actual_revoked_payload_outbox_tail_http_and_new_connections_stay_denied(self):
        from execution_agent import cycle
        from agent import request
        r=self.server.registry; session=str(uuid.uuid4())
        await cycle(self.base,self.token,self.state,self.runtime,1,session)
        reader,writer=await self.open(); await self.accepted(reader)
        box=UsageOutbox(Path(self.temp.name)/'usage.db',self.runtime.usage_snapshot()['server_id'])
        box.capture(session,self.runtime.usage_snapshot()); report=box.next_report(session)
        r.authorize_node_user(self.nid,self.uid,revoke=True); await self.sync()
        with self.assertRaises(urllib.error.HTTPError) as caught:
            await asyncio.to_thread(request,self.base,self.token,'/agent/usage',report)
        self.assertEqual(caught.exception.code,403)
        receipt=await asyncio.to_thread(request,self.base,self.token,'/agent/usage/settle',report)
        retry=await asyncio.to_thread(request,self.base,self.token,'/agent/usage/settle',report)
        self.assertTrue(retry['duplicate']); box.acknowledge(report,receipt)
        denied,_=await self.open(); await self.rejected(denied)
        with self.request(f'/api/users/{self.uid}/usage') as response: total=json.load(response)
        self.assertEqual((total['upload_bytes'],total['download_bytes']),(5,5))
        self.assertIsNone(box.next_report(session))
        for token in ('fixture-only','bad'):
            with self.assertRaises(urllib.error.HTTPError) as error:
                await asyncio.to_thread(request,self.base,token,'/agent/usage/settle',report)
            self.assertEqual(error.exception.code,401)

    async def test_actual_drained_cycle_stops_payload_and_preserves_http_tail(self):
        from execution_agent import cycle
        from agent import request
        from usage_migration import UsageMigration
        import time
        r=self.server.registry; session=str(uuid.uuid4()); now=time.time()
        initialized_cycle(r,'drain-real',self.uid,now-1,now+120)
        with r.connect() as db:
            db.execute('INSERT INTO usage_cycle_budgets VALUES(?,?,?)',('drain-real',10,10))
        from usage_cycles import UsageCycles
        UsageCycles(r).set_total_budget('drain-real',10)
        await cycle(self.base,self.token,self.state,self.runtime,1,session)
        reader,_=await self.open(); await self.accepted(reader)
        box=UsageOutbox(Path(self.temp.name)/'drain-tail.db',self.runtime.usage_snapshot()['server_id'])
        box.capture(session,self.runtime.usage_snapshot()); report=box.next_report(session)
        from total_allocation import TotalAllocations
        snapshot=r.configuration(self.token); node=snapshot['nodes'][0]; client=node['clients'][0]
        TotalAllocations(r).reserve(self.token,dict(request_id=str(uuid.uuid4()),session_id=session,
            cycle_id='drain-real',node_id=node['id'],user_id=client['user_id'],credential=client['uuid'],requested_bytes=6))
        with self.assertRaises(urllib.error.HTTPError) as caught:
            await asyncio.to_thread(request,self.base,self.token,'/agent/usage',report)
        self.assertEqual(caught.exception.code,409)
        self.assertEqual(box.next_report(session),report)
        UsageCycles(r).set_budget('drain-real',dict(upload_bytes=0,download_bytes=0))
        UsageMigration(r).begin_drain('drain-real')
        await cycle(self.base,self.token,self.state,self.runtime,2,session)
        denied,_=await self.open(); await self.rejected(denied)
        receipt=await asyncio.to_thread(request,self.base,self.token,'/agent/usage/settle',report)
        box.acknowledge(report,receipt)
        with r.connect() as db:
            rows=db.execute('SELECT cycle_id,upload_bytes,download_bytes FROM usage_deltas').fetchall()
        self.assertEqual([tuple(row) for row in rows],[('drain-real',5,5)])
        with r.connect() as db:
            proof=db.execute('SELECT budget_json,baseline_json FROM usage_execution_direction_receipts').fetchone()
        self.assertEqual(json.loads(proof[0]),dict(upload_bytes=10,download_bytes=10))
        self.assertEqual(json.loads(proof[1]),dict(upload_bytes=0,download_bytes=0))
        self.assertIsNone(box.next_report(session))
        self.assertEqual(r.configuration(self.token)['nodes'][0]['clients'],[])
