import asyncio
import json
import sqlite3
import unittest
import uuid
from unittest import mock
from core import Error, Registry
from usage_ledger import UsageLedger, MAX_COUNTER
import test_usage_ledger
import test_usage_delivery


class PeriodTests(unittest.TestCase):
    setUp=test_usage_ledger.UsageTests.setUp
    tearDown=test_usage_ledger.UsageTests.tearDown
    fixtures=test_usage_ledger.UsageTests.fixtures
    fixtures=test_usage_ledger.UsageTests.fixtures
    fixture=test_usage_ledger.UsageTests.fixture
    report=test_usage_ledger.UsageTests.report

    def at(self,when,sequence=1,items=None):
        with mock.patch('usage_ledger.time.time',return_value=when): return self.report(sequence,items)

    def test_window_boundary_growth_retry_and_restart(self):
        r=self.fixture(); self.at(100)
        self.item.update(upload_bytes=17,download_bytes=23); self.at(200,2)
        self.at(300,2)  # retry must not shift the second delta to another window
        ledger=UsageLedger(Registry(r.path))
        first=ledger.user_period(self.uid,100,200); second=ledger.user_period(self.uid,200,300)
        self.assertEqual((first['upload_bytes'],first['download_bytes']),(10,20))
        self.assertEqual((second['upload_bytes'],second['download_bytes']),(7,3))
        self.assertEqual(ledger.user_period(self.uid,300,400)['upload_bytes'],0)
        self.assertEqual(first['unattributed_upload_bytes'],0)
        self.assertEqual(first['basis'],'received_at')
        self.assertEqual(ledger.user(self.uid)['upload_bytes'],17)

    def test_late_invalid_identity_and_cursor_failure_rollback_all_deltas(self):
        r=self.fixture(); bad=dict(self.item,credential=str(uuid.uuid4()))
        with self.assertRaises(Error): self.at(100,items=[self.item,bad])
        self.assertEqual(self.ledger.user_period(self.uid,0,200)['upload_bytes'],0)
        with r.connect() as db:
            db.execute("CREATE TRIGGER fail_period_commit BEFORE INSERT ON usage_reports BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.at(100)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_deltas').fetchone()[0],0)
            db.execute('DROP TRIGGER fail_period_commit')
        self.at(101)
        self.assertEqual(self.ledger.user_period(self.uid,0,200)['upload_bytes'],10)

    def test_upgrade_keeps_historical_bytes_unattributed(self):
        r=self.fixture(); self.at(100)
        # Model a pre-period schema: existing counters, no per-delta timestamps.
        with r.connect() as db: db.execute('DROP TABLE usage_deltas')
        self.ledger=UsageLedger(Registry(r.path))
        self.item.update(upload_bytes=14,download_bytes=22); self.at(200,2)
        period=self.ledger.user_period(self.uid,0,300)
        self.assertEqual((period['upload_bytes'],period['download_bytes']),(4,2))
        self.assertEqual((period['unattributed_upload_bytes'],period['unattributed_download_bytes']),(10,20))
        self.at(201,2)
        self.assertEqual(self.ledger.user_period(self.uid,0,300),period)

    def test_concurrent_reports_new_epoch_and_cross_server_sums(self):
        import concurrent.futures
        import test_accounts
        r=self.fixture()
        with mock.patch('usage_ledger.time.time',return_value=100),concurrent.futures.ThreadPoolExecutor(4) as pool:
            list(pool.map(lambda _:self.report(),range(4)))
        self.session=str(uuid.uuid4()); r.begin_session(self.token,dict(session_id=self.session))
        self.item.update(upload_bytes=3,download_bytes=4); self.at(101)
        sid=r.save('servers',dict(name='second'))['id']; nid=r.save('nodes',dict(name='second',server_id=sid))['id']
        r.configure_node(nid,test_accounts.test_xray.VLESS)
        token=r.provision_agent(sid)['token']; session=str(uuid.uuid4()); r.begin_session(token,dict(session_id=session))
        auth=r.authorize_node_user(nid,self.uid)
        with mock.patch('usage_ledger.time.time',return_value=102):
            self.ledger.report(token,dict(session_id=session,sequence=1,items=[dict(self.item,node_id=nid,credential=auth['uuid'],upload_bytes=7)]))
        period=self.ledger.user_period(self.uid,100,103)
        self.assertEqual((period['upload_bytes'],period['download_bytes']),(20,28))
        self.assertEqual(len(period['nodes']),2)
        self.assertEqual(period['unattributed_upload_bytes'],0)

    def test_empty_zero_growth_and_invalid_window(self):
        self.fixture()
        self.assertEqual(self.ledger.user_period(self.uid,0,1)['nodes'],[])
        self.at(100); self.at(150,2)
        self.assertEqual(self.ledger.user_period(self.uid,150,200)['nodes'],[])
        for start,end in ((1,1),(2,1),(-1,2),(True,2),(0,float('nan')),(0,float('inf')),(0,'2'),(0,10**1000)):
            with self.subTest(start=start,end=end),self.assertRaises(Error): self.ledger.user_period(self.uid,start,end)
        with self.assertRaises(Error) as caught: self.ledger.user_period(999,0,1)
        self.assertEqual(caught.exception.status,404)

    def test_deleted_identity_does_not_leak_events_to_reused_user(self):
        r=self.fixture(); self.at(100); r.delete('users',self.uid)
        uid=r.save('users',dict(name='replacement'))['id']
        self.assertEqual(uid,self.uid)
        period=self.ledger.user_period(uid,0,200)
        self.assertEqual((period['upload_bytes'],period['unattributed_upload_bytes']),(0,0))

    def test_large_aggregate_and_credential_rotation_remain_exact(self):
        r=self.fixture(); self.item.update(upload_bytes=MAX_COUNTER,download_bytes=MAX_COUNTER); self.at(100)
        r.authorize_node_user(self.nid,self.uid,revoke=True)
        self.item['credential']=r.authorize_node_user(self.nid,self.uid)['uuid']; self.at(101,2)
        result=self.ledger.user_period(self.uid,0,200)
        self.assertEqual(result['upload_bytes'],MAX_COUNTER*2)
        self.assertEqual(result['unattributed_upload_bytes'],0)


@unittest.skipUnless(test_usage_delivery.test_xray.BINARY.is_file(),'Set XRAY_BINARY')
class PeriodRealTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_usage_delivery.DeliveryRealTests.setUp
    tearDown=test_usage_delivery.DeliveryRealTests.tearDown
    request=test_usage_delivery.DeliveryRealTests.request
    asyncSetUp=test_usage_delivery.DeliveryRealTests.asyncSetUp
    asyncTearDown=test_usage_delivery.DeliveryRealTests.asyncTearDown
    open=test_usage_delivery.DeliveryRealTests.open
    accepted=test_usage_delivery.DeliveryRealTests.accepted
    close_writer=test_usage_delivery.DeliveryRealTests.close_writer

    async def test_actual_payload_http_deltas_retry_and_window_query(self):
        import time
        from agent import request
        from execution_agent import cycle
        from usage_delivery import UsageDelivery
        session=str(uuid.uuid4()); start=time.time()
        await cycle(self.base,self.token,self.state,self.runtime,1,session)
        reader,writer=await self.open(); await self.accepted(reader)
        worker=UsageDelivery(self.base,self.token,self.state,self.runtime,session)
        def lost(*args): request(*args); raise OSError('lost after commit')
        with mock.patch('usage_delivery.request',side_effect=lost),mock.patch('builtins.print'): await worker.step()
        ledger=UsageLedger(self.server.registry)
        first=ledger.user_period(self.uid,start,time.time()+1)
        self.assertEqual((first['upload_bytes'],first['download_bytes']),(5,5))
        with worker.box.connect() as db: db.execute('UPDATE retries SET next_attempt=0')
        await worker.step()  # same frozen report: no additional delta
        writer.write(b'more'); await writer.drain()
        self.assertEqual(await asyncio.wait_for(reader.readexactly(4),2),b'more')
        await worker.step()
        result=ledger.user_period(self.uid,start,time.time()+1)
        self.assertEqual((result['upload_bytes'],result['download_bytes']),(9,9))
        self.assertEqual(result['unattributed_upload_bytes'],0)
        with self.server.registry.connect() as db:
            deltas=db.execute('SELECT upload_bytes FROM usage_deltas ORDER BY sequence').fetchall()
        self.assertEqual([r[0] for r in deltas],[5,4])

class CycleBindingTests(unittest.TestCase):
    setUp=test_usage_ledger.UsageTests.setUp
    tearDown=test_usage_ledger.UsageTests.tearDown
    fixtures=test_usage_ledger.UsageTests.fixtures
    fixture=test_usage_ledger.UsageTests.fixture
    report=test_usage_ledger.UsageTests.report

    def test_report_delta_binds_receipt_to_identity_cycle(self):
        r=self.fixture(); uid=self.uid
        with r.connect() as db:
            db.execute('INSERT INTO usage_cycles(id,user_id,starts_at,ends_at) VALUES(?,?,?,?)',('cycle-a',uid,90,200))
            from usage_accounting import initialize_cycle
            initialize_cycle(db,'cycle-a')
            db.execute('INSERT INTO usage_cycles(id,user_id,starts_at,ends_at) VALUES(?,?,?,?)',('cycle-b',uid,200,400))
            initialize_cycle(db,'cycle-b')
        with mock.patch('usage_ledger.time.time',return_value=100): self.report()
        self.item.update(upload_bytes=15,download_bytes=25)
        with mock.patch('usage_ledger.time.time',return_value=250): self.report(2)
        with r.connect() as db:
            rows=db.execute('SELECT cycle_id,upload_bytes FROM usage_deltas WHERE user_id=? ORDER BY sequence',(uid,)).fetchall()
        self.assertEqual([(x['cycle_id'],x['upload_bytes']) for x in rows],[('cycle-a',10),('cycle-b',5)])

    def test_legacy_delta_migration_stays_unattributed(self):
        r=self.fixture()
        with r.connect() as db:
            db.execute('DROP TABLE usage_deltas')
            db.execute('''CREATE TABLE usage_deltas(server_id INTEGER,session_id TEXT,sequence INTEGER,node_id INTEGER,user_id INTEGER,credential TEXT,received_at REAL,upload_bytes INTEGER,download_bytes INTEGER,PRIMARY KEY(server_id,session_id,sequence,node_id,user_id,credential))''')
        r2=Registry(r.path)
        with r2.connect() as db:
            cols=[x['name'] for x in db.execute('PRAGMA table_info(usage_deltas)')]
        self.assertIn('cycle_id',cols)

    def test_cycle_budget_rejects_overage_atomically(self):
        from usage_cycles import UsageCycles
        r=self.fixture(); uid=self.uid
        with r.connect() as db:
            db.execute('INSERT INTO usage_cycles(id,user_id,starts_at,ends_at) VALUES(?,?,?,?)',('budget-cycle',uid,0,200))
            from usage_accounting import initialize_cycle
            initialize_cycle(db,'budget-cycle')
        UsageCycles(r).set_budget('budget-cycle',{'upload_bytes':10,'download_bytes':20})
        with mock.patch('usage_ledger.time.time',return_value=100):
            self.report()
            self.item['upload_bytes']=11
            with self.assertRaises(Error): self.report(2)
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],10)

    def test_cycle_budget_and_usage_are_carried_into_execution_snapshot(self):
        from usage_cycles import UsageCycles
        import time
        r=self.fixture(); uid=self.uid; now=time.time()
        with r.connect() as db:
            db.execute('INSERT INTO usage_cycles(id,user_id,starts_at,ends_at) VALUES(?,?,?,?)',('snapshot-cycle',uid,now-10,now+200))
            from usage_accounting import initialize_cycle
            initialize_cycle(db,'snapshot-cycle')
        UsageCycles(r).set_budget('snapshot-cycle',{'upload_bytes':100,'download_bytes':200})
        with mock.patch('usage_ledger.time.time',return_value=now): self.report()
        snap=r.configuration(self.token)
        client=snap['nodes'][0]['clients'][0]
        self.assertEqual(client['cycle_id'],'snapshot-cycle')
        self.assertEqual(client['cycle_budget'],{'upload_bytes':100,'download_bytes':200})
        self.assertEqual(client['cycle_usage'],{'upload_bytes':10,'download_bytes':20})
