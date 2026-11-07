import asyncio
import concurrent.futures
import sqlite3
import time
import unittest
from unittest.mock import patch
from core import Registry, Error
from usage_cycles import UsageCycles
from plan_lifecycle import Lifecycle
import test_plan_lifecycle as life
import test_accounts
import test_xray


class CycleTests(unittest.TestCase):
    setUp=life.LifecycleTests.setUp
    tearDown=life.LifecycleTests.tearDown
    fixtures=life.LifecycleTests.fixtures
    setup_plan=life.LifecycleTests.setup_plan

    def test_adjacent_windows_revision_uuid_and_persistence(self):
        r,uid,nid,token,pid,auth=self.setup_plan(); now=time.time(); m=UsageCycles(r)
        first=m.create(uid,dict(starts_at=now-10,ends_at=now+100))
        second=m.create(uid,dict(starts_at=now+100,ends_at=now+200))
        before=r.configuration(token)
        self.assertEqual(before['nodes'][0]['clients'][0]['valid_until'],now+100)
        with patch('usage_cycles.time.time',return_value=now+100):
            after=r.configuration(token)
            self.assertEqual(after['revision'],before['revision']+1)
            self.assertEqual(after['nodes'][0]['clients'][0]['valid_until'],now+200)
            self.assertEqual(after['nodes'][0]['clients'][0]['uuid'],auth['uuid'])
            self.assertEqual(r.configuration(token),after)
            rows=UsageCycles(Registry(r.path)).list(uid)
        self.assertEqual([row['id'] for row in rows],[first['id'],second['id']])
        self.assertEqual([row['expired'] for row in rows],[1,0])
        # Moving the clock back cannot reopen the expired first period.
        self.assertEqual(r.configuration(token)['nodes'][0]['clients'],[])

    def test_future_gap_and_lifecycle_intersection(self):
        r,uid,nid,token,pid,auth=self.setup_plan(); now=time.time()
        UsageCycles(r).create(uid,dict(starts_at=now+100,ends_at=now+300))
        self.assertEqual(r.configuration(token)['nodes'][0]['clients'],[])
        Lifecycle(r).manage(uid,life.rule(now+200))
        with patch('usage_cycles.time.time',return_value=now+101):
            self.assertEqual(r.configuration(token)['nodes'][0]['clients'][0]['valid_until'],now+200)
            Lifecycle(r).manage(uid,life.rule(now+200,False))
            self.assertEqual(r.configuration(token)['nodes'][0]['clients'],[])

    def test_active_budget_changes_advance_revision_atomically_but_identical_and_future_do_not(self):
        r,uid,nid,token,pid,auth=self.setup_plan(); now=time.time(); m=UsageCycles(r)
        active=m.create(uid,dict(starts_at=now-1,ends_at=now+100))['id']
        future=m.create(uid,dict(starts_at=now+100,ends_at=now+200))['id']
        before=r.configuration(token)
        m.set_budget(active,dict(upload_bytes=20,download_bytes=30))
        after=r.configuration(token)
        self.assertEqual(after['revision'],before['revision']+1)
        m.set_budget(active,dict(upload_bytes=20,download_bytes=30))
        m.set_budget(future,dict(upload_bytes=40,download_bytes=50))
        self.assertEqual(r.configuration(token),after)
        with r.connect() as db:
            db.execute("CREATE TRIGGER reject_budget_revision BEFORE UPDATE ON server_configs BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            m.set_budget(active,dict(upload_bytes=60,download_bytes=70))
        self.assertEqual(m.budget(active)['upload_bytes'],20)
        self.assertEqual(r.configuration(token),after)

    def test_validation_overlap_and_concurrent_create(self):
        r,uid,nid,token,pid,auth=self.setup_plan(); now=time.time(); m=UsageCycles(r)
        for bad in ({},[],dict(starts_at=True,ends_at=now+10),dict(starts_at=0,ends_at=float('nan')),dict(starts_at=0,ends_at=10**1000),dict(starts_at=now,ends_at=now-1)):
            with self.assertRaises(Error): m.create(uid,bad)
        with self.assertRaises(Error): m.create(999,dict(starts_at=now,ends_at=now+10))
        data=dict(starts_at=now,ends_at=now+100)
        def create(_):
            try: m.create(uid,data); return True
            except Error: return False
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            self.assertEqual(sum(pool.map(create,range(4))),1)
        for start,end in ((now-1,now+1),(now+1,now+99),(now-1,now+101)):
            with self.assertRaises(Error): m.create(uid,dict(starts_at=start,ends_at=end))
        self.assertEqual(len(m.list(uid)),1)

    def test_creation_and_expiry_roll_back_with_revision(self):
        r,uid,nid,token,pid,auth=self.setup_plan(); now=time.time(); m=UsageCycles(r)
        with r.connect() as db: db.execute("CREATE TRIGGER fail_cycle BEFORE UPDATE ON server_configs BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): m.create(uid,dict(starts_at=now,ends_at=now+100))
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_cycles').fetchone()[0],0)
            db.execute('DROP TRIGGER fail_cycle')
        m.create(uid,dict(starts_at=now,ends_at=now+100))
        with r.connect() as db: db.execute("CREATE TRIGGER fail_cycle BEFORE UPDATE ON server_configs BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with patch('usage_cycles.time.time',return_value=now+101):
            with self.assertRaises(sqlite3.IntegrityError): r.configuration(token)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT expired FROM usage_cycles').fetchone()[0],0)
            db.execute('DROP TRIGGER fail_cycle')
        with patch('usage_cycles.time.time',return_value=now+101): self.assertEqual(r.configuration(token)['nodes'][0]['clients'],[])

    def test_child_isolation_and_initialized_owner_cycle_restricts_delete(self):
        from accounts import Accounts
        r,uid,nid,token,pid,auth=self.setup_plan(); now=time.time()
        child=Accounts(r).create(uid,dict(name='second',plan_id=test_accounts.plan(r,'two'),node_ids=[nid]))
        UsageCycles(r).create(uid,dict(starts_at=now+100,ends_at=now+200))
        self.assertEqual([c['user_id'] for c in r.configuration(token)['nodes'][0]['clients']],[child['user_id']])
        Accounts(r).delete(uid,child['user_id'])
        with self.assertRaises(Error) as caught: r.delete('users',uid)
        self.assertEqual(caught.exception.status,409)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_cycles').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT count(*) FROM usage_cycle_accounting_anchors').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT total_bytes FROM usage_cycle_confirmed_totals').fetchone()[0],'0')

    def test_additive_schema_upgrade(self):
        r,uid,nid,token,pid,auth=self.setup_plan()
        with r.connect() as db:
            db.execute('DROP TABLE usage_cycle_state'); db.execute('DROP TABLE usage_cycles')
        r=Registry(r.path)
        self.assertEqual(UsageCycles(r).list(uid),[])
        self.assertEqual(r.configuration(token)['nodes'][0]['clients'][0]['uuid'],auth['uuid'])


@unittest.skipUnless(test_xray.BINARY.is_file(),'Set XRAY_BINARY')
class CycleRealTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=life.LifecycleRealTests.asyncSetUp
    asyncTearDown=life.LifecycleRealTests.asyncTearDown
    setUp=life.LifecycleRealTests.setUp
    tearDown=life.LifecycleRealTests.tearDown
    request=life.LifecycleRealTests.request
    open=life.LifecycleRealTests.open
    accepted=life.LifecycleRealTests.accepted
    rejected=life.LifecycleRealTests.rejected
    close_writer=life.LifecycleRealTests.close_writer

    async def test_offline_period_end_closes_only_identity_until_resync(self):
        r=self.server.registry; r.assign_plan(self.uid,{'plan_id':test_accounts.plan(r,rate=None)})
        other=r.save('users',{'name':'other'})['id']; other_uuid=r.authorize_node_user(self.nid,other)['uuid']
        end=time.time()+1.5; m=UsageCycles(r)
        m.create(self.uid,dict(starts_at=end-10,ends_at=end))
        m.create(self.uid,dict(starts_at=end,ends_at=end+100))
        await self.runtime.apply(r.configuration(self.token))
        reader,_=await self.open(); await self.accepted(reader)
        unrelated,writer=await self.open(other_uuid); await self.accepted(unrelated)
        await asyncio.sleep(max(0,end-time.time())+.15)
        await self.rejected(reader)
        denied,_=await self.open(); await self.rejected(denied)
        writer.write(b'live'); await writer.drain()
        self.assertEqual(await asyncio.wait_for(unrelated.readexactly(4),2),b'live')
        await self.runtime.apply(r.configuration(self.token))
        reader,_=await self.open(); await self.accepted(reader)


class DeadlineWakeTests(unittest.IsolatedAsyncioTestCase):
    async def test_early_wakeup_rechecks_identity_deadline(self):
        from unittest.mock import AsyncMock
        from vless_runtime import VlessRuntime
        runtime=VlessRuntime.__new__(VlessRuntime)
        runtime.deadlines={1:1.0}; runtime.task_users={}
        runtime.lifecycle=asyncio.Lock(); runtime.expiry_task=None
        with patch('vless_runtime.time.time',side_effect=[0,.5,1,1]), patch('vless_runtime.asyncio.sleep',new_callable=AsyncMock) as sleep:
            await runtime._expire(1)
            self.assertEqual(sleep.await_count,2)

    async def test_legacy_and_reality_do_not_close_on_early_wakeup(self):
        from unittest.mock import AsyncMock
        from vless_runtime import VlessRuntime
        from reality_runtime import RealityRuntime
        for cls,module in ((VlessRuntime,'vless_runtime'),(RealityRuntime,'reality_runtime')):
            runtime=cls.__new__(cls); runtime.deadlines={}; runtime.close=AsyncMock()
            with patch(module+'.time.time',side_effect=[0,.5,1]), patch(module+'.asyncio.sleep',new_callable=AsyncMock) as sleep:
                await runtime._expire(1)
                self.assertEqual(sleep.await_count,2)
                runtime.close.assert_awaited_once()
