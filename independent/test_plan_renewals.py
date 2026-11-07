import asyncio
import concurrent.futures
from contextlib import contextmanager
import json
import sqlite3
import time
import threading
from types import SimpleNamespace
import unittest
import urllib.error
import urllib.request
from unittest.mock import patch
from core import Registry,Error
from plan_lifecycle import Lifecycle
from plan_renewals import Renewals
import test_accounts
import test_core
import test_plan_lifecycle
import test_xray
import test_reality
from accounts import Accounts


class RenewalTests(unittest.TestCase):
    setUp=test_core.RegistryTests.setUp
    tearDown=test_core.RegistryTests.tearDown
    fixtures=test_accounts.AccountTests.fixtures
    setup_plan=test_plan_lifecycle.LifecycleTests.setup_plan

    def pending(self,enabled=True):
        r,uid,nid,token,pid,auth=self.setup_plan()
        end=time.time()+1000; Lifecycle(r).manage(uid,dict(enabled=enabled,expires_at=end))
        item=Renewals(r).create(uid,dict(expires_at=end+1000))
        return r,uid,nid,token,pid,auth,end,item

    def test_create_persist_no_execution_change_approve_once_preserves_uuid(self):
        r,uid,nid,token,pid,auth=self.setup_plan(); end=time.time()+1000
        Lifecycle(r).manage(uid,dict(enabled=True,expires_at=end)); before=r.configuration(token)
        m=Renewals(r); item=m.create(uid,dict(expires_at=end+1000))
        self.assertEqual(r.configuration(token),before)
        self.assertEqual(Renewals(Registry(r.path)).list(uid),[item])
        approved=m.decide(item['id'],{'decision':'approve'}); after=r.configuration(token)
        self.assertEqual(approved['status'],'approved'); self.assertIsNotNone(approved['decided_at'])
        self.assertEqual(after['revision'],before['revision']+1)
        client=after['nodes'][0]['clients'][0]; self.assertEqual(client['uuid'],auth['uuid'])
        self.assertEqual(client['valid_until'],end+1000)
        self.assertEqual(m.decide(item['id'],{'decision':'approve'}),approved)
        self.assertEqual(r.configuration(token),after)
        with self.assertRaises(Error): m.decide(item['id'],{'decision':'reject'})

    def test_reject_duplicate_pending_and_new_request(self):
        r,uid,nid,token,pid,auth,end,item=self.pending(); m=Renewals(r); before=r.configuration(token)
        with self.assertRaises(Error) as caught: m.create(uid,dict(expires_at=end+2000))
        self.assertEqual(caught.exception.status,409)
        rejected=m.decide(item['id'],{'decision':'reject'})
        self.assertEqual(m.decide(item['id'],{'decision':'reject'}),rejected)
        self.assertEqual(r.configuration(token),before)
        self.assertGreater(m.create(uid,dict(expires_at=end+2000))['id'],item['id'])

    def test_disabled_identity_is_not_enabled_by_approval(self):
        r,uid,nid,token,pid,auth,end,item=self.pending(False)
        Renewals(r).decide(item['id'],{'decision':'approve'})
        state=Lifecycle(r).manage(uid); self.assertEqual(state['status'],'disabled')
        self.assertEqual(state['expires_at'],end+1000); self.assertEqual(r.configuration(token)['nodes'][0]['clients'],[])

    def test_expiry_during_review_does_not_invalidate_valid_extension(self):
        r,uid,nid,token,pid,auth,end,item=self.pending(); m=Renewals(r)
        with patch('plan_lifecycle.time.time',return_value=end+1):
            self.assertEqual(r.configuration(token)['nodes'][0]['clients'],[])
            m.decide(item['id'],{'decision':'approve'})
            self.assertEqual(r.configuration(token)['nodes'][0]['clients'][0]['uuid'],auth['uuid'])

    def test_manual_edit_and_binding_round_trip_invalidate_old_requests(self):
        r,uid,nid,token,pid,auth,end,item=self.pending(); m=Renewals(r)
        # Same value saves are safe, but changing away and back cannot revive a pending request.
        Lifecycle(r).manage(uid,dict(enabled=True,expires_at=end))
        self.assertEqual(m.list(uid)[0]['status'],'pending')
        Lifecycle(r).manage(uid,dict(enabled=False,expires_at=end))
        Lifecycle(r).manage(uid,dict(enabled=True,expires_at=end))
        with self.assertRaises(Error): m.decide(item['id'],{'decision':'approve'})
        item=m.create(uid,dict(expires_at=end+1000))
        new=test_accounts.plan(r,'new'); r.assign_plan(uid,{'plan_id':new}); r.assign_plan(uid,{'plan_id':pid})
        Lifecycle(r).manage(uid,dict(enabled=True,expires_at=end))
        with self.assertRaises(Error): m.decide(item['id'],{'decision':'approve'})
        self.assertEqual(m.list(uid)[0]['status'],'invalidated')

    def test_approval_rollback_on_execution_revision_failure(self):
        r,uid,nid,token,pid,auth,end,item=self.pending(); before=r.configuration(token)
        with r.connect() as db: db.execute("CREATE TRIGGER fail_renew BEFORE UPDATE ON server_configs BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): Renewals(r).decide(item['id'],{'decision':'approve'})
        self.assertEqual(Renewals(r).list(uid)[0]['status'],'pending')
        self.assertEqual(r.configuration(token),before)
        with r.connect() as db: db.execute('DROP TRIGGER fail_renew')
        self.assertEqual(Renewals(r).decide(item['id'],{'decision':'approve'})['status'],'approved')

    def test_invalid_input_expired_request_unbound_and_delete_cleanup(self):
        r,uid,nid,token,pid,auth,end,item=self.pending(); m=Renewals(r)
        for data in (None,[],{},dict(expires_at=None),dict(expires_at=True),dict(expires_at=float('nan')),dict(expires_at=10**1000),dict(expires_at=end-1),dict(expires_at=1)):
            with self.assertRaises(Error): m.create(uid,data)
        for data in (None,[],{},dict(decision='bad'),dict(decision='approve',user_id=uid)):
            with self.assertRaises(Error): m.decide(item['id'],data)
        with patch('plan_renewals.time.time',return_value=end+2000):
            with self.assertRaises(Error): m.decide(item['id'],dict(decision='approve'))
        other=r.save('users',{'name':'other'})['id']
        with self.assertRaises(Error): m.create(other,dict(expires_at=end+1000))
        r.assign_plan(other,{'plan_id':pid})
        with self.assertRaises(Error): m.create(other,dict(expires_at=end+1000))
        with self.assertRaises(Error): m.list(999)
        r.delete('users',uid)
        with r.connect() as db: self.assertEqual(db.execute('SELECT count(*) FROM plan_renewals').fetchone()[0],0)

    def test_child_approval_isolated_and_old_database_upgrade(self):
        r,uid,nid,token,pid,auth,end,item=self.pending()
        child=Accounts(r).create(uid,dict(name='second',plan_id=test_accounts.plan(r,'child'),node_ids=[nid]))
        cid=child['user_id']; Lifecycle(r).manage(cid,dict(enabled=True,expires_at=end))
        child_request=Renewals(r).create(cid,dict(expires_at=end+3000))
        Renewals(r).decide(child_request['id'],dict(decision='approve'))
        self.assertEqual(Lifecycle(r).manage(uid)['expires_at'],end)
        self.assertEqual(Lifecycle(r).manage(cid)['expires_at'],end+3000)
        self.assertEqual(Renewals(r).list(uid)[0]['status'],'pending')
        Accounts(r).delete(uid,cid)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM plan_renewals WHERE user_id=?',(cid,)).fetchone()[0],0)
            db.execute('DROP TABLE plan_renewals')
        self.assertEqual(Renewals(Registry(r.path)).list(uid),[])
        self.assertEqual(Lifecycle(r).manage(uid)['expires_at'],end)

    def test_concurrent_create_and_approval_are_serialized(self):
        r,uid,nid,token,pid,auth,end,item=self.pending(); m=Renewals(r)
        before=r.configuration(token)
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            replies=list(pool.map(lambda _:m.decide(item['id'],dict(decision='approve')),range(2)))
        self.assertEqual(replies[0],replies[1]); self.assertEqual(r.configuration(token)['revision'],before['revision']+1)
        def create(_):
            try: return m.create(uid,dict(expires_at=end+3000))['status']
            except Error as error: return error.status
        with concurrent.futures.ThreadPoolExecutor(2) as pool: results=list(pool.map(create,range(2)))
        self.assertCountEqual(results,['pending',409])

    def deadline_while_waiting(self, approve):
        r,uid,nid,token,pid,auth,end,item=self.pending()
        m=Renewals(r)
        if not approve: m.decide(item['id'],dict(decision='reject'))
        before=r.configuration(token); connect=r.connect
        waiting=threading.Event(); clock=[end+10]
        class Connection:
            def __init__(self,db): self.db=db
            def execute(self,sql,*args):
                if sql=='BEGIN IMMEDIATE': waiting.set()
                return self.db.execute(sql,*args)
            def __getattr__(self,name): return getattr(self.db,name)
        @contextmanager
        def delayed():
            with connect() as db: yield Connection(db)
        with connect() as blocker:
            blocker.execute('BEGIN IMMEDIATE')
            with patch.object(r,'connect',delayed), patch('plan_renewals.time',SimpleNamespace(time=lambda:clock[0])):
                with concurrent.futures.ThreadPoolExecutor(1) as pool:
                    if approve:
                        future=pool.submit(m.decide,item['id'],dict(decision='approve'))
                    else:
                        future=pool.submit(m.create,uid,dict(expires_at=end+1000))
                    try:
                        self.assertTrue(waiting.wait(3),'writer did not reach transaction lock')
                        clock[0]=end+1001
                    finally: blocker.commit()
                    with self.assertRaises(Error): future.result(timeout=5)
        self.assertEqual(r.configuration(token),before)
        self.assertEqual(m.list(uid)[0]['status'],'pending' if approve else 'rejected')
        self.assertEqual(len(m.list(uid)),1)

    def test_create_rechecks_deadline_after_real_sqlite_lock_wait(self):
        self.deadline_while_waiting(False)

    def test_approval_rechecks_deadline_after_real_sqlite_lock_wait(self):
        self.deadline_while_waiting(True)


class RenewalApiTests(unittest.TestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown
    request=test_core.ApiTests.request
    def test_no_self_service_via_anonymous_agent_or_forged_principal(self):
        r=self.server.registry; uid=r.save('users',{'name':'one'})['id']
        r.assign_plan(uid,{'plan_id':test_accounts.plan(r)})
        end=time.time()+1000; Lifecycle(r).manage(uid,dict(enabled=True,expires_at=end))
        path=f'/api/users/{uid}/plan-renewals'; before=Lifecycle(r).manage(uid)
        sid=r.save('servers',{'name':'agent'})['id']; token=r.provision_agent(sid)['token']
        for headers in ({},{'Authorization':'Bearer '+token}):
            for method in ('GET','POST'):
                req=urllib.request.Request(self.base+path,
                    data=json.dumps(dict(expires_at=end+1000)).encode() if method=='POST' else None,
                    headers=headers,method=method)
                with self.assertRaises(urllib.error.HTTPError) as caught: self.opener.open(req,timeout=3)
                self.assertEqual(caught.exception.code,401)
        for extra in ('user_id','plan_id','decision','enabled'):
            with self.assertRaises(urllib.error.HTTPError) as caught:
                self.request(path,'POST',dict(expires_at=end+1000,**{extra:uid}))
            self.assertEqual(caught.exception.code,400)
        self.assertEqual(Renewals(r).list(uid),[])
        self.assertEqual(Lifecycle(r).manage(uid),before)

    def test_admin_api_no_agent_or_anonymous_approval(self):
        r=self.server.registry; uid=r.save('users',{'name':'one'})['id']; r.assign_plan(uid,{'plan_id':test_accounts.plan(r)})
        end=time.time()+1000; Lifecycle(r).manage(uid,dict(enabled=True,expires_at=end))
        path=f'/api/users/{uid}/plan-renewals'; item=json.load(self.request(path,'POST',dict(expires_at=end+1000)))
        self.assertEqual(json.load(self.request(path)),[item]); decision=f"/api/plan-renewals/{item['id']}/decision"
        with self.assertRaises(urllib.error.HTTPError) as caught: self.request(decision,'POST',dict(decision='approve'),auth=False)
        self.assertEqual(caught.exception.code,401)
        sid=r.save('servers',{'name':'agent'})['id']; token=r.provision_agent(sid)['token']
        req=urllib.request.Request(self.base+decision,data=json.dumps(dict(decision='approve')).encode(),headers={'Authorization':'Bearer '+token},method='POST')
        with self.assertRaises(urllib.error.HTTPError) as caught: self.opener.open(req,timeout=3)
        self.assertEqual(caught.exception.code,401)
        self.assertEqual(json.load(self.request(decision,'POST',dict(decision='approve')))['status'],'approved')


@unittest.skipUnless(test_xray.BINARY.is_file(),'Set XRAY_BINARY')
class RenewalRealTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=test_plan_lifecycle.LifecycleRealTests.asyncSetUp
    asyncTearDown=test_plan_lifecycle.LifecycleRealTests.asyncTearDown
    setUp=test_plan_lifecycle.LifecycleRealTests.setUp
    tearDown=test_plan_lifecycle.LifecycleRealTests.tearDown
    request=test_plan_lifecycle.LifecycleRealTests.request
    open=test_plan_lifecycle.LifecycleRealTests.open
    accepted=test_plan_lifecycle.LifecycleRealTests.accepted
    rejected=test_plan_lifecycle.LifecycleRealTests.rejected
    close_writer=test_plan_lifecycle.LifecycleRealTests.close_writer
    sync=test_plan_lifecycle.LifecycleRealTests.sync
    async def test_expired_user_stays_blocked_until_review_then_original_credential_works(self):
        r=self.server.registry; r.assign_plan(self.uid,{'plan_id':test_accounts.plan(r,rate=None)})
        Lifecycle(r).manage(self.uid,dict(enabled=True,expires_at=time.time()-10)); await self.sync()
        reader,_=await self.open(); await self.rejected(reader)
        m=Renewals(r); item=m.create(self.uid,dict(expires_at=time.time()+1000)); await self.sync()
        reader,_=await self.open(); await self.rejected(reader)
        m.decide(item['id'],dict(decision='approve')); await self.sync()
        reader,_=await self.open(); await self.accepted(reader)


@unittest.skipUnless(test_xray.BINARY.is_file(),'Set XRAY_BINARY')
class RenewalRealityTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=test_reality.RealityRealTests.asyncSetUp
    asyncTearDown=test_reality.RealityRealTests.asyncTearDown
    echo=test_reality.RealityRealTests.echo
    client=test_reality.RealityRealTests.client
    async def test_approved_renewal_restores_real_encrypted_traffic(self):
        r=self.registry; r.assign_plan(self.uid,{'plan_id':test_accounts.plan(r,rate=None)})
        Lifecycle(r).manage(self.uid,dict(enabled=True,expires_at=time.time()-10))
        await self.runtime.apply(r.configuration(self.token))
        self.assertEqual(r.configuration(self.token)['nodes'][0]['clients'],[])
        m=Renewals(r); item=m.create(self.uid,dict(expires_at=time.time()+1000))
        self.assertEqual(r.configuration(self.token)['nodes'][0]['clients'],[])
        m.decide(item['id'],dict(decision='approve'))
        await self.runtime.apply(r.configuration(self.token))
        reader,writer=await self.client()
        writer.write(b'approved'); await writer.drain()
        self.assertEqual(await asyncio.wait_for(reader.readexactly(8),2),b'approved')
        writer.close(); await writer.wait_closed()
