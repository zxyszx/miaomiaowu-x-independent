import asyncio
import json
import sqlite3
import time
import unittest
import urllib.error
from unittest.mock import patch
from core import Registry,Error
from accounts import Accounts
from plan_lifecycle import Lifecycle
import test_core
import test_accounts
import test_xray
import test_user_connections
import test_reality
from execution_agent import cycle


def rule(end=None,enabled=True): return dict(enabled=enabled,expires_at=end)


class LifecycleTests(unittest.TestCase):
    setUp=test_core.RegistryTests.setUp
    tearDown=test_core.RegistryTests.tearDown
    fixtures=test_accounts.AccountTests.fixtures

    def setup_plan(self):
        r,uid,nid,token=self.fixtures(); pid=test_accounts.plan(r)
        r.assign_plan(uid,{'plan_id':pid}); auth=r.authorize_node_user(nid,uid)
        return r,uid,nid,token,pid,auth

    def test_persist_disable_renew_keeps_uuid_and_other_child(self):
        r,uid,nid,token,pid,auth=self.setup_plan(); m=Lifecycle(r)
        child=Accounts(r).create(uid,dict(name='second',plan_id=test_accounts.plan(r,'two'),node_ids=[nid]))
        end=time.time()+3600; before=r.configuration(token)
        self.assertEqual(m.manage(uid,rule(end))['status'],'active')
        active=r.configuration(token); self.assertEqual(active['nodes'][0]['clients'][0]['valid_until'],end)
        m.manage(uid,rule(end)); self.assertEqual(r.configuration(token),active)
        self.assertEqual(Lifecycle(Registry(r.path)).manage(uid)['expires_at'],end)
        m.manage(uid,rule(end,False)); clients=r.configuration(token)['nodes'][0]['clients']
        self.assertEqual([c['user_id'] for c in clients],[child['user_id']])
        m.manage(uid,rule()); clients=r.configuration(token)['nodes'][0]['clients']
        self.assertEqual(clients[0]['uuid'],auth['uuid']); self.assertNotIn('valid_until',clients[0])
        self.assertGreater(r.configuration(token)['revision'],before['revision'])

    def test_expiry_bumps_once_renew_restores_and_clock_rollback_does_not_reactivate(self):
        r,uid,nid,token,pid,auth=self.setup_plan(); m=Lifecycle(r); now=time.time()
        m.manage(uid,rule(now+100)); before=r.configuration(token)
        with patch('plan_lifecycle.time.time',return_value=now+101):
            after=r.configuration(token); self.assertEqual(after['nodes'][0]['clients'],[])
            self.assertEqual(after['revision'],before['revision']+1)
            self.assertEqual(r.configuration(token),after)
            self.assertEqual(m.manage(uid)['status'],'expired')
        # Expiry is sticky until explicit renewal, including after a clock rollback.
        self.assertEqual(r.configuration(token)['nodes'][0]['clients'],[])
        m.manage(uid,rule(now+200)); self.assertEqual(r.configuration(token)['nodes'][0]['clients'][0]['uuid'],auth['uuid'])

    def test_switch_and_unbind_clear_old_validity_but_idempotent_bind_preserves(self):
        r,uid,nid,token,pid,auth=self.setup_plan(); m=Lifecycle(r)
        m.manage(uid,rule(enabled=False)); r.assign_plan(uid,{'plan_id':pid})
        self.assertEqual(m.manage(uid)['status'],'disabled')
        r.assign_plan(uid,{'plan_id':test_accounts.plan(r,'new')})
        self.assertEqual(m.manage(uid)['status'],'active'); self.assertEqual(r.configuration(token)['nodes'][0]['clients'][0]['uuid'],auth['uuid'])
        m.manage(uid,rule(enabled=False)); r.assign_plan(uid,{'plan_id':None})
        self.assertEqual(r.configuration(token)['nodes'][0]['clients'][0]['uuid'],auth['uuid'])
        with self.assertRaises(Error): m.manage(uid)

    def test_invalid_missing_identity_and_atomic_rollback(self):
        r,uid,nid,token,pid,auth=self.setup_plan(); m=Lifecycle(r)
        for bad in ({},[],rule(True),rule(float('nan')),rule(float('inf')),rule(-1),rule(253402300800),rule(10**1000),rule('1'),rule(None,1)):
            with self.assertRaises(Error): m.manage(uid,bad)
        with self.assertRaises(Error): m.manage(999,rule())
        before=r.configuration(token)
        with r.connect() as db: db.execute("CREATE TRIGGER fail_life BEFORE UPDATE ON server_configs BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): m.manage(uid,rule(enabled=False))
        self.assertEqual(m.manage(uid)['status'],'active'); self.assertEqual(r.configuration(token),before)

    def test_batch_expiry_one_revision_per_host_and_atomic_rollback(self):
        r,uid,nid,token,pid,auth=self.setup_plan(); m=Lifecycle(r); now=time.time()
        other=r.save('users',{'name':'other'})['id']; r.assign_plan(other,{'plan_id':pid}); r.authorize_node_user(nid,other)
        for user in (uid,other): m.manage(user,rule(now+100))
        before=r.configuration(token)
        with r.connect() as db: db.execute("CREATE TRIGGER fail_expiry BEFORE UPDATE ON server_configs BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with patch('plan_lifecycle.time.time',return_value=now+101):
            with self.assertRaises(sqlite3.IntegrityError): r.configuration(token)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT sum(active) FROM plan_lifecycle').fetchone()[0],2)
            db.execute('DROP TRIGGER fail_expiry')
        with patch('plan_lifecycle.time.time',return_value=now+101):
            after=r.configuration(token); self.assertEqual(after['revision'],before['revision']+1)
            self.assertEqual(after['nodes'][0]['clients'],[])

    def test_old_database_upgrade_and_delete_cascade(self):
        r,uid,nid,token,pid,auth=self.setup_plan()
        with r.connect() as db: db.execute('DROP TABLE plan_lifecycle')
        r=Registry(r.path); Lifecycle(r).manage(uid,rule(enabled=False)); r.delete('users',uid)
        with r.connect() as db: self.assertEqual(db.execute('SELECT COUNT(*) FROM plan_lifecycle').fetchone()[0],0)


class LifecycleApiTests(unittest.TestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown
    request=test_core.ApiTests.request

    def test_admin_api_and_agent_cannot_edit(self):
        r=self.server.registry; uid=r.save('users',{'name':'test'})['id']; r.assign_plan(uid,{'plan_id':test_accounts.plan(r)})
        path=f'/api/users/{uid}/plan-lifecycle'
        self.assertEqual(json.load(self.request(path))['status'],'active')
        self.assertEqual(json.load(self.request(path,'PUT',rule(enabled=False)))['status'],'disabled')
        for data in (None,{},rule(0)):
            with self.assertRaises(urllib.error.HTTPError): self.request(path,'PUT',data)
        with self.assertRaises(urllib.error.HTTPError) as caught: self.request(path,auth=False)
        self.assertEqual(caught.exception.code,401)
        sid=r.save('servers',{'name':'host'})['id']; agent=r.provision_agent(sid)['token']
        request=urllib.request.Request(self.base+path,headers={'Authorization':'Bearer '+agent})
        with self.assertRaises(urllib.error.HTTPError) as caught: self.opener.open(request,timeout=3)
        self.assertEqual(caught.exception.code,401)


@unittest.skipUnless(test_xray.BINARY.is_file(),'Set XRAY_BINARY')
class LifecycleRealTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=test_user_connections.ConnectionRealTests.asyncSetUp
    asyncTearDown=test_user_connections.ConnectionRealTests.asyncTearDown
    open=test_user_connections.ConnectionRealTests.open
    accepted=test_user_connections.ConnectionRealTests.accepted
    rejected=test_user_connections.ConnectionRealTests.rejected
    setUp=test_user_connections.ConnectionRealTests.setUp
    tearDown=test_user_connections.ConnectionRealTests.tearDown
    request=test_user_connections.ConnectionRealTests.request
    close_writer=test_user_connections.ConnectionRealTests.close_writer
    async def sync(self): await self.runtime.apply(self.server.registry.configuration(self.token))

    async def test_offline_expiry_closes_live_user_then_renewal_restores_credential(self):
        r=self.server.registry; r.assign_plan(self.uid,{'plan_id':test_accounts.plan(r,rate=None)})
        other=r.save('users',{'name':'unrelated'})['id']; credential=r.authorize_node_user(self.nid,other)['uuid']
        end=time.time()+1.5; Lifecycle(r).manage(self.uid,rule(end)); await self.sync()
        independent,writer=await self.open(credential); await self.accepted(independent)
        reader,_=await self.open(); await self.accepted(reader)
        await asyncio.sleep(max(0,end-time.time())+.15)
        await self.rejected(reader); denied,_=await self.open(); await self.rejected(denied)
        writer.write(b'live'); await writer.drain(); self.assertEqual(await asyncio.wait_for(independent.readexactly(4),2),b'live')
        Lifecycle(r).manage(self.uid,rule()); await self.sync(); reader,_=await self.open(); await self.accepted(reader)


@unittest.skipUnless(test_xray.BINARY.is_file(),'Set XRAY_BINARY')
class LifecycleRealityTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=test_reality.RealityRealTests.asyncSetUp
    asyncTearDown=test_reality.RealityRealTests.asyncTearDown
    echo=test_reality.RealityRealTests.echo
    client=test_reality.RealityRealTests.client
    async def test_disabled_plan_closes_encrypted_session_and_renewal_restores(self):
        r=self.registry; r.assign_plan(self.uid,{'plan_id':test_accounts.plan(r,rate=None)})
        await self.runtime.apply(r.configuration(self.token))
        reader,writer=await self.client()
        writer.write(b'hello'); await writer.drain(); self.assertEqual(await asyncio.wait_for(reader.readexactly(5),2),b'hello')
        Lifecycle(r).manage(self.uid,rule(enabled=False)); await self.runtime.apply(r.configuration(self.token))
        self.assertEqual(await asyncio.wait_for(reader.read(1),2),b'')
        writer.close(); await writer.wait_closed()
        Lifecycle(r).manage(self.uid,rule()); await self.runtime.apply(r.configuration(self.token))
        reader,writer=await self.client()
        writer.write(b'new'); await writer.drain(); self.assertEqual(await asyncio.wait_for(reader.readexactly(3),2),b'new')
        writer.close(); await writer.wait_closed()
