import asyncio
import concurrent.futures
import json
import sqlite3
import time
import unittest
import urllib.error
import urllib.request
from accounts import Accounts
from core import Registry,Error
from plan_lifecycle import Lifecycle
from plan_order import PlanOrder
from plan_renewals import Renewals
import test_accounts
import test_core
import test_plan_lifecycle
import test_xray


class PlanOrderTests(unittest.TestCase):
    setUp=test_core.RegistryTests.setUp
    tearDown=test_core.RegistryTests.tearDown
    fixtures=test_accounts.AccountTests.fixtures
    setup_plan=test_plan_lifecycle.LifecycleTests.setup_plan
    def family(self):
        r,uid,nid,token,pid,auth=self.setup_plan()
        child=Accounts(r).create(uid,dict(name='second',plan_id=test_accounts.plan(r,'second'),node_ids=[nid]))['user_id']
        return r,uid,nid,token,pid,auth,child

    def test_default_legacy_first_and_persist_primary_is_not_main_credentials(self):
        r,uid,nid,token,pid,auth,child=self.family(); m=PlanOrder(r)
        initial=m.manage(uid); self.assertEqual(initial['primary_user_id'],uid)
        self.assertEqual([x['user_id'] for x in initial['items']],[uid,child])
        saved=m.manage(uid,dict(user_ids=[child,uid]))
        self.assertEqual(saved['primary_user_id'],child)
        self.assertFalse(saved['items'][0]['is_main_account']); self.assertTrue(saved['items'][1]['is_main_account'])
        self.assertTrue(saved['items'][0]['is_primary']); self.assertFalse(saved['items'][1]['is_primary'])
        self.assertEqual(PlanOrder(Registry(r.path)).manage(uid),saved)
        self.assertEqual(m.manage(uid,dict(user_ids=[child,uid])),saved)

    def test_order_does_not_change_uuid_expiry_limits_renewal_or_agent_snapshot(self):
        r,uid,nid,token,pid,auth,child=self.family(); end=time.time()+1000
        Lifecycle(r).manage(uid,dict(enabled=True,expires_at=end))
        Lifecycle(r).manage(child,dict(enabled=False,expires_at=end+10))
        pending=Renewals(r).create(uid,dict(expires_at=end+1000))
        before=r.configuration(token); policies={u:(r.bandwidth_policy(u),r.connections_policy(u)) for u in (uid,child)}
        PlanOrder(r).manage(uid,dict(user_ids=[child,uid]))
        self.assertEqual(r.configuration(token),before)
        self.assertEqual({u:(r.bandwidth_policy(u),r.connections_policy(u)) for u in (uid,child)},policies)
        self.assertEqual(Lifecycle(r).manage(uid)['expires_at'],end)
        self.assertEqual(Lifecycle(r).manage(child)['status'],'disabled')
        self.assertEqual(Renewals(r).list(uid),[pending])

    def test_deleted_primary_falls_back_new_identity_appends_and_no_other_parent_leak(self):
        r,uid,nid,token,pid,auth,child=self.family(); m=PlanOrder(r)
        m.manage(uid,dict(user_ids=[child,uid])); Accounts(r).delete(uid,child)
        self.assertEqual(m.manage(uid)['primary_user_id'],uid)
        new=Accounts(r).create(uid,dict(name='third',plan_id=test_accounts.plan(r,'third'),node_ids=[]))['user_id']
        self.assertEqual([x['user_id'] for x in m.manage(uid)['items']],[uid,new])
        other=r.save('users',dict(name='other'))['id']; r.assign_plan(other,dict(plan_id=pid))
        with self.assertRaises(Error) as caught: m.manage(uid,dict(user_ids=[other,uid,new]))
        self.assertEqual(caught.exception.status,409)
        self.assertEqual(m.manage(other)['primary_user_id'],other)

    def test_unbind_main_keeps_child_selected_and_empty_family_works(self):
        r,uid,nid,token,pid,auth,child=self.family(); m=PlanOrder(r)
        m.manage(uid,dict(user_ids=[uid,child])); r.assign_plan(uid,dict(plan_id=None))
        self.assertEqual(m.manage(uid)['primary_user_id'],child)
        m.manage(uid,dict(user_ids=[child])); Accounts(r).delete(uid,child)
        self.assertEqual(m.manage(uid),dict(parent_id=uid,primary_user_id=None,items=[]))
        self.assertEqual(m.manage(uid,dict(user_ids=[]))['items'],[])
        r.assign_plan(uid,dict(plan_id=pid)); self.assertEqual(m.manage(uid)['primary_user_id'],uid)

    def test_invalid_stale_incomplete_child_parent_and_unknown_inputs(self):
        r,uid,nid,token,pid,auth,child=self.family(); m=PlanOrder(r); before=m.manage(uid)
        for data in ([],{},dict(user_ids=None),dict(user_ids=[True,child]),dict(user_ids=[uid,uid]),dict(user_ids=[uid]),dict(user_ids=[uid,-1]),dict(user_ids=[uid,999]),dict(user_ids=[uid,child],primary=uid)):
            with self.assertRaises(Error): m.manage(uid,data)
        with self.assertRaises(Error): m.manage(child)
        with self.assertRaises(Error): m.manage(999)
        self.assertEqual(m.manage(uid),before)

    def test_failed_reorder_rolls_back_previous_order(self):
        r,uid,nid,token,pid,auth,child=self.family(); m=PlanOrder(r)
        before=m.manage(uid,dict(user_ids=[uid,child]))
        with r.connect() as db: db.execute("CREATE TRIGGER fail_order BEFORE INSERT ON plan_order BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): m.manage(uid,dict(user_ids=[child,uid]))
        self.assertEqual(m.manage(uid),before)

    def test_concurrent_reorders_commit_whole_permutations(self):
        r,uid,nid,token,pid,auth,child=self.family(); m=PlanOrder(r)
        choices=[[uid,child],[child,uid]]
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            replies=list(pool.map(lambda ids:m.manage(uid,dict(user_ids=ids)),choices))
        self.assertEqual([[item['user_id'] for item in reply['items']] for reply in replies],choices)
        self.assertIn([item['user_id'] for item in m.manage(uid)['items']],choices)

    def test_old_database_upgrade_and_user_cleanup(self):
        r,uid,nid,token,pid,auth,child=self.family()
        with r.connect() as db: db.execute('DROP TABLE plan_order')
        r=Registry(r.path); PlanOrder(r).manage(uid,dict(user_ids=[child,uid]))
        Accounts(r).delete(uid,child); r.delete('users',uid)
        with r.connect() as db: self.assertEqual(db.execute('SELECT count(*) FROM plan_order').fetchone()[0],0)


class PlanOrderApiTests(unittest.TestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown
    request=test_core.ApiTests.request
    def test_admin_api_and_agent_anonymous_cannot_reorder(self):
        r=self.server.registry; uid=r.save('users',dict(name='parent'))['id']; r.assign_plan(uid,dict(plan_id=test_accounts.plan(r)))
        path=f'/api/users/{uid}/plan-order'
        before=json.load(self.request(path)); self.assertEqual(json.load(self.request(path,'PUT',dict(user_ids=[uid]))),before)
        for data in (None,dict(user_ids=[])):
            with self.assertRaises(urllib.error.HTTPError): self.request(path,'PUT',data)
        with self.assertRaises(urllib.error.HTTPError) as caught: self.request(path,auth=False)
        self.assertEqual(caught.exception.code,401)
        sid=r.save('servers',dict(name='host'))['id']; token=r.provision_agent(sid)['token']
        req=urllib.request.Request(self.base+path,data=json.dumps(dict(user_ids=[uid])).encode(),headers={'Authorization':'Bearer '+token},method='PUT')
        with self.assertRaises(urllib.error.HTTPError) as caught: self.opener.open(req,timeout=3)
        self.assertEqual(caught.exception.code,401)


@unittest.skipUnless(test_xray.BINARY.is_file(),'Set XRAY_BINARY')
class PlanOrderRealTests(unittest.IsolatedAsyncioTestCase):
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
    async def test_primary_change_does_not_disconnect_existing_parent_or_child(self):
        r=self.server.registry; r.assign_plan(self.uid,dict(plan_id=test_accounts.plan(r,rate=None,maximum=2)))
        child=Accounts(r).create(self.uid,dict(name='second',plan_id=test_accounts.plan(r,'child',rate=None,maximum=2),node_ids=[self.nid]))
        await self.sync(); before=r.configuration(self.token)
        parent_reader,parent_writer=await self.open(); await self.accepted(parent_reader)
        child_reader,child_writer=await self.open(child['nodes'][0]['uuid']); await self.accepted(child_reader)
        PlanOrder(r).manage(self.uid,dict(user_ids=[child['user_id'],self.uid])); await self.sync()
        self.assertEqual(r.configuration(self.token),before)
        for reader,writer in ((parent_reader,parent_writer),(child_reader,child_writer)):
            writer.write(b'kept'); await writer.drain()
            self.assertEqual(await asyncio.wait_for(reader.readexactly(4),2),b'kept')
