"""Cumulative report replay, server/session fencing and independent identities."""
import concurrent.futures
import json
import sqlite3
import unittest
import urllib.error
import urllib.request
import uuid
from core import Registry, Error
from usage_ledger import UsageLedger, MAX_COUNTER
from accounts import Accounts
import test_accounts
import test_core


class UsageTests(unittest.TestCase):
    setUp=test_core.RegistryTests.setUp
    tearDown=test_core.RegistryTests.tearDown
    fixtures=test_accounts.AccountTests.fixtures

    def fixture(self):
        r,uid,nid,token=self.fixtures()
        credential=r.authorize_node_user(nid,uid)['uuid']
        session=str(uuid.uuid4()); r.begin_session(token,dict(session_id=session))
        self.token=token; self.uid=uid; self.nid=nid; self.session=session
        self.item=dict(node_id=nid,user_id=uid,credential=credential,upload_bytes=10,download_bytes=20)
        self.ledger=UsageLedger(r)
        return r

    def report(self,sequence=1,items=None):
        return self.ledger.report(self.token,dict(session_id=self.session,sequence=sequence,items=[dict(self.item)] if items is None else items))

    def test_retry_new_sequence_and_registry_restart_do_not_double_count(self):
        r=self.fixture(); before=r.configuration(self.token)
        self.assertFalse(self.report()['duplicate']); self.assertTrue(self.report()['duplicate'])
        self.item['upload_bytes']=17; self.report(2)
        self.ledger=UsageLedger(Registry(r.path)); self.assertTrue(self.report(2)['duplicate'])
        total=self.ledger.user(self.uid)
        self.assertEqual((total['upload_bytes'],total['download_bytes']),(17,20))
        self.assertEqual(r.configuration(self.token),before)

    def test_retired_session_and_rotated_token_rejected_new_epoch_adds(self):
        r=self.fixture(); self.report(); old=self.session
        self.session=str(uuid.uuid4()); r.begin_session(self.token,dict(session_id=self.session))
        with self.assertRaises(Error) as caught:
            self.ledger.report(self.token,dict(session_id=old,sequence=2,items=[self.item]))
        self.assertEqual(caught.exception.status,403)
        self.item.update(upload_bytes=3,download_bytes=4); self.report()
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],13)
        with r.connect() as db: sid=db.execute('SELECT server_id FROM nodes WHERE id=?',(self.nid,)).fetchone()[0]
        previous=self.token; self.token=r.provision_agent(sid)['token']
        with self.assertRaises(Error): self.report(2)
        with self.assertRaises(Error): self.ledger.report(previous,dict(session_id=self.session,sequence=2,items=[self.item]))
        self.session=str(uuid.uuid4()); r.begin_session(self.token,dict(session_id=self.session)); self.report()
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],16)

    def test_cross_server_and_credential_spoof_rejected_without_partial_commit(self):
        r=self.fixture(); sid=r.save('servers',dict(name='other'))['id']
        nid=r.save('nodes',dict(name='foreign',server_id=sid))['id']
        r.configure_node(nid,test_accounts.test_xray.VLESS)
        auth=r.authorize_node_user(nid,self.uid)
        foreign=dict(self.item,node_id=nid,credential=auth['uuid'])
        for bad in (foreign,dict(self.item,credential=str(uuid.uuid4()))):
            with self.assertRaises(Error) as caught: self.report(items=[self.item,bad])
            self.assertEqual(caught.exception.status,403)
            self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],0)
        self.assertFalse(self.report()['duplicate'])

    def test_invalid_and_duplicate_items_cannot_mutate_ledger(self):
        r=self.fixture(); data=dict(session_id=self.session,sequence=1,items=[self.item])
        invalid=[None,{},dict(data,sequence=True),dict(data,sequence=MAX_COUNTER+1),dict(data,items=[]),dict(data,items=[self.item]*2),dict(data,items=[self.item]*65),dict(data,session_id=None)]
        for field,value in (('upload_bytes',True),('download_bytes',-1),('upload_bytes',1.1),('upload_bytes',MAX_COUNTER+1),('node_id',True),('credential','bad')):
            invalid.append(dict(data,items=[dict(self.item,**{field:value})]))
        for value in invalid:
            with self.subTest(data=value),self.assertRaises(Error): self.ledger.report(self.token,value)
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],0)
        self.report()

    def test_out_of_order_conflicts_and_cumulative_rollback_are_rejected(self):
        self.fixture(); self.report(4)
        self.item['upload_bytes']=11
        for seq in (3,4):
            with self.assertRaises(Error) as caught: self.report(seq)
            self.assertEqual(caught.exception.status,409)
        self.item['download_bytes']=19
        with self.assertRaises(Error): self.report(5)
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],10)
        self.item['download_bytes']=21; self.report(5)
        self.assertEqual(self.ledger.user(self.uid)['download_bytes'],21)

    def test_revoked_grant_rejects_new_report_and_reissued_uuid_is_new_counter(self):
        r=self.fixture(); self.report()
        r.authorize_node_user(self.nid,self.uid,revoke=True)
        self.assertTrue(self.report()['duplicate']) # acknowledged retry requires no current grant
        with self.assertRaises(Error): self.report(2)
        self.item['credential']=r.authorize_node_user(self.nid,self.uid)['uuid']
        self.item.update(upload_bytes=2,download_bytes=3); self.report(2)
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],12)

    def test_parent_child_and_multiple_servers_do_not_merge_identities(self):
        r=self.fixture(); r.assign_plan(self.uid,dict(plan_id=test_accounts.plan(r)))
        child=Accounts(r).create(self.uid,dict(name='child',plan_id=test_accounts.plan(r,'two'),node_ids=[self.nid]))
        other=dict(self.item,user_id=child['user_id'],credential=child['nodes'][0]['uuid'],upload_bytes=55)
        self.report(items=[self.item,other]); self.assertTrue(self.report(items=[other,self.item])['duplicate'])
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],10)
        self.assertEqual(self.ledger.user(child['user_id'])['upload_bytes'],55)
        sid=r.save('servers',dict(name='second'))['id']; nid=r.save('nodes',dict(name='second',server_id=sid))['id']
        r.configure_node(nid,test_accounts.test_xray.VLESS); token=r.provision_agent(sid)['token']
        auth=r.authorize_node_user(nid,self.uid); session=str(uuid.uuid4()); r.begin_session(token,dict(session_id=session))
        self.ledger.report(token,dict(session_id=session,sequence=1,items=[dict(self.item,node_id=nid,credential=auth['uuid'],upload_bytes=7)]))
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],17)
        self.assertEqual(len(self.ledger.user(self.uid)['nodes']),2)

    def test_concurrent_identical_reports_account_once(self):
        self.fixture()
        with concurrent.futures.ThreadPoolExecutor(4) as pool: replies=list(pool.map(lambda _:self.report(),range(4)))
        self.assertEqual(sum(not x['duplicate'] for x in replies),1)
        self.assertEqual(self.ledger.user(self.uid)['download_bytes'],20)

    def test_partial_batch_keeps_omitted_counter_and_checks_it_on_return(self):
        r=self.fixture(); other=r.save('users',dict(name='other'))['id']
        credential=r.authorize_node_user(self.nid,other)['uuid']
        item=dict(self.item,user_id=other,credential=credential,upload_bytes=30)
        self.report(items=[self.item,item]); self.item['upload_bytes']=20
        self.report(2)
        self.assertEqual(self.ledger.user(other)['upload_bytes'],30)
        item['upload_bytes']=29
        with self.assertRaises(Error): self.report(3,items=[item])
        item['upload_bytes']=31; self.report(3,items=[item])
        self.assertEqual(self.ledger.user(other)['upload_bytes'],31)
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],20)

    def test_late_batch_failure_rolls_back_earlier_counter_update(self):
        r=self.fixture(); other=r.save('users',dict(name='other'))['id']
        credential=r.authorize_node_user(self.nid,other)['uuid']
        item=dict(self.item,user_id=other,credential=credential)
        self.report(items=[self.item,item]); self.item['upload_bytes']=99; item['download_bytes']=19
        with self.assertRaises(Error): self.report(2,items=[self.item,item])
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],10)
        item['download_bytes']=21; self.report(2,items=[self.item,item])
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],99)

    def test_failed_cursor_commit_rolls_back_counters_and_retry_succeeds(self):
        r=self.fixture()
        with r.connect() as db: db.execute("CREATE TRIGGER fail_usage BEFORE INSERT ON usage_reports BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.report()
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],0)
        with r.connect() as db: db.execute('DROP TRIGGER fail_usage')
        self.report(); self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],10)

    def test_old_schema_upgrade_and_deleted_resource_does_not_leak_to_reused_id(self):
        r=self.fixture()
        with r.connect() as db:
            db.execute('DROP TABLE usage_counters'); db.execute('DROP TABLE usage_reports')
        self.ledger=UsageLedger(Registry(r.path)); self.report()
        r.delete('users',self.uid)
        with self.assertRaises(Error): self.ledger.user(self.uid)
        uid=r.save('users',dict(name='replacement'))['id']; self.assertEqual(uid,self.uid)
        self.assertEqual(self.ledger.user(uid)['upload_bytes'],0)

    def test_aggregate_above_sqlite_integer_limit_is_exact(self):
        r=self.fixture(); self.item.update(upload_bytes=MAX_COUNTER,download_bytes=MAX_COUNTER); self.report()
        self.session=str(uuid.uuid4()); r.begin_session(self.token,dict(session_id=self.session)); self.report()
        self.assertEqual(self.ledger.user(self.uid)['upload_bytes'],MAX_COUNTER*2)


class UsageApiTests(unittest.TestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown
    request=test_core.ApiTests.request
    def test_real_http_agent_ingestion_and_admin_only_read(self):
        r=self.server.registry; sid=r.save('servers',dict(name='host'))['id']; uid=r.save('users',dict(name='user'))['id']
        nid=r.save('nodes',dict(name='node',server_id=sid))['id']; r.configure_node(nid,test_accounts.test_xray.VLESS)
        token=r.provision_agent(sid)['token']; session=str(uuid.uuid4()); r.begin_session(token,dict(session_id=session))
        credential=r.authorize_node_user(nid,uid)['uuid']
        data=dict(session_id=session,sequence=1,items=[dict(node_id=nid,user_id=uid,credential=credential,upload_bytes=123,download_bytes=456)])
        def post(value):
            req=urllib.request.Request(self.base+'/agent/usage',data=json.dumps(data).encode(),headers={'Authorization':'Bearer '+value},method='POST')
            return self.opener.open(req,timeout=3)
        for value in ('',self.server.token):
            with self.assertRaises(urllib.error.HTTPError) as caught: post(value)
            self.assertEqual(caught.exception.code,401)
        with post(token) as response: self.assertFalse(json.load(response)['duplicate'])
        with post(token) as response: self.assertTrue(json.load(response)['duplicate'])
        path=f'/api/users/{uid}/usage'
        self.assertEqual(json.load(self.request(path))['download_bytes'],456)
        for value in ('',token):
            req=urllib.request.Request(self.base+path,headers={'Authorization':'Bearer '+value})
            with self.assertRaises(urllib.error.HTTPError) as caught: self.opener.open(req,timeout=3)
            self.assertEqual(caught.exception.code,401)
