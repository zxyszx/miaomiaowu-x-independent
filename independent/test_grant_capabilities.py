"""Authenticated negotiation does not install grants or upgrade old Agents."""
import copy
import json
import sqlite3
import threading
import unittest
import urllib.error
import uuid
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer
from unittest.mock import patch
from core import Registry,Error
from server import Handler
from grant_capabilities import GrantCapabilities,STAGING,negotiate,receipt
from agent import request
import test_agent_sessions as base

class GrantCapabilityTests(unittest.TestCase):
    setUp=base.SessionTests.setUp
    tearDown=base.SessionTests.tearDown
    claim=base.SessionTests.claim

    def body(self,session=None,offered=None):
        return dict(session_id=session or self.a,protocols=[STAGING] if offered is None else offered)

    def offer(self,body=None):
        return GrantCapabilities(self.registry).negotiate(self.token,self.body() if body is None else body)

    def rows(self):
        with self.registry.connect() as db:
            return [tuple(r) for r in db.execute('SELECT * FROM grant_capabilities')]

    def test_explicit_negotiation_reopen_never_applies(self):
        self.claim(self.a); self.assertEqual(self.rows(),[])
        result=self.offer();self.assertEqual(result,receipt(self.sid,self.a,[STAGING]))
        reopened=GrantCapabilities(Registry(self.path))
        self.assertEqual(reopened.negotiate(self.token,self.body()),result)
        with self.registry.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_first_total_applied').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM runtime_receipts').fetchone()[0],0)

    def test_execution_offer_unknown_and_empty_never_enable_execution(self):
        self.claim(self.a)
        result=self.offer(self.body(offered=['independent.total-grant.execution.v1']))
        self.assertEqual(result['selected'],[]);self.assertFalse(result['execution_ready'])
        self.claim(self.b)
        result=self.offer(self.body(self.b,[]));self.assertEqual(result['selected'],[])

    def test_bad_fields_names_duplicates_and_boolean_reject_without_rows(self):
        self.claim(self.a)
        for data in ([],{},dict(self.body(),execution_ready=True),self.body(offered=[STAGING,STAGING]),
                     self.body(offered='bad'),self.body(offered=[True]),self.body(offered=['bad/name']),
                     self.body(offered=['a'*97]),self.body(offered=[str(i) for i in range(9)])):
            with self.subTest(data=data),self.assertRaises(Error):self.offer(data)
        self.assertEqual(self.rows(),[])

    def test_offer_immutable_reordered_retry_and_session_replacement(self):
        self.claim(self.a)
        self.offer(self.body(offered=['unknown.v1',STAGING]));saved=self.rows()
        self.offer(self.body(offered=[STAGING,'unknown.v1']));self.assertEqual(self.rows(),saved)
        with self.assertRaises(Error) as caught:self.offer()
        self.assertEqual(caught.exception.status,409);self.assertEqual(self.rows(),saved)
        self.claim(self.b);self.offer(self.body(self.b));self.assertEqual(self.rows()[0][1],self.b)
        with self.assertRaises(Error):self.offer()

    def test_credential_rotation_and_cross_server_cannot_reuse_offer(self):
        self.claim(self.a);self.offer();saved=self.rows()
        sid=self.registry.save('servers',{'name':'other'})['id']
        other=self.registry.provision_agent(sid)['token']
        with self.assertRaises(Error):GrantCapabilities(self.registry).negotiate(other,self.body())
        self.assertEqual(self.rows(),saved)
        fresh=self.registry.provision_agent(self.sid)['token']
        with self.assertRaises(Error):self.offer()
        with self.assertRaises(Error):GrantCapabilities(self.registry).negotiate(fresh,self.body())
        new=str(uuid.uuid4());self.registry.begin_session(fresh,dict(session_id=new))
        reply=GrantCapabilities(self.registry).negotiate(fresh,self.body(new))
        self.assertEqual(reply['session_id'],new);self.assertFalse(reply['execution_ready'])

    def test_concurrent_identical_offers_unique_and_insert_failure_rollback(self):
        self.claim(self.a)
        with self.registry.connect() as db:
            db.execute("CREATE TRIGGER fail_cap BEFORE INSERT ON grant_capabilities BEGIN SELECT RAISE(ABORT,'disk'); END")
        with self.assertRaises(sqlite3.IntegrityError):self.offer()
        self.assertEqual(self.rows(),[])
        with self.registry.connect() as db:db.execute('DROP TRIGGER fail_cap')
        with ThreadPoolExecutor(2) as pool:replies=list(pool.map(lambda _:self.offer(),range(2)))
        self.assertEqual(replies[0],replies[1]);self.assertEqual(len(self.rows()),1)

    def start_http(self):
        self.claim(self.a)
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.registry=self.registry
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        self.addCleanup(server.server_close);self.addCleanup(thread.join);self.addCleanup(server.shutdown)
        return 'http://127.0.0.1:%s'%server.server_port

    def test_http_negotiation_auth_method_and_body_bounds(self):
        url=self.start_http()
        result=negotiate(url,self.token,self.sid,self.a,[STAGING]);self.assertEqual(result['selected'],[STAGING])
        for token,body,status in (('invalid',self.body(),401),(self.token,dict(self.body(),extra=1),400),
                                  (self.token,{'padding':'x'*16384},413)):
            with self.assertRaises(urllib.error.HTTPError) as caught:
                request(url,token,'/agent/grants/capabilities',body)
            self.assertEqual(caught.exception.code,status)
            self.assertEqual(caught.exception.headers['Cache-Control'],'no-store')
            caught.exception.close()
        with self.assertRaises(urllib.error.HTTPError) as caught:request(url,self.token,'/agent/grants/capabilities')
        self.assertEqual(caught.exception.code,404);caught.exception.close()

    def test_client_rejects_wrong_scope_extra_fields_and_type_confusion(self):
        expected=receipt(self.sid,self.a,[STAGING])
        for changes in ({'server_id':True},{'session_id':str(uuid.uuid4())},{'selected':[]},
                        {'execution_ready':True},{'accepted':1},{'extra':1}):
            response=copy.deepcopy(expected);response.update(changes)
            with patch('agent.request',return_value=response),self.assertRaises(ValueError):
                negotiate('http://127.0.0.1',self.token,self.sid,self.a,[STAGING])
        with patch('agent.request',return_value=expected):
            self.assertEqual(negotiate('http://127.0.0.1',self.token,self.sid,self.a,[STAGING]),expected)
