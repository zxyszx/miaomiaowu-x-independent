"""Authenticated HTTP evidence retries are not execution or billing proof."""
import http.client
import json
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from agent import request as send
from allocation_outbox import acknowledge
from allocation_quota import AllocationQuota
from server import Handler
from total_usage import current
import test_allocation_evidence as base

PATH='/agent/usage/allocation-evidence'

class EvidenceHTTPTests(unittest.TestCase):
    setUp=base.EvidenceTests.setUp
    tearDown=base.EvidenceTests.tearDown
    fixtures=base.EvidenceTests.fixtures
    fixture=base.EvidenceTests.fixture
    setup_total=base.EvidenceTests.setup_total
    request=base.EvidenceTests.request
    reserved=base.EvidenceTests.reserved
    prepare=base.EvidenceTests.prepare
    count=base.EvidenceTests.count

    def start(self):
        self.prepare()
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler); server.registry=self.r
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        self.addCleanup(server.server_close);self.addCleanup(thread.join);self.addCleanup(server.shutdown)
        self.url='http://127.0.0.1:%s'%server.server_port

    def error(self,body,status,token=None):
        with self.assertRaises(urllib.error.HTTPError) as caught:
            send(self.url,self.token if token is None else token,PATH,body)
        self.assertEqual(caught.exception.code,status)
        self.assertEqual(caught.exception.headers['Cache-Control'],'no-store')
        self.assertIsInstance(json.loads(caught.exception.read())['error'],str)

    def test_lost_receipt_reopen_and_exact_http_retry_never_bill_or_refund(self):
        self.start();_,ticket=self.quota.reserve_write(0,5)
        frozen=self.quota.freeze_evidence()
        reply=send(self.url,self.token,PATH,frozen) # Deliberately do not persist receipt.
        self.assertFalse(reply['duplicate']);self.quota.complete_write(ticket)
        reopened=AllocationQuota(self.local,self.scope)
        self.assertEqual(reopened.freeze_evidence(),frozen)
        retry=send(self.url,self.token,PATH,frozen);self.assertTrue(retry['duplicate'])
        self.assertTrue(acknowledge(reopened,retry))
        latest=reopened.freeze_evidence();self.assertEqual(latest['sequence'],2)
        self.assertEqual(latest['successful_upload_bytes'],5)
        self.assertTrue(acknowledge(reopened,send(self.url,self.token,PATH,latest)))
        self.assertEqual(self.count(),2);self.assertEqual(self.reserved(self.r),7)
        with self.r.connect() as db:
            db.execute('BEGIN');self.assertEqual(current(db,self.period),0)
            self.assertEqual(db.execute('SELECT count(*) FROM usage_deltas').fetchone()[0],0)

    def test_invalid_fields_counter_types_and_auth_do_not_persist(self):
        self.start();frozen=self.quota.freeze_evidence()
        for data in ([],{},dict(frozen,extra=True),dict(frozen,sequence=True),
                     dict(frozen,successful_upload_bytes=1),dict(frozen,ready_for_execution=True)):
            self.error(data,400)
        self.error(frozen,401,token='invalid');self.error(frozen,401,token='')
        self.assertEqual(self.count(),0);self.assertEqual(self.quota.freeze_evidence(),frozen)

    def test_cross_server_and_rotation_cannot_consume_pending(self):
        self.start();frozen=self.quota.freeze_evidence()
        sid=self.r.save('servers',dict(name='other'))['id']
        other=self.r.provision_agent(sid)['token']
        self.error(frozen,403,token=other)
        replacement=self.r.provision_agent(self.scope['server_id'])['token']
        self.error(frozen,401);self.error(frozen,403,token=replacement)
        self.assertEqual(self.count(),0);self.assertEqual(self.quota.freeze_evidence(),frozen)

    def test_conflicting_sequence_preserves_original_and_strict_receipt(self):
        self.start();frozen=self.quota.freeze_evidence()
        receipt=send(self.url,self.token,PATH,frozen)
        _,ticket=self.quota.reserve_write(1,2)
        altered=dict(self.quota.snapshot(),sequence=1)
        self.error(altered,409);self.assertEqual(self.count(),1)
        for bad in (dict(receipt,digest='bad'),dict(receipt,accepted=1),dict(receipt,extra=True)):
            with self.assertRaises(ValueError):acknowledge(self.quota,bad)
        self.assertEqual(self.quota.freeze_evidence(),frozen)
        self.assertTrue(acknowledge(self.quota,receipt))
        self.assertEqual(self.quota.snapshot()['unknown_download_bytes'],2)

    def test_body_size_methods_and_bad_json_are_bounded(self):
        self.start()
        connection=http.client.HTTPConnection('127.0.0.1',int(self.url.rsplit(':',1)[1]))
        try:
            connection.putrequest('POST',PATH)
            connection.putheader('Authorization','Bearer '+self.token)
            connection.putheader('Content-Length','16385');connection.endheaders()
            response=connection.getresponse();self.assertEqual(response.status,413);response.read()
        finally:connection.close()
        with self.assertRaises(urllib.error.HTTPError) as caught:send(self.url,self.token,PATH)
        self.assertEqual(caught.exception.code,404)
        for raw in (b'{',b'null',b'\xff'):
            req=urllib.request.Request(self.url+PATH,data=raw,headers={'Authorization':'Bearer '+self.token})
            with self.assertRaises(urllib.error.HTTPError) as caught:urllib.request.urlopen(req)
            self.assertEqual(caught.exception.code,400)
        self.assertEqual(self.count(),0)

    def test_http_storage_failure_rolls_back_without_ack_or_internal_details(self):
        self.start();frozen=self.quota.freeze_evidence()
        with self.r.connect() as db:
            db.execute("CREATE TRIGGER fail_evidence BEFORE INSERT ON usage_allocation_evidence BEGIN SELECT RAISE(ABORT,'private-storage-marker'); END")
        with self.assertRaises(urllib.error.HTTPError) as caught:
            send(self.url,self.token,PATH,frozen)
        self.assertEqual(caught.exception.code,500)
        message=caught.exception.read().decode()
        self.assertNotIn('private-storage-marker',message)
        self.assertEqual(self.count(),0);self.assertEqual(self.quota.freeze_evidence(),frozen)
        with self.r.connect() as db:db.execute('DROP TRIGGER fail_evidence')
        self.assertTrue(acknowledge(self.quota,send(self.url,self.token,PATH,frozen)))
        self.assertEqual(self.reserved(self.r),7)
