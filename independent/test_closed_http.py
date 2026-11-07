"""Real HTTP fragment boundaries and binding; isolated historical identity fixture."""
import json
import http.client
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from agent import request
from server import Handler
from closed_transport import fragment_receipt
import test_closed_receiver as base

class ClosedHTTPTests(unittest.TestCase):
    setUp=base.ClosedReceiverTests.setUp
    tearDown=base.ClosedReceiverTests.tearDown
    fixtures=base.ClosedReceiverTests.fixtures
    fixture=base.ClosedReceiverTests.fixture
    setup_total=base.ClosedReceiverTests.setup_total
    applied=base.ClosedReceiverTests.applied
    data=base.ClosedReceiverTests.data
    report=base.ClosedReceiverTests.report
    setup_drain=base.ClosedReceiverTests.setup_drain
    setup_fragments=base.ClosedReceiverTests.setup_fragments
    counts=base.ClosedReceiverTests.counts

    def start(self):
        self.setup_fragments()
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.registry=self.r
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        self.addCleanup(server.server_close);self.addCleanup(thread.join);self.addCleanup(server.shutdown)
        self.url='http://127.0.0.1:%s'%server.server_port

    def payload(self,index):
        return dict(cycle_id=self.period,envelope=self.manifest,fragment=self.parts[index])

    def send(self,index):
        reply=request(self.url,self.token,'/agent/usage/closed/fragment',self.payload(index))
        return fragment_receipt(self.period,self.manifest,self.parts[index],reply)

    def error(self,body,status,token=None,path='/agent/usage/closed/fragment'):
        with self.assertRaises(urllib.error.HTTPError) as caught:
            request(self.url,token or self.token,path,body)
        self.assertEqual(caught.exception.code,status)
        self.assertEqual(caught.exception.headers['Cache-Control'],'no-store')
        self.assertIsInstance(json.loads(caught.exception.read())['error'],str)

    def test_real_http_out_of_order_duplicate_and_completion_binding(self):
        self.start(); self.assertFalse(self.send(2)['complete'])
        self.assertFalse(self.send(0)['complete']);self.assertFalse(self.send(0)['complete'])
        result=self.send(1);self.assertTrue(result['complete']);self.assertFalse(result['duplicate'])
        self.assertTrue(self.send(1)['duplicate']);self.assertEqual(self.counts(),(1,3,1))

    def test_strict_request_body_and_authentication_no_persist(self):
        self.start(); good=self.payload(0)
        for body in ([],dict(good,extra=True),{'cycle_id':self.period},dict(good,envelope=[])):
            self.error(body,400)
        self.error(good,401,token='invalid')
        self.assertEqual(self.counts(),(0,0,0))

    def test_route_size_limit_does_not_expand_existing_routes(self):
        self.start();good=self.payload(0)
        connection=http.client.HTTPConnection('127.0.0.1',int(self.url.rsplit(':',1)[1]))
        try:
            connection.putrequest('POST','/agent/usage/closed/fragment')
            connection.putheader('Authorization','Bearer '+self.token)
            connection.putheader('Content-Length',str(1024*1024+1))
            connection.endheaders()
            response=connection.getresponse();self.assertEqual(response.status,413);response.read()
        finally:connection.close()
        self.error({'padding':'x'*16384},413,path='/agent/usage/closed')
        # The ordinary 64-identity part exceeds 16 KiB after harmless JSON
        # whitespace: only the fragment route admits it, without changing data.
        raw=json.dumps(good,indent=12).encode();self.assertGreater(len(raw),16384)
        req=urllib.request.Request(self.url+'/agent/usage/closed/fragment',data=raw,
            headers={'Authorization':'Bearer '+self.token,'Content-Type':'application/json'})
        with urllib.request.urlopen(req) as response:
            self.assertEqual(response.headers['Cache-Control'],'no-store')
            reply=json.load(response)
        self.assertFalse(fragment_receipt(self.period,self.manifest,self.parts[0],reply)['complete'])
        self.assertEqual(self.counts(),(1,1,0))

    def test_wrong_method_and_invalid_json_are_bounded_errors(self):
        self.start()
        with self.assertRaises(urllib.error.HTTPError) as caught:
            request(self.url,self.token,'/agent/usage/closed/fragment')
        self.assertEqual(caught.exception.code,404)
        for raw in (b'null',b'{',b'\xff'):
            req=urllib.request.Request(self.url+'/agent/usage/closed/fragment',data=raw,
                headers={'Authorization':'Bearer '+self.token})
            with self.assertRaises(urllib.error.HTTPError) as caught: urllib.request.urlopen(req)
            self.assertEqual(caught.exception.code,400)
        self.assertEqual(self.counts(),(0,0,0))

    def test_last_part_http_conflict_keeps_original_parts_for_retry(self):
        self.start();self.send(0);self.send(1)
        item=self.parts[2]['body']['items'][0]
        key=(self.sid,self.session,item['node_id'],item['user_id'],item['credential'])
        with self.r.connect() as db:
            db.execute('DELETE FROM usage_counters WHERE server_id=? AND session_id=? AND node_id=? AND user_id=? AND credential=?',key)
        self.error(self.payload(2),409);self.assertEqual(self.counts(),(1,2,0))
        with self.r.connect() as db: db.execute('INSERT INTO usage_counters VALUES(?,?,?,?,?,?,?)',(*key,4,3))
        self.assertTrue(self.send(2)['complete'])

    def test_receipt_mismatch_cannot_be_confused_with_ack_or_completion(self):
        self.start();partial=self.send(0)
        bad_values={'accepted':False,'server_id':True,'cycle_id':'wrong','session_id':'wrong',
            'manifest_digest':'f'*64,'part':True,'complete':1,'ready_for_execution':True}
        for key,value in bad_values.items():
            with self.subTest(key=key),self.assertRaises(ValueError):
                fragment_receipt(self.period,self.manifest,self.parts[0],dict(partial,**{key:value}))
        for bad in ([],dict(partial,extra=True),dict(partial,digest=self.final['digest']),dict(partial,complete=True)):
            with self.assertRaises(ValueError): fragment_receipt(self.period,self.manifest,self.parts[0],bad)
        self.send(1);final=self.send(2)
        for bad in (dict(final,digest='f'*64),dict(final,duplicate=1),dict(final,complete=False)):
            with self.assertRaises(ValueError): fragment_receipt(self.period,self.manifest,self.parts[2],bad)
        # Validation only: it cannot delete local original close evidence.
        self.assertEqual(self.counts(),(1,3,1))

    def test_http_rotated_credentials_cannot_resume_partial_or_complete(self):
        self.start();self.send(0);self.token=self.r.provision_agent(self.sid)['token']
        self.error(self.payload(1),403);self.assertEqual(self.counts(),(1,1,0))

if __name__=='__main__':unittest.main()
