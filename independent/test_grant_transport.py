"""Real authenticated transport remains staging-only and rechecks policy."""
import copy
import sqlite3
import threading
import unittest
import urllib.error
import uuid
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch
from agent import request
from server import Handler
from grant_agent import GrantStageStore
from grant_capabilities import GrantCapabilities,STAGE_IDENTITY,STAGING
from grant_configuration import GrantConfiguration
from grant_transport import FIRST,fetch_and_stage,resolve_ledger
import test_grant_configuration as fixture


class GrantTransportTests(unittest.TestCase):
    def start_http(self,offer=True):
        self.r=self.start()
        self.r.heartbeat(self.token,dict(uptime=1,hostname='transport-test',
                                         session_id=self.session))
        self.store=GrantStageStore(Path(self.r.path).parent/'transport-stage.db',
                                   self.g['server_id'],self.session)
        if offer is not False:
            GrantCapabilities(self.r).negotiate(self.token,
                dict(session_id=self.session,protocols=[STAGING,STAGE_IDENTITY] if offer is True else offer))
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.registry=self.r
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        self.addCleanup(server.server_close);self.addCleanup(thread.join);self.addCleanup(server.shutdown)
        self.url='http://127.0.0.1:'+str(server.server_port)

    def fetch(self):return fetch_and_stage(self.url,self.token,self.store,self.g['grant_id'])

    def post(self,data,token=None):
        return request(self.url,self.token if token is None else token,'/agent/grants/configuration',data)

    def rejected(self,status,fn):
        with self.assertRaises(urllib.error.HTTPError) as caught:fn()
        try:self.assertEqual(caught.exception.code,status)
        finally:caught.exception.close()

    def test_actual_http_duplicate_reopen_stages_without_applied_or_runtime(self):
        self.start_http();first=self.fetch()
        self.store=GrantStageStore(self.store.path,self.store.server_id,self.session)
        self.assertEqual(self.fetch(),first)
        self.assertEqual(first['state'],'staged');self.assertFalse(first['ready_for_execution'])
        with sqlite3.connect(self.store.path) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM staged_grants').fetchone()[0],1)
        with self.r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_first_total_applied').fetchone()[0],0)
        self.assertEqual(self.r.configuration(self.token)['nodes'][0]['clients'],[])

    def test_no_negotiation_cannot_bind_or_stage(self):
        self.start_http(offer=False);self.rejected(409,self.fetch)
        with self.r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_first_total_configurations').fetchone()[0],0)
        self.assertFalse(self.store.path.exists())

    def test_unsupported_execution_offer_cannot_pull(self):
        self.start_http(offer=['independent.total-grant.execution.v1'])
        self.rejected(409,self.fetch);self.assertFalse(self.store.path.exists())

    def test_request_identity_authentication_and_fields_rejected(self):
        self.start_http();data=dict(grant_id=self.g['grant_id'],session_id=self.session)
        for body in ({},[],dict(data,applied=True),dict(data,session_id=str(uuid.uuid4())),
                     dict(data,grant_id=True)):
            self.rejected(409 if isinstance(body,dict) and body.get('session_id') not in (None,self.session) else 400,
                          lambda body=body:self.post(body))
        self.rejected(401,lambda:self.post(data,token='invalid'))
        self.rejected(413,lambda:self.post(dict(data,padding='x'*17000)))
        self.assertFalse(self.store.path.exists())

    def test_policy_change_after_success_blocks_retry_preserves_stage(self):
        self.start_http();first=self.fetch()
        self.cycles.set_total_budget(self.period,0)
        self.rejected(409,self.fetch)
        with sqlite3.connect(self.store.path) as db:
            self.assertEqual(db.execute('SELECT digest FROM staged_grants').fetchone()[0],first['digest'])

    def test_rotated_credential_does_not_deliver(self):
        self.start_http()
        with self.r.connect() as db:db.execute("UPDATE agents SET token_hash='revoked'")
        self.rejected(401,self.fetch);self.assertFalse(self.store.path.exists())

    def test_retired_session_and_stale_capability_do_not_deliver(self):
        self.start_http()
        with self.r.connect() as db:
            db.execute("UPDATE grant_capabilities SET session_id=?",(str(uuid.uuid4()),))
        self.rejected(409,self.fetch)
        self.r.begin_session(self.token,dict(session_id=str(uuid.uuid4())))
        self.rejected(403,self.fetch);self.assertFalse(self.store.path.exists())

    def test_valid_but_wrong_grant_response_rejected_before_disk_write(self):
        self.start_http();value=GrantConfiguration(self.r).bind(self.token,self.g['grant_id'])
        from node_config import digest
        value['grant_id']=str(uuid.uuid4())
        value['digest']=digest({k:v for k,v in value.items() if k not in ('digest','delivery_state','ready_for_execution')})
        with patch('agent.request',return_value=value),self.assertRaisesRegex(ValueError,'其他授予'):self.fetch()
        self.assertFalse(self.store.path.exists())

    def test_tampered_response_and_expiry_at_storage_rejected(self):
        self.start_http();value=GrantConfiguration(self.r).bind(self.token,self.g['grant_id'])
        bad=copy.deepcopy(value);bad['reserved_bytes']+=1
        with patch('agent.request',return_value=bad),self.assertRaises(ValueError):self.fetch()
        self.assertFalse(self.store.path.exists())
        with patch('agent.request',return_value=value),patch('grant_agent.time.time',return_value=value['valid_until']),self.assertRaises(ValueError):self.fetch()
        self.assertFalse(self.store.path.exists())

    def test_binding_storage_failure_http_500_does_not_stage(self):
        self.start_http()
        with self.r.connect() as db:
            db.execute("CREATE TRIGGER reject_binding BEFORE INSERT ON usage_first_total_configurations BEGIN SELECT RAISE(ABORT,'private storage detail'); END")
        self.rejected(500,self.fetch)
        with self.r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_first_total_configurations').fetchone()[0],0)
        self.assertFalse(self.store.path.exists())

    def test_resolver_rejects_unhashable_kind_and_boolean_type_confusion(self):
        self.start_http(); value=GrantConfiguration(self.r).bind(self.token,self.g['grant_id'])
        body=dict(session_id=self.session,ledger_id=self.g['grant_id'],
                  binding_digest=value['digest'],expected_ledger_kind=[])
        self.rejected(400,lambda:request(self.url,self.token,'/agent/grants/resolve',body))
        good={'accepted':True,'state':'ledger_resolved','ledger_kind':FIRST,
              'ledger_id':self.g['grant_id'],'binding_digest':value['digest'],
              'ready_for_execution':False}
        for changes in ({'accepted':1},{'ready_for_execution':0},
                        {'ledger_kind':[]},{'extra':1}):
            bad=copy.deepcopy(good); bad.update(changes)
            with self.subTest(changes=changes),patch('agent.request',return_value=bad),self.assertRaises(ValueError):
                resolve_ledger(self.url,self.token,self.session,self.g['grant_id'],
                               value['digest'],FIRST)
        for kind in ([],{},True):
            with self.subTest(kind=kind),self.assertRaises(ValueError):
                resolve_ledger(self.url,self.token,self.session,self.g['grant_id'],
                               value['digest'],kind)

    def test_resolver_is_exact_read_only_minimal_and_collision_closed(self):
        self.start_http(); value=GrantConfiguration(self.r).bind(self.token,self.g['grant_id'])
        body=dict(session_id=self.session,ledger_id=self.g['grant_id'],
                  binding_digest=value['digest'],expected_ledger_kind=FIRST)
        with self.r.connect() as db:
            before=(db.total_changes,tuple(db.execute('SELECT * FROM agents').fetchall()),
                    tuple(db.execute('SELECT * FROM grant_capabilities').fetchall()),
                    tuple(db.execute('SELECT * FROM usage_first_total_configurations').fetchall()))
        result=request(self.url,self.token,'/agent/grants/resolve',body)
        self.assertEqual(result,{'accepted':True,'state':'ledger_resolved',
            'ledger_kind':FIRST,'ledger_id':self.g['grant_id'],
            'binding_digest':value['digest'],'ready_for_execution':False})
        self.assertFalse({'token_hash','session_id','payload','node_config','credential'} &
                         set(result))
        with self.r.connect() as db:
            after=(db.total_changes,tuple(db.execute('SELECT * FROM agents').fetchall()),
                   tuple(db.execute('SELECT * FROM grant_capabilities').fetchall()),
                   tuple(db.execute('SELECT * FROM usage_first_total_configurations').fetchall()))
        self.assertEqual(before,after)
        wrong=dict(body,expected_ledger_kind='multi_allocation')
        self.rejected(409,lambda:request(self.url,self.token,'/agent/grants/resolve',wrong))
        missing=dict(body,ledger_id=str(uuid.uuid4()))
        self.rejected(404,lambda:request(self.url,self.token,'/agent/grants/resolve',missing))
        with self.r.connect() as db:
            first=db.execute('SELECT * FROM usage_first_total_grants').fetchone()
            db.execute('''INSERT INTO usage_multi_agent_allocations(
                request_id,allocation_id,first_grant_id,cycle_id,policy_id,seal_digest,
                server_id,session_id,token_hash,node_id,user_id,credential,requested_bytes,
                reserved_bytes,baseline_bytes,issued_at,valid_until)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (str(uuid.uuid4()),first['grant_id'],first['grant_id'],first['cycle_id'],
                 first['policy_id'],first['seal_digest'],first['server_id'],first['session_id'],
                 first['token_hash'],first['node_id'],first['user_id'],first['credential'],
                 first['requested_bytes'],first['reserved_bytes'],first['baseline_bytes'],
                 first['issued_at'],first['valid_until']))
        self.rejected(409,lambda:request(self.url,self.token,'/agent/grants/resolve',body))

    def test_resolver_rejects_retired_session_without_disk_write(self):
        self.start_http(); value=GrantConfiguration(self.r).bind(self.token,self.g['grant_id'])
        old=self.session
        self.r.begin_session(self.token,dict(session_id=str(uuid.uuid4())))
        body=dict(session_id=old,ledger_id=self.g['grant_id'],
                  expected_ledger_kind=None,binding_digest=value['digest'])
        with self.r.connect() as db:
            before=tuple(db.execute('SELECT * FROM usage_first_total_configurations').fetchall())
        self.rejected(403,lambda:request(self.url,self.token,'/agent/grants/resolve',body))
        with self.r.connect() as db:
            self.assertEqual(tuple(db.execute(
                'SELECT * FROM usage_first_total_configurations').fetchall()),before)


for name in ('setUp','tearDown','fixtures','fixture','setup_total','setup_drain','applied',
             'report','data','envelope','accept','sealed','request','prepare','start'):
    setattr(GrantTransportTests,name,getattr(fixture.GrantConfigurationTests,name))
GrantTransportTests.encode=staticmethod(fixture.GrantConfigurationTests.encode)
