"""Later multi-Agent allocations reuse binding-v1 HTTP only for staging."""
import asyncio
import json
import sqlite3
import threading
import unittest
import urllib.error
import uuid
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

from agent import request
from core import Error,Registry
from grant_ack import fact_for
from grant_agent import GrantStageStore
from grant_capabilities import GrantCapabilities,INSTALLATION,STAGE_IDENTITY,STAGING
from grant_scheduler import GrantScheduler
from grant_transport import fetch_and_stage
from multi_agent_allocation import MultiAgentAllocations
from server import Handler
from total_allocation import held
from total_usage import current as confirmed_total
import test_multi_agent_allocation as allocation_base
import test_accounts


class MultiAgentDeliveryTests(unittest.TestCase):
    setUp=allocation_base.MultiAgentAllocationTests.setUp
    tearDown=allocation_base.MultiAgentAllocationTests.tearDown
    fixtures=allocation_base.MultiAgentAllocationTests.fixtures
    fixture=allocation_base.MultiAgentAllocationTests.fixture
    setup_total=allocation_base.MultiAgentAllocationTests.setup_total
    setup_drain=allocation_base.MultiAgentAllocationTests.setup_drain
    applied=allocation_base.MultiAgentAllocationTests.applied
    report=allocation_base.MultiAgentAllocationTests.report
    data=allocation_base.MultiAgentAllocationTests.data
    encode=staticmethod(allocation_base.MultiAgentAllocationTests.encode)
    envelope=allocation_base.MultiAgentAllocationTests.envelope
    accept=allocation_base.MultiAgentAllocationTests.accept
    sealed=allocation_base.MultiAgentAllocationTests.sealed
    first_request=allocation_base.MultiAgentAllocationTests.first_request
    second_host=allocation_base.MultiAgentAllocationTests.second_host
    start=allocation_base.MultiAgentAllocationTests.start
    host=allocation_base.MultiAgentAllocationTests.host
    allocation_request=allocation_base.MultiAgentAllocationTests.request
    balances=allocation_base.MultiAgentAllocationTests.balances

    def start_http(self,protocols=(STAGING,)):
        self.r=self.start(total=100)
        self.token_a,self.host_a=self.host(self.r,'delivery-a')
        self.token_b,self.host_b=self.host(self.r,'delivery-b')
        api=MultiAgentAllocations(self.r)
        self.a=api.allocate(self.token_a,self.allocation_request(self.host_a,10))
        self.b=api.allocate(self.token_b,self.allocation_request(self.host_b,10))
        self.sid_a=self.a['server_id'];self.sid_b=self.b['server_id']
        GrantCapabilities(self.r).negotiate(self.token_a,
            dict(session_id=self.host_a['session_id'],protocols=list(protocols)))
        GrantCapabilities(self.r).negotiate(self.token_b,
            dict(session_id=self.host_b['session_id'],protocols=list(protocols)))
        root=Path(self.r.path).parent
        self.store_a=GrantStageStore(root/'multi-stage-a.db',self.sid_a,
                                     self.host_a['session_id'])
        self.store_b=GrantStageStore(root/'multi-stage-b.db',self.sid_b,
                                     self.host_b['session_id'])
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        server.registry=self.r
        thread=threading.Thread(target=server.serve_forever,daemon=True)
        thread.start()
        self.addCleanup(server.server_close);self.addCleanup(thread.join)
        self.addCleanup(server.shutdown)
        self.server=server
        self.url='http://127.0.0.1:'+str(server.server_port)

    def post(self,token,allocation,host=None,path='/agent/grants/configuration'):
        host=host or (self.host_a if token==self.token_a else self.host_b)
        return request(self.url,token,path,dict(
            grant_id=allocation['allocation_id'],session_id=host['session_id']))

    def rejected(self,status,call,hidden=None):
        with self.assertRaises(urllib.error.HTTPError) as caught:
            call()
        try:
            self.assertEqual(caught.exception.code,status)
            body=caught.exception.read().decode()
            if hidden is not None:self.assertNotIn(hidden,body)
        finally:
            caught.exception.close()

    def master_counts(self):
        with self.r.connect() as db:
            db.execute('BEGIN')
            names=('usage_multi_agent_configurations','usage_first_total_applied',
                   'usage_first_total_installations','usage_first_total_lifecycle',
                   'usage_first_total_execution','usage_first_total_meter')
            counts={name:db.execute('SELECT count(*) FROM '+name).fetchone()[0]
                    for name in names}
            return counts,confirmed_total(db,self.period),held(db,self.period)

    def test_two_agents_pull_and_stage_only_their_immutable_allocations(self):
        self.start_http()
        first=fetch_and_stage(self.url,self.token_a,self.store_a,
                              self.a['allocation_id'])
        second=fetch_and_stage(self.url,self.token_b,self.store_b,
                               self.b['allocation_id'])
        self.assertEqual((first['state'],second['state']),('staged','staged'))
        self.assertFalse(first['ready_for_execution']);self.assertFalse(second['ready_for_execution'])
        for store,row in ((self.store_a,self.a),(self.store_b,self.b)):
            with sqlite3.connect(store.path) as db:
                saved=db.execute('SELECT grant_id,payload,digest FROM staged_grants').fetchone()
            payload=json.loads(saved[1])
            self.assertEqual(saved[0],row['allocation_id'])
            self.assertEqual(payload['grant_id'],row['allocation_id'])
            self.assertEqual(payload['reserved_bytes'],row['reserved_bytes'])
            self.assertEqual(payload['baseline_bytes'],row['baseline_bytes'])
            self.assertEqual(payload['valid_until'],row['valid_until'])
        counts,confirmed,balance=self.master_counts()
        self.assertEqual(counts['usage_multi_agent_configurations'],2)
        self.assertTrue(all(counts[name]==0 for name in counts
                            if name!='usage_multi_agent_configurations'))
        self.assertEqual((confirmed,balance),(7,22))

    def test_lost_response_reopen_returns_identical_body_digest_and_deadline(self):
        self.start_http();data=dict(grant_id=self.a['allocation_id'],
                                    session_id=self.host_a['session_id'])
        first=request(self.url,self.token_a,'/agent/grants/configuration',data)
        with self.r.connect() as db:
            created=db.execute('''SELECT created_at FROM usage_multi_agent_configurations
                WHERE allocation_id=?''',(self.a['allocation_id'],)).fetchone()[0]
        self.server.registry=Registry(self.r.path)
        second=request(self.url,self.token_a,'/agent/grants/configuration',data)
        self.assertEqual(second,first)
        self.assertEqual(second['valid_until'],self.a['valid_until'])
        self.assertEqual(second['delivery_state'],'not_delivered')
        self.assertFalse(second['ready_for_execution'])
        with self.r.connect() as db:
            self.assertEqual(db.execute('''SELECT created_at FROM usage_multi_agent_configurations
                WHERE allocation_id=?''',(self.a['allocation_id'],)).fetchone()[0],created)
        reopened=GrantStageStore(self.store_a.path,self.sid_a,self.host_a['session_id'])
        self.assertEqual(fetch_and_stage(self.url,self.token_a,reopened,
                                         self.a['allocation_id']),
                         fetch_and_stage(self.url,self.token_a,reopened,
                                         self.a['allocation_id']))

    def test_cross_agent_unknown_old_session_rotation_and_offline_fail_closed(self):
        self.start_http()
        self.rejected(403,lambda:self.post(self.token_b,self.a,self.host_b))
        self.rejected(404,lambda:request(self.url,self.token_a,
            '/agent/grants/configuration',dict(grant_id=str(uuid.uuid4()),
                session_id=self.host_a['session_id'])))
        old_token=self.token_a
        self.r.begin_session(old_token,dict(session_id=str(uuid.uuid4())))
        self.rejected(403,lambda:self.post(old_token,self.a,self.host_a))
        new_token=self.r.provision_agent(self.sid_a)['token']
        self.rejected(401,lambda:self.post(old_token,self.a,self.host_a))
        self.rejected(403,lambda:self.post(new_token,self.a,self.host_a))
        self.r.revoke_agent(self.sid_a)
        self.rejected(401,lambda:self.post(new_token,self.a,self.host_a))
        with self.r.connect() as db:
            last=db.execute('SELECT last_seen FROM agents WHERE server_id=?',
                            (self.sid_b,)).fetchone()[0]
        with patch('multi_agent_configuration.time.time',return_value=last+45):
            self.rejected(409,lambda:self.post(self.token_b,self.b,self.host_b))
        self.assertEqual(self.balances(self.r),(22,20))

    def test_policy_and_expiry_changes_preserve_hold(self):
        self.start_http();first=self.post(self.token_a,self.a,self.host_a)
        self.cycles.set_total_budget(self.period,99)
        self.rejected(409,lambda:self.post(self.token_a,self.a,self.host_a))
        with patch('multi_agent_configuration.time.time',return_value=self.b['valid_until']):
            self.rejected(409,lambda:self.post(self.token_b,self.b,self.host_b))
        with self.r.connect() as db:
            stored=db.execute('''SELECT digest FROM usage_multi_agent_configurations
                WHERE allocation_id=?''',(self.a['allocation_id'],)).fetchone()[0]
        self.assertEqual(stored,first['digest'])
        self.assertEqual(self.balances(self.r),(22,20))

    def test_direction_baseline_change_rejects_without_configuration_or_refund(self):
        self.start_http()
        with self.r.connect() as db:
            db.execute("UPDATE usage_cycle_confirmed_totals SET total_bytes='8'")
            db.execute("UPDATE usage_cycle_confirmed_directions SET upload_bytes='5'")
        self.rejected(409,lambda:self.post(self.token_b,self.b,self.host_b))
        with self.r.connect() as db:
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_multi_agent_configurations').fetchone()[0],0)
            sources=[tuple(row) for row in db.execute('''SELECT ledger_kind,
                reserved_bytes,upload_bytes,download_bytes
                FROM usage_attributed_sources ORDER BY ledger_kind,ledger_id''')]
        self.assertEqual(sum(int(row[1])-int(row[2])-int(row[3]) for row in sources),22)
        self.assertEqual(sum(int(row[1])-int(row[2])-int(row[3])
                             for row in sources if row[0]=='multi_allocation'),20)
        with self.assertRaises(Error): self.balances(self.r)

    def test_configuration_digest_change_cannot_rewrite_persisted_binding(self):
        self.start_http();first=self.post(self.token_a,self.a,self.host_a)
        with self.r.connect() as db:
            payload=json.loads(db.execute('SELECT payload FROM node_configs WHERE node_id=?',
                                          (self.host_a['node_id'],)).fetchone()[0])
        self.r.configure_node(self.host_a['node_id'],
                              dict(payload,listen_port=payload['listen_port']+100))
        self.rejected(409,lambda:self.post(self.token_a,self.a,self.host_a))
        with self.r.connect() as db:
            stored=db.execute('''SELECT digest FROM usage_multi_agent_configurations
                WHERE allocation_id=?''',(self.a['allocation_id'],)).fetchone()[0]
        self.assertEqual(stored,first['digest'])
        self.assertEqual(self.balances(self.r),(22,20))

    def test_plan_binding_change_cannot_rewrite_persisted_binding(self):
        self.start_http();first=self.post(self.token_a,self.a,self.host_a)
        replacement=test_accounts.plan(self.r,'replacement-plan',65536,2)
        self.r.assign_plan(self.uid,{'plan_id':replacement})
        self.rejected(409,lambda:self.post(self.token_a,self.a,self.host_a))
        with self.r.connect() as db:
            stored=db.execute('''SELECT digest FROM usage_multi_agent_configurations
                WHERE allocation_id=?''',(self.a['allocation_id'],)).fetchone()[0]
        self.assertEqual(stored,first['digest'])
        self.assertEqual(self.balances(self.r),(22,20))

    def test_binding_storage_failure_rolls_back_and_hides_private_detail(self):
        self.start_http();secret='private multi allocation storage detail'
        with self.r.connect() as db:
            db.execute(f'''CREATE TRIGGER reject_multi_configuration BEFORE INSERT
                ON usage_multi_agent_configurations BEGIN SELECT RAISE(ABORT,'{secret}'); END''')
        self.rejected(500,lambda:self.post(self.token_a,self.a,self.host_a),secret)
        with self.r.connect() as db:
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_multi_agent_configurations').fetchone()[0],0)
        self.assertFalse(self.store_a.path.exists())
        self.assertEqual(self.balances(self.r),(22,20))

    def test_ack_lifecycle_execution_and_usage_cannot_promote_staged_allocation(self):
        self.start_http();value=self.post(self.token_a,self.a,self.host_a)
        installation=fact_for(value)
        self.rejected(403,lambda:request(self.url,self.token_a,
            '/agent/grants/installed',installation))
        self.rejected(404,lambda:request(self.url,self.token_a,
            '/agent/grants/lifecycle',dict(action='begin',
                grant_id=self.a['allocation_id'],session_id=self.host_a['session_id'])))
        execution=dict(installation,execution_id=str(uuid.uuid4()))
        self.rejected(403,lambda:request(self.url,self.token_a,
            '/agent/grants/execution',dict(fact=execution)))
        self.rejected(403,lambda:request(self.url,self.token_a,
            '/agent/grants/usage',dict(grant_id=self.a['allocation_id'],
                policy_id=self.a['policy_id'],session_id=self.host_a['session_id'],
                sequence=1,upload_bytes=0,download_bytes=0)))
        ordinary=self.r.configuration(self.token_a)
        ack={key:ordinary[key] for key in ('revision','digest')}
        ack['session_id']=self.host_a['session_id']
        request(self.url,self.token_a,'/agent/config/ack',ack)
        request(self.url,self.token_a,'/agent/config/applied',ack)
        counts,confirmed,balance=self.master_counts()
        self.assertEqual(counts['usage_multi_agent_configurations'],1)
        self.assertTrue(all(counts[name]==0 for name in counts
                            if name!='usage_multi_agent_configurations'))
        self.assertEqual((confirmed,balance),(7,22))

    def test_existing_scheduler_stops_before_runtime_installation(self):
        self.start_http(protocols=(STAGING,STAGE_IDENTITY,INSTALLATION))
        calls={'factory':0,'install':0,'close':0}
        store=self.store_a
        class Runtime:
            def __init__(self):self.stage=store
            async def install(self,value):calls['install']+=1
            async def close(self):calls['close']+=1
        def factory(value):calls['factory']+=1;return Runtime()
        state=Path(self.r.path).parent/'scheduler'
        value=self.post(self.token_a,self.a,self.host_a)
        staged=GrantStageStore(state/'grant-stage.sqlite',self.sid_a,
                               self.host_a['session_id'])
        staged.stage(value,{'accepted':True,'state':'ledger_resolved',
            'ledger_kind':'multi_allocation','ledger_id':self.a['allocation_id'],
            'binding_digest':value['digest'],'ready_for_execution':False})
        scheduler=GrantScheduler(self.url,self.token_a,state,
            Path('/nonexistent'),self.sid_a,self.host_a['session_id'],
            self.a['allocation_id'],runtime_factory=factory)
        with self.assertRaises(urllib.error.HTTPError) as caught:
            asyncio.run(scheduler.step())
        caught.exception.close()
        self.assertEqual(calls['factory'],0)
        self.assertEqual(calls['install'],0)
        self.assertEqual(calls['close'],0)
        counts,confirmed,balance=self.master_counts()
        self.assertEqual(counts['usage_multi_agent_configurations'],1)
        self.assertEqual(counts['usage_first_total_lifecycle'],0)
        self.assertEqual((confirmed,balance),(7,22))


if __name__=='__main__':
    unittest.main()
