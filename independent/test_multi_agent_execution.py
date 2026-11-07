"""C7-C isolated chain and real loopback Xray, never a production gate."""
import asyncio
import copy
import sqlite3
import threading
import unittest
import uuid
from http.server import ThreadingHTTPServer
from unittest.mock import patch

from core import Registry,Error
from agent import request
from grant_agent import GrantStageStore
from grant_capabilities import GrantCapabilities,STAGING,STAGE_IDENTITY
from multi_agent_allocation import MultiAgentAllocations
from multi_agent_configuration import MultiAgentConfiguration
from multi_agent_lifecycle import MultiAgentLifecycle,MULTI_EXECUTION
from multi_agent_installation import MultiAgentInstallation,fact_for
from multi_agent_execution import MultiAgentExecution
from multi_agent_test_runtime import MultiAgentTestHarness
from server import Handler
from test_xray import BINARY,VLESS
from test_runtime import free_port
import test_multi_agent_allocation as base

class MultiExecutionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.h=MultiAgentTestHarness()
        self.f=base.MultiAgentAllocationTests()
        self.f.setUp()
        self.f.registry=self.h.registry
        self.r=self.f.start(total=100)
        self.token,self.host=self.f.host(self.r)
        self.r.configure_node(self.host['node_id'],dict(VLESS,listen_host='127.0.0.1',listen_port=free_port()))
        self.a=MultiAgentAllocations(self.r).allocate(self.token,self.f.request(self.host,10))
        self.session=self.host['session_id']
        GrantCapabilities(self.r).negotiate(self.token,dict(session_id=self.session,
            protocols=[STAGING,STAGE_IDENTITY,MULTI_EXECUTION]))
        self.value=MultiAgentConfiguration(self.r).bind(self.token,self.a['allocation_id'],
                                                       session_id=self.session)
        self.body=dict(ledger_kind='multi_allocation',ledger_id=self.a['allocation_id'],
                       session_id=self.session,binding_digest=self.value['digest'])
        self.fact=fact_for(self.value,str(uuid.uuid4()))
        self.l=MultiAgentLifecycle(self.r)
        self.i=MultiAgentInstallation(self.r)
        self.e=MultiAgentExecution(self.r)

    async def asyncTearDown(self):
        await self.h.close(); self.f.tearDown()

    def begin(self): return self.l.advance(self.token,dict(self.body,action='begin'))
    def install(self): return self.i.accept(self.token,dict(fact=self.fact))
    def installed(self): return self.l.advance(self.token,dict(self.body,action='installed'))
    def execute(self): return self.e.record(self.token,dict(fact=self.fact))

    def counts(self):
        names=('usage_multi_configuration_refs','usage_multi_lifecycle',
               'usage_multi_installations','usage_multi_applied','usage_multi_execution')
        with self.r.connect() as db:
            return tuple(db.execute('SELECT count(*) FROM '+name).fetchone()[0] for name in names)

    def invariant(self):
        with self.r.connect() as db:
            db.execute('BEGIN')
            from total_allocation import held
            from total_usage import directions
            return (held(db,self.f.period),directions(db,self.f.period),tuple(
                db.execute('SELECT count(*) FROM '+name).fetchone()[0] for name in (
                'usage_first_total_installations','usage_first_total_applied',
                'usage_first_total_lifecycle','usage_first_total_execution','usage_first_total_meter')))

    async def test_chain_immutable_reopen_no_first_meter_or_hold_change(self):
        before=self.invariant()
        self.begin(); self.install(); self.installed(); response=self.execute()
        self.assertFalse(response['ready_for_execution'])
        with self.r.connect() as db:
            original=tuple(tuple(row) for row in db.execute('SELECT * FROM usage_multi_execution'))
        self.assertEqual(self.execute(),response)
        self.assertEqual(self.counts(),(1,1,1,1,1))
        self.assertEqual(self.invariant(),before)
        self.r=self.h.reopen(); self.e=MultiAgentExecution(self.r)
        self.assertEqual(self.execute(),response)
        with self.r.connect() as db:
            self.assertEqual(tuple(tuple(row) for row in db.execute('SELECT * FROM usage_multi_execution')),original)
        changed=copy.deepcopy(self.fact); changed['execution_id']=str(uuid.uuid4())
        with self.assertRaises(Error): self.e.record(self.token,dict(fact=changed))

    async def test_missing_chain_wrong_kind_and_forged_types_reject(self):
        for call in (self.install,self.installed,self.execute):
            with self.assertRaises(Error): call()
        self.assertEqual(self.counts(),(0,0,0,0,0))
        for changes in ({'ledger_kind':'first_grant'},{'ledger_kind':[]},
                        {'ledger_id':str(uuid.uuid4())},{'session_id':str(uuid.uuid4())},
                        {'binding_digest':'0'*64},{'extra':True}):
            with self.subTest(changes=changes),self.assertRaises((Error,ValueError)):
                self.l.advance(self.token,dict(self.body,action='begin',**changes))
        self.begin()
        for changes in ({'reserved_bytes':True},{'server_id':True},
                        {'baseline_bytes':0},{'execute_until':self.fact['execute_until']+1},
                        {'runtime_digest':'0'*64},{'protocol':'reality'},{'extra':1}):
            fact=dict(self.fact,**changes)
            with self.subTest(changes=changes),self.assertRaises((Error,ValueError)):
                self.i.accept(self.token,dict(fact=fact))
        self.assertEqual(self.counts(),(1,1,0,0,0))

    async def test_storage_failure_install_and_execution_roll_back(self):
        self.begin()
        with self.r.connect() as db:
            db.execute("CREATE TRIGGER fail BEFORE INSERT ON usage_multi_applied BEGIN SELECT RAISE(ABORT,'disk'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.install()
        self.assertEqual(self.counts(),(1,1,0,0,0))
        with self.r.connect() as db: db.execute('DROP TRIGGER fail')
        self.install(); self.installed()
        with self.r.connect() as db:
            db.execute("CREATE TRIGGER fail BEFORE UPDATE ON usage_multi_lifecycle BEGIN SELECT RAISE(ABORT,'disk'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.execute()
        self.assertEqual(self.counts(),(1,1,1,1,0))
        with self.r.connect() as db: db.execute('DROP TRIGGER fail')
        self.execute()

    async def test_offline_retired_session_rotated_token_and_expiry(self):
        self.begin()
        with self.r.connect() as db:
            last=db.execute('SELECT last_seen FROM agents WHERE server_id=?',
                            (self.value['server_id'],)).fetchone()[0]
        with patch('multi_agent_configuration.time.time',return_value=last+45):
            with self.assertRaises(Error): self.install()
        with patch('multi_agent_configuration.time.time',return_value=self.value['valid_until']):
            with self.assertRaises(Error): self.install()
        self.r.begin_session(self.token,dict(session_id=str(uuid.uuid4())))
        with self.assertRaises(Error): self.install()
        self.r.provision_agent(self.value['server_id'])
        with self.assertRaises(Error): self.install()
        self.assertEqual(self.counts(),(1,1,0,0,0))

    async def test_production_registry_and_http_cannot_enable_gate(self):
        ordinary=Registry(self.r.path) # reopening alone never confers fixture gate
        offer=GrantCapabilities(ordinary).negotiate(self.token,dict(session_id=self.session,
            protocols=[STAGING,STAGE_IDENTITY,MULTI_EXECUTION]))
        self.assertNotIn(MULTI_EXECUTION,offer['selected'])
        with self.assertRaises(Error): MultiAgentLifecycle(ordinary).advance(
            self.token,dict(self.body,action='begin'))
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler); server.registry=ordinary
        thread=threading.Thread(target=server.serve_forever,daemon=True); thread.start()
        import urllib.error
        try:
            with self.assertRaises(urllib.error.HTTPError) as caught:
                request('http://127.0.0.1:'+str(server.server_port),self.token,
                        '/agent/multi/lifecycle',dict(self.body,action='begin',enable=True))
            self.assertEqual(caught.exception.code,403); caught.exception.close()
        finally:
            server.shutdown(); thread.join(); server.server_close()
        self.assertEqual(self.counts(),(0,0,0,0,0))

    async def test_same_fixture_identity_cannot_begin_second_allocation(self):
        second=MultiAgentAllocations(self.r).allocate(self.token,self.f.request(self.host,3))
        value=MultiAgentConfiguration(self.r).bind(self.token,second['allocation_id'],
                                                  session_id=self.session)
        self.begin()
        with self.assertRaises(Error):
            self.l.advance(self.token,dict(self.body,ledger_id=second['allocation_id'],
                                          binding_digest=value['digest'],action='begin'))
        self.assertEqual(self.counts(),(1,1,0,0,0))

    async def test_cross_agent_and_policy_change_reject(self):
        other,_=self.f.host(self.r)
        with self.assertRaises(Error):
            self.l.advance(other,dict(self.body,action='begin'))
        self.begin()
        self.f.cycles.set_total_budget(self.f.period,0)
        with self.assertRaises(Error): self.install()
        self.assertEqual(self.counts(),(1,1,0,0,0))

    async def test_double_ledger_id_collision_never_creates_chain(self):
        with self.r.connect() as db:
            db.execute('PRAGMA foreign_keys=OFF')
            db.execute('UPDATE usage_first_total_grants SET grant_id=?',
                       (self.a['allocation_id'],))
        with self.assertRaises(Error): self.begin()
        self.assertEqual(self.counts(),(0,0,0,0,0))

    async def test_external_fixture_address_rejected_before_process_construction(self):
        from multi_agent_test_runtime import configuration
        for host in ('0.0.0.0','::','fixture.example','192.0.2.1'):
            value=copy.deepcopy(self.value); value['node_config']['listen_host']=host
            with self.subTest(host=host),self.assertRaises(Error): configuration(value)
        self.assertEqual(self.h.runtimes,[])

    async def test_begin_insert_failure_rolls_back_reference(self):
        with self.r.connect() as db:
            db.execute("CREATE TRIGGER fail BEFORE INSERT ON usage_multi_lifecycle BEGIN SELECT RAISE(ABORT,'disk'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.begin()
        self.assertEqual(self.counts(),(0,0,0,0,0))

    async def test_execution_duplicate_does_not_reopen_closing_or_blocked(self):
        self.begin(); self.install(); self.installed(); self.execute()
        self.l.advance(self.token,dict(self.body,action='close'))
        with self.assertRaises(Error): self.execute()
        self.l.advance(self.token,dict(self.body,action='block'))
        with self.assertRaises(Error): self.install()
        self.assertEqual(self.counts(),(1,1,1,1,1))

    async def test_http_chain_and_missing_applied_ref_are_not_rebuilt(self):
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler); server.registry=self.r
        thread=threading.Thread(target=server.serve_forever,daemon=True); thread.start()
        url='http://127.0.0.1:'+str(server.server_port)
        try:
            for path,data in (
                ('lifecycle',dict(self.body,action='begin')),
                ('installed',dict(fact=self.fact)),
                ('lifecycle',dict(self.body,action='installed')),
                ('execution',dict(fact=self.fact))):
                response=await asyncio.to_thread(request,url,self.token,'/agent/multi/'+path,data)
                self.assertIs(response['accepted'],True)
                self.assertIs(response['ready_for_execution'],False)
            with self.r.connect() as db:
                db.execute('DELETE FROM usage_multi_execution')
                db.execute('DELETE FROM usage_multi_applied')
            with self.assertRaises(Error): self.execute()
            with self.assertRaises(Error): self.begin()
            with self.r.connect() as db:
                db.execute('PRAGMA foreign_keys=OFF')
                db.execute('DELETE FROM usage_multi_configuration_refs')
            with self.assertRaises(Error): self.install()
            self.assertEqual(self.counts(),(0,1,1,0,0))
        finally:
            server.shutdown(); thread.join(); server.server_close()

    async def live(self):
        store=GrantStageStore(self.h.path.parent/'stop-stage.db',self.value['server_id'],self.session)
        store.stage(self.value,dict(accepted=True,state='ledger_resolved',ledger_kind='multi_allocation',
            ledger_id=self.a['allocation_id'],binding_digest=self.value['digest'],ready_for_execution=False))
        runtime,_=await self.h.install(self.token,store,self.value,BINARY)
        return runtime,store

    async def assert_stopped(self,process):
        self.assertIsNotNone(process.returncode)
        with self.assertRaises(OSError):
            await asyncio.open_connection('127.0.0.1',self.value['node_config']['listen_port'])

    @unittest.skipUnless(BINARY.is_file(),'real Xray required')
    async def test_http_close_after_reopen_stops_before_response_and_block_retry(self):
        runtime,_=await self.live(); process=runtime.process
        self.r=self.h.reopen()
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler); server.registry=self.r
        thread=threading.Thread(target=server.serve_forever,daemon=True); thread.start()
        try:
            for action in ('close','block'):
                result=await asyncio.to_thread(request,'http://127.0.0.1:'+str(server.server_port),
                    self.token,'/agent/multi/lifecycle',dict(self.body,action=action))
                self.assertEqual(result['state'],'closing' if action=='close' else 'blocked')
                await self.assert_stopped(process)
        finally:
            server.shutdown(); thread.join(); server.server_close()

    @unittest.skipUnless(BINARY.is_file(),'real Xray required')
    async def test_block_failure_keeps_busy_then_real_retry_stops(self):
        runtime,_=await self.live(); process=runtime.process
        second=MultiAgentAllocations(self.r).allocate(self.token,self.f.request(self.host,3))
        value=MultiAgentConfiguration(self.r).bind(self.token,second['allocation_id'],session_id=self.session)
        with patch.object(runtime,'close',side_effect=OSError('stop failed')):
            with self.assertRaises(Error):
                await asyncio.to_thread(self.l.advance,self.token,dict(self.body,action='block'))
        self.assertIsNone(process.returncode)
        with self.assertRaises(Error):
            await asyncio.to_thread(self.l.advance,self.token,dict(self.body,
                ledger_id=second['allocation_id'],binding_digest=value['digest'],action='begin'))
        result=await asyncio.to_thread(self.l.advance,self.token,dict(self.body,action='block'))
        self.assertEqual(result['state'],'blocked')
        await self.assert_stopped(process)

    @unittest.skipUnless(BINARY.is_file(),'real Xray required')
    async def test_concurrent_close_block_install_and_begin_no_second_instance(self):
        runtime,store=await self.live(); process=runtime.process
        second=MultiAgentAllocations(self.r).allocate(self.token,self.f.request(self.host,3))
        value=MultiAgentConfiguration(self.r).bind(self.token,second['allocation_id'],session_id=self.session)
        results=await asyncio.gather(
            asyncio.to_thread(self.l.advance,self.token,dict(self.body,action='close')),
            asyncio.to_thread(self.l.advance,self.token,dict(self.body,action='block')),
            self.h.install(self.token,store,self.value,BINARY),
            asyncio.to_thread(self.l.advance,self.token,dict(self.body,action='begin',
                ledger_id=second['allocation_id'],binding_digest=value['digest'])),
            return_exceptions=True)
        self.assertTrue(any(type(item) is dict for item in results))
        self.assertEqual(len(self.h.runtimes),1)
        await self.assert_stopped(process)

    @unittest.skipUnless(BINARY.is_file(),'real Xray required')
    async def test_install_record_failure_stops_actual_process_and_keeps_busy(self):
        with self.r.connect() as db:
            db.execute("CREATE TRIGGER fail BEFORE INSERT ON usage_multi_applied BEGIN SELECT RAISE(ABORT,'disk'); END")
        with self.assertRaises(sqlite3.IntegrityError): await self.live()
        self.assertEqual(len(self.h.runtimes),1)
        runtime=self.h.runtimes[0]
        self.assertIsNone(runtime.process)
        with self.r.connect() as db:
            self.assertEqual(db.execute('SELECT state FROM usage_multi_lifecycle').fetchone()[0],'blocked')
        with self.assertRaises(OSError):
            await asyncio.open_connection('127.0.0.1',self.value['node_config']['listen_port'])

    @unittest.skipUnless(BINARY.is_file(),'real Xray required')
    async def test_real_fixture_loopback_process_persists_chain_no_payload_or_meter(self):
        before=self.invariant()
        store=GrantStageStore(self.h.path.parent/'stage.db',self.value['server_id'],self.session)
        store.stage(self.value,dict(accepted=True,state='ledger_resolved',ledger_kind='multi_allocation',
                    ledger_id=self.a['allocation_id'],binding_digest=self.value['digest'],
                    ready_for_execution=False))
        runtime,response=await self.h.install(self.token,store,self.value,BINARY)
        self.assertIsNone(runtime.process.returncode)
        self.assertEqual(response['state'],'execution_recorded')
        received=[]
        async def target(reader,writer):
            received.append(await reader.read(100))
            writer.close(); await writer.wait_closed()
        destination=await asyncio.start_server(target,'127.0.0.1',0)
        try:
            port=destination.sockets[0].getsockname()[1]
            reader,writer=await asyncio.open_connection('127.0.0.1',self.value['node_config']['listen_port'])
            writer.write(b'\0'+uuid.UUID(self.value['client']['uuid']).bytes+b'\0\1'+
                         port.to_bytes(2,'big')+b'\1\x7f\0\0\1blocked-payload')
            await writer.drain()
            try: await asyncio.wait_for(reader.read(100),1)
            except (OSError,asyncio.TimeoutError): pass
            writer.close(); await writer.wait_closed()
            self.assertEqual(received,[])
        finally:
            destination.close(); await destination.wait_closed()
        self.assertEqual(runtime.configuration_factory(None)['outbounds'],
                         [{'tag':'deny-all','protocol':'blackhole'}])
        self.assertEqual(self.invariant(),before)
        self.assertEqual(self.counts(),(1,1,1,1,1))
        await asyncio.to_thread(self.h.stop_sync,self.value['grant_id'])

if __name__=='__main__': unittest.main()
