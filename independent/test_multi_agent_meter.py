"""C7-D isolated accounting; fixtures are not production execution evidence."""
import asyncio
import json
import sqlite3
import time
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from core import Registry,Error
from multi_agent_meter import MultiAgentMeter
from multi_agent_test_runtime import MultiAgentTestHarness
from multi_agent_allocation import MultiAgentAllocations
from multi_agent_configuration import MultiAgentConfiguration
from multi_agent_lifecycle import MultiAgentLifecycle,MULTI_EXECUTION
from multi_agent_installation import MultiAgentInstallation,fact_for
from multi_agent_execution import MultiAgentExecution
from grant_capabilities import GrantCapabilities,STAGING,STAGE_IDENTITY
from grant_meter import GrantMeter
from total_usage import directions
from total_allocation import held
from usage_accounting import liability,MULTI
from test_xray import VLESS
from test_runtime import free_port
import test_multi_agent_allocation as base


class MultiMeterTests(unittest.TestCase):
    def setUp(self):
        self.h=MultiAgentTestHarness()
        self.f=base.MultiAgentAllocationTests();self.f.setUp()
        self.f.registry=self.h.registry
        self.r=self.f.start(total=100,first_bytes=6)
        self.m=MultiAgentMeter(self.r)
        self.a=self.prepare()

    def tearDown(self):
        asyncio.run(self.h.close());self.f.tearDown()

    def prepare(self,execute=True):
        token,host=self.f.host(self.r)
        self.r.configure_node(host['node_id'],dict(VLESS,listen_host='127.0.0.1',listen_port=free_port()))
        allocation=MultiAgentAllocations(self.r).allocate(token,self.f.request(host,10))
        GrantCapabilities(self.r).negotiate(token,dict(session_id=host['session_id'],protocols=[STAGING,STAGE_IDENTITY,MULTI_EXECUTION]))
        value=MultiAgentConfiguration(self.r).bind(token,allocation['allocation_id'],session_id=host['session_id'])
        body=dict(ledger_kind=MULTI,ledger_id=allocation['allocation_id'],session_id=host['session_id'],binding_digest=value['digest'])
        fact=fact_for(value,str(uuid.uuid4()))
        if execute:
            MultiAgentLifecycle(self.r).advance(token,dict(body,action='begin'))
            MultiAgentInstallation(self.r).accept(token,dict(fact=fact))
            MultiAgentLifecycle(self.r).advance(token,dict(body,action='installed'))
            MultiAgentExecution(self.r).record(token,dict(fact=fact))
        return token,body,fact,value

    def data(self,a=None,seq=1,up=2,down=1):
        _,body,fact,_=a or self.a
        return dict(body,policy_id=fact['policy_id'],execution_id=fact['execution_id'],sequence=seq,upload_bytes=up,download_bytes=down)

    def send(self,a=None,**kw):
        a=a or self.a
        return self.m.report(a[0],self.data(a,**kw))

    def balances(self):
        with self.r.connect() as db:
            db.execute('BEGIN')
            return directions(db,self.f.period),held(db,self.f.period),liability(db,self.f.period)

    def snapshot(self):
        with self.r.connect() as db:
            return tuple(tuple(tuple(row) for row in db.execute('SELECT * FROM '+t)) for t in
                         ('usage_attributed_sources','usage_cycle_accounting_events','usage_multi_meter',
                          'usage_cycle_confirmed_totals','usage_cycle_confirmed_directions'))

    def seed_first(self):
        g=self.f.first
        fact=dict(grant_id=g['grant_id'],server_id=g['server_id'],session_id=self.f.session,
                  revision=1,digest='a'*64,runtime_digest='b'*64,protocol='vless',valid_until=g['valid_until'])
        with self.r.connect() as db:
            db.execute('INSERT INTO usage_first_total_configurations VALUES(?,?,?,?)',(g['grant_id'],'{}','a'*64,time.time()))
            db.execute('INSERT INTO usage_first_total_installations VALUES(?,?)',(g['grant_id'],json.dumps(fact,sort_keys=True,separators=(',',':'))))
            db.execute('INSERT INTO usage_first_total_applied VALUES(?,?,?,?)',(g['grant_id'],1,'a'*64,g['valid_until']))
            db.execute('INSERT INTO usage_first_total_lifecycle VALUES(?,?,?,?,?,?)',(g['grant_id'],g['server_id'],self.f.session,'a'*64,'installed',time.time()))
            fact['execution_id']=str(uuid.uuid4())
            db.execute('INSERT INTO usage_first_total_execution VALUES(?,?,?)',(g['grant_id'],json.dumps(fact,sort_keys=True,separators=(',',':')),time.time()))

    def test_interleaved_two_multi_and_first_conserve_hold(self):
        b=self.prepare();self.seed_first();before=self.balances()
        self.send();self.send(b,up=1,down=3)
        g=self.f.first
        event=dict(grant_id=g['grant_id'],session_id=self.f.session,policy_id=g['policy_id'],sequence=1,upload_bytes=1,download_bytes=1)
        GrantMeter(self.r).report(self.f.token,event)
        self.send(seq=2,up=3,down=2)
        self.send(b,seq=2,up=2,down=3)
        GrantMeter(self.r).report(self.f.token,dict(event,sequence=2,upload_bytes=2))
        after=self.balances()
        self.assertEqual(after,((11,9,0),13,before[2]))
        self.assertEqual(sum(after[0])+after[1],sum(before[0])+before[1])
        self.assertFalse(self.send()['ready_for_execution'])

    def test_duplicates_reopen_expiry_stop_and_no_refund(self):
        self.send();self.send(seq=2,up=3,down=3)
        before=self.snapshot()
        self.m=MultiAgentMeter(self.h.reopen())
        with patch('multi_agent_meter.time.time',return_value=self.a[2]['execute_until']+1):
            retry=self.send();self.assertTrue(retry['duplicate']);self.assertEqual(retry['confirmed_bytes'],6)
            with self.assertRaises(Error):self.send(seq=3,up=4,down=3)
        self.assertEqual(self.snapshot(),before)
        MultiAgentLifecycle(self.r).advance(self.a[0],dict(self.a[1],action='close'))
        self.assertTrue(self.send()['duplicate'])
        with self.assertRaises(Error):self.send(seq=3,up=4,down=3)
        self.assertEqual(self.snapshot(),before)

    def test_policy_lowering_allows_only_existing_hold_conversion(self):
        before=self.balances()
        self.f.cycles.set_total_budget(self.f.period,0)
        self.send()
        self.assertEqual(self.balances()[2],before[2])
        with self.assertRaises(Error):MultiAgentAllocations(self.r).allocate(self.a[0],self.f.request(dict(session_id=self.a[1]['session_id'],node_id=self.a[2]['node_id'],credential=self.a[2]['credential']),1))
        self.assertEqual(self.balances()[2],before[2])

    def test_identity_types_monotonic_conflicts_and_missing_execution(self):
        before=self.snapshot()
        for changes in ({'ledger_kind':[]},{'ledger_kind':'first_grant'},{'execution_id':str(uuid.uuid4())},
                        {'binding_digest':'0'*64},{'policy_id':True},{'sequence':True},
                        {'upload_bytes':True},{'download_bytes':-1},{'extra':1}):
            with self.subTest(changes=changes),self.assertRaises(Error):
                self.m.report(self.a[0],dict(self.data(),**changes))
        self.assertEqual(self.snapshot(),before)
        missing=self.prepare(False)
        with self.assertRaises(Error):self.send(missing)
        self.send();before=self.snapshot()
        for kw in (dict(up=1,down=2),dict(seq=2,up=1,down=3),dict(seq=2,up=6,down=5),dict(seq=0)):
            with self.subTest(kw=kw),self.assertRaises(Error):self.send(**kw)
        self.assertEqual(self.snapshot(),before)

    def test_atomic_failures_at_every_write_and_second_connection(self):
        for table,action in (('usage_cycle_accounting_events','INSERT'),('usage_attributed_sources','UPDATE'),
                             ('usage_cycle_confirmed_totals','UPDATE'),('usage_cycle_confirmed_directions','UPDATE'),('usage_multi_meter','INSERT')):
            before=self.snapshot()
            with self.r.connect() as db:
                db.execute('CREATE TRIGGER fail BEFORE '+action+' ON '+table+" BEGIN SELECT RAISE(ABORT,'fixture'); END")
            with self.assertRaises(sqlite3.IntegrityError):self.send()
            self.assertEqual(self.snapshot(),before)
            with self.r.connect() as db:db.execute('DROP TRIGGER fail')
        self.send()

    def test_concurrent_duplicate_and_conflicting_reports(self):
        with ThreadPoolExecutor(4) as pool:rows=list(pool.map(lambda _:self.send(),range(4)))
        self.assertEqual(sum(not r['duplicate'] for r in rows),1)
        before=self.balances()
        def send(up):
            try:return self.send(seq=2,up=up,down=2)['accepted']
            except Error:return False
        with ThreadPoolExecutor(2) as pool:rows=list(pool.map(send,[3,4]))
        self.assertEqual(sum(rows),1);self.assertEqual(self.balances()[2],before[2])

    def test_clock_and_deadline_boundaries(self):
        for now in (self.a[2]['issued_at']-1,self.a[2]['execute_until'],self.a[2]['execute_until']+1):
            with patch('multi_agent_meter.time.time',return_value=now),self.assertRaises(Error):self.send()
        self.send()
        with patch('multi_agent_meter.time.time',return_value=self.a[2]['issued_at']-1):
            self.assertTrue(self.send()['duplicate'])
            with self.assertRaises(Error):self.send(seq=2,up=3)

    def test_gate_and_cross_agent_session_rotation(self):
        before=self.snapshot()
        with self.assertRaises(Error):MultiAgentMeter(Registry(self.r.path)).report(self.a[0],self.data())
        self.assertEqual(self.snapshot(),before)
        other=self.prepare()
        with self.assertRaises(Error):self.m.report(other[0],self.data())
        before=self.snapshot()
        self.r.begin_session(self.a[0],dict(session_id=str(uuid.uuid4())))
        with self.assertRaises(Error):self.send()
        self.assertEqual(self.snapshot(),before)

    def test_second_connection_never_observes_partial_conversion(self):
        before=self.snapshot()
        from multi_agent_meter import accept_attributed as original
        def observe(*args):
            original(*args)
            self.assertEqual(self.snapshot(),before)
        with patch('multi_agent_meter.accept_attributed',side_effect=observe):self.send()
        self.assertNotEqual(self.snapshot(),before)

    def test_pure_ledger_legacy_and_two_multi_concurrent_events(self):
        # Pure accounting fixture only: does not bypass the sealed production
        # legacy endpoint or claim that ordinary legacy growth is authorized.
        b=self.prepare();before=self.balances()
        def legacy():
            from usage_accounting import accept_legacy
            with self.r.connect() as db:
                db.execute('BEGIN IMMEDIATE')
                accept_legacy(db,self.f.period,server_id=self.f.first['server_id'],
                    session_id=self.f.session,sequence=900,node_id=self.f.nid,
                    user_id=self.f.uid,credential=self.f.item['credential'],
                    upload=1,download=2,accepted_at=time.time())
        with ThreadPoolExecutor(3) as pool:
            futures=[pool.submit(self.send),pool.submit(self.send,b),pool.submit(legacy)]
            for future in futures:future.result()
        self.assertEqual(self.balances(),((9,7,0),20,before[2]+3))
        self.assertTrue(self.send()['duplicate']);self.assertTrue(self.send(b)['duplicate'])
        # The actual authenticated sealed legacy endpoint still rejects growth.
        self.assertTrue(self.f.report()['duplicate'])
        self.f.item['upload_bytes']+=1
        with self.assertRaises(Error):self.f.report(2)

    def test_each_missing_chain_component_blocks_without_writes(self):
        tables=('usage_multi_configuration_refs','usage_multi_installations','usage_multi_applied',
                'usage_multi_execution','usage_multi_agent_configurations')
        # Temporarily remove evidence with FK disabled, then restore byte for
        # byte; each report opens an independent FK-enabled connection.
        for table in tables:
            with self.r.connect() as db:
                db.execute('PRAGMA foreign_keys=OFF')
                rows=db.execute('SELECT * FROM '+table).fetchall()
                db.execute('DELETE FROM '+table)
            before=self.snapshot()
            with self.subTest(table=table),self.assertRaises(Error):self.send()
            self.assertEqual(self.snapshot(),before)
            with self.r.connect() as db:
                db.execute('PRAGMA foreign_keys=OFF')
                for row in rows:db.execute('INSERT INTO '+table+' VALUES('+','.join('?' for _ in row)+')',tuple(row))

    def test_token_rotation_and_offline_reject_without_refund(self):
        self.send();before=self.snapshot();balance=self.balances()
        with self.r.connect() as db:
            last=db.execute('SELECT last_seen FROM agents WHERE server_id=?',(self.a[2]['server_id'],)).fetchone()[0]
        with patch('multi_agent_meter.time.time',return_value=last+45):
            with self.assertRaises(Error):self.send(seq=2,up=3)
        self.r.provision_agent(self.a[2]['server_id'])
        with self.assertRaises(Error):self.send()
        self.assertEqual(self.snapshot(),before);self.assertEqual(self.balances(),balance)

    def test_zero_event_joint_deletion_and_initial_baseline_tamper_block(self):
        self.send(up=0,down=0)
        with self.r.connect() as db:
            db.execute('SAVEPOINT damage')
            db.execute("DELETE FROM usage_cycle_accounting_events WHERE event_kind='multi_allocation'")
            db.execute('DELETE FROM usage_multi_meter')
            from usage_accounting import require_cycle
            with self.assertRaises(Error):require_cycle(db,self.f.period)
            db.execute('ROLLBACK TO damage');db.execute('RELEASE damage')
            db.execute("UPDATE usage_attributed_sources SET initial_sequence=1 WHERE ledger_kind='multi_allocation'")
        with self.assertRaises(Error):self.send()

    def test_real_blackhole_runtime_stops_after_meter_changes_baseline(self):
        from test_xray import BINARY
        from grant_agent import GrantStageStore
        if not BINARY.is_file():self.skipTest('real Xray required')
        token,body,_,value=self.prepare(False)
        stage=GrantStageStore(self.h.path.parent/'meter-stage.db',value['server_id'],value['session_id'])
        stage.stage(value,dict(accepted=True,state='ledger_resolved',ledger_kind=MULTI,
                              ledger_id=body['ledger_id'],binding_digest=value['digest'],ready_for_execution=False))
        runtime,_=asyncio.run(self.h.install(token,stage,value,BINARY))
        with self.r.connect() as db:
            fact=json.loads(db.execute('SELECT fact FROM usage_multi_execution WHERE allocation_id=?',(body['ledger_id'],)).fetchone()[0])
        a=token,body,fact,value
        self.send(a);before=self.balances();process=runtime.process
        self.assertIsNone(process.returncode)
        MultiAgentLifecycle(self.r).advance(token,dict(body,action='close'))
        self.assertIsNotNone(process.returncode)
        import socket
        with self.assertRaises(OSError):socket.create_connection(('127.0.0.1',value['node_config']['listen_port']),timeout=1)
        with self.assertRaises(Error):self.send(a,seq=2,up=3)
        self.assertEqual(self.balances(),before)

    def test_source_event_and_meter_damage_block_without_repair(self):
        self.send()
        for statement in ("UPDATE usage_attributed_sources SET upload_bytes='1' WHERE ledger_kind='multi_allocation'",
                          "UPDATE usage_multi_meter SET upload_bytes=1",
                          "DELETE FROM usage_multi_meter"):
            with self.r.connect() as db:
                db.execute('SAVEPOINT damage')
                db.execute(statement)
                from usage_accounting import require_cycle
                with self.assertRaises(Error):require_cycle(db,self.f.period)
                db.execute('ROLLBACK TO damage');db.execute('RELEASE damage')
        self.assertTrue(self.send()['duplicate'])
