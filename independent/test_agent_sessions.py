"""Session changes fence delayed/legacy receipts, independently of uptime."""
import concurrent.futures
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
import uuid
from core import Registry, Error
from agent import begin_session, send, sync
import test_core


class SessionTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.path=Path(self.temp.name)/'registry.db'
        self.registry=Registry(self.path)
        self.sid=self.registry.save('servers',{'name':'fixture'})['id']
        self.token=self.registry.provision_agent(self.sid)['token']
        self.a=str(uuid.uuid4()); self.b=str(uuid.uuid4())
    def tearDown(self): self.temp.cleanup()
    def claim(self,session): return self.registry.begin_session(self.token,{'session_id':session})
    def beat(self,session,uptime=1):
        data=dict(hostname='fixture',uptime=uptime)
        if session is not None: data['session_id']=session
        return self.registry.heartbeat(self.token,data)
    def ack(self,session):
        data={key:self.registry.configuration(self.token)[key] for key in ('revision','digest')}
        if session is not None: data['session_id']=session
        self.registry.acknowledge_configuration(self.token,data)
        self.registry.acknowledge_runtime(self.token,data)
        return data
    def denied(self,operation):
        with self.assertRaises(Error) as caught: operation()
        self.assertEqual(caught.exception.status,403)

    def test_new_session_clears_legacy_and_old_receipts_even_with_longer_uptime(self):
        self.beat(None,1); legacy=self.ack(None)
        self.claim(self.a); self.beat(self.a,20)
        self.assertFalse(self.registry.agent_status()[0]['listeners_current'])
        self.denied(lambda:self.registry.acknowledge_runtime(self.token,legacy))
        old=self.ack(self.a)
        self.claim(self.b); self.beat(self.b,100)
        self.assertFalse(self.registry.agent_status()[0]['listeners_current'])
        self.assertIsNone(self.registry.agent_status()[0]['staged_revision'])
        self.denied(lambda:self.beat(None,101))
        self.denied(lambda:self.beat(self.a,101))
        for method in (self.registry.acknowledge_configuration,self.registry.acknowledge_runtime):
            self.denied(lambda:method(self.token,old))
        self.ack(self.b)
        self.denied(lambda:self.registry.acknowledge_runtime(self.token,old))
        self.assertTrue(self.registry.agent_status()[0]['listeners_current'])

    def test_retired_session_cannot_reclaim_after_master_restart_or_token_rotation(self):
        self.claim(self.a); self.ack(self.a)
        self.claim(self.b); self.ack(self.b)
        self.registry=Registry(self.path)
        self.denied(lambda:self.claim(self.a))
        self.claim(self.b) # network retry is idempotent; does not clear receipts
        self.assertFalse(self.registry.agent_status()[0]['listeners_current'])
        self.beat(self.b,1)
        self.assertTrue(self.registry.agent_status()[0]['listeners_current'])
        self.token=self.registry.provision_agent(self.sid)['token']
        self.denied(lambda:self.claim(self.a))
        self.denied(lambda:self.claim(self.b))
        self.claim(str(uuid.uuid4()))
        self.assertFalse(self.registry.agent_status()[0]['listeners_current'])

    def test_invalid_claim_and_heartbeat_cannot_mutate_current_session(self):
        self.claim(self.a); self.beat(self.a); self.ack(self.a)
        for value in (None,True,1,'',self.a.upper(),'not-a-uuid'):
            with self.subTest(value=value),self.assertRaises(Error): self.claim(value)
        with self.assertRaises(Error):
            self.registry.heartbeat(self.token,dict(hostname='',uptime=.1,session_id=self.b))
        self.assertTrue(self.registry.agent_status()[0]['listeners_current'])
        self.beat(self.a,2)
        with self.assertRaises(Error):
            self.registry.heartbeat(self.token,dict(hostname='fixture',uptime=3,session_id=None))

    def test_concurrent_claims_have_one_current_session_and_other_server_is_untouched(self):
        other=self.registry.save('servers',{'name':'other'})['id']
        token=self.registry.provision_agent(other)['token']
        current=self.registry.configuration(token)
        receipt={k:current[k] for k in ('revision','digest')}
        self.registry.acknowledge_configuration(token,receipt)
        self.registry.acknowledge_runtime(token,receipt)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(self.claim,(self.a,self.b)))
        with self.registry.connect() as db:
            active=db.execute('SELECT session_id FROM agents WHERE server_id=?',(self.sid,)).fetchone()[0]
        retired=self.b if active==self.a else self.a
        self.denied(lambda:self.claim(retired))
        self.ack(active)
        self.assertTrue(all(not row['listeners_current']
                            for row in self.registry.agent_status()))
        self.beat(active,1)
        self.registry.heartbeat(token,dict(hostname='other',uptime=1))
        self.assertTrue(all(row['listeners_current'] for row in self.registry.agent_status()))

    def test_failed_session_switch_rolls_back_history_and_current_receipts(self):
        self.claim(self.a); self.beat(self.a); self.ack(self.a)
        with self.registry.connect() as db:
            db.execute("CREATE TRIGGER block_session BEFORE UPDATE OF session_id ON agents BEGIN SELECT RAISE(ABORT,'fixture failure'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.claim(self.b)
        self.beat(self.a,2)
        self.assertTrue(self.registry.agent_status()[0]['listeners_current'])
        with self.registry.connect() as db:
            self.assertIsNone(db.execute('SELECT 1 FROM agent_sessions WHERE session_id=?',(self.b,)).fetchone())
            db.execute('DROP TRIGGER block_session')
        self.claim(self.b)
        self.assertFalse(self.registry.agent_status()[0]['listeners_current'])

    def test_legacy_database_additive_migration_is_concurrent_and_idempotent(self):
        path=Path(self.temp.name)/'legacy.db'
        with sqlite3.connect(path) as db:
            db.executescript('''CREATE TABLE servers(id INTEGER PRIMARY KEY,name TEXT NOT NULL);
                CREATE TABLE agents(server_id INTEGER PRIMARY KEY REFERENCES servers(id),
                token_hash TEXT NOT NULL UNIQUE,last_seen REAL,uptime REAL,hostname TEXT);''')
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _:Registry(path),range(8)))
        with sqlite3.connect(path) as db:
            self.assertEqual([row[1] for row in db.execute('PRAGMA table_info(agents)')].count('session_id'),1)


class SessionHttpTests(unittest.TestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown
    request=test_core.ApiTests.request

    def test_staging_cli_uses_new_session_and_old_http_receipts_are_rejected(self):
        import subprocess
        import sys
        import urllib.error
        sid=self.server.registry.save('servers',{'name':'http'})['id']
        token=self.server.registry.provision_agent(sid)['token']
        old=str(uuid.uuid4()); begin_session(self.base,token,old)
        send(self.base,token,1,old)
        state=Path(self.temp.name)/'agent-state'
        sync(self.base,token,state,old)
        snapshot=json.loads((state/'staged-config.json').read_text())
        config=Path(self.temp.name)/'agent.json'
        config.write_text(json.dumps(dict(master=self.base,token=token)))
        for _ in range(2):
            result=subprocess.run([sys.executable,str(Path(__file__).with_name('agent.py')),
                                   '--config',str(config),'--state',str(state),'--once'],
                                  capture_output=True,text=True,timeout=10)
            self.assertEqual(result.returncode,0,result.stderr)
            self.assertIn('not running',result.stdout)
            self.assertFalse(self.server.registry.agent_status()[0]['listeners_current'])
        with self.assertRaises(urllib.error.HTTPError) as caught: send(self.base,token,50,old)
        self.assertEqual(caught.exception.code,403)
        from agent import request
        with self.assertRaises(urllib.error.HTTPError) as caught:
            request(self.base,token,'/agent/config/applied',dict(revision=snapshot['revision'],digest=snapshot['digest'],session_id=old))
        self.assertEqual(caught.exception.code,403)


class SessionProcessTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown
    request=test_core.ApiTests.request

    async def test_execution_cli_restart_and_port_failure_keep_receipts_session_scoped(self):
        import asyncio
        import contextlib
        import socket
        import sys
        import urllib.error
        from test_runtime import free_port,snapshot
        registry=self.server.registry
        sid=registry.save('servers',{'name':'process'})['id']
        token=registry.provision_agent(sid)['token']
        nid=registry.save('nodes',{'name':'tcp','server_id':sid})['id']
        async def echo(reader,writer):
            try:
                while data:=await reader.read(1024):
                    writer.write(data); await writer.drain()
            finally:
                writer.close()
                with contextlib.suppress(OSError): await writer.wait_closed()
        target=await asyncio.start_server(echo,'127.0.0.1',0)
        port=free_port()
        registry.configure_node(nid,snapshot(port,target.sockets[0].getsockname()[1])['nodes'][0]['config'])
        state=Path(self.temp.name)/'process-state'
        config=Path(self.temp.name)/'agent.json'
        config.write_text(json.dumps(dict(master=self.base,token=token)))
        processes=[]; writer=None; busy=None
        async def launch():
            process=await asyncio.create_subprocess_exec(sys.executable,str(Path(__file__).with_name('execution_agent.py')),
                '--config',str(config),'--state',str(state),stdout=asyncio.subprocess.DEVNULL,stderr=asyncio.subprocess.DEVNULL)
            processes.append(process)
            return process
        async def until(predicate,seconds=5):
            async def wait():
                while not predicate():
                    self.assertIsNone(processes[-1].returncode,'execution CLI exited unexpectedly')
                    await asyncio.sleep(.05)
            await asyncio.wait_for(wait(),seconds)
        def active_session():
            with registry.connect() as db: return db.execute('SELECT session_id FROM agents WHERE server_id=?',(sid,)).fetchone()[0]
        try:
            first=await launch()
            await until(lambda:registry.agent_status()[0]['listeners_current'])
            old_session=active_session()
            self.assertIsNotNone(old_session)
            reader,writer=await asyncio.open_connection('127.0.0.1',port)
            writer.write(b'before'); await writer.drain()
            self.assertEqual(await asyncio.wait_for(reader.readexactly(6),2),b'before')
            first.terminate(); await asyncio.wait_for(first.wait(),5)
            self.assertEqual(await asyncio.wait_for(reader.read(),2),b'')
            busy=socket.socket(); busy.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
            busy.bind(('127.0.0.1',port)); busy.listen()
            await launch()
            await until(lambda:active_session()!=old_session and registry.agent_status()[0]['staged_revision'] is not None)
            self.assertTrue(registry.agent_status()[0]['online'])
            self.assertFalse(registry.agent_status()[0]['listeners_current'])
            self.assertIsNone(registry.agent_status()[0]['applied_revision'])
            with self.assertRaises(urllib.error.HTTPError) as caught:
                await asyncio.to_thread(begin_session,self.base,token,old_session)
            self.assertEqual(caught.exception.code,403)
            busy.close(); busy=None
            # The CLI intentionally retains its production 15-second retry interval.
            await until(lambda:registry.agent_status()[0]['listeners_current'],20)
            restored_reader,restored_writer=await asyncio.open_connection('127.0.0.1',port)
            try:
                restored_writer.write(b'after'); await restored_writer.drain()
                self.assertEqual(await asyncio.wait_for(restored_reader.readexactly(5),2),b'after')
            finally:
                restored_writer.close(); await restored_writer.wait_closed()
        finally:
            if busy: busy.close()
            for process in processes:
                if process.returncode is None:
                    process.terminate()
                    try: await asyncio.wait_for(process.wait(),5)
                    except asyncio.TimeoutError:
                        process.kill(); await process.wait()
            if writer:
                writer.close()
                with contextlib.suppress(OSError): await writer.wait_closed()
            target.close(); await target.wait_closed()
