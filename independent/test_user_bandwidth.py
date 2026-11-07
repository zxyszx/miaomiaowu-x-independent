import asyncio
import copy
import json
from pathlib import Path
import socket
import sys
import time
import unittest
import urllib.error
import uuid

from core import Error, Registry
from execution_agent import cycle
from node_config import digest, validate_snapshot
from vless_runtime import VlessRuntime
from xray_runtime import configuration
import test_core
import test_xray
from test_runtime import free_port


class UserBandwidthApiTests(unittest.TestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown
    request=test_core.ApiTests.request

    def test_admin_validation_idempotence_and_multi_server_revision_persistence(self):
        registry=self.server.registry
        user=registry.save('users',{'name':'limited'})['id']
        path=f'/api/users/{user}/bandwidth'
        with self.assertRaises(urllib.error.HTTPError) as caught: self.request(path,auth=False)
        self.assertEqual(caught.exception.code,401)
        self.assertEqual(json.load(self.request(path)),dict(upload_bps=None,download_bps=None))
        tokens=[]
        for name in ('first','second'):
            server=registry.save('servers',{'name':name})['id']
            node=registry.save('nodes',{'name':name,'server_id':server})['id']
            registry.configure_node(node,test_xray.VLESS)
            registry.authorize_node_user(node,user)
            tokens.append(registry.provision_agent(server)['token'])
        before=[registry.configuration(token) for token in tokens]
        for bad in (None,{},dict(upload_bps=True,download_bps=None),dict(upload_bps=0,download_bps=1)):
            with self.assertRaises(urllib.error.HTTPError): self.request(path,'PUT',bad)
        limits=dict(upload_bps=32768,download_bps=65536)
        self.assertEqual(json.load(self.request(path,'PUT',limits)),limits)
        after=[registry.configuration(token) for token in tokens]
        for old,new in zip(before,after):
            self.assertGreater(new['revision'],old['revision'])
            self.assertEqual(new['nodes'][0]['clients'][0]['bandwidth'],limits)
        json.load(self.request(path,'PUT',limits))
        restored=Registry(registry.path)
        for token,snapshot in zip(tokens,after): self.assertEqual(restored.configuration(token),snapshot)
        self.assertEqual(restored.user_bandwidth(user),limits)
        with self.assertRaises(ValueError): configuration(after[0])
        bad=copy.deepcopy(after[0])
        bad['nodes'][0]['clients'][0]['bandwidth']['upload_bps']=False
        bad['digest']=digest({k:v for k,v in bad.items() if k!='digest'})
        with self.assertRaises(ValueError): validate_snapshot(bad)
        registry.delete('users',user)
        with registry.connect() as db: self.assertEqual(db.execute('SELECT count(*) FROM user_bandwidth').fetchone()[0],0)
        with self.assertRaises(Error): restored.user_bandwidth(user)


@unittest.skipUnless(test_xray.BINARY.is_file(),'Set XRAY_BINARY for real user bandwidth tests')
class UserBandwidthRealTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_xray.XrayRealTests.setUp
    tearDown=test_xray.XrayRealTests.tearDown
    request=test_xray.XrayRealTests.request
    asyncTearDown=test_xray.XrayRealTests.asyncTearDown

    async def asyncSetUp(self):
        await test_xray.XrayRealTests.asyncSetUp(self)
        await self.runtime.close()
        self.runtime=VlessRuntime(test_xray.BINARY,Path(self.temp.name)/'paced-xray',
                                 local_test_target=('127.0.0.1',self.target_port))

    def limits(self,upload=None,download=None,user=None):
        value=dict(upload_bps=upload,download_bps=download)
        self.assertEqual(json.load(self.request(f'/api/users/{user or self.uid}/bandwidth','PUT',value)),value)

    async def sync(self): await cycle(self.base,self.token,self.state,self.runtime,1)

    async def transfer(self,credential,port=None,size=16384):
        reader,writer=await asyncio.open_connection('127.0.0.1',port or self.port)
        payload=b'x'*size
        header=b'\0'+uuid.UUID(credential).bytes+b'\0\1'+self.target_port.to_bytes(2,'big')+b'\1\x7f\0\0\1'
        started=time.monotonic()
        try:
            writer.write(header+payload); await writer.drain()
            self.assertEqual(await asyncio.wait_for(reader.readexactly(2),3),b'\0\0')
            self.assertEqual(await asyncio.wait_for(reader.readexactly(size),4),payload)
            return time.monotonic()-started
        finally:
            writer.close(); await writer.wait_closed()

    async def test_real_upload_and_download_and_removing_limit(self):
        for direction in ('upload','download'):
            self.limits(**{direction:32768})
            await self.sync()
            elapsed=await self.transfer(self.credential['uuid'])
            self.assertGreaterEqual(elapsed,.48)
            self.assertLess(elapsed,2.5)
        self.limits(); await self.sync()
        self.assertLess(await self.transfer(self.credential['uuid']),.4)

    async def test_connections_and_nodes_share_user_allowance(self):
        registry=self.server.registry
        second=registry.save('nodes',{'name':'second-node','server_id':self.sid})['id']
        other_port=free_port()
        registry.configure_node(second,{**test_xray.VLESS,'listen_port':other_port})
        other=registry.authorize_node_user(second,self.uid)
        for direction in ('upload','download'):
            self.limits(**{direction:32768}); await self.sync()
            for ports,credentials in (((self.port,self.port),(self.credential['uuid'],)*2),
                                      ((self.port,other_port),(self.credential['uuid'],other['uuid']))):
                started=time.monotonic()
                await asyncio.gather(*(self.transfer(credential,port) for credential,port in zip(credentials,ports)))
                elapsed=time.monotonic()-started
                self.assertGreaterEqual(elapsed,.95)
                self.assertLess(elapsed,3)

    async def test_distinct_users_have_independent_allowances_and_no_unlimited_leak(self):
        registry=self.server.registry
        other=registry.save('users',{'name':'second'})['id']
        credential=registry.authorize_node_user(self.nid,other)
        self.limits(upload=32768)
        await self.sync()
        limited,unlimited=await asyncio.gather(self.transfer(self.credential['uuid']),self.transfer(credential['uuid']))
        self.assertGreaterEqual(limited,.48)
        self.assertLess(unlimited,.4)
        self.limits(upload=32768,user=other); await self.sync()
        started=time.monotonic()
        await asyncio.gather(self.transfer(self.credential['uuid']),self.transfer(credential['uuid']))
        self.assertLess(time.monotonic()-started,.95)

    async def test_execution_restart_restores_saved_limits_and_crashed_core_recovers(self):
        self.limits(download=32768)
        await cycle(self.base,self.token,self.state,self.runtime,120)
        self.assertGreaterEqual(await self.transfer(self.credential['uuid']),.48)
        await self.runtime.close()
        # Reopen the persisted master registry and use a genuinely fresh runtime
        # object rather than reusing the closed object's cached configuration.
        self.server.registry=Registry(self.server.registry.path)
        self.runtime=VlessRuntime(test_xray.BINARY,Path(self.temp.name)/'paced-xray',
                                 local_test_target=('127.0.0.1',self.target_port))
        from agent import send
        await asyncio.to_thread(send,self.base,self.token,.1)
        self.assertFalse(self.server.registry.agent_status()[0]['listeners_current'])
        self.assertFalse(self.runtime.listeners)
        await self.sync()
        self.assertTrue(self.server.registry.agent_status()[0]['listeners_current'])
        self.assertGreaterEqual(await self.transfer(self.credential['uuid']),.48)
        crashed=self.runtime.core.process
        crashed.kill(); await crashed.wait()
        await self.sync()
        self.assertNotEqual(crashed.pid,self.runtime.core.process.pid)
        self.assertGreaterEqual(await self.transfer(self.credential['uuid']),.48)

    async def test_session_replacement_restores_limits_and_old_session_closes_listener(self):
        first,second=str(uuid.uuid4()),str(uuid.uuid4())
        self.limits(download=32768)
        await cycle(self.base,self.token,self.state,self.runtime,1,first)
        self.assertGreaterEqual(await self.transfer(self.credential['uuid']),.48)
        await self.runtime.close()
        self.runtime=VlessRuntime(test_xray.BINARY,Path(self.temp.name)/'paced-xray',
                                 local_test_target=('127.0.0.1',self.target_port))
        await cycle(self.base,self.token,self.state,self.runtime,100,second)
        self.assertGreaterEqual(await self.transfer(self.credential['uuid']),.48)
        with self.assertRaises(urllib.error.HTTPError) as caught:
            await cycle(self.base,self.token,self.state,self.runtime,101,first)
        self.assertEqual(caught.exception.code,403)
        self.assertFalse(self.runtime.listeners)
        await cycle(self.base,self.token,self.state,self.runtime,102,second)
        self.assertGreaterEqual(await self.transfer(self.credential['uuid']),.48)

    async def test_low_rate_does_not_throttle_protocol_authentication(self):
        self.limits(upload=1); await self.sync()
        elapsed=await self.transfer(self.credential['uuid'],size=1)
        self.assertGreaterEqual(elapsed,.95)
        self.assertLess(elapsed,2.5)

    async def test_non_policy_bind_failure_preserves_previous_limited_listener(self):
        self.limits(upload=32768); await self.sync()
        active=self.runtime.core.process
        before=self.runtime.snapshot
        with socket.socket() as busy:
            busy.bind(('127.0.0.1',0)); busy.listen()
            self.server.registry.configure_node(self.nid,{**test_xray.VLESS,'listen_port':busy.getsockname()[1]})
            with self.assertRaises(OSError): await self.sync()
        self.assertIs(self.runtime.core.process,active)
        self.assertEqual(self.runtime.snapshot,before)
        self.assertFalse(self.server.registry.agent_status()[0]['listeners_current'])
        self.assertGreaterEqual(await self.transfer(self.credential['uuid']),.48)

    async def test_rule_change_disconnects_existing_flow_and_failed_update_stays_closed(self):
        await self.sync()
        previous_snapshot=copy.deepcopy(self.runtime.snapshot)
        reader,writer=await test_xray.XrayRealTests.connect_client(self,self.credential['uuid'])
        try:
            self.assertEqual(await asyncio.wait_for(reader.readexactly(20),3),b'\0\0authenticated data')
            self.limits(upload=32768); await self.sync()
            self.assertEqual(await asyncio.wait_for(reader.read(),1),b'')
            self.assertGreaterEqual(await self.transfer(self.credential['uuid']),.48)
            old=self.runtime.core.process
            self.limits(upload=16384)
            with socket.socket() as busy:
                busy.bind(('127.0.0.1',0)); busy.listen()
                self.server.registry.configure_node(self.nid,{**test_xray.VLESS,'listen_port':busy.getsockname()[1]})
                with self.assertRaises(OSError): await self.sync()
            self.assertIsNotNone(old.returncode)
            self.assertIsNone(self.runtime.snapshot)
            self.assertFalse(self.runtime.listeners)
            with self.assertRaises(OSError): await asyncio.open_connection('127.0.0.1',self.port)
            # A failed restrictive update must not erase the revision fence.
            with self.assertRaises(ValueError): await self.runtime.apply(previous_snapshot)
            retry=self.server.registry.configuration(self.token)
            await self.runtime.apply(retry)
            self.assertEqual(self.runtime.snapshot['revision'],retry['revision'])
        finally:
            writer.close(); await writer.wait_closed()

    async def test_unknown_revoked_and_unsupported_requests_never_pass(self):
        await self.sync()
        for command,identity in ((1,uuid.uuid4().bytes),(2,uuid.UUID(self.credential['uuid']).bytes),
                                 (3,uuid.UUID(self.credential['uuid']).bytes)):
            reader,writer=await asyncio.open_connection('127.0.0.1',self.port)
            try:
                writer.write(b'\0'+identity+b'\0'+bytes([command])+b'\0\x50\1\x7f\0\0\1payload')
                await writer.drain()
                try: response=await asyncio.wait_for(reader.read(),1)
                except ConnectionResetError: response=b''
                self.assertEqual(response,b'')
            finally:
                writer.close()
                try: await writer.wait_closed()
                except OSError: pass
        self.server.registry.authorize_node_user(self.nid,self.uid,revoke=True)
        await self.sync()
        await test_xray.XrayRealTests.assert_rejected(self,self.credential['uuid'])

    connect_client=test_xray.XrayRealTests.connect_client

    async def test_handshake_timeout_and_connection_budget_release(self):
        self.runtime.handshake_timeout=.1
        self.runtime.max_connections=1
        await self.sync()
        reader,writer=await asyncio.open_connection('127.0.0.1',self.port)
        try:
            writer.write(b'\0'); await writer.drain()
            await asyncio.sleep(.03)
            denied,second=await asyncio.open_connection('127.0.0.1',self.port)
            try: self.assertEqual(await asyncio.wait_for(denied.read(),.5),b'')
            finally: second.close(); await second.wait_closed()
            self.assertEqual(await asyncio.wait_for(reader.read(),.5),b'')
        finally:
            writer.close(); await writer.wait_closed()
        await self.transfer(self.credential['uuid'],size=20)
        reader,writer=await asyncio.open_connection('127.0.0.1',self.port)
        try:
            writer.write(b'\0'); await writer.drain()
            await asyncio.sleep(.02)
            await self.runtime.close()
            # Closing an incomplete TCP request can produce either FIN or RST.
            try: self.assertEqual(await asyncio.wait_for(reader.read(),.5),b'')
            except ConnectionResetError: pass
            self.assertFalse(self.runtime.tasks)
        finally:
            writer.close()
            try: await writer.wait_closed()
            except ConnectionResetError: pass

    async def test_conflicting_user_limits_are_rejected_before_starting(self):
        self.limits(upload=32768)
        value=self.server.registry.configuration(self.token)
        second=copy.deepcopy(value['nodes'][0])
        second['id']+=1; second['config']['listen_port']=free_port()
        second['clients'][0]['uuid']=str(uuid.uuid4())
        second['clients'][0]['bandwidth']['upload_bps']=16384
        value['nodes'].append(second)
        value['digest']=digest({k:v for k,v in value.items() if k!='digest'})
        with self.assertRaises(ValueError): await self.runtime.apply(value)
        self.assertFalse(self.runtime.listeners)
        self.assertIsNone(self.runtime.core.process)

    async def test_actual_cli_uses_user_pacing_and_sigterm_cleans_core(self):
        self.limits(upload=32768)
        config=Path(self.temp.name)/'cli.json'
        config.write_text(json.dumps(dict(master=self.base,token=self.token)))
        config.chmod(0o600)
        state=Path(self.temp.name)/'cli-state'
        # The production CLI deliberately has no private-target allow switch.
        # Supply a fixture executable that adds the same exact echo-target
        # exception used by constructor-based tests, then execs the real core.
        binary=Path(self.temp.name)/'fixture-xray'
        binary.write_text('#!'+sys.executable+'\n'+
            'import json, os, sys\n'+
            "path=sys.argv[sys.argv.index('-config')+1]\n"+
            'with open(path) as file: value=json.load(file)\n'+
            "value['outbounds'][0]['settings']['finalRules']=["+
            repr(dict(action='allow',network='tcp',ip=['127.0.0.1/32'],port=str(self.target_port)))+
            ']\n'+
            "with open(path,'w') as file: json.dump(value,file)\n"+
            'os.execv('+repr(str(test_xray.BINARY.resolve()))+','+
            '['+repr(str(test_xray.BINARY.resolve()))+']+sys.argv[1:])\n')
        binary.chmod(0o700)
        process=await asyncio.create_subprocess_exec(sys.executable,
            str(Path(__file__).parent/'execution_agent.py'),'--engine','xray',
            '--xray-binary',str(binary),'--config',str(config),'--state',str(state),
            stdout=asyncio.subprocess.DEVNULL,stderr=asyncio.subprocess.DEVNULL)
        try:
            # The real core has separate 10 s configuration validation and 5 s
            # listener startup budgets. A 5 s end-to-end deadline falsely fails
            # cold fixture interpreters before the runtime itself times out.
            deadline=time.monotonic()+20
            while not self.server.registry.agent_status()[0]['listeners_current']:
                if process.returncode is not None or time.monotonic()>deadline:
                    staged=(state/'staged-config.json').exists()
                    self.fail(f'CLI 未成功启动和回报：exit={process.returncode}, staged={staged}')
                await asyncio.sleep(.05)
            self.assertGreaterEqual(await self.transfer(self.credential['uuid']),.48)
            generated=json.loads(next((state/'xray').glob('*.json')).read_text())
            inner_port=generated['inbounds'][0]['port']
            process.terminate(); await asyncio.wait_for(process.wait(),5)
            self.assertEqual(process.returncode,0)
            for port in (self.port,inner_port):
                with self.assertRaises(OSError): await asyncio.open_connection('127.0.0.1',port)
            self.assertEqual(list((state/'xray').glob('*.json')),[])
        finally:
            if process.returncode is None:
                process.terminate()
                try: await asyncio.wait_for(process.wait(),5)
                except asyncio.TimeoutError: process.kill(); await process.wait()
