import asyncio
import copy
import json
import os
from pathlib import Path
import socket
import tempfile
import unittest
from unittest import mock
import urllib.error
import uuid

from core import Error, Registry
from execution_agent import cycle
from node_config import digest, profile, validate_snapshot, XRAY_SCHEMA
from xray_runtime import configuration, XrayRuntime
from test_runtime import free_port
import test_core

VLESS = dict(protocol='vless', listen_host='127.0.0.1', listen_port=24500, enabled=True)
BINARY = Path(os.environ.get('XRAY_BINARY', Path(__file__).parent / 'data/tools/xray-v26.10.21/xray'))


class UserAuthorizationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.registry = Registry(Path(self.temp.name) / 'registry.db')
        self.server = self.registry.save('servers', {'name': 'xray'})['id']
        self.node = self.registry.save('nodes', {'name': 'vless', 'server_id': self.server})['id']
        self.user = self.registry.save('users', {'name': 'test-user'})['id']
        self.token = self.registry.provision_agent(self.server)['token']
        self.registry.configure_node(self.node, VLESS)

    def tearDown(self): self.temp.cleanup()

    def test_grant_is_idempotent_and_persists_with_scoped_credentials(self):
        credential = self.registry.authorize_node_user(self.node, self.user)
        before = self.registry.configuration(self.token)
        self.assertEqual(before['schema'], XRAY_SCHEMA)
        validate_snapshot(before)
        self.assertEqual(before['nodes'][0]['clients'], [credential])
        self.assertEqual(self.registry.authorize_node_user(self.node, self.user), credential)
        self.assertEqual(self.registry.configuration(self.token), before)
        self.assertNotIn('uuid', self.registry.list('users')[0])
        self.assertEqual(Registry(self.registry.path).configuration(self.token), before)
        self.assertEqual(configuration(before)['inbounds'][0]['settings']['clients'][0]['id'], credential['uuid'])

    def test_revoke_and_user_delete_update_all_affected_server_versions(self):
        original = self.registry.authorize_node_user(self.node, self.user)
        self.registry.authorize_node_user(self.node, self.user, revoke=True)
        self.assertEqual(self.registry.configuration(self.token)['nodes'][0]['clients'], [])
        replacement = self.registry.authorize_node_user(self.node, self.user)
        self.assertNotEqual(original['uuid'], replacement['uuid'])
        before = self.registry.configuration(self.token)
        second=self.registry.save('servers',{'name':'second'})['id']
        second_node=self.registry.save('nodes',{'name':'second-node','server_id':second})['id']
        second_token=self.registry.provision_agent(second)['token']
        self.registry.configure_node(second_node,VLESS)
        self.registry.authorize_node_user(second_node,self.user)
        second_before=self.registry.configuration(second_token)
        self.registry.delete('users', self.user)
        after = self.registry.configuration(self.token)
        self.assertGreater(after['revision'], before['revision'])
        self.assertEqual(after['nodes'][0]['clients'], [])
        second_after=self.registry.configuration(second_token)
        self.assertGreater(second_after['revision'],second_before['revision'])
        self.assertEqual(second_after['nodes'][0]['clients'],[])

    def test_unknown_user_tcp_grants_and_transport_change_are_rejected(self):
        with self.assertRaises(Error): self.registry.authorize_node_user(self.node, 999)
        with self.assertRaises(Error): self.registry.authorize_node_user(self.node, self.user, revoke=True)
        tcp = dict(listen_host='127.0.0.1', listen_port=24501, target_host='127.0.0.1', target_port=80, enabled=True)
        self.registry.authorize_node_user(self.node, self.user)
        with self.assertRaises(Error): self.registry.configure_node(self.node, tcp)
        self.registry.authorize_node_user(self.node, self.user, revoke=True)
        self.registry.configure_node(self.node, tcp)
        with self.assertRaises(Error): self.registry.authorize_node_user(self.node, self.user)

    def test_public_cleartext_listeners_and_bad_clients_rejected(self):
        for fields in ({'listen_host': '0.0.0.0'}, {'listen_host': '::'}, {'listen_port': True}, {'enabled': 1}):
            with self.subTest(fields=fields), self.assertRaises(ValueError): profile({**VLESS, **fields})
        credential = self.registry.authorize_node_user(self.node, self.user)
        before = self.registry.configuration(self.token)
        bad = copy.deepcopy(before)
        bad['nodes'][0]['clients'].append(credential)
        bad['digest'] = digest({k: v for k, v in bad.items() if k != 'digest'})
        with self.assertRaises(ValueError): validate_snapshot(bad)


class UserAuthorizationHttpTests(unittest.TestCase):
    setUp = test_core.ApiTests.setUp
    tearDown = test_core.ApiTests.tearDown
    request = test_core.ApiTests.request

    def test_user_grants_require_admin_and_do_not_leak_through_user_list(self):
        registry=self.server.registry
        server=registry.save('servers',{'name':'fixture'})['id']
        node=registry.save('nodes',{'name':'vless','server_id':server})['id']
        user=registry.save('users',{'name':'fixture-user'})['id']
        registry.configure_node(node,VLESS)
        path=f'/api/nodes/{node}/users/{user}'
        with self.assertRaises(urllib.error.HTTPError) as caught: self.request(path,'POST',auth=False)
        self.assertEqual(caught.exception.code,401)
        credential=json.load(self.request(path,'POST'))
        self.assertEqual(json.load(self.request('/api/users')),[{'id':user,'name':'fixture-user'}])
        self.assertEqual((Path(self.temp.name)/'registry.db').stat().st_mode & 0o777,0o600)
        self.assertIsInstance(credential['uuid'],str)


@unittest.skipUnless(BINARY.is_file(), 'Set XRAY_BINARY to a verified Xray executable for real engine tests')
class XrayRealTests(unittest.IsolatedAsyncioTestCase):
    setUp = test_core.ApiTests.setUp
    tearDown = test_core.ApiTests.tearDown
    request = test_core.ApiTests.request

    async def asyncSetUp(self):
        registry = self.server.registry
        self.sid = registry.save('servers', {'name': 'xray-test'})['id']
        self.nid = registry.save('nodes', {'name': 'vless-test', 'server_id': self.sid})['id']
        self.uid = registry.save('users', {'name': 'client'})['id']
        self.token = registry.provision_agent(self.sid)['token']
        self.port = free_port()
        registry.configure_node(self.nid, {**VLESS, 'listen_port': self.port})
        self.credential = json.load(self.request(f'/api/nodes/{self.nid}/users/{self.uid}', 'POST'))
        self.state = Path(self.temp.name) / 'agent-state'
        async def echo(reader, writer):
            try:
                while data := await reader.read(65536):
                    writer.write(data); await writer.drain()
            except OSError: pass
            finally:
                writer.close()
                try: await writer.wait_closed()
                except OSError: pass
        self.target = await asyncio.start_server(echo, '127.0.0.1', 0)
        self.target_port = self.target.sockets[0].getsockname()[1]
        self.runtime = XrayRuntime(BINARY, Path(self.temp.name) / 'xray-state',
                                  local_test_target=('127.0.0.1',self.target_port))

    async def asyncTearDown(self):
        await self.runtime.close()
        self.target.close(); await self.target.wait_closed()

    async def connect_client(self, credential, payload=b'authenticated data'):
        reader, writer = await asyncio.open_connection('127.0.0.1', self.port)
        # VLESS v0 / UUID / no extensions / TCP / destination port / IPv4 address.
        header = b'\x00' + uuid.UUID(credential).bytes + b'\x00\x01' + self.target_port.to_bytes(2, 'big') + b'\x01\x7f\x00\x00\x01'
        writer.write(header + payload); await writer.drain()
        return reader, writer

    async def assert_accepted(self, credential):
        reader, writer = await self.connect_client(credential)
        try:
            head = await asyncio.wait_for(reader.readexactly(2), 3)
            self.assertEqual(head, b'\x00\x00')
            self.assertEqual(await asyncio.wait_for(reader.readexactly(18), 3), b'authenticated data')
        finally:
            writer.close(); await writer.wait_closed()

    async def assert_rejected(self, credential):
        reader, writer = await self.connect_client(credential)
        try:
            try: data = await asyncio.wait_for(reader.read(100), 2)
            except (asyncio.TimeoutError, ConnectionResetError): data = b''
            self.assertNotIn(b'authenticated data', data)
        finally:
            writer.close()
            try: await writer.wait_closed()
            except OSError: pass

    async def test_real_xray_user_auth_revoke_and_regrant(self):
        await cycle(self.base, self.token, self.state, self.runtime, 1)
        await self.assert_accepted(self.credential['uuid'])
        await self.assert_rejected(str(uuid.uuid4()))
        old_process = self.runtime.process
        self.request(f'/api/nodes/{self.nid}/users/{self.uid}', 'DELETE').close()
        await cycle(self.base, self.token, self.state, self.runtime, 2)
        self.assertIsNotNone(old_process.returncode)
        await self.assert_rejected(self.credential['uuid'])
        replacement = json.load(self.request(f'/api/nodes/{self.nid}/users/{self.uid}', 'POST'))
        await cycle(self.base, self.token, self.state, self.runtime, 3)
        await self.assert_accepted(replacement['uuid'])
        await self.assert_rejected(self.credential['uuid'])
        self.assertTrue(self.server.registry.agent_status()[0]['listeners_current'])
        self.assertEqual(self.runtime.active_file.stat().st_mode & 0o777, 0o600)

    async def test_busy_port_rolls_back_to_previous_real_process(self):
        await cycle(self.base, self.token, self.state, self.runtime, 1)
        old = self.runtime.snapshot
        with socket.socket() as busy:
            busy.bind(('127.0.0.1', 0)); busy.listen()
            self.server.registry.configure_node(self.nid, {**VLESS, 'listen_port': busy.getsockname()[1]})
            with self.assertRaises(RuntimeError): await cycle(self.base, self.token, self.state, self.runtime, 2)
        self.assertEqual(self.runtime.snapshot, old)
        self.assertFalse(self.server.registry.agent_status()[0]['listeners_current'])
        await self.assert_accepted(self.credential['uuid'])

    async def test_real_xray_large_payload(self):
        await cycle(self.base,self.token,self.state,self.runtime,1)
        payload=bytes(range(256))*4096
        reader,writer=await self.connect_client(self.credential['uuid'],payload)
        try:
            self.assertEqual(await asyncio.wait_for(reader.readexactly(2),3),b'\x00\x00')
            self.assertEqual(await asyncio.wait_for(reader.readexactly(len(payload)),5),payload)
        finally:
            writer.close();await writer.wait_closed()

    async def test_deleted_user_disconnects_active_session_and_disabled_node_stops(self):
        await cycle(self.base,self.token,self.state,self.runtime,1)
        reader,writer=await self.connect_client(self.credential['uuid'])
        try:
            self.assertEqual(await asyncio.wait_for(reader.readexactly(20),3),b'\x00\x00authenticated data')
            self.server.registry.delete('users',self.uid)
            await cycle(self.base,self.token,self.state,self.runtime,2)
            self.assertEqual(await asyncio.wait_for(reader.read(),2),b'')
            await self.assert_rejected(self.credential['uuid'])
            self.server.registry.configure_node(self.nid,{**VLESS,'listen_port':self.port,'enabled':False})
            await cycle(self.base,self.token,self.state,self.runtime,3)
            self.assertIsNone(self.runtime.process)
            with self.assertRaises(OSError): await asyncio.open_connection('127.0.0.1',self.port)
        finally:
            writer.close();await writer.wait_closed()

    async def test_child_crash_restarts_and_revoked_agent_stops_child(self):
        await cycle(self.base, self.token, self.state, self.runtime, 1)
        crashed = self.runtime.process
        crashed.kill(); await crashed.wait()
        await cycle(self.base, self.token, self.state, self.runtime, 2)
        self.assertNotEqual(self.runtime.process.pid, crashed.pid)
        await self.assert_accepted(self.credential['uuid'])
        active = self.runtime.process
        self.server.registry.revoke_agent(self.sid)
        with self.assertRaises(urllib.error.HTTPError): await cycle(self.base, self.token, self.state, self.runtime, 3)
        self.assertIsNotNone(active.returncode)
        with self.assertRaises(OSError): await asyncio.open_connection('127.0.0.1', self.port)

    async def test_revocation_with_failed_activation_never_restores_revoked_user(self):
        await cycle(self.base,self.token,self.state,self.runtime,1)
        previous_snapshot=copy.deepcopy(self.runtime.snapshot)
        active=self.runtime.process
        self.server.registry.authorize_node_user(self.nid,self.uid,revoke=True)
        with socket.socket() as busy:
            busy.bind(('127.0.0.1',0));busy.listen()
            self.server.registry.configure_node(self.nid,{**VLESS,'listen_port':busy.getsockname()[1]})
            with self.assertRaises(RuntimeError): await cycle(self.base,self.token,self.state,self.runtime,2)
        self.assertIsNotNone(active.returncode)
        self.assertIsNone(self.runtime.process)
        self.assertIsNone(self.runtime.snapshot)
        with self.assertRaises(OSError): await asyncio.open_connection('127.0.0.1',self.port)
        with self.assertRaises(ValueError): await self.runtime.apply(previous_snapshot)
        retry=self.server.registry.configuration(self.token)
        await self.runtime.apply(retry)
        self.assertEqual(self.runtime.snapshot['revision'],retry['revision'])

    async def test_failed_xray_config_test_keeps_old_child(self):
        await cycle(self.base, self.token, self.state, self.runtime, 1)
        active = self.runtime.process
        self.server.registry.save('nodes', {'name': 'renamed', 'server_id': self.sid}, self.nid)
        with mock.patch('xray_runtime.configuration', return_value={'inbounds': [{'protocol': 'invalid-protocol'}]}):
            with self.assertRaises(ValueError): await cycle(self.base, self.token, self.state, self.runtime, 2)
        self.assertIs(self.runtime.process, active)
        self.assertIsNone(active.returncode)
        await self.assert_accepted(self.credential['uuid'])


if __name__ == '__main__': unittest.main()
