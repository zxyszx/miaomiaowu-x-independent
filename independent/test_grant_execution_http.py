"""Real VLESS/REALITY execution audit, lost replies and stopped cores."""
import asyncio
import json
import sqlite3
import shutil
import ssl
import types
import unittest
import uuid
from unittest.mock import patch

from agent import request
from grant_capabilities import STAGING, INSTALLATION
from grant_configuration import GrantConfiguration
from grant_confirmation import confirm_installed
from grant_execution_client import confirm_execution
from grant_runtime import GrantVlessRuntime
from grant_reality_runtime import GrantRealityRuntime
import test_reality as reality_fixture
import test_grant_reality_runtime as reality_grant_fixture
from test_runtime import free_port
from test_xray import BINARY, VLESS
import test_grant_transport as fixture


@unittest.skipUnless(BINARY.is_file(), 'Set XRAY_BINARY for real grant execution')
class GrantExecutionHTTPTests(unittest.IsolatedAsyncioTestCase):
    async def reality_peer(self):
        peer = reality_fixture.RealityRealTests()
        peer.received = bytearray()
        peer.echo = types.MethodType(reality_grant_fixture.GrantRealityRealTests.echo, peer)
        await peer.asyncSetUp()
        self.addAsyncCleanup(peer.asyncTearDown)
        await peer.runtime.close()
        self.start_http(offer=[STAGING, INSTALLATION])
        self.r.configure_node(self.nid, peer.config)
        await asyncio.to_thread(self.fetch)
        value = GrantConfiguration(self.r).bind(self.token, self.g['grant_id'])
        peer.credential = value['client']['uuid']
        def make():
            return GrantRealityRuntime(BINARY, self.store.path.parent/'execution-reality',
                self.store, local_test_target=('127.0.0.1', peer.target_port),
                reality_test_target=('127.0.0.1', peer.decoy.sockets[0].getsockname()[1]))
        peer.runtime = make()
        await peer.runtime.install(value)
        await confirm_installed(peer.runtime, self.url, self.token)
        for action in ('begin', 'installed'):
            await asyncio.to_thread(request, self.url, self.token,
                '/agent/grants/lifecycle', dict(action=action,
                    grant_id=value['grant_id'], session_id=value['session_id']))
        return peer, value, make

    @unittest.skipUnless(shutil.which('openssl') and ssl.HAS_TLSv1_3,
                         'OpenSSL CLI and TLS 1.3 required')
    async def test_reality_payload_lost_execution_reply_reopens_without_refund(self):
        peer, value, make = await self.reality_peer()
        reader, writer = await peer.client()
        try:
            writer.write(b'abcd')
            await writer.drain()
            self.assertEqual(await asyncio.wait_for(reader.read(), 4), b'ab')
        finally:
            writer.close()
            await writer.wait_closed()
        seen = []
        def lost(*args):
            result = request(*args)
            seen.append(result)
            raise OSError('REALITY audit response lost after commit')
        with patch('agent.request', side_effect=lost):
            with self.assertRaises(OSError):
                await confirm_execution(peer.runtime, self.url, self.token)
        self.assertIsNone(peer.runtime.front.process)
        self.assertIsNone(peer.runtime.paced.core.process)
        peer.runtime = make()
        await peer.runtime.install(value)
        result = await confirm_execution(peer.runtime, self.url, self.token)
        self.assertEqual(result, seen[0])
        self.assertFalse(result['ready_for_execution'])
        self.assertEqual(peer.runtime.paced.grant_quota.snapshot()['remaining_bytes'], 0)
        reader, writer = await peer.client()
        try:
            writer.write(b'xx')
            await writer.drain()
            self.assertEqual(await asyncio.wait_for(reader.read(), 4), b'')
        finally:
            writer.close()
            await writer.wait_closed()
        self.assertEqual(peer.received, b'abcd')
        with self.r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_first_total_execution').fetchone()[0], 1)
        with sqlite3.connect(self.store.path) as db:
            saved = db.execute('SELECT receipt FROM local_grant_execution_claims').fetchone()[0]
            self.assertEqual(json.loads(saved), result)

    @unittest.skipUnless(shutil.which('openssl') and ssl.HAS_TLSv1_3,
                         'OpenSSL CLI and TLS 1.3 required')
    async def test_reality_stopped_front_cannot_claim_execution(self):
        await self.stopped_reality_component('front')

    @unittest.skipUnless(shutil.which('openssl') and ssl.HAS_TLSv1_3,
                         'OpenSSL CLI and TLS 1.3 required')
    async def test_reality_stopped_metered_core_cannot_claim_execution(self):
        await self.stopped_reality_component('paced')

    async def stopped_reality_component(self, component):
        peer, _, _ = await self.reality_peer()
        core = peer.runtime.front if component == 'front' else peer.runtime.paced.core
        core.process.kill()
        await core.process.wait()
        with patch('agent.request') as send:
            with self.assertRaises(OSError):
                await confirm_execution(peer.runtime, self.url, self.token)
            send.assert_not_called()
        with self.r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_first_total_execution').fetchone()[0], 0)

    async def test_actual_payload_lost_audit_reply_and_reopen_exact_retry(self):
        self.start_http(offer=[STAGING, INSTALLATION])
        received = bytearray()
        async def echo(reader, writer):
            try:
                data = await reader.readexactly(4)
                received.extend(data)
                writer.write(data[:2])
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
        target = await asyncio.start_server(echo, '127.0.0.1', 0)
        target_port = target.sockets[0].getsockname()[1]
        port = free_port()
        self.r.configure_node(self.nid, dict(VLESS, listen_port=port))
        await asyncio.to_thread(self.fetch)
        value = GrantConfiguration(self.r).bind(self.token, self.g['grant_id'])
        def make():
            return GrantVlessRuntime(BINARY, self.store.path.parent/'execution-runtime',
                self.store, local_test_target=('127.0.0.1', target_port))
        runtime = make()
        try:
            await runtime.install(value)
            await confirm_installed(runtime, self.url, self.token)
            for action in ('begin', 'installed'):
                result = await asyncio.to_thread(request, self.url, self.token,
                    '/agent/grants/lifecycle', dict(action=action,
                        grant_id=value['grant_id'], session_id=value['session_id']))
                self.assertTrue(result['accepted'])
            reader, writer = await asyncio.open_connection('127.0.0.1', port)
            try:
                header = (b'\0'+uuid.UUID(value['client']['uuid']).bytes+b'\0\1'+
                    target_port.to_bytes(2, 'big')+b'\1\x7f\0\0\1')
                writer.write(header+b'abcd')
                await writer.drain()
                self.assertEqual(await asyncio.wait_for(reader.readexactly(2), 3), b'\0\0')
                self.assertEqual(await asyncio.wait_for(reader.read(), 3), b'ab')
            finally:
                writer.close()
                await writer.wait_closed()
            seen = []
            def lost(*args):
                response = request(*args)
                seen.append(response)
                raise OSError('audit response lost after master commit')
            with patch('agent.request', side_effect=lost):
                with self.assertRaises(OSError):
                    await confirm_execution(runtime, self.url, self.token)
            self.assertFalse(runtime.listeners)
            self.assertEqual(received, b'abcd')
            with self.r.connect() as db:
                self.assertEqual(db.execute('SELECT count(*) FROM usage_first_total_execution').fetchone()[0], 1)
            runtime = make()
            await runtime.install(value)
            response = await confirm_execution(runtime, self.url, self.token)
            self.assertEqual(response, seen[0])
            self.assertFalse(response['ready_for_execution'])
            self.assertEqual(runtime.grant_quota.snapshot()['remaining_bytes'], 0)
            with sqlite3.connect(self.store.path) as db:
                fact, saved = db.execute('SELECT fact,receipt FROM local_grant_execution_claims').fetchone()
                self.assertEqual(json.loads(fact), response['execution'])
                self.assertEqual(json.loads(saved), response)
        finally:
            await runtime.close()
            target.close()
            await target.wait_closed()

for name in ('setUp', 'tearDown', 'fixtures', 'fixture', 'setup_total', 'setup_drain',
             'applied', 'report', 'data', 'envelope', 'accept', 'sealed', 'request',
             'prepare', 'start', 'start_http', 'fetch', 'post', 'rejected'):
    setattr(GrantExecutionHTTPTests, name, getattr(fixture.GrantTransportTests, name))
GrantExecutionHTTPTests.encode = staticmethod(fixture.GrantTransportTests.encode)
