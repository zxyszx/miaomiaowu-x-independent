import asyncio
import json
from pathlib import Path
import socket
import unittest
from unittest import mock

from core import Error
from execution_agent import cycle
from forward_runtime import ForwardRuntime
from node_config import SCHEMA, digest
import test_core


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def snapshot(port, target, revision=1, enabled=True):
    value = dict(schema=SCHEMA, server_id=1, server_name='fixture', revision=revision,
                 nodes=[dict(id=1, name='forward', config=dict(listen_host='127.0.0.1',
                     listen_port=port, target_host='127.0.0.1', target_port=target, enabled=enabled))])
    return {**value, 'digest': digest(value)}


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.runtime = ForwardRuntime()
        self.upstreams = []

    async def asyncTearDown(self):
        await self.runtime.close()
        for server in self.upstreams:
            server.close()
            await server.wait_closed()

    async def upstream(self, prefix=b''):
        async def echo(reader, writer):
            try:
                while data := await reader.read(65536):
                    writer.write(prefix + data)
                    await writer.drain()
            except OSError:
                pass
            finally:
                writer.close()
                try: await writer.wait_closed()
                except OSError: pass
        server = await asyncio.start_server(echo, '127.0.0.1', 0)
        self.upstreams.append(server)
        return server.sockets[0].getsockname()[1]

    async def exchange(self, port, data):
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        try:
            writer.write(data)
            await writer.drain()
            writer.write_eof()
            return await asyncio.wait_for(reader.read(), 3)
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_retired_connection_cannot_survive_cancelled_connect_completion(self):
        # Reproduce wait_for's Python 3.9 completion/cancellation race without
        # relying on machine load or a particular socket scheduling order.
        target,port=await self.upstream(),free_port()
        await self.runtime.apply(snapshot(port,target))
        opened=asyncio.Event()
        original_wait=asyncio.wait_for
        async def completing_wait(awaitable,timeout):
            if timeout!=5: return await original_wait(awaitable,timeout)
            result=await awaitable
            opened.set()
            try: await asyncio.Future()
            except asyncio.CancelledError: return result
        writer=None
        try:
            with mock.patch('forward_runtime.asyncio.wait_for',side_effect=completing_wait):
                reader,writer=await asyncio.open_connection('127.0.0.1',port)
                await asyncio.wait_for(opened.wait(),2)
                await asyncio.wait_for(self.runtime.close(),2)
            self.assertEqual(await asyncio.wait_for(reader.read(),2),b'')
            self.assertFalse(self.runtime.listeners)
        finally:
            if writer:
                writer.close(); await writer.wait_closed()

    async def test_real_bidirectional_large_transfer_and_half_close(self):
        target = await self.upstream()
        port = free_port()
        await self.runtime.apply(snapshot(port, target))
        payload = bytes(range(256)) * 4096
        self.assertEqual(await self.exchange(port, payload), payload)

    async def test_target_switch_on_same_listener_and_idempotency(self):
        first, second = await self.upstream(b'first:'), await self.upstream(b'second:')
        port = free_port()
        original = snapshot(port, first)
        await self.runtime.apply(original)
        listener = self.runtime.listeners[('127.0.0.1', port)].server
        self.assertEqual(await self.exchange(port, b'hello'), b'first:hello')
        await self.runtime.apply(original)
        self.assertIs(self.runtime.listeners[('127.0.0.1', port)].server, listener)
        await self.runtime.apply(snapshot(port, second, revision=2))
        self.assertEqual(await self.exchange(port, b'hello'), b'second:hello')
        # Caller mutations cannot change a runtime's owned copy.
        original['nodes'][0]['config']['target_port'] = second
        self.assertEqual(self.runtime.snapshot['revision'], 2)

    async def test_bind_failure_leaves_old_listener_and_revision_working(self):
        target = await self.upstream()
        port = free_port()
        await self.runtime.apply(snapshot(port, target))
        with socket.socket() as busy:
            busy.bind(('127.0.0.1', 0))
            busy.listen()
            occupied = busy.getsockname()[1]
            with self.assertRaises(OSError):
                await self.runtime.apply(snapshot(occupied, target, revision=2))
        self.assertEqual(self.runtime.snapshot['revision'], 1)
        self.assertEqual(await self.exchange(port, b'still works'), b'still works')

    async def test_partial_bind_failure_cleans_new_sockets(self):
        target = await self.upstream()
        port, new_port = free_port(), free_port()
        await self.runtime.apply(snapshot(port, target))
        with socket.socket() as busy:
            busy.bind(('127.0.0.1', 0)); busy.listen()
            value = snapshot(new_port, target, revision=2)
            other = snapshot(busy.getsockname()[1], target)['nodes'][0]
            other['id'] = 2
            value['nodes'].append(other)
            value['digest'] = digest({k: v for k, v in value.items() if k != 'digest'})
            with self.assertRaises(OSError): await self.runtime.apply(value)
        with self.assertRaises(OSError): await asyncio.open_connection('127.0.0.1', new_port)
        self.assertEqual(await self.exchange(port, b'old works'), b'old works')

    async def test_failed_limit_update_keeps_revision_fence_and_can_retry(self):
        target,port=await self.upstream(),free_port()
        previous=snapshot(port,target)
        await self.runtime.apply(previous)
        with socket.socket() as busy:
            busy.bind(('127.0.0.1',0)); busy.listen()
            desired=snapshot(busy.getsockname()[1],target,revision=2)
            desired['bandwidth']={'upload_bps':32768,'download_bps':None}
            desired['digest']=digest({k:v for k,v in desired.items() if k!='digest'})
            with self.assertRaises(OSError): await self.runtime.apply(desired)
        self.assertFalse(self.runtime.listeners)
        with self.assertRaises(ValueError): await self.runtime.apply(previous)
        await self.runtime.apply(desired)
        self.assertEqual(await self.exchange(desired['nodes'][0]['config']['listen_port'],b'retry'),b'retry')

    async def test_failed_node_limit_update_does_not_keep_old_unlimited_listener(self):
        target,port=await self.upstream(),free_port()
        previous=snapshot(port,target)
        await self.runtime.apply(previous)
        with socket.socket() as busy:
            busy.bind(('127.0.0.1',0)); busy.listen()
            desired=snapshot(port,target,revision=2)
            desired['nodes'][0]['config']['bandwidth']={'upload_bps':32768,'download_bps':None}
            other=snapshot(busy.getsockname()[1],target)['nodes'][0]
            other['id']=2
            desired['nodes'].append(other)
            desired['digest']=digest({k:v for k,v in desired.items() if k!='digest'})
            with self.assertRaises(OSError): await self.runtime.apply(desired)
        self.assertFalse(self.runtime.listeners)
        with self.assertRaises(OSError): await asyncio.open_connection('127.0.0.1',port)
        with self.assertRaises(ValueError): await self.runtime.apply(previous)
        await self.runtime.apply(desired)
        self.assertEqual(await self.exchange(port,b'limited retry'),b'limited retry')

    async def test_disable_closes_listener_and_existing_connection(self):
        target = await self.upstream()
        port = free_port()
        await self.runtime.apply(snapshot(port, target))
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        writer.write(b'probe'); await writer.drain()
        self.assertEqual(await reader.readexactly(5), b'probe')
        await self.runtime.apply(snapshot(port, target, revision=2, enabled=False))
        self.assertEqual(await asyncio.wait_for(reader.read(), 2), b'')
        writer.close(); await writer.wait_closed()
        with self.assertRaises(OSError): await asyncio.open_connection('127.0.0.1', port)

    async def test_invalid_and_old_configuration_keeps_running_snapshot(self):
        target = await self.upstream()
        port = free_port()
        good = snapshot(port, target, revision=2)
        await self.runtime.apply(good)
        for bad in (snapshot(port, target, revision=1), dict(good, digest='bad')):
            with self.assertRaises(ValueError): await self.runtime.apply(bad)
        self.assertEqual(await self.exchange(port, b'ok'), b'ok')

    async def test_unreachable_target_closes_connection_without_crashing_listener(self):
        port, missing = free_port(), free_port()
        await self.runtime.apply(snapshot(port, missing))
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        self.assertEqual(await asyncio.wait_for(reader.read(), 2), b'')
        writer.close(); await writer.wait_closed()
        self.assertIn(('127.0.0.1', port), self.runtime.listeners)


class ExecutionHttpTests(unittest.IsolatedAsyncioTestCase):
    setUp = test_core.ApiTests.setUp
    tearDown = test_core.ApiTests.tearDown
    request = test_core.ApiTests.request

    async def test_http_to_agent_to_listener_and_stale_receipt(self):
        async def echo(reader, writer):
            try:
                data = await reader.read(100)
                writer.write(data); await writer.drain()
            finally:
                writer.close(); await writer.wait_closed()
        target = await asyncio.start_server(echo, '127.0.0.1', 0)
        runtime = ForwardRuntime()
        registry = self.server.registry
        server = registry.save('servers', {'name': 'execution'})['id']
        node = registry.save('nodes', {'name': 'tcp', 'server_id': server})['id']
        token = registry.provision_agent(server)['token']
        port = free_port()
        config = snapshot(port, target.sockets[0].getsockname()[1])['nodes'][0]['config']
        registry.configure_node(node, config)
        state = Path(self.temp.name) / 'execution-state'
        try:
            receipt = await cycle(self.base, token, state, runtime, 1)
            self.assertEqual(receipt['state'], 'listening')
            self.assertTrue(registry.agent_status()[0]['listeners_current'])
            reader, writer = await asyncio.open_connection('127.0.0.1', port)
            writer.write(b'real flow'); await writer.drain()
            self.assertEqual(await asyncio.wait_for(reader.read(), 3), b'real flow')
            writer.close(); await writer.wait_closed()
            with mock.patch('core.time.time', return_value=registry.agent_status()[0]['applied_at'] + 46):
                self.assertFalse(registry.agent_status()[0]['listeners_current'])
            registry.configure_node(node, {**config, 'target_port': free_port()})
            self.assertFalse(registry.agent_status()[0]['listeners_current'])
            old = json.loads((state / 'staged-config.json').read_text())
            with self.assertRaises(Error):
                registry.acknowledge_runtime(token, {k: old[k] for k in ('revision', 'digest')})
            registry.revoke_agent(server)
            self.assertIsNone(registry.agent_status()[0]['applied_revision'])
            with self.assertRaises(OSError): await cycle(self.base, token, state, runtime, 2)
            self.assertEqual(runtime.listeners, {})
            with self.assertRaises(OSError): await asyncio.open_connection('127.0.0.1', port)
        finally:
            await runtime.close()
            target.close(); await target.wait_closed()

    async def test_restart_then_failed_apply_does_not_reuse_old_runtime_receipt(self):
        from agent import send
        registry=self.server.registry
        server=registry.save('servers',{'name':'restart'})['id']
        node=registry.save('nodes',{'name':'tcp','server_id':server})['id']
        token=registry.provision_agent(server)['token']
        port=free_port()
        registry.configure_node(node,snapshot(port,free_port())['nodes'][0]['config'])
        state=Path(self.temp.name)/'restart-state'
        old=ForwardRuntime(); restarted=ForwardRuntime()
        try:
            await cycle(self.base,token,state,old,120)
            self.assertTrue(registry.agent_status()[0]['listeners_current'])
            await old.close()
            # The new process reports a reset uptime before attempting apply.
            await asyncio.to_thread(send,self.base,token,.1)
            self.assertFalse(registry.agent_status()[0]['listeners_current'])
            with socket.socket() as busy:
                busy.bind(('127.0.0.1',port)); busy.listen()
                with self.assertRaises(OSError): await cycle(self.base,token,state,restarted,.2)
                status=registry.agent_status()[0]
                self.assertIsNotNone(status['staged_revision'])
                self.assertIsNone(status['applied_revision'])
                self.assertFalse(status['listeners_current'])
                self.assertFalse(restarted.listeners)
            await cycle(self.base,token,state,restarted,.3)
            self.assertTrue(registry.agent_status()[0]['listeners_current'])
            self.assertTrue(restarted.listeners)
        finally:
            await old.close(); await restarted.close()

    async def test_staged_configuration_cannot_claim_listening_before_staged_ack(self):
        registry = self.server.registry
        server = registry.save('servers', {'name': 'fixture'})['id']
        token = registry.provision_agent(server)['token']
        current = registry.configuration(token)
        with self.assertRaises(Error):
            registry.acknowledge_runtime(token, {k: current[k] for k in ('revision', 'digest')})
        self.assertFalse(registry.agent_status()[0]['listeners_current'])


if __name__ == '__main__': unittest.main()
