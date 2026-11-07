import asyncio
import copy
from pathlib import Path
import time
import unittest
from unittest import mock

from bandwidth import BytePacer, CombinedPacer
from core import Registry
from execution_agent import cycle
from forward_runtime import ForwardRuntime
from node_config import digest, profile
import test_core
import test_runtime


def limited(port, target, upload=None, download=None, revision=1):
    value = test_runtime.snapshot(port, target, revision)
    value['nodes'][0]['config']['bandwidth'] = dict(upload_bps=upload, download_bps=download)
    value['digest'] = digest({k:v for k,v in value.items() if k != 'digest'})
    return value


class BandwidthValidationTests(unittest.TestCase):
    def test_invalid_rates_and_xray_rules_are_not_silently_accepted(self):
        config = limited(24001, 24002)['nodes'][0]['config']
        for bad in (True, 0, -1, 1.5, '100', 1073741825):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                profile({**config, 'bandwidth':dict(upload_bps=bad, download_bps=None)})
        for bad in ({}, {'upload_bps':1}, {'upload_bps':1,'download_bps':2,'other':3}):
            with self.assertRaises(ValueError): profile({**config,'bandwidth':bad})
        with self.assertRaises(ValueError):
            profile(dict(protocol='vless',listen_host='127.0.0.1',listen_port=24001,
                         enabled=True,bandwidth=config['bandwidth']))
        self.assertEqual(profile(config), config)


class CombinedAllowanceTests(unittest.IsolatedAsyncioTestCase):
    async def test_buffered_unlimited_writes_yield_between_blocks(self):
        class Sink:
            count=0
            def write(self,data): self.count+=1
            async def drain(self): pass
        for pacer in (BytePacer(None),CombinedPacer(BytePacer(None))):
            with self.subTest(pacer=type(pacer).__name__):
                sink=Sink(); observed=[]
                async def transfer():
                    for _ in range(32): await pacer.write(sink,b'x')
                task=asyncio.create_task(transfer())
                asyncio.get_running_loop().call_soon(lambda:observed.append(sink.count))
                await task
                self.assertEqual(sink.count,32)
                self.assertEqual(observed,[1])

    async def test_unlimited_allowances_deliver_without_scheduling_a_rate_wait(self):
        class Sink:
            data=b''
            drained=False
            def write(self,data): self.data+=data
            async def drain(self): self.drained=True
        sink=Sink()
        with mock.patch('bandwidth.asyncio.sleep',new_callable=mock.AsyncMock) as pause:
            await CombinedPacer(BytePacer(None),BytePacer(None)).write(sink,b'payload')
            pause.assert_awaited_once_with(0)
        self.assertEqual(sink.data,b'payload')
        self.assertTrue(sink.drained)

    async def test_independent_allowances_wait_together_and_both_gate_delivery(self):
        class Sink:
            data=b''
            def write(self,data): self.data+=data
            async def drain(self): pass
        for rates in ((100,100),(100,200),(200,100)):
            with self.subTest(rates=rates):
                entered=[asyncio.Event(),asyncio.Event()]
                released=[asyncio.Event(),asyncio.Event()]
                waits=[]
                async def waiting(delay):
                    index=len(waits); waits.append(delay)
                    entered[index].set()
                    await released[index].wait()
                sink=Sink()
                pacer=CombinedPacer(*(BytePacer(rate) for rate in rates))
                with mock.patch('bandwidth.asyncio.sleep',side_effect=waiting):
                    task=asyncio.create_task(pacer.write(sink,b'x'))
                    try:
                        # Both allowances must begin waiting before either is
                        # released: serial charging would deadlock this barrier.
                        await asyncio.wait_for(asyncio.gather(*(e.wait() for e in entered)),2)
                        self.assertCountEqual(waits,[1/rate for rate in rates])
                        self.assertEqual(sink.data,b'')
                        released[0].set()
                        await entered[1].wait()
                        self.assertEqual(sink.data,b'')
                        released[1].set()
                        await asyncio.wait_for(task,2)
                        self.assertEqual(sink.data,b'x')
                    finally:
                        task.cancel()
                        await asyncio.gather(task,return_exceptions=True)


class BandwidthRuntimeTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_runtime.RuntimeTests.asyncSetUp
    asyncTearDown = test_runtime.RuntimeTests.asyncTearDown
    upstream = test_runtime.RuntimeTests.upstream
    exchange = test_runtime.RuntimeTests.exchange

    async def timed_exchange(self, port, payload):
        started = time.monotonic()
        self.assertEqual(await self.exchange(port,payload),payload)
        return time.monotonic()-started

    async def test_each_direction_enforces_real_byte_rate(self):
        target,port = await self.upstream(),test_runtime.free_port()
        for revision,direction in enumerate(('upload','download'),1):
            await self.runtime.apply(limited(port,target,revision=revision,**{direction:32768}))
            elapsed = await self.timed_exchange(port,b'x'*16384)
            self.assertGreaterEqual(elapsed,.48)
            self.assertLess(elapsed,2.5)

    async def test_multiple_connections_share_one_allowance(self):
        target,port = await self.upstream(),test_runtime.free_port()
        for revision,direction in enumerate(('upload','download'),1):
            await self.runtime.apply(limited(port,target,revision=revision,**{direction:32768}))
            started = time.monotonic()
            results=await asyncio.gather(*(self.exchange(port,b'x'*16384) for _ in range(2)))
            self.assertEqual(results,[b'x'*16384]*2)
            elapsed = time.monotonic()-started
            self.assertGreaterEqual(elapsed,.95)
            self.assertLess(elapsed,2.8)

    async def test_upload_and_download_have_independent_allowances(self):
        # A server sends immediately, independently of the uploaded request.
        received = asyncio.get_running_loop().create_future()
        payload=b'd'*16384
        async def duplex(reader,writer):
            try:
                writer.write(payload); await writer.drain()
                data=await reader.readexactly(len(payload))
                if not received.done(): received.set_result(data)
            finally:
                writer.close(); await writer.wait_closed()
        target=await asyncio.start_server(duplex,'127.0.0.1',0)
        self.upstreams.append(target)
        port=test_runtime.free_port()
        await self.runtime.apply(limited(port,target.sockets[0].getsockname()[1],upload=32768,download=32768))
        reader,writer=await asyncio.open_connection('127.0.0.1',port)
        started=time.monotonic()
        try:
            writer.write(payload); await writer.drain()
            result=await asyncio.wait_for(asyncio.gather(reader.readexactly(len(payload)),received),2)
            self.assertEqual(result,[payload,payload])
            elapsed=time.monotonic()-started
            self.assertGreaterEqual(elapsed,.48)
            self.assertLess(elapsed,.95)
        finally:
            writer.close(); await writer.wait_closed()

    async def test_rule_change_disconnects_old_flow_and_unlimited_removes_delay(self):
        target,port = await self.upstream(),test_runtime.free_port()
        await self.runtime.apply(limited(port,target,upload=1024))
        reader,writer=await asyncio.open_connection('127.0.0.1',port)
        writer.write(b'x'*16384); await writer.drain()
        await asyncio.sleep(.08)
        await self.runtime.apply(limited(port,target,revision=2))
        self.assertLess(len(await asyncio.wait_for(reader.read(),1)),16384)
        writer.close(); await writer.wait_closed()
        self.assertLess(await self.timed_exchange(port,b'x'*16384),.4)

    async def test_nodes_do_not_share_allowances(self):
        target=await self.upstream()
        port,other=test_runtime.free_port(),test_runtime.free_port()
        value=limited(port,target,upload=32768)
        second=copy.deepcopy(value['nodes'][0]); second['id']=2
        second['config']['listen_port']=other
        value['nodes'].append(second)
        value['digest']=digest({k:v for k,v in value.items() if k!='digest'})
        await self.runtime.apply(value)
        started=time.monotonic()
        await asyncio.gather(self.exchange(port,b'x'*16384),self.exchange(other,b'y'*16384))
        self.assertLess(time.monotonic()-started,.95)

    async def test_cancelled_pacing_releases_waiters_without_future_debt(self):
        class Sink:
            def __init__(self): self.data=b''
            def write(self,data): self.data+=data
            async def drain(self): pass
        sink,pacer=Sink(),BytePacer(100)
        task=asyncio.create_task(pacer.write(sink,b'x'*100))
        await asyncio.sleep(.02)
        task.cancel()
        await asyncio.gather(task,return_exceptions=True)
        started=time.monotonic()
        await asyncio.wait_for(pacer.write(sink,b'ok'),.3)
        self.assertLess(time.monotonic()-started,.3)
        self.assertEqual(sink.data,b'ok')


class BandwidthHttpTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown
    request=test_core.ApiTests.request

    async def test_http_rules_survive_database_and_execution_restart(self):
        async def echo(reader,writer):
            try:
                while data:=await reader.read(65536):
                    writer.write(data); await writer.drain()
            finally:
                writer.close(); await writer.wait_closed()
        target=await asyncio.start_server(echo,'127.0.0.1',0)
        runtime=ForwardRuntime()
        registry=self.server.registry
        server=registry.save('servers',{'name':'limited'})['id']
        node=registry.save('nodes',{'name':'limited','server_id':server})['id']
        token=registry.provision_agent(server)['token']
        port=test_runtime.free_port()
        config=limited(port,target.sockets[0].getsockname()[1],upload=32768)['nodes'][0]['config']
        state=Path(self.temp.name)/'limited-state'
        try:
            with self.request(f'/api/nodes/{node}/config',method='PUT',data=config) as response:
                self.assertEqual(response.status,200)
            persisted=Registry(registry.path).configuration(token)
            self.assertEqual(persisted['nodes'][0]['config'],config)
            for _ in range(2):
                await cycle(self.base,token,state,runtime,1)
                reader,writer=await asyncio.open_connection('127.0.0.1',port)
                started=time.monotonic()
                writer.write(b'x'*16384); await writer.drain(); writer.write_eof()
                self.assertEqual(await asyncio.wait_for(reader.read(),2.5),b'x'*16384)
                self.assertGreaterEqual(time.monotonic()-started,.48)
                writer.close(); await writer.wait_closed()
                self.assertTrue(registry.agent_status()[0]['listeners_current'])
                await runtime.close()
                runtime=ForwardRuntime()
        finally:
            await runtime.close()
            target.close(); await target.wait_closed()
