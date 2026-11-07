import asyncio
import copy
import json
import socket
import time
import unittest
import urllib.error
from core import Registry
from node_config import digest, validate_snapshot
from xray_runtime import configuration
import test_core
import test_runtime
import test_user_bandwidth
import test_xray


class ServerBandwidthApiTests(unittest.TestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown
    request=test_core.ApiTests.request

    def test_auth_persistence_revision_validation_and_server_isolation(self):
        registry=self.server.registry
        first=registry.save('servers',{'name':'first'})['id']
        second=registry.save('servers',{'name':'second'})['id']
        tokens=[registry.provision_agent(s)['token'] for s in (first,second)]
        path=f'/api/servers/{first}/bandwidth'
        with self.assertRaises(urllib.error.HTTPError): self.request(path,auth=False)
        self.assertEqual(json.load(self.request(path)),dict(upload_bps=None,download_bps=None))
        before=[registry.configuration(t) for t in tokens]
        for bad in (None,{},dict(upload_bps=False,download_bps=None),dict(upload_bps=0,download_bps=1)):
            with self.assertRaises(urllib.error.HTTPError): self.request(path,'PUT',bad)
        rule=dict(upload_bps=32768,download_bps=None)
        self.assertEqual(json.load(self.request(path,'PUT',rule)),rule)
        after=[registry.configuration(t) for t in tokens]
        self.assertGreater(after[0]['revision'],before[0]['revision'])
        self.assertEqual(after[1],before[1])
        self.assertEqual(validate_snapshot(after[0])['bandwidth'],rule)
        with self.assertRaises(ValueError): configuration(after[0])
        json.load(self.request(path,'PUT',rule))
        restored=Registry(registry.path)
        self.assertEqual(restored.configuration(tokens[0]),after[0])
        self.assertEqual(restored.server_bandwidth(first),rule)
        registry.delete('servers',first)
        with registry.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM server_bandwidth').fetchone()[0],0)


@unittest.skipUnless(test_xray.BINARY.is_file(),'Set XRAY_BINARY for real server bandwidth tests')
class ServerBandwidthRealTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_xray.XrayRealTests.setUp
    tearDown=test_xray.XrayRealTests.tearDown
    request=test_xray.XrayRealTests.request
    asyncSetUp=test_user_bandwidth.UserBandwidthRealTests.asyncSetUp
    asyncTearDown=test_xray.XrayRealTests.asyncTearDown
    sync=test_user_bandwidth.UserBandwidthRealTests.sync
    limits=test_user_bandwidth.UserBandwidthRealTests.limits
    transfer=test_user_bandwidth.UserBandwidthRealTests.transfer

    def server_limits(self,upload=None,download=None):
        rule=dict(upload_bps=upload,download_bps=download)
        self.assertEqual(json.load(self.request(f'/api/servers/{self.sid}/bandwidth','PUT',rule)),rule)

    async def test_users_and_nodes_share_server_total_in_both_directions(self):
        registry=self.server.registry
        node=registry.save('nodes',{'name':'other','server_id':self.sid})['id']
        port=test_runtime.free_port()
        registry.configure_node(node,{**test_xray.VLESS,'listen_port':port})
        user=registry.save('users',{'name':'other'})['id']
        credential=registry.authorize_node_user(node,user)['uuid']
        for direction in ('upload','download'):
            self.server_limits(**{direction:32768}); await self.sync()
            start=time.monotonic()
            await asyncio.gather(self.transfer(self.credential['uuid']),self.transfer(credential,port))
            elapsed=time.monotonic()-start
            self.assertGreaterEqual(elapsed,.95)
            self.assertLess(elapsed,2.8)

    async def test_nested_limits_use_stricter_allowance_without_adding_delays(self):
        for direction in ('upload','download'):
            for server_rate,user_rate in ((32768,32768),(32768,65536),(65536,32768)):
                self.server_limits(**{direction:server_rate})
                self.limits(**{direction:user_rate}); await self.sync()
                elapsed=await self.transfer(self.credential['uuid'])
                self.assertGreaterEqual(elapsed,.48)
                # Concurrent charging is verified with gated allowances in
                # CombinedAllowanceTests, without treating CPU scheduling lag
                # as a serial-rate bug. Keep real I/O and throughput bounds.
                self.assertLess(elapsed,2.5)
        self.server_limits(); self.limits(); await self.sync()
        # Verify cleared policy as well as real delivery. The deterministic
        # unlimited-pacer test rejects any rate sleep; wall-clock scheduling
        # latency alone is not evidence of a leftover bandwidth rule.
        self.assertTrue(all(p.rate is None for p in self.runtime.server_pacers))
        self.assertTrue(all(p.rate is None for pair in self.runtime.pacers.values() for p in pair))
        self.assertLess(await self.transfer(self.credential['uuid']),2.5)

    async def test_server_rule_failed_replacement_is_closed(self):
        await self.sync()
        old_port=self.port
        self.server_limits(upload=32768)
        occupied=socket.socket(); occupied.bind(('127.0.0.1',0)); occupied.listen()
        try:
            self.server.registry.configure_node(self.nid,{**test_xray.VLESS,'listen_port':occupied.getsockname()[1]})
            with self.assertRaises(OSError): await self.sync()
            self.assertIsNone(self.runtime.snapshot)
            self.assertIsNone(self.runtime.core.process)
            with self.assertRaises(OSError): await asyncio.open_connection('127.0.0.1',old_port)
        finally: occupied.close()

    async def test_slow_user_does_not_hold_server_allowance_and_restart_restores_total(self):
        registry=self.server.registry
        user=registry.save('users',{'name':'fast'})['id']
        credential=registry.authorize_node_user(self.nid,user)['uuid']
        self.server_limits(upload=32768)
        self.limits(upload=1024)
        await self.sync()
        slow,fast=await asyncio.gather(self.transfer(self.credential['uuid'],size=1024),
                                       self.transfer(credential))
        self.assertGreaterEqual(slow,.95)
        self.assertLess(fast,.95)
        await self.runtime.close()
        await self.sync()
        elapsed=await self.transfer(credential)
        self.assertGreaterEqual(elapsed,.48)
        self.assertLess(elapsed,.95)


class ServerBandwidthTcpTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp=test_runtime.RuntimeTests.asyncSetUp
    asyncTearDown=test_runtime.RuntimeTests.asyncTearDown
    upstream=test_runtime.RuntimeTests.upstream
    exchange=test_runtime.RuntimeTests.exchange

    async def test_two_nodes_share_total_and_keep_budget_on_unrelated_revision(self):
        target=await self.upstream()
        ports=[test_runtime.free_port(),test_runtime.free_port()]
        value=test_runtime.snapshot(ports[0],target)
        other=copy.deepcopy(value['nodes'][0]); other['id']+=1
        other['config']['listen_port']=ports[1]; value['nodes'].append(other)
        for revision in (1,2):
            value['revision']=revision
            value['bandwidth']=dict(upload_bps=32768,download_bps=None)
            value['digest']=digest({k:v for k,v in value.items() if k!='digest'})
            previous=self.runtime.server_pacers['upload']
            await self.runtime.apply(value)
            if revision==2: self.assertIs(self.runtime.server_pacers['upload'],previous)
            start=time.monotonic()
            results=await asyncio.gather(*(self.exchange(p,b'x'*16384) for p in ports))
            self.assertEqual(results,[b'x'*16384]*2)
            self.assertGreaterEqual(time.monotonic()-start,.95)
