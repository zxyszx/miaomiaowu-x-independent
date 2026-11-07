"""Actual local Xray install -> authenticated HTTP master installation record."""
import asyncio
import unittest
import time
import uuid
import shutil
import ssl
import types
from agent import request
from grant_delivery import GrantDelivery
from grant_worker import GrantUsageWorker
from grant_payload import GrantPayloadQuota
from grant_capabilities import STAGING, INSTALLATION
from grant_configuration import GrantConfiguration
from grant_confirmation import confirm_installed
from grant_runtime import GrantVlessRuntime
from grant_meter import GrantMeter
from grant_execution_client import confirm_execution
from test_runtime import free_port
from test_xray import BINARY, VLESS
import test_grant_transport as fixture
import test_reality as reality_fixture
import test_grant_reality_runtime as grant_reality_fixture
from grant_reality_runtime import GrantRealityRuntime


async def confirm_running(runtime,base,token,value):
    for action in ('begin','installed'):
        await asyncio.to_thread(request,base,token,'/agent/grants/lifecycle',
            dict(action=action,grant_id=value['grant_id'],session_id=value['session_id']))
    return await confirm_execution(runtime,base,token)


@unittest.skipUnless(BINARY.is_file(), 'Set XRAY_BINARY for real grant execution')
class GrantConfirmationHTTPTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_install_http_ack_and_retry_after_meter_baseline_changes(self):
        self.start_http(offer=[STAGING, INSTALLATION])
        self.r.configure_node(self.nid, dict(VLESS, listen_port=free_port()))
        await asyncio.to_thread(self.fetch)
        value=GrantConfiguration(self.r).bind(self.token,self.g['grant_id'])
        runtime=GrantVlessRuntime(BINARY,self.store.path.parent/'http-runtime',self.store)
        try:
            await runtime.install(value)
            result=await confirm_installed(runtime,self.url,self.token)
            self.assertEqual(result['state'],'installation_recorded')
            self.assertFalse(result['ready_for_execution'])
            with self.r.connect() as db:
                applied=tuple(db.execute('SELECT * FROM usage_first_total_applied').fetchone())
                self.assertEqual(applied,(value['grant_id'],value['revision'],value['digest'],value['valid_until']))
                self.assertEqual(db.execute('SELECT count(*) FROM usage_first_total_installations').fetchone()[0],1)
            await confirm_running(runtime,self.url,self.token,value)
            # Internal ledger call only: this does NOT claim automatic payload delivery.
            metered=GrantMeter(self.r).report(self.token,dict(grant_id=self.g['grant_id'],
                policy_id=self.g['policy_id'],session_id=self.session,sequence=1,
                upload_bytes=1,download_bytes=0))
            self.assertEqual(metered['confirmed_bytes'],1)
            self.assertEqual(await confirm_installed(runtime,self.url,self.token),result)
            with self.r.connect() as db:
                self.assertEqual(tuple(db.execute('SELECT * FROM usage_first_total_applied').fetchone()),applied)
                self.assertEqual(db.execute('SELECT count(*) FROM usage_first_total_installations').fetchone()[0],1)
        finally:
            await runtime.close()

    async def test_real_payload_http_lost_response_reopen_counts_once(self):
        await self.payload_delivery(False)

    async def test_worker_real_payload_http_lost_response_reopen_counts_once(self):
        await self.payload_delivery(True)

    @unittest.skipUnless(shutil.which('openssl') and ssl.HAS_TLSv1_3,
                         'OpenSSL CLI and TLS 1.3 required')
    async def test_reality_worker_authenticated_payload_reopen_never_refunds(self):
        # Separate real TLS decoy/client fixture; business state belongs to this
        # authenticated HTTP master, not the fixture's independent registry.
        peer = reality_fixture.RealityRealTests()
        peer.received = bytearray()
        peer.echo = types.MethodType(grant_reality_fixture.GrantRealityRealTests.echo, peer)
        await peer.asyncSetUp()
        self.addAsyncCleanup(peer.asyncTearDown)
        await peer.runtime.close()
        self.start_http(offer=[STAGING, INSTALLATION])
        self.r.configure_node(self.nid, peer.config)
        await asyncio.to_thread(self.fetch)
        value = GrantConfiguration(self.r).bind(self.token, self.g['grant_id'])
        peer.credential = value['client']['uuid']

        def runtime():
            return GrantRealityRuntime(BINARY, self.store.path.parent/'reality-http-runtime',
                self.store, local_test_target=('127.0.0.1', peer.target_port),
                reality_test_target=('127.0.0.1', peer.decoy.sockets[0].getsockname()[1]))

        async def transfer(payload):
            reader, writer = await peer.client()
            try:
                writer.write(payload)
                await writer.drain()
                return await asyncio.wait_for(reader.read(), 4)
            finally:
                writer.close()
                await writer.wait_closed()

        peer.runtime = runtime()
        await peer.runtime.install(value)
        receipt = await confirm_installed(peer.runtime, self.url, self.token)
        self.assertEqual(receipt['state'], 'installation_recorded')
        self.assertFalse(receipt['ready_for_execution'])
        await confirm_running(peer.runtime,self.url,self.token,value)
        self.assertEqual(await transfer(b'abcd'), b'ab')
        await peer.runtime.close()
        self.assertEqual(peer.received, b'abcd')
        seen = []

        def lost(*args):
            seen.append(args[-1])
            request(*args)
            raise OSError('REALITY accounting response lost after commit')

        now = time.time()
        self.assertEqual(await GrantUsageWorker(peer.runtime, self.url, self.token).step(
            now=now, transport=lost), 'retry')
        self.assertEqual((seen[0]['upload_bytes'], seen[0]['download_bytes']), (4, 2))
        # Reopen both real runtime processes and the persistent quota/worker.
        # Installation retry must not overwrite the original execution baseline.
        peer.runtime = runtime()
        await peer.runtime.install(value)
        self.assertEqual(await confirm_installed(peer.runtime, self.url, self.token), receipt)
        self.assertEqual(await transfer(b'xx'), b'')
        self.assertEqual(peer.received, b'abcd')

        def recovered(*args):
            seen.append(args[-1])
            return request(*args)

        self.assertEqual(await GrantUsageWorker(peer.runtime, self.url, self.token).step(
            now=now+5, transport=recovered), 'accepted')
        self.assertEqual(seen, [seen[0], seen[0]])
        self.assertEqual(peer.runtime.paced.grant_quota.snapshot()['remaining_bytes'], 0)
        with self.r.connect() as db:
            self.assertEqual([tuple(row) for row in db.execute(
                'SELECT upload_bytes,download_bytes FROM usage_first_total_meter')], [(4, 2)])
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_first_total_installations').fetchone()[0], 1)

    async def payload_delivery(self, automatic):
        self.start_http(offer=[STAGING, INSTALLATION])
        port=free_port()
        self.r.configure_node(self.nid,dict(VLESS,listen_port=port))
        await asyncio.to_thread(self.fetch)
        value=GrantConfiguration(self.r).bind(self.token,self.g['grant_id'])
        received=bytearray()
        async def echo(reader,writer):
            try:
                while data:=await reader.read(1024):
                    received.extend(data)
                    writer.write(data);await writer.drain()
            except OSError: pass
            finally:
                writer.close()
                try: await writer.wait_closed()
                except OSError: pass
        target=await asyncio.start_server(echo,'127.0.0.1',0)
        target_port=target.sockets[0].getsockname()[1]
        runtime=GrantVlessRuntime(BINARY,self.store.path.parent/'payload-http-runtime',self.store,
            local_test_target=('127.0.0.1',target_port))
        try:
            await runtime.install(value)
            await confirm_installed(runtime,self.url,self.token)
            await confirm_running(runtime,self.url,self.token,value)
            reader,writer=await asyncio.open_connection('127.0.0.1',port)
            try:
                header=(b'\0'+uuid.UUID(value['client']['uuid']).bytes+b'\0\1'+
                    target_port.to_bytes(2,'big')+b'\1\x7f\0\0\1')
                writer.write(header+b'abcd');await writer.drain()
                self.assertEqual(await asyncio.wait_for(reader.readexactly(2),3),b'\0\0')
                self.assertEqual(await asyncio.wait_for(reader.read(),3),b'ab')
            finally:
                writer.close();await writer.wait_closed()
            await runtime.close()
            self.assertEqual(received,b'abcd')
            quota=runtime.grant_quota
            body=None if automatic else await asyncio.to_thread(quota.freeze_evidence)
            seen=[]
            def lost(*args):
                seen.append(args[-1]);request(*args)
                raise OSError('response lost after committed accounting')
            now=time.time()
            if automatic:
                self.assertEqual(await GrantUsageWorker(runtime,self.url,self.token).step(
                    now=now,transport=lost),'retry')
                body=seen[0]
            else:
                self.assertEqual(await asyncio.to_thread(GrantDelivery(quota).step,
                    self.url,self.token,now,transport=lost),'retry')
            self.assertEqual((body['upload_bytes'],body['download_bytes']),(4,2))
            reopened=GrantPayloadQuota(runtime.grant_path,self.store,value)
            def recovered(*args):
                seen.append(args[-1]);return request(*args)
            if automatic:
                runtime.grant_quota=reopened
                self.assertEqual(await GrantUsageWorker(runtime,self.url,self.token).step(
                    now=now+5,transport=recovered),'accepted')
            else:
                self.assertEqual(await asyncio.to_thread(GrantDelivery(reopened).step,
                    self.url,self.token,now+5,transport=recovered),'accepted')
            self.assertEqual(seen,[body,body])
            self.assertEqual(reopened.snapshot()['remaining_bytes'],0)
            with self.r.connect() as db:
                rows=db.execute('SELECT upload_bytes,download_bytes FROM usage_first_total_meter').fetchall()
                self.assertEqual([tuple(row) for row in rows],[(4,2)])
        finally:
            await runtime.close()
            target.close();await target.wait_closed()

for name in ('setUp','tearDown','fixtures','fixture','setup_total','setup_drain','applied',
             'report','data','envelope','accept','sealed','request','prepare','start',
             'start_http','fetch','post','rejected'):
    setattr(GrantConfirmationHTTPTests,name,getattr(fixture.GrantTransportTests,name))
GrantConfirmationHTTPTests.encode=staticmethod(fixture.GrantTransportTests.encode)
