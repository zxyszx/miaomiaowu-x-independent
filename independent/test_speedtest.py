import asyncio
import json
from pathlib import Path
import shutil
import ssl
import subprocess
import tempfile
import threading
import time
import unittest
from unittest import mock
import urllib.error

from core import Error,Registry
from speedtest import SpeedTests,options,resolve
import test_core
from test_xray import BINARY,VLESS
from test_runtime import free_port
from vless_runtime import VlessRuntime

SETTINGS={'url':'https://speed.fixture/file','mode':'local','threads':1,'size_mib':1}


class ValidationTests(unittest.TestCase):
    def test_supported_parameters_and_reject_unsafe_or_ambiguous_input(self):
        for threads in (1,8,16,32,64): self.assertEqual(options({**SETTINGS,'threads':threads})['threads'],threads)
        for change in ({'url':'http://speed.fixture/'},{'url':'https://127.0.0.1/'},
                       {'url':'https://u:p@speed.fixture/'},{'url':'https://speed.fixture/#secret'},
                       {'url':'https://speed.fixture/\r\nX:bad'},{'threads':True},{'size_mib':2},
                       {'mode':'node'},{'user_id':1},{'extra':1}):
            with self.subTest(change=change),self.assertRaises(Error): options({**SETTINGS,**change})

    def test_restart_marks_abandoned_jobs_interrupted(self):
        with tempfile.TemporaryDirectory() as directory:
            registry=Registry(Path(directory)/'db'); manager=SpeedTests(registry)
            with registry.connect() as db:
                db.execute('INSERT INTO speed_tests(id,settings,status,created_at) VALUES(?,?,?,?)',('old',json.dumps(SETTINGS),'running',time.time()))
            manager.close(); again=SpeedTests(registry)
            self.assertEqual(again.get('old')['status'],'interrupted'); again.close()


@unittest.skipUnless(shutil.which('openssl'),'OpenSSL CLI required for trusted TLS fixture')
class DownloadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.root=Path(self.temp.name)
        cert=self.root/'cert'; key=self.root/'key'
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
                        '-subj','/CN=speed.fixture','-addext','subjectAltName=DNS:speed.fixture',
                        '-keyout',str(key),'-out',str(cert)],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); context.load_cert_chain(cert,key)
        context.set_alpn_protocols(['h2','http/1.1'])
        self.trusted=ssl.create_default_context(cafile=str(cert))
        self.connections=set(); self.mode='ok'; self.received=0
        self.tls=await asyncio.start_server(self.download,'127.0.0.1',0,ssl=context)
        self.port=self.tls.sockets[0].getsockname()[1]
        self.settings={**SETTINGS,'url':f'https://speed.fixture:{self.port}/file'}
        self.registry=Registry(self.root/'db')
        async def resolver(host,port,deadline): return '127.0.0.1'
        self.resolver=resolver
        self.manager=SpeedTests(self.registry,resolver=resolver,context=self.trusted,timeout=3)
        self.runtime=None

    async def download(self,reader,writer):
        task=asyncio.current_task(); self.connections.add(task)
        try:
            request=await reader.readuntil(b'\r\n\r\n')
            self.received+=1
            if self.mode=='slow': await asyncio.sleep(5)
            size=int(request.split(b'Range: bytes=0-',1)[1].split(b'\r\n',1)[0])+1
            if self.mode=='redirect': writer.write(b'HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1/\r\nContent-Length: 0\r\n\r\n')
            elif self.mode=='chunked': writer.write(b'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n')
            elif self.mode=='small': writer.write(b'HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nx')
            elif self.mode=='truncated': writer.write(f'HTTP/1.1 200 OK\r\nContent-Length: {size}\r\n\r\nx'.encode())
            else:
                writer.write(f'HTTP/1.1 206 Partial Content\r\nContent-Length: {size}\r\nContent-Range: bytes 0-{size-1}/{size}\r\n\r\n'.encode())
                for offset in range(0,size,16384):
                    writer.write(b'x'*min(16384,size-offset)); await writer.drain()
            await writer.drain()
        except (OSError,asyncio.IncompleteReadError): pass
        finally:
            writer.close()
            try: await writer.wait_closed()
            except OSError: pass
            self.connections.discard(task)

    async def finished(self,ident):
        for _ in range(600):
            result=self.manager.get(ident)
            if result['status']!='running': return result
            await asyncio.sleep(.01)
        self.fail('Measurement did not finish')

    async def asyncTearDown(self):
        await asyncio.to_thread(self.manager.close)
        if self.runtime: await self.runtime.close()
        self.tls.close(); await self.tls.wait_closed()
        for task in list(self.connections): task.cancel()
        await asyncio.gather(*self.connections,return_exceptions=True)
        self.temp.cleanup()

    async def test_parallel_download_byte_budget_rate_and_saved_results(self):
        for threads in (1,8,64):
            before=self.received
            result=await self.finished(self.manager.start({**self.settings,'threads':threads})['id'])
            self.assertEqual(result['status'],'completed',result)
            self.assertEqual(self.received-before,threads)
            value=result['result']; self.assertEqual(value['bytes'],1048576)
            self.assertGreater(value['download_bps'],0); self.assertGreater(value['ttfb_ms'],0)
            self.assertAlmostEqual(value['download_bps']*value['elapsed_seconds'],1048576,delta=200)
        self.manager.close(); self.manager=SpeedTests(self.registry,resolver=self.resolver,context=self.trusted)
        self.assertEqual(len(self.manager.get()),3)
        self.assertTrue(all(row['status']=='completed' for row in self.manager.get()))
        self.assertNotIn('credential',json.dumps(self.manager.get()))

    async def test_explicit_http_and_incomplete_feedback_no_redirect_following(self):
        for mode,error in (('redirect','http_status_302'),('chunked','unsupported_response_encoding'),
                           ('small','response_too_small'),('truncated','incomplete_download')):
            self.mode=mode
            result=await self.finished(self.manager.start(self.settings)['id'])
            self.assertEqual(result['status'],'failed'); self.assertEqual(result['result']['error'],error)

    async def test_timeout_cancel_capacity_and_close(self):
        self.mode='slow'
        first=self.manager.start(self.settings)['id']; second=self.manager.start(self.settings)['id']
        with self.assertRaises(Error) as caught: self.manager.start(self.settings)
        self.assertEqual(caught.exception.status,409)
        await asyncio.sleep(.05)
        self.assertEqual(self.manager.cancel(first)['status'],'cancelled')
        self.assertEqual((await self.finished(first))['status'],'cancelled')
        self.assertEqual((await self.finished(second))['result']['error'],'timeout')
        self.manager.close()
        with self.assertRaises(Error): self.manager.start(self.settings)

    async def test_untrusted_certificate_fails(self):
        self.manager.context=ssl.create_default_context()
        result=await self.finished(self.manager.start(self.settings)['id'])
        self.assertEqual(result['result']['error'],'certificate_error')

    async def test_public_dns_guard_and_dns_errors(self):
        addresses=[(2,1,6,'',('127.0.0.1',443)),(2,1,6,'',('1.1.1.1',443))]
        with mock.patch('speedtest.socket.getaddrinfo',return_value=addresses):
            with self.assertRaisesRegex(Error,'non_public_address'): await resolve('speed.fixture',443,time.monotonic()+1)
        with mock.patch('speedtest.socket.getaddrinfo',side_effect=OSError()):
            with self.assertRaisesRegex(Error,'dns_error'): await resolve('speed.fixture',443,time.monotonic()+1)

    async def test_stuck_dns_is_bounded_and_does_not_outlive_job_cancellation(self):
        release=threading.Event()
        def blocked(*args,**kwargs):
            release.wait(2)
            return [(2,1,6,'',('1.1.1.1',443))]
        with mock.patch('speedtest.socket.getaddrinfo',side_effect=blocked):
            tasks=[asyncio.create_task(resolve('speed.fixture',443,time.monotonic()+.08)) for _ in range(2)]
            await asyncio.sleep(.02)
            try:
                with self.assertRaises(Error) as caught: await resolve('speed.fixture',443,time.monotonic()+1)
                self.assertEqual(caught.exception.status,503)
                results=await asyncio.gather(*tasks,return_exceptions=True)
                self.assertTrue(all(isinstance(value,TimeoutError) for value in results))
            finally: release.set()
            # Resolver threads return and release their original slots.
            await asyncio.sleep(.05)

    async def prepare_node(self):
        if not BINARY.is_file(): self.skipTest('Real Xray required')
        r=self.registry; self.sid=r.save('servers',{'name':'own'})['id']
        self.nid=r.save('nodes',{'name':'node','server_id':self.sid})['id']
        self.uid=r.save('users',{'name':'user'})['id']
        self.token=r.provision_agent(self.sid)['token']
        r.configure_node(self.nid,{**VLESS,'listen_port':free_port()})
        r.authorize_node_user(self.nid,self.uid)
        self.runtime=VlessRuntime(BINARY,self.root/'runtime',local_test_target=('127.0.0.1',self.port))
        await self.runtime.apply(r.configuration(self.token))
        self.manager.local_server_id=self.sid
        return {**self.settings,'mode':'node','node_id':self.nid,'user_id':self.uid}

    async def test_real_vless_measurement_obeys_user_download_cap(self):
        settings=await self.prepare_node()
        self.registry.user_bandwidth(self.uid,dict(upload_bps=None,download_bps=1048576))
        await self.runtime.apply(self.registry.configuration(self.token))
        result=await self.finished(self.manager.start(settings)['id'])
        self.assertEqual(result['status'],'completed',result)
        self.assertEqual(result['result']['source'],'node')
        self.assertGreater(result['result']['elapsed_seconds'],.9)
        self.assertLess(result['result']['download_bps'],1200000)

    async def test_wrong_source_or_revoked_credential_cannot_fall_back_to_direct(self):
        settings=await self.prepare_node()
        self.manager.local_server_id=self.sid+1
        with self.assertRaisesRegex(Error,'未绑定'): self.manager.start(settings)
        self.manager.local_server_id=self.sid
        self.registry.authorize_node_user(self.nid,self.uid,revoke=True)
        with self.assertRaises(Error): self.manager.start(settings)
        self.assertEqual(self.received,0)

    async def test_real_reality_download_and_client_cleanup(self):
        from reality_runtime import RealityRuntime
        settings=await self.prepare_node()
        keys=subprocess.check_output([str(BINARY),'x25519'],text=True)
        pairs=dict(line.split(': ',1) for line in keys.strip().splitlines())
        private=pairs.get('PrivateKey') or pairs.get('Private key')
        config={**VLESS,'listen_port':free_port(),'reality':dict(target_host='speed.fixture',target_port=443,
            server_name='speed.fixture',private_key=private,short_id='0123456789abcdef')}
        self.registry.configure_node(self.nid,config)
        await self.runtime.close()
        self.runtime=RealityRuntime(BINARY,self.root/'reality',local_test_target=('127.0.0.1',self.port),
                                   reality_test_target=('127.0.0.1',self.port))
        await self.runtime.apply(self.registry.configuration(self.token))
        with self.assertRaisesRegex(Error,'Xray'): self.manager.start(settings)
        self.manager.xray_binary=BINARY
        result=await self.finished(self.manager.start(settings)['id'])
        self.assertEqual(result['status'],'completed',result)
        self.assertEqual(result['result']['bytes'],1048576)
        self.assertNotIn(private,json.dumps(self.manager.get()))
        # Completion is recorded only after the generated client exits.
        await asyncio.sleep(.05)
        self.assertFalse(self.manager.active)


class ApiTests(unittest.TestCase):
    setUp=test_core.ApiTests.setUp
    request=test_core.ApiTests.request
    def tearDown(self):
        if hasattr(self.server,'speed_tests'): self.server.speed_tests.close()
        test_core.ApiTests.tearDown(self)

    def test_auth_not_enabled_invalid_input_and_status(self):
        with self.assertRaises(urllib.error.HTTPError) as caught: self.request('/api/speed-tests',auth=False)
        self.assertEqual(caught.exception.code,401)
        with self.assertRaises(urllib.error.HTTPError) as caught: self.request('/api/speed-tests')
        self.assertEqual(caught.exception.code,503)
        self.server.speed_tests=SpeedTests(self.server.registry)
        with self.request('/api/speed-tests/status') as response:
            self.assertEqual(response.headers['Cache-Control'],'no-store')
            self.assertFalse(json.load(response)['remote_agent_available'])
        with self.assertRaises(urllib.error.HTTPError): self.request('/api/speed-tests','POST',{'threads':2})
        self.assertEqual(json.load(self.request('/api/speed-tests')),[])
        with self.assertRaises(urllib.error.HTTPError) as caught: self.request('/api/speed-tests/missing','DELETE')
        self.assertEqual(caught.exception.code,404)

    def test_start_poll_cancel_and_restart_history_over_http(self):
        async def waiting(host,port,deadline): await asyncio.sleep(60)
        self.server.speed_tests=SpeedTests(self.server.registry,resolver=waiting)
        with self.request('/api/speed-tests','POST',SETTINGS) as response:
            self.assertEqual(response.status,202); value=json.load(response)
        path='/api/speed-tests/'+value['id']
        self.assertEqual(json.load(self.request(path))['status'],'running')
        self.assertEqual(json.load(self.request(path,'DELETE'))['status'],'cancelled')
        self.server.speed_tests.close()
        self.server.speed_tests=SpeedTests(self.server.registry)
        self.assertEqual(json.load(self.request(path))['status'],'cancelled')
        self.assertEqual(len(json.load(self.request('/api/speed-tests'))),1)
