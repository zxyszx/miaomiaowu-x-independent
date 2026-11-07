import asyncio
import base64
import copy
import json
from pathlib import Path
import shutil
import socket
import ssl
import subprocess
import tempfile
import time
import unittest
from unittest import mock
import uuid

from core import Registry
from domain_pool import DomainPool
from node_config import digest, profile
from reality_runtime import RealityRuntime
from test_runtime import free_port
from test_xray import BINARY, VLESS
from xray_runtime import configuration


def seal(value):
    value['digest']=digest({k:v for k,v in value.items() if k!='digest'})
    return value


class RealityConfigTests(unittest.TestCase):
    def config(self):
        return {**VLESS,'reality':dict(target_host='fixture.example',target_port=443,
            server_name='fixture.example',private_key=base64.urlsafe_b64encode(b'x'*32).decode().rstrip('='),
            short_id='0123456789abcdef')}

    def test_strict_profile_and_encrypted_public_listener(self):
        config=self.config()
        self.assertEqual(profile({**config,'listen_host':'0.0.0.0'})['listen_host'],'0.0.0.0')
        for change in ({'short_id':''},{'short_id':'AB0123456789abcd'},{'private_key':3},
                       {'private_key':'x'*43},{'private_key':base64.urlsafe_b64encode(bytes(32)).decode().rstrip('=')},
                       {'target_host':'127.0.0.1'},{'target_host':'https://fixture.example'},
                       {'server_name':'other.example'},{'target_port':True},{'extra':1}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                profile({**config,'reality':{**config['reality'],**change}})
        with self.assertRaises(ValueError): profile({**VLESS,'listen_host':'0.0.0.0'})

    def test_registry_persistence_revision_and_no_private_key_in_resource_list(self):
        with tempfile.TemporaryDirectory() as directory:
            registry=Registry(Path(directory)/'db')
            sid=registry.save('servers',{'name':'server'})['id']
            nid=registry.save('nodes',{'name':'node','server_id':sid})['id']
            token=registry.provision_agent(sid)['token']
            registry.configure_node(nid,self.config())
            snapshot=registry.configuration(token)
            self.assertEqual(snapshot['nodes'][0]['config'],self.config())
            self.assertNotIn('private_key',json.dumps(registry.list('nodes')))
            self.assertEqual(Registry(registry.path).configuration(token),snapshot)
            with self.assertRaises(ValueError): configuration(snapshot)
            changed=self.config(); changed['reality']['short_id']='fedcba9876543210'
            registry.configure_node(nid,changed)
            self.assertGreater(registry.configuration(token)['revision'],snapshot['revision'])


@unittest.skipUnless(BINARY.is_file() and shutil.which('openssl') and ssl.HAS_TLSv1_3,
                     'Real Xray, OpenSSL CLI and Python TLS 1.3 required')
class RealityRealTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.root=Path(self.temp.name)
        self.writers=set(); self.clients=[]; self.tasks=set()
        cert=self.root/'cert.pem'; key=self.root/'key.pem'
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
            '-subj','/CN=fixture.example','-addext','subjectAltName=DNS:fixture.example',
            '-keyout',str(key),'-out',str(cert)],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version=ssl.TLSVersion.TLSv1_3
        context.load_cert_chain(str(cert),str(key)); context.set_alpn_protocols(['h2'])
        self.decoy=await asyncio.start_server(self.echo,'127.0.0.1',0,ssl=context)
        self.target=await asyncio.start_server(self.echo,'127.0.0.1',0)
        self.target_port=self.target.sockets[0].getsockname()[1]
        keys=subprocess.check_output([str(BINARY),'x25519'],text=True)
        pairs=dict(line.split(': ',1) for line in keys.strip().splitlines())
        self.public=pairs.get('Password (PublicKey)') or pairs.get('Password') or pairs.get('PublicKey') or pairs.get('Public key')
        private=pairs.get('PrivateKey') or pairs.get('Private key')
        self.assertTrue(self.public); self.assertTrue(private)
        self.registry=Registry(self.root/'db')
        sid=self.registry.save('servers',{'name':'fixture'})['id']
        self.nid=self.registry.save('nodes',{'name':'encrypted','server_id':sid})['id']
        self.uid=self.registry.save('users',{'name':'one'})['id']
        self.token=self.registry.provision_agent(sid)['token']; self.port=free_port()
        self.config={**VLESS,'listen_port':self.port,'reality':dict(target_host='fixture.example',target_port=443,
            server_name='fixture.example',private_key=private,short_id='0123456789abcdef')}
        self.registry.configure_node(self.nid,self.config)
        self.credential=self.registry.authorize_node_user(self.nid,self.uid)['uuid']
        self.runtime=RealityRuntime(BINARY,self.root/'server',local_test_target=('127.0.0.1',self.target_port),
            reality_test_target=('127.0.0.1',self.decoy.sockets[0].getsockname()[1]))

    async def echo(self,reader,writer):
        self.writers.add(writer); task=asyncio.current_task(); self.tasks.add(task)
        try:
            while data:=await reader.read(65536): writer.write(data); await writer.drain()
        except (OSError,asyncio.IncompleteReadError): pass
        finally:
            writer.close()
            try: await writer.wait_closed()
            except OSError: pass
            self.writers.discard(writer); self.tasks.discard(task)

    async def asyncTearDown(self):
        for process in self.clients: await self.runtime.front._stop(process)
        await self.runtime.close()
        self.target.close(); self.decoy.close()
        await self.target.wait_closed(); await self.decoy.wait_closed()
        for writer in list(self.writers): writer.close()
        for task in list(self.tasks): task.cancel()
        await asyncio.gather(*self.tasks,return_exceptions=True)
        self.temp.cleanup()

    async def apply(self):
        snapshot=self.registry.configuration(self.token)
        await self.runtime.apply(snapshot)
        return snapshot

    async def client(self,credential=None,*,server_host="127.0.0.1",server_port=None,**reality_changes):
        port=free_port(); path=self.root/f'client-{uuid.uuid4().hex}.json'
        value={'log':{'loglevel':'none'},'inbounds':[{'listen':'127.0.0.1','port':port,
            'protocol':'dokodemo-door','settings':{'address':'127.0.0.1','port':self.target_port,'network':'tcp'}}],
            'outbounds':[{'protocol':'vless','settings':{'vnext':[{'address':server_host,'port':server_port or self.port,
                'users':[{'id':credential or self.credential,'encryption':'none'}]}]},
                'streamSettings':{'network':'raw','security':'reality','realitySettings':{
                    'serverName':'fixture.example','fingerprint':'chrome','password':self.public,
                    'shortId':'0123456789abcdef',**reality_changes}}}]}
        path.write_text(json.dumps(value)); path.chmod(0o600)
        process=await asyncio.create_subprocess_exec(str(BINARY),'run','-config',str(path),
            stdout=asyncio.subprocess.DEVNULL,stderr=asyncio.subprocess.DEVNULL)
        self.clients.append(process)
        for _ in range(60):
            self.assertIsNone(process.returncode,'Client core stopped unexpectedly')
            try: return await asyncio.open_connection('127.0.0.1',port)
            except OSError: await asyncio.sleep(.05)
        self.fail('Client listener unavailable')

    async def exchange(self,credential=None,payload=b'encrypted fixture',timeout=4,**changes):
        reader,writer=await self.client(credential,**changes)
        try:
            writer.write(payload); await writer.drain()
            return await asyncio.wait_for(reader.readexactly(len(payload)),timeout)
        finally:
            writer.close()
            try: await writer.wait_closed()
            except OSError: pass

    async def test_real_handshake_data_and_private_core_paths(self):
        await self.apply()
        self.assertEqual(await self.exchange(),b'encrypted fixture')
        generated=self.runtime._configuration(self.runtime.snapshot)
        self.assertEqual(generated['outbounds'][0]['protocol'],'blackhole')
        for outbound in generated['outbounds']:
            if outbound['protocol']=='freedom':
                port=self.runtime.ports[self.nid]
                self.assertEqual(outbound['settings']['redirect'],f'127.0.0.1:{port}')
                self.assertEqual(outbound['settings']['finalRules'],[{
                    'action':'allow','network':'tcp','ip':['127.0.0.1/32'],'port':str(port)}])
        self.assertTrue(all(n['config']['listen_host']=='127.0.0.1' and 'reality' not in n['config']
                            for n in self.runtime.paced.snapshot['nodes']))
        for file in (self.runtime.front.active_file,self.runtime.paced.core.active_file):
            self.assertEqual(file.stat().st_mode&0o777,0o600)

    async def test_pool_binding_snapshot_reaches_real_encrypted_runtime(self):
        # The fixture replaces only external candidate probing and DNS target;
        # node persistence, snapshot, Xray validation and handshake are real.
        original = await self.apply()
        self.assertEqual(await self.exchange(), b'encrypted fixture')
        pool = DomainPool(self.registry)
        item = pool.save(dict(domain='fixture.example', port=8443, enabled=True))
        self.registry.create_reality_probe_job(original['server_id'], item['id'])
        claim = self.registry.claim_reality_probe(self.token)['job']
        self.registry.complete_reality_probe(self.token, claim['job_id'], claim['lease_token'], dict(
            ok=True, error=None, tls_version='TLSv1.3', alpn='h2', latency_ms=1))
        pool.bind(self.nid, dict(domain_id=item['id'], domain_revision=item['revision']))
        snapshot = await self.apply()
        self.assertEqual(snapshot['revision'], original['revision']+1)
        self.assertEqual(snapshot['nodes'][0]['config']['reality']['target_port'], 8443)
        self.assertEqual(await self.exchange(payload=b'after pool application'), b'after pool application')

    async def test_automatic_pool_switch_reaches_encrypted_runtime_and_receipt(self):
        from domain_failover import Failover
        original = await self.apply()
        pool = DomainPool(self.registry)
        current = pool.save(dict(domain='fixture.example', port=443, enabled=True))
        backup = pool.save(dict(domain='fixture.example', port=8443, enabled=True))
        Failover(self.registry).policy(self.nid, dict(
            domain_ids=[current['id'], backup['id']], failure_threshold=2, cooldown_seconds=300))

        def acknowledge(snapshot):
            receipt = {key: snapshot[key] for key in ('revision', 'digest')}
            self.registry.acknowledge_configuration(self.token, receipt)
            self.registry.acknowledge_runtime(self.token, receipt)

        def probe(domain, result):
            self.registry.create_reality_probe_job(original['server_id'], domain['id'])
            job = self.registry.claim_reality_probe(self.token)['job']
            self.registry.complete_reality_probe(self.token, job['job_id'], job['lease_token'], result)

        acknowledge(original)
        probe(backup, dict(ok=True, error=None, tls_version='TLSv1.3', alpn='h2', latency_ms=1))
        probe(current, dict(ok=False, error='certificate_error'))
        self.assertEqual(self.registry.configuration(self.token)['revision'], original['revision'])
        probe(current, dict(ok=False, error='certificate_error'))
        switched = await self.apply()
        self.assertEqual(switched['revision'], original['revision']+1)
        self.assertEqual(switched['nodes'][0]['config']['reality']['target_port'], 8443)
        self.assertEqual(await self.exchange(payload=b'after automatic switch'), b'after automatic switch')
        acknowledge(switched)
        self.registry.configuration(self.token)
        self.assertEqual(Failover(self.registry).policy(self.nid)['state']['message'], 'applied')

    async def test_automatic_download_threshold_is_enforced_after_decryption(self):
        import time
        self.registry.automatic_bandwidth(self.uid,dict(enabled=True,threshold_bps=10000,
            sustain_seconds=1,download_bps=8192,duration_seconds=3))
        await self.apply()
        self.assertEqual(await self.exchange(payload=b'x'*32768),b'x'*32768)
        await asyncio.sleep(1.05)
        started=time.monotonic()
        self.assertEqual(await self.exchange(payload=b'x'*8192),b'x'*8192)
        self.assertGreater(time.monotonic()-started,0.85)
        self.assertGreater(self.runtime.paced.automatic.controllers[self.uid].until,time.monotonic())

    async def test_encrypted_distinct_ip_admission_and_release(self):
        self.registry.user_connections(self.uid,{'max_connections':None,'max_ips':1})
        sid=self.registry.configuration(self.token)['server_id']
        nid=self.registry.save('nodes',{'name':'ipv6','server_id':sid})['id']
        port=free_port()
        self.registry.configure_node(nid,{**self.config,'listen_host':'::1','listen_port':port})
        credential=self.registry.authorize_node_user(nid,self.uid)['uuid']
        await self.apply()
        first,writer=await self.client()
        writer.write(b'first'); await writer.drain()
        self.assertEqual(await asyncio.wait_for(first.readexactly(5),4),b'first')
        self.assertEqual(set(self.runtime.paced.task_ips.values()),{'127.0.0.1'})
        self.assertEqual(await self.exchange(),b'encrypted fixture')
        denied,blocked=await self.client(credential,server_host='::1',server_port=port)
        blocked.write(b'blocked'); await blocked.drain()
        try: result=await asyncio.wait_for(denied.read(7),4)
        except ConnectionResetError: result=b''
        self.assertEqual(result,b'')
        blocked.close(); await blocked.wait_closed()
        writer.write(b'alive'); await writer.drain()
        self.assertEqual(await asyncio.wait_for(first.readexactly(5),4),b'alive')
        writer.close(); await writer.wait_closed()
        async def released():
            while self.runtime.paced.task_ips: await asyncio.sleep(.02)
        await asyncio.wait_for(released(),8)
        reader,writer=await self.client(credential,server_host='::1',server_port=port)
        writer.write(b'ipv6'); await writer.drain()
        self.assertEqual(await asyncio.wait_for(reader.readexactly(4),4),b'ipv6')
        self.assertEqual(set(self.runtime.paced.task_ips.values()),{'::1'})
        writer.close(); await writer.wait_closed()

    async def test_forged_local_source_without_bridge_secret_rejected(self):
        await self.apply()
        import ipaddress
        proxy=(b'\r\n\r\n\0\r\nQUIT\n'+bytes([0x21,0x11,0,12])+
               ipaddress.ip_address('192.0.2.1').packed+
               ipaddress.ip_address('127.0.0.1').packed+bytes([1,1,2,2]))
        request=b'\0'+uuid.UUID(self.credential).bytes+b'\0\1'+self.target_port.to_bytes(2,'big')+b'\1'+bytes([127,0,0,1])
        for prefix in (b'',proxy+b'GET /forged HTTP/1.1\r\n\r\n',
                       proxy[:12]+bytes([0x21,0x11,255,255])):
            reader,writer=await asyncio.open_connection('127.0.0.1',self.runtime.ports[self.nid])
            writer.write(prefix+request+b'forged'); await writer.drain()
            try: result=await asyncio.wait_for(reader.read(1),2)
            except ConnectionResetError: result=b''
            self.assertEqual(result,b'')
            writer.close(); await writer.wait_closed()
            self.assertFalse(self.runtime.paced.task_ips)
        self.assertEqual(await self.exchange(),b'encrypted fixture')

    async def test_reality_to_raw_drops_bridge_requirement(self):
        await self.apply()
        self.assertEqual(await self.exchange(),b'encrypted fixture')
        raw={k:v for k,v in self.config.items() if k!='reality'}
        self.registry.configure_node(self.nid,raw)
        await self.apply()
        self.assertNotIn(self.nid,self.runtime.paced.trusted_ingress)
        reader,writer=await asyncio.open_connection('127.0.0.1',self.port)
        request=b'\0'+uuid.UUID(self.credential).bytes+b'\0\1'+self.target_port.to_bytes(2,'big')+b'\1'+bytes([127,0,0,1])
        writer.write(request+b'raw'); await writer.drain()
        self.assertEqual(await asyncio.wait_for(reader.readexactly(5),4),b'\0\0raw')
        writer.close(); await writer.wait_closed()
        self.registry.configure_node(self.nid,self.config)
        await self.apply()
        self.assertEqual(await self.exchange(),b'encrypted fixture')

    async def test_failed_ip_tightening_closes_old_ingress_and_fences_revision(self):
        old=await self.apply()
        reader,writer=await self.client()
        writer.write(b'old'); await writer.drain()
        self.assertEqual(await asyncio.wait_for(reader.readexactly(3),4),b'old')
        occupied=socket.socket(); occupied.bind(('127.0.0.1',0)); occupied.listen()
        try:
            self.registry.user_connections(self.uid,{'max_connections':None,'max_ips':1})
            changed={**self.config,'listen_port':occupied.getsockname()[1]}
            self.registry.configure_node(self.nid,changed)
            desired=self.registry.configuration(self.token)
            with self.assertRaises((ValueError,OSError,RuntimeError)):
                await self.runtime.apply(desired)
            try: result=await asyncio.wait_for(reader.read(1),4)
            except ConnectionResetError: result=b''
            self.assertEqual(result,b'')
            self.assertFalse(self.runtime.paced.task_ips)
            self.assertFalse(self.runtime.paced.listeners)
            self.assertIsNone(self.runtime.front.process)
            with self.assertRaisesRegex(ValueError,'旧配置'):
                await self.runtime.apply(old)
        finally:
            occupied.close(); writer.close(); await writer.wait_closed()
        await self.runtime.apply(desired)
        self.assertEqual(await self.exchange(server_port=changed['listen_port']),b'encrypted fixture')

    async def test_encrypted_connections_share_user_limit_and_release_slot(self):
        self.registry.user_connections(self.uid,{'max_connections':1})
        await self.apply()
        first,writer=await self.client()
        writer.write(b'first'); await writer.drain()
        self.assertEqual(await asyncio.wait_for(first.readexactly(5),4),b'first')
        denied,blocked=await self.client()
        try:
            blocked.write(b'blocked'); await blocked.drain()
            try: result=await asyncio.wait_for(denied.read(7),4)
            except ConnectionResetError: result=b''
            self.assertEqual(result,b'')
        finally:
            blocked.close()
            try: await blocked.wait_closed()
            except OSError: pass
        # Rejection must not damage the already admitted encrypted connection.
        writer.write(b'alive'); await writer.drain()
        self.assertEqual(await asyncio.wait_for(first.readexactly(5),4),b'alive')
        writer.close(); await writer.wait_closed()
        async def released():
            while self.runtime.paced.task_users: await asyncio.sleep(.01)
        await asyncio.wait_for(released(),8)
        self.assertEqual(await self.exchange(payload=b'replaced'),b'replaced')

    async def test_wrong_uuid_short_id_and_key_cannot_reach_application(self):
        await self.apply()
        for credential,changes in ((str(uuid.uuid4()),{}),(None,{'shortId':'fedcba9876543210'}),
                                   (None,{'password':base64.urlsafe_b64encode(b'y'*32).decode().rstrip('=')})):
            with self.subTest(changes=list(changes)), self.assertRaises((OSError,asyncio.IncompleteReadError,asyncio.TimeoutError)):
                await self.exchange(credential,timeout=.8,**changes)
        self.assertEqual(await self.exchange(),b'encrypted fixture')

    async def test_revoke_closes_existing_connection_and_rejects_old_credential(self):
        await self.apply(); reader,writer=await self.client()
        try:
            writer.write(b'before'); await writer.drain()
            self.assertEqual(await asyncio.wait_for(reader.readexactly(6),3),b'before')
            self.registry.authorize_node_user(self.nid,self.uid,revoke=True)
            await self.apply()
            self.assertEqual(await asyncio.wait_for(reader.read(1),3),b'')
            with self.assertRaises((OSError,asyncio.IncompleteReadError,asyncio.TimeoutError)):
                await self.exchange(timeout=.8)
        finally: writer.close(); await writer.wait_closed()

    async def test_user_and_server_bandwidth_are_enforced_after_decryption(self):
        snapshot=self.registry.configuration(self.token)
        snapshot['nodes'][0]['clients'][0]['bandwidth']={'upload_bps':None,'download_bps':8192}
        seal(snapshot); await self.runtime.apply(snapshot)
        start=time.monotonic()
        self.assertEqual(await self.exchange(payload=b'x'*4096),b'x'*4096)
        self.assertGreaterEqual(time.monotonic()-start,.45)
        snapshot['revision']+=1
        snapshot['bandwidth']={'upload_bps':None,'download_bps':4096}
        seal(snapshot); await self.runtime.apply(snapshot)
        start=time.monotonic()
        self.assertEqual(await self.exchange(payload=b'x'*4096),b'x'*4096)
        self.assertGreaterEqual(time.monotonic()-start,.95)

    async def test_server_allowance_is_shared_across_encrypted_users(self):
        second=self.registry.save('users',{'name':'two'})['id']
        credential=self.registry.authorize_node_user(self.nid,second)['uuid']
        snapshot=self.registry.configuration(self.token)
        snapshot['bandwidth']={'upload_bps':None,'download_bps':8192}
        seal(snapshot); await self.runtime.apply(snapshot)
        start=time.monotonic()
        results=await asyncio.gather(self.exchange(payload=b'a'*4096),
                                     self.exchange(credential,payload=b'b'*4096))
        self.assertEqual(results,[b'a'*4096,b'b'*4096])
        self.assertGreaterEqual(time.monotonic()-start,.95)

    async def test_encrypted_upload_obeys_user_and_server_limits(self):
        snapshot=self.registry.configuration(self.token)
        snapshot['nodes'][0]['clients'][0]['bandwidth']={'upload_bps':8192,'download_bps':None}
        for server_rate,minimum in ((None,.45),(4096,.95)):
            snapshot['revision']+=1
            snapshot['bandwidth']={'upload_bps':server_rate,'download_bps':None}
            seal(snapshot); await self.runtime.apply(snapshot)
            start=time.monotonic()
            self.assertEqual(await self.exchange(payload=b'u'*4096),b'u'*4096)
            self.assertGreaterEqual(time.monotonic()-start,minimum)

    async def test_same_revision_restarts_dead_front_and_inner_core(self):
        snapshot=await self.apply()
        for core in (self.runtime.front,self.runtime.paced.core):
            await core._stop(core.process)
            await self.runtime.apply(snapshot)
            self.assertIsNone(core.process.returncode)
            self.assertEqual(await self.exchange(),b'encrypted fixture')

    async def test_existing_plaintext_loopback_nodes_still_work(self):
        config=copy.deepcopy(self.config); config.pop('reality')
        self.registry.configure_node(self.nid,config)
        await self.apply()
        self.assertIsNone(self.runtime.front.process)
        reader,writer=await asyncio.open_connection('127.0.0.1',self.port)
        try:
            header=b'\0'+uuid.UUID(self.credential).bytes+b'\0\x01'+self.target_port.to_bytes(2,'big')+b'\x01\x7f\0\0\x01'
            writer.write(header+b'legacy'); await writer.drain()
            self.assertEqual(await asyncio.wait_for(reader.readexactly(8),3),b'\0\0legacy')
        finally: writer.close(); await writer.wait_closed()

    async def test_user_expiry_preserves_other_encrypted_connections(self):
        other=self.registry.save('users',{'name':'unaffected'})['id']
        credential=self.registry.authorize_node_user(self.nid,other)['uuid']
        deadline=time.time()+5
        self.registry.temporary_bandwidth(self.uid,{'bandwidth':{'upload_bps':None,'download_bps':None},'expires_at':deadline})
        await self.apply()
        affected,old=await self.client()
        reader,writer=await self.client(credential)
        try:
            for source,destination in ((affected,old),(reader,writer)):
                destination.write(b'before'); await destination.drain()
                self.assertEqual(await asyncio.wait_for(source.readexactly(6),3),b'before')
            await asyncio.sleep(max(0,deadline-time.time())+.1)
            self.assertEqual(await asyncio.wait_for(affected.read(),2),b'')
            writer.write(b'after'); await writer.drain()
            self.assertEqual(await asyncio.wait_for(reader.readexactly(5),2),b'after')
            self.assertEqual(await self.exchange(credential),b'encrypted fixture')
            self.assertIsNone(self.runtime.front.process.returncode)
            self.assertIsNone(self.runtime.paced.core.process.returncode)
            with self.assertRaises((asyncio.TimeoutError,asyncio.IncompleteReadError,OSError)):
                await self.exchange(timeout=1)
        finally:
            for stream in (old,writer):
                stream.close()
                try: await stream.wait_closed()
                except OSError: pass

    async def test_expiry_closes_front_gateway_and_core(self):
        # Allow two real cores to validate/start before the expiry observation.
        snapshot=self.registry.configuration(self.token); snapshot['valid_until']=time.time()+5
        seal(snapshot); await self.runtime.apply(snapshot)
        self.assertEqual(await self.exchange(),b'encrypted fixture')
        await asyncio.sleep(max(0,snapshot['valid_until']-time.time())+.4)
        self.assertIsNone(self.runtime.front.process)
        self.assertIsNone(self.runtime.paced.core.process)
        self.assertEqual(self.runtime.paced.listeners,{})

    async def test_failed_front_update_fails_closed_and_replay_is_rejected(self):
        previous=await self.apply()
        with socket.socket() as occupied:
            occupied.bind(('127.0.0.1',0)); occupied.listen()
            changed=copy.deepcopy(previous); changed['revision']+=1
            changed['nodes'][0]['config']['listen_port']=occupied.getsockname()[1]; seal(changed)
            with self.assertRaises(RuntimeError): await self.runtime.apply(changed)
        self.assertIsNone(self.runtime.front.process)
        self.assertEqual(self.runtime.paced.listeners,{})
        old=copy.deepcopy(previous); old['revision']-=1; seal(old)
        with self.assertRaises(ValueError): await self.runtime.apply(old)
        with self.assertRaises(ValueError): await self.runtime.apply(previous)
        restored=copy.deepcopy(previous); restored['revision']=changed['revision']+1; seal(restored)
        await self.runtime.apply(restored)
        self.assertEqual(await self.exchange(),b'encrypted fixture')

