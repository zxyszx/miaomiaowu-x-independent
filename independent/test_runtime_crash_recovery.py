"""Kill complete VLESS/REALITY executor/core groups and recover durable tails."""
import asyncio
import json
import os
from pathlib import Path
import signal
import shutil
import ssl
import sqlite3
import sys
import unittest
import uuid

from execution_agent import cycle
from execution_epochs import ExecutionEpochs
from usage_outbox import UsageOutbox
from vless_runtime import VlessRuntime
import test_usage_delivery
import test_xray
import test_reality_checkpoint
from reality_runtime import RealityRuntime


CHILD=r'''
import asyncio,json,os,signal,sys
from contextlib import contextmanager
from pathlib import Path
from execution_agent import cycle
from execution_epochs import ExecutionEpochs
from vless_runtime import VlessRuntime

def stop():
    print('STOP',flush=True)
    os.kill(os.getpid(),signal.SIGSTOP)
    raise AssertionError('parent must kill executor')

async def main():
    config=json.loads(Path(sys.argv[1]).read_text())
    state=Path(config['state'])
    if config['protocol']=='reality':
        from reality_runtime import RealityRuntime
        runtime=RealityRuntime(config['binary'],state/'crash-core',
            local_test_target=('127.0.0.1',config['target']),
            reality_test_target=('127.0.0.1',config['decoy']))
    else:
        runtime=VlessRuntime(config['binary'],state/'crash-core',
            local_test_target=('127.0.0.1',config['target']))
    epochs=ExecutionEpochs(config['base'],config['token'],state,runtime,config['session'])
    original_intent=epochs.delivery._prepare_write
    original_checkpoint=epochs.delivery._checkpoint
    def intent(snapshot):
        original_intent(snapshot)
        if config['phase']=='before_write' and any(item[config['direction']+'_bytes']==9 for item in snapshot['items']): stop()
    def checkpoint(snapshot):
        armed=any(item[config['direction']+'_bytes']==9 for item in snapshot['items'])
        if armed and config['phase']=='before_checkpoint': stop()
        box=epochs.delivery.box
        original_connect=box.connect if box is not None else None
        @contextmanager
        def connect(*args,**kwargs):
            with original_connect(*args,**kwargs) as db:
                db.set_trace_callback(lambda sql:stop() if sql=='COMMIT' else None)
                yield db
        if armed and config['phase']=='before_commit': box.connect=connect
        original_checkpoint(snapshot)
        if armed and config['phase']=='after_commit': stop()
    runtime.set_usage_write_intent(intent)
    runtime.set_incremental_usage_checkpoint(checkpoint)
    await cycle(config['base'],config['token'],state,runtime,1,config['session'],epochs)
    cores=([runtime.front.process,runtime.paced.core.process]
           if config['protocol']=='reality' else [runtime.core.process])
    print('READY '+json.dumps([p.pid for p in cores]),flush=True)
    await asyncio.Future()
asyncio.run(main())
'''


@unittest.skipUnless(os.name=='posix' and test_xray.BINARY.is_file(),'requires POSIX and real Xray')
class RuntimeCrashTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_usage_delivery.DeliveryRealTests.setUp
    tearDown=test_usage_delivery.DeliveryRealTests.tearDown
    request=test_usage_delivery.DeliveryRealTests.request
    asyncSetUp=test_usage_delivery.DeliveryRealTests.asyncSetUp
    asyncTearDown=test_usage_delivery.DeliveryRealTests.asyncTearDown
    open=test_usage_delivery.DeliveryRealTests.open
    accepted=test_usage_delivery.DeliveryRealTests.accepted
    close_writer=test_usage_delivery.DeliveryRealTests.close_writer

    protocol="vless"

    def replacement_runtime(self):
        options=dict(local_test_target=("127.0.0.1",self.target_port))
        if self.protocol=="reality":
            options["reality_test_target"]=("127.0.0.1",self.decoy.sockets[0].getsockname()[1])
        factory=RealityRuntime if self.protocol=="reality" else VlessRuntime
        return factory(test_xray.BINARY,self.state/"crash-core",**options)

    async def assert_cores_stopped(self,pids):
        # Orphaned children may briefly remain zombies until the OS reaps them.
        # A zombie is exited; any running/sleeping/stopped core is a leak.
        for _ in range(100):
            process=await asyncio.create_subprocess_exec("ps","-o","stat=","-p",
                ",".join(map(str,pids)),stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE)
            output,error=await asyncio.wait_for(process.communicate(),3)
            self.assertIn(process.returncode,(0,1))
            self.assertEqual(error,b'')
            states=output.decode().split()
            if all(state.startswith("Z") for state in states): return
            await asyncio.sleep(.02)
        self.fail("core processes survived executor SIGKILL: "+str(states))

    async def crash(self,phase,direction="upload"):
        from usage_cycles import UsageCycles
        from quota_store import QuotaStore,PersistentCycleQuota
        from test_usage_delivery import scoped_period_clock
        clock=scoped_period_clock(self); now=clock()
        await self.runtime.close()
        registry=self.server.registry
        with registry.connect() as db:
            db.execute('INSERT INTO usage_cycles(id,user_id,starts_at,ends_at) VALUES(?,?,?,?)',
                       ('crash-period',self.uid,now-60,now+3600))
            from usage_accounting import initialize_cycle
            initialize_cycle(db,'crash-period')
        limits=(10,100) if direction=='upload' else (100,10)
        index=0 if direction=='upload' else 1
        UsageCycles(registry).set_budget('crash-period',dict(zip(('upload_bytes','download_bytes'),limits)))
        # Observe actual application bytes at the target, not just intent totals.
        self.target.close(); await self.target.wait_closed()
        received=bytearray(); observed_tasks=set(); observed_writers=set()
        async def cleanup_target():
            for writer in list(observed_writers): writer.close()
            for task in list(observed_tasks): task.cancel()
            await asyncio.gather(*observed_tasks,return_exceptions=True)
        self.addAsyncCleanup(cleanup_target)
        async def echo(reader,writer):
            task=asyncio.current_task()
            observed_tasks.add(task); observed_writers.add(writer)
            try:
                while data:=await reader.read(65536):
                    received.extend(data); writer.write(data); await writer.drain()
            except OSError: pass
            finally:
                writer.close()
                try: await writer.wait_closed()
                except OSError: pass
                observed_tasks.discard(task); observed_writers.discard(writer)
        self.target=await asyncio.start_server(echo,'127.0.0.1',self.target_port)
        old=str(uuid.uuid4())
        config=self.state.parent/'crash-fixture.json'
        config.write_text(json.dumps(dict(state=str(self.state),binary=str(test_xray.BINARY.resolve()),
            base=self.base,token=self.token,target=self.target_port,session=old,phase=phase,protocol=self.protocol,direction=direction,
            decoy=self.decoy.sockets[0].getsockname()[1] if self.protocol=="reality" else None)))
        config.chmod(0o600)
        child=await asyncio.create_subprocess_exec(sys.executable,'-c',CHILD,str(config),
            cwd=Path(__file__).resolve().parent,stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,start_new_session=True)
        try:
            ready=await asyncio.wait_for(child.stdout.readline(),10)
            if not ready.startswith(b'READY '):
                _,error=await asyncio.wait_for(child.communicate(),5)
                self.fail('executor startup failed: '+error.decode('utf-8','replace'))
            pids=json.loads(ready[6:])
            self.assertEqual(len(pids),2 if self.protocol=="reality" else 1)
            self.assertTrue(all(os.getpgid(pid)==child.pid for pid in pids))
            reader,writer=await self.open(); await self.accepted(reader)
            if direction=='upload':
                writer.write(b'abcd'); await writer.drain()
            else:
                self.assertEqual(len(observed_writers),1)
                target_writer=next(iter(observed_writers))
                target_writer.write(b'abcd'); await target_writer.drain()
            self.assertEqual(await asyncio.wait_for(child.stdout.readline(),5),b'STOP\n')
            os.killpg(child.pid,signal.SIGKILL)
            output,error=await asyncio.wait_for(child.communicate(),5)
            self.assertEqual(child.returncode,-signal.SIGKILL)
            self.assertEqual(output,b'')
            self.assertEqual(error,b'')
            await self.assert_cores_stopped(pids)
        finally:
            if child.returncode is None:
                os.killpg(child.pid,signal.SIGKILL)
                await asyncio.wait_for(child.communicate(),5)
        # Let bytes already queued in the core reach the target before checking.
        for _ in range(100):
            if len(received)>=(5 if phase=='before_write' or direction=='download' else 9): break
            await asyncio.sleep(.01)
        # Killing the core may discard its queued upload: the meter measures
        # successful transport.write, not end-to-end delivery acknowledgement.
        self.assertTrue(bytes(received) in (b'hello',b'helloabcd'),bytes(received))
        if phase=='before_write' or direction=='download': self.assertEqual(bytes(received),b'hello')
        box=UsageOutbox(self.state/'usage-outbox.sqlite',self.sid)
        frozen=box.next_report(old)
        expected=9 if phase=='after_commit' else 5
        committed=(expected,5) if direction=='upload' else (5,expected)
        self.assertEqual((frozen['items'][0]['upload_bytes'],frozen['items'][0]['download_bytes']),committed)
        unknown=box.uncertain_writes()
        self.assertEqual(box.write_resolution_history(),[])
        if phase=='after_commit': self.assertEqual(unknown,[])
        else:
            self.assertEqual(len(unknown),1)
            self.assertEqual(unknown,[dict(session=old,node=self.nid,user=self.uid,
                credential=frozen['items'][0]['credential'],committed_upload=5,committed_download=5,
                possible_upload=9 if direction=='upload' else 5,
                possible_download=9 if direction=='download' else 5)])
        quota_path=self.state/'crash-core/cycle-quota.sqlite'
        quota=PersistentCycleQuota(QuotaStore(quota_path,self.sid),(self.uid,'crash-period')).reconcile(*limits)
        self.assertEqual((quota.used[index],quota.remaining[index]),(9,1))
        for path in (box.path,quota_path):
            with sqlite3.connect(path) as db: self.assertEqual(db.execute('PRAGMA integrity_check').fetchone()[0],'ok')
        # A genuinely new runtime/session may spend only the remaining byte.
        self.runtime=self.replacement_runtime()
        new=str(uuid.uuid4()); epochs=ExecutionEpochs(self.base,self.token,self.state,self.runtime,new)
        await cycle(self.base,self.token,self.state,self.runtime,2,new,epochs)
        reader,_=await self.open()
        await asyncio.wait_for(reader.read(),3)
        await epochs.delivery.capture()
        current=epochs.delivery.box.next_report(new)
        fresh=(1,0) if direction=='upload' else (5,1)
        self.assertEqual((current['items'][0]['upload_bytes'],current['items'][0]['download_bytes']),fresh)
        self.assertEqual(epochs.delivery.box.next_report(old),frozen)
        self.assertEqual(epochs.delivery.box.uncertain_writes(),unknown)
        await epochs.delivery.step(); await epochs.delivery.step(); await epochs.delivery.step()
        from agent import request
        retry=await asyncio.to_thread(request,self.base,self.token,'/agent/usage/settle',frozen)
        self.assertTrue(retry['duplicate'])
        total=await asyncio.to_thread(request,self.base,self.server.token,f'/api/users/{self.uid}/usage')
        self.assertEqual((total['upload_bytes'],total['download_bytes']),tuple(a+b for a,b in zip(committed,fresh)))
        self.assertEqual(epochs.delivery.box.outstanding_sessions(),[])
        # Confirmed lower bounds do not erase uncertainty or refund reservations.
        self.assertEqual(epochs.delivery.box.uncertain_writes(),unknown)
        quota=PersistentCycleQuota(QuotaStore(quota_path,self.sid),(self.uid,'crash-period')).reconcile(*limits)
        self.assertEqual((quota.used[index],quota.remaining[index]),(10,0))
        self.assertEqual(UsageOutbox(box.path,self.sid).uncertain_writes(),unknown)
        self.assertEqual(epochs.delivery.box.write_resolution_history(),[])
        cores=([self.runtime.front.process,self.runtime.paced.core.process]
               if self.protocol=='reality' else [self.runtime.core.process])
        await self.runtime.close()
        self.assertTrue(all(core.returncode is not None for core in cores))
        with self.assertRaises(OSError):
            await asyncio.open_connection('127.0.0.1',self.port)
        if self.protocol=='reality':
            for client in self.clients:
                await self.runtime.front._stop(client)
                self.assertIsNotNone(client.returncode,'REALITY client core leaked')

    async def test_kill_before_write_then_restart_preserves_unknown_and_budget(self):
        await self.crash('before_write')

    async def test_kill_after_write_then_restart_settles_only_committed_lower_bound(self):
        await self.crash('before_checkpoint')

    async def test_kill_before_commit_then_restart_rolls_back_and_keeps_unknown(self):
        await self.crash('before_commit')

    async def test_kill_after_commit_then_restart_settles_exact_counters_once(self):
        await self.crash('after_commit')


@unittest.skipUnless(os.name=='posix' and test_xray.BINARY.is_file() and
                     shutil.which('openssl') and ssl.HAS_TLSv1_3,
                     'requires POSIX, real Xray, OpenSSL and TLS 1.3')
class RealityCrashTests(unittest.IsolatedAsyncioTestCase):
    protocol="reality"
    asyncSetUp=test_reality_checkpoint.RealityCheckpointTests.asyncSetUp
    asyncTearDown=test_reality_checkpoint.RealityCheckpointTests.asyncTearDown
    echo=test_reality_checkpoint.RealityCheckpointTests.echo
    client=test_reality_checkpoint.RealityCheckpointTests.client
    replacement_runtime=RuntimeCrashTests.replacement_runtime
    assert_cores_stopped=RuntimeCrashTests.assert_cores_stopped
    crash=RuntimeCrashTests.crash

    async def open(self):
        reader,writer=await self.client()
        self.addAsyncCleanup(RuntimeCrashTests.close_writer,self,writer)
        writer.write(b'hello'); await writer.drain()
        return reader,writer

    async def accepted(self,reader):
        self.assertEqual(await asyncio.wait_for(reader.readexactly(5),5),b'hello')

    test_kill_before_write_then_restart_preserves_unknown_and_budget=RuntimeCrashTests.test_kill_before_write_then_restart_preserves_unknown_and_budget
    test_kill_after_write_then_restart_settles_only_committed_lower_bound=RuntimeCrashTests.test_kill_after_write_then_restart_settles_only_committed_lower_bound
    test_kill_before_commit_then_restart_rolls_back_and_keeps_unknown=RuntimeCrashTests.test_kill_before_commit_then_restart_rolls_back_and_keeps_unknown
    test_kill_after_commit_then_restart_settles_exact_counters_once=RuntimeCrashTests.test_kill_after_commit_then_restart_settles_exact_counters_once

    async def test_download_kill_before_write_preserves_unknown_and_budget(self):
        await self.crash('before_write','download')

    async def test_download_kill_after_write_settles_only_committed_lower_bound(self):
        await self.crash('before_checkpoint','download')

    async def test_download_kill_before_commit_rolls_back_and_keeps_unknown(self):
        await self.crash('before_commit','download')

    async def test_download_kill_after_commit_settles_exact_counters_once(self):
        await self.crash('after_commit','download')
