"""Explicit ephemeral test harness. Never imported by execution_agent/scheduler.

This is a process-liveness test only: every outbound is blackholed, no meter.
"""
import asyncio
import copy
import json
import os
import sqlite3
from pathlib import Path
import tempfile
import time
import uuid
import weakref
import threading
from contextlib import contextmanager

from core import Registry,Error
from grant_agent import validate

_GATES=weakref.WeakKeyDictionary()

def gate_for(registry):
    harness=_GATES.get(registry)
    if (harness is None or harness.closed or
            Path(registry.path).resolve()!=harness.path.resolve()): return None
    return harness

def configuration(value):
    node=value['node_config']
    if (node.get('listen_host')!='127.0.0.1' or node.get('protocol')!='vless'
            or 'reality' in node or node.get('enabled') is not True):
        raise Error('隔离multi仅允许fixture回环VLESS',409)
    return {'log':{'loglevel':'none'},'inbounds':[{
        'tag':'isolated-multi','listen':'127.0.0.1','port':node['listen_port'],
        'protocol':'vless','settings':{'decryption':'none',
        'clients':[{'id':value['client']['uuid']}]},
        'streamSettings':{'network':'raw','security':'none'}}],
        'outbounds':[{'tag':'deny-all','protocol':'blackhole'}]}

class MultiAgentTestHarness:
    def __init__(self):
        self.temp=tempfile.TemporaryDirectory(prefix='multi-execution-fixture-')
        self.path=Path(self.temp.name)/'registry.db'
        self.closed=False
        self.registry=Registry(self.path)
        _GATES[self.registry]=self
        self.lock=asyncio.Lock()
        self.runtimes=[]
        self.allocations={}
        self.tail_stores={}
        self.fixture_host_id=str(uuid.uuid4())
        self.fixture_boot_id=str(uuid.uuid4())
        self.transition_lock=threading.RLock()
        self.loop=asyncio.new_event_loop()
        self.thread=threading.Thread(target=self.loop.run_forever,daemon=True)
        self.thread.start()

    @contextmanager
    def transition(self):
        acquired=self.transition_lock.acquire(blocking=threading.current_thread() is not self.thread)
        if not acquired: raise Error('隔离生命周期正在推进',409)
        try:
            yield
        finally:
            self.transition_lock.release()

    async def _stop(self,ident):
        runtime=self.allocations.get(ident)
        if runtime is None: return
        process=runtime.process
        await runtime.close()
        if process is not None and process.returncode is None:
            raise RuntimeError('隔离进程关闭未确认')
        port=runtime.fixture_port
        try:
            _,writer=await asyncio.open_connection('127.0.0.1',port)
        except OSError:
            return
        writer.close(); await writer.wait_closed()
        raise RuntimeError('隔离监听关闭未确认')

    def stop_sync(self,ident):
        # Runtime owns a separate loop, so HTTP and callers can wait for actual
        # process termination without starving the process child watcher.
        if threading.current_thread() is self.thread:
            raise RuntimeError('停止必须在runtime loop之外等待')
        asyncio.run_coroutine_threadsafe(self._stop(ident),self.loop).result(timeout=15)

    def reopen(self):
        if self.closed: raise ValueError('fixture harness已关闭')
        self.registry=Registry(self.path)
        _GATES[self.registry]=self
        return self.registry

    async def install(self,token,stage,value,binary):
        future=asyncio.run_coroutine_threadsafe(self._install(token,stage,value,binary),self.loop)
        return await asyncio.wrap_future(future)

    async def _install(self,token,stage,value,binary):
        from multi_agent_lifecycle import MultiAgentLifecycle
        from multi_agent_installation import MultiAgentInstallation,fact_for
        from multi_agent_execution import MultiAgentExecution
        from xray_runtime import XrayRuntime
        async with self.lock:
          with self.transition():
            if gate_for(self.registry) is not self: raise ValueError('隔离gate关闭')
            value=copy.deepcopy(value)
            validate(value,stage.server_id,stage.session_id)
            generated=configuration(value)
            with self.registry.connect() as db:
                binding=db.execute('SELECT payload,digest FROM usage_multi_agent_configurations WHERE allocation_id=?',
                                   (value['grant_id'],)).fetchone()
            if binding is None or binding['digest']!=value['digest']:
                raise ValueError('非fixture配置')
            from multi_agent_tail import attach
            with self.registry.connect() as db:value=attach(db,value)
            store=self.tail_stores.get(value['grant_id'])
            if 'tail_authority' in value and (store is None or store.recovered or store.failed
                    or store.auth!=value['tail_authority']):
                raise ValueError('tail安装缺少原新版本本地持久store')
            with sqlite3.connect(stage.path) as db:
                rows=db.execute('SELECT ledger_kind,payload,digest FROM staged_grants WHERE grant_id=?',
                                 (value['grant_id'],)).fetchall()
            if rows!=[('multi_allocation',binding['payload'],binding['digest'])]:
                raise ValueError('隔离multi暂存身份不一致')
            # Verify fixture ownership and the complete master gate before process construction.
            lifecycle=MultiAgentLifecycle(self.registry)
            body=dict(ledger_kind='multi_allocation',ledger_id=value['grant_id'],
                      session_id=value['session_id'],binding_digest=value['digest'])
            state=lifecycle.advance(token,dict(body,action='begin'))
            if state['state']!='installing': raise ValueError('隔离执行不可重装')
            root=Path(self.temp.name)/'grants'/'multi_allocation'/value['grant_id']
            root.mkdir(parents=True,exist_ok=True)
            journal=root/'execution.json'
            if journal.exists():
                fact=json.loads(journal.read_text())
            else:
                fact=fact_for(value,value['tail_authority']['execution_id'] if 'tail_authority' in value else str(uuid.uuid4()))
                # Exclusive creation: an ambiguous execution never gets a new UUID.
                with journal.open('x') as file:
                    json.dump(fact,file,sort_keys=True,allow_nan=False)
                    file.flush(); os.fsync(file.fileno())
            from grant_snapshot import snapshot_for
            snapshot=snapshot_for(value)
            runtime=XrayRuntime(binary,root/'runtime',configuration_factory=lambda _:generated)
            self.runtimes.append(runtime)
            if value['grant_id'] in self.allocations:
                raise ValueError('隔离allocation已有执行实例')
            self.allocations[value['grant_id']]=runtime
            runtime.fixture_port=generated['inbounds'][0]['port']
            try:
                await runtime.apply(snapshot)
                validate({k:v for k,v in value.items() if k!='tail_authority'},stage.server_id,stage.session_id)
                if runtime.process is None or runtime.process.returncode is not None:
                    raise ValueError('隔离runtime未存活')
                runtime.fixture_process=runtime.process
                runtime.fixture_execution_id=fact['execution_id']
                runtime.fixture_instance_id=str(uuid.uuid4())
                runtime.fixture_start_id=str(uuid.uuid4())
                if 'tail_authority' in value:
                    with self.registry.connect() as db:
                        db.execute('INSERT INTO usage_multi_fixture_instances VALUES(?,?,?,?,?,?,?,?,?)',
                            (value['grant_id'],fact['execution_id'],value['tail_authority']['body_digest'],
                             self.fixture_host_id,self.fixture_boot_id,runtime.fixture_instance_id,
                             runtime.fixture_start_id,runtime.fixture_process.pid,time.time()))
                installation=MultiAgentInstallation(self.registry).accept(token,dict(fact=fact))
                lifecycle.advance(token,dict(body,action='installed'))
                response=MultiAgentExecution(self.registry).record(token,dict(fact=fact))
                if 'tail_authority' in value:
                    store.acknowledge(installation,response)
                with (root/'execution-receipt.json').open('w') as file:
                    json.dump(response,file,sort_keys=True,allow_nan=False)
                    file.flush(); os.fsync(file.fileno())
                async def expire():
                    await asyncio.sleep(max(0,value['valid_until']-time.time()))
                    try:
                        if store is not None:await asyncio.to_thread(store.stop)
                        else:await self._stop(value['grant_id'])
                    finally:
                        # Keep blocked allocations busy even if shutdown failed.
                        with self.registry.connect() as db:
                            db.execute("UPDATE usage_multi_lifecycle SET state=?,updated_at=? WHERE allocation_id=? AND state!='closed'",
                                       ('blocked',time.time(),value['grant_id']))
                runtime.fixture_expiry=asyncio.create_task(expire())
                return runtime,response
            except BaseException:
                try:
                    await self._stop(value['grant_id'])
                finally:
                    with self.registry.connect() as db:
                        db.execute('UPDATE usage_multi_lifecycle SET state=?,updated_at=? WHERE allocation_id=?',
                                   ('blocked',time.time(),value['grant_id']))
                raise

    async def close(self):
        self.closed=True
        await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(self._close(),self.loop))
        self.loop.call_soon_threadsafe(self.loop.stop)
        await asyncio.to_thread(self.thread.join)
        self.loop.close()
        self.temp.cleanup()

    async def _close(self):
        for runtime in self.runtimes:
            task=getattr(runtime,'fixture_expiry',None)
            if task:
                task.cancel()
                await asyncio.gather(task,return_exceptions=True)
            await runtime.close()
