"""Local VLESS TCP admission and per-user pacing before an isolated Xray core."""
import asyncio
import copy
import ipaddress
import hmac
import logging
from dataclasses import dataclass, field
import socket
import time
import uuid

from bandwidth import BytePacer, CombinedPacer
from automatic_bandwidth import DownloadStore
from payload_meter import PayloadMeter, MeteredWriter, QuotaExceeded, _stored
from quota_store import QuotaStore, PersistentCycleQuota
from pathlib import Path
from node_config import digest, validate_snapshot, user_deadlines
from xray_runtime import XrayRuntime

logger=logging.getLogger(__name__)


def usage_only_update(previous, current):
    """Only receipt baselines may change without replacing live transports."""
    if previous is None: return False
    def policy(value):
        value=copy.deepcopy(value)
        value.pop('revision',None)
        value.pop('digest',None)
        for node in value['nodes']:
            for client in node['clients']:
                client.pop('cycle_usage',None)
        return value
    return policy(previous)==policy(current)


@dataclass
class Endpoint:
    node: dict
    inner_port: int
    server: object = None
    clients: dict = field(default_factory=dict)


class VlessRuntime:
    def __init__(self, binary, state_dir, *, local_test_target=None, max_connections=256,
                 handshake_timeout=5):
        self.core=XrayRuntime(binary,state_dir,local_test_target=local_test_target)
        self.snapshot=None
        self.deadlines={}
        self.desired_identity=None
        self.listeners={}
        self.inner_ports={}
        self.pacers={}
        self.node_pacers={}
        self.automatic=DownloadStore(
            Path(state_dir)/'automatic-bandwidth.json',
            async_save=lambda uid,state:self._storage(self.automatic.save,uid,state))
        self.meter=PayloadMeter()
        self.quotas={}
        # Cache active periods only; historical reservations remain durable.
        self.cycle_quotas={}
        self.quota_path=Path(state_dir)/'cycle-quota.sqlite'
        self.server_pacers=(BytePacer(None),BytePacer(None))
        self.tasks=set()
        # Own accepted callbacks before their coroutine gets its first turn.
        self.accept_tasks=set()
        self.task_users={}
        self.task_ips={}
        self.task_scopes={}
        # Constructor-owned REALITY bridge secrets; never accepted from snapshots.
        self.trusted_ingress={}
        self.max_connections=max_connections
        self.handshake_timeout=handshake_timeout
        self.updating=False
        self.expiry_task=None
        self.lifecycle=asyncio.Lock()

    @staticmethod
    def _policies(snapshot):
        policies={}
        if snapshot is None: return policies
        for node in snapshot['nodes']:
            if not node['config']['enabled']: continue
            for client in node['clients']:
                limits=client.get('bandwidth',{'upload_bps':None,'download_bps':None})
                policies[(node['id'],client['uuid'])]=(client['user_id'],limits,client.get('connections',{'max_connections':None}),client.get('automatic_bandwidth'),client.get('node_bandwidth'),client.get('node_connections'),client.get('cycle_id'),client.get('cycle_budget'),client.get('cycle_usage'))
        return policies

    async def _serve(self, entry, reader, writer):
        task=asyncio.current_task()
        upstream=None
        phase='client_header'
        if self.updating or len(self.tasks)>=self.max_connections or (self.snapshot and not self.deadlines and self.snapshot.get('valid_until',float('inf'))<=time.time()):
            writer.close()
            try: await writer.wait_closed()
            except OSError: pass
            return
        self.tasks.add(task)
        try:
            async def header():
                source=None
                secret=self.trusted_ingress.get(entry.node['id'])
                if secret is not None:
                    peer=ipaddress.ip_address(writer.get_extra_info('peername')[0])
                    if not peer.is_loopback: raise ValueError('桥接来源无效')
                    proxy=await reader.readexactly(16)
                    if proxy[:12]!=b'\r\n\r\n\0\r\nQUIT\n' or proxy[12]!=0x21:
                        raise ValueError('来源协议无效')
                    length=int.from_bytes(proxy[14:16],'big')
                    expected={0x11:12,0x21:36}.get(proxy[13])
                    if length!=expected: raise ValueError('来源地址无效')
                    addresses=await reader.readexactly(length)
                    source=str(ipaddress.ip_address(addresses[:4 if expected==12 else 16]))
                    # Authenticate the local transport before trusting PROXY data.
                    http=await reader.readuntil(b'\r\n\r\n')
                    if len(http)>4096 or not hmac.compare_digest(http.split(b'\r\n',1)[0],
                            f'GET /{secret} HTTP/1.1'.encode()):
                        raise ValueError('桥接认证失败')
                identity=await reader.readexactly(17)
                if identity[0]!=0: raise ValueError('不支持的 VLESS 版本')
                client=entry.clients.get(str(uuid.UUID(bytes=identity[1:])))
                if client is None: raise ValueError('用户未授权')
                size=await reader.readexactly(1)
                # No flow/extension negotiation in the supported raw TCP mode.
                if size!=b'\0': raise ValueError('当前入口不支持 VLESS 附加流控')
                address=await reader.readexactly(4)  # TCP command, port, address type
                if address[0]!=1 or address[1:3]==b'\0\0': raise ValueError('仅支持有效 TCP 目标')
                if address[3]==1: tail=await reader.readexactly(4)
                elif address[3]==3: tail=await reader.readexactly(16)
                elif address[3]==2:
                    length=await reader.readexactly(1)
                    if not length[0]: raise ValueError('域名为空')
                    tail=length+await reader.readexactly(length[0])
                else: raise ValueError('地址类型无效')
                return client,identity+size+address+tail,source
            client,request,trusted_source=await asyncio.wait_for(header(),self.handshake_timeout)
            if task not in self.tasks: return
            deadline=client.get('valid_until', self.snapshot.get('valid_until') if not self.deadlines else None)
            if deadline is not None and deadline<=time.time(): return
            # Reserve before awaiting upstream connection. All nodes of this
            # runtime share reservations; node overrides use their own pool while
            # inherited nodes share the general pool. Online IPs stay global.
            scoped='node_connections' in client
            scope=(client['user_id'],entry.node['id'] if scoped else None)
            maximum=(client['node_connections'] if scoped else client.get('connections',{})).get('max_connections')
            if maximum is not None and sum(existing==scope for existing in self.task_scopes.values())>=maximum:
                return
            maximum_ips=client.get('connections',{}).get('max_ips')
            peer=writer.get_extra_info('peername')
            address=ipaddress.ip_address(trusted_source or peer[0])
            # IPv4-mapped IPv6 and IPv4 represent the same online address.
            address=str(getattr(address,'ipv4_mapped',None) or address)
            online={self.task_ips[t] for t,ident in self.task_users.items() if ident==client['user_id']}
            if maximum_ips is not None and address not in online and len(online)>=maximum_ips:
                return
            self.task_users[task]=client['user_id']
            self.task_scopes[task]=scope
            self.task_ips[task]=address
            phase='core_connect'
            remote,upstream=await asyncio.wait_for(asyncio.open_connection('127.0.0.1',entry.inner_port),5)
            if task not in self.tasks: return
            if deadline is not None and deadline<=time.time(): return
            effective_pacers=self.node_pacers.get((entry.node['id'],client['user_id']),self.pacers[client['user_id']])
            upload,download=(CombinedPacer(user,total,deadline=deadline) for user,total in zip(effective_pacers,self.server_pacers))
            # Only application payload is paced. Throttling the authentication
            # header could make a valid low-rate user fail Xray's handshake timer.
            phase='core_header'
            upstream.write(request); await upstream.drain()

            def before_payload():
                # Storage awaits can outlast a lease or a revocation. The parent
                # connection is removed before cancellation is awaited.
                if self.updating or task not in self.tasks:
                    raise OSError('连接配置已撤销')
                if deadline is not None and time.time()>=deadline:
                    raise asyncio.TimeoutError('临时配置已到期')
                self._payload_guard()

            async def pump(source,destination,pacer,direction,automatic=None):
                destination=MeteredWriter(destination,self.meter,
                    (entry.node['id'],client['user_id'],client['uuid']),direction,
                    self.quotas.get((client['user_id'],client.get('cycle_id'))),
                    before_write=before_payload)
                while True:
                    data=await source.read(65536)
                    if not data:
                        if destination.can_write_eof():
                            destination.write_eof(); await destination.drain()
                        return
                    if automatic is None: await pacer.write(destination,data)
                    else:
                        await automatic.write(destination,data,effective_pacers[1],self.server_pacers[1],deadline)
            async def response():
                prelude=await remote.readexactly(2)
                if prelude[0]!=0: raise ValueError('Xray 响应版本无效')
                addons=await remote.readexactly(prelude[1])
                if trusted_source is not None:
                    writer.write(b'HTTP/1.1 200 OK\r\n\r\n')
                writer.write(prelude+addons); await writer.drain()
                await pump(remote,writer,download,1,self.automatic.controllers.get(client['user_id']))
            directions=[asyncio.create_task(pump(reader,upstream,upload,0)),
                        asyncio.create_task(response())]
            phase='payload'
            try: await asyncio.gather(*directions)
            finally:
                for direction in directions: direction.cancel()
                await asyncio.gather(*directions,return_exceptions=True)
        except (OSError,ValueError,QuotaExceeded,asyncio.IncompleteReadError,asyncio.LimitOverrunError,asyncio.TimeoutError) as error:
            # No addresses, UUIDs, payload or exception text in diagnostics.
            # Quota exhaustion is an expected close, not a transport failure.
            if not isinstance(error,QuotaExceeded):
                process=self.core.process
                logger.debug('VLESS connection closed: phase=%s error=%s errno=%s revision=%s core_pid=%s core_returncode=%s',
                             phase,type(error).__name__,getattr(error,'errno',None),
                             self.snapshot.get('revision') if self.snapshot else None,
                             process.pid if process else None,process.returncode if process else None)
        finally:
            for stream in (writer,upstream):
                if stream: stream.close()
            for stream in (writer,upstream):
                if stream:
                    try: await stream.wait_closed()
                    except OSError: pass
            self.tasks.discard(task)
            self.task_users.pop(task,None)
            self.task_ips.pop(task,None)
            self.task_scopes.pop(task,None)

    def _accept(self,entry,reader,writer):
        task=asyncio.create_task(self._serve(entry,reader,writer))
        self.accept_tasks.add(task)
        def completed(task):
            self.accept_tasks.discard(task)
            # A task cancelled before its first turn cannot run its finally.
            writer.close()
        task.add_done_callback(completed)

    async def _cancel_connections(self):
        tasks=list(self.tasks | self.accept_tasks)
        self.tasks.clear()
        self.accept_tasks.clear()
        for task in tasks: task.cancel()
        if tasks: await asyncio.gather(*tasks,return_exceptions=True)

    async def apply(self,value):
        async with self.lifecycle:
            await self._apply(value)

    def usage_storage_boundary(self):
        return self.meter.io_lock

    async def _storage(self, function, *args):
        async with self.meter.io_lock:
            return await _stored(function,*args)

    def usage_snapshot(self):
        return self.meter.snapshot()

    def set_usage_checkpoint(self, checkpoint):
        self.meter.set_checkpoint(checkpoint)

    def set_usage_write_intent(self, callback):
        self.meter.set_write_intent(callback)

    def set_incremental_usage_checkpoint(self, checkpoint):
        self.meter.set_incremental_checkpoint(checkpoint)

    async def _apply(self,value):
        snapshot=copy.deepcopy(validate_snapshot(value))
        for node in snapshot['nodes']:
            if node['config'].get('protocol')!='vless': raise ValueError('VLESS 执行端不支持 TCP 转发节点')
            if 'reality' in node['config']: raise ValueError('REALITY 必须通过 RealityRuntime 加密入口')
        if snapshot.get('valid_until',float('inf'))<=time.time():
            raise ValueError('临时配置已到期，请重新同步')
        policies=self._policies(snapshot)
        user_limits={}
        auto_rules={}
        for user_id,limits,_connections,auto,_node_limits,_node_connections,_cycle_id,_cycle_budget,_cycle_usage in policies.values():
            if user_id in user_limits and limits!=user_limits[user_id]:
                raise ValueError('同一用户的节点限速配置不一致')
            user_limits[user_id]=limits
            if auto and auto['enabled']: auto_rules[user_id]=auto
        identity=(snapshot['server_id'],snapshot['revision'],snapshot['digest'])
        if self.desired_identity is not None:
            server,revision,checksum=self.desired_identity
            if identity[0]!=server: raise ValueError('服务器身份变化')
            if identity[1]<revision: raise ValueError('拒绝旧配置版本')
            if identity[1]==revision and identity[2]!=checksum:
                raise ValueError('相同配置版本内容不一致')
        # Keep the desired version across shutdown/failure; snapshot describes
        # only the running configuration and may be cleared by _close().
        self.desired_identity=identity
        await self._storage(self.meter.bind,snapshot['server_id'])
        if self.snapshot:
            if snapshot['server_id']!=self.snapshot['server_id']: raise ValueError('服务器身份变化')
            if snapshot['revision']<self.snapshot['revision']: raise ValueError('拒绝旧配置版本')
            if snapshot['revision']==self.snapshot['revision']:
                if snapshot['digest']!=self.snapshot['digest']: raise ValueError('相同配置版本内容不一致')
                if self.core.process and self.core.process.returncode is None: return
                if not any(n['config']['enabled'] for n in snapshot['nodes']): return
            if (usage_only_update(self.snapshot,snapshot) and
                    self.core.process and self.core.process.returncode is None):
                try:
                    # Existing MeteredWriters hold these same durable quota objects.
                    # Reconcile high water in place; never refund local reservations.
                    for policy in policies.values():
                        user_id=policy[0]
                        cycle_id,budget,used=policy[-3:]
                        if cycle_id is None: continue
                        budget=budget or {}; used=used or {}
                        await self._storage(self.quotas[(user_id,cycle_id)].reconcile,
                            budget.get('upload_bytes'),budget.get('download_bytes'),
                            used.get('upload_bytes',0),used.get('download_bytes',0))
                    self.snapshot=snapshot
                    return
                except BaseException:
                    # A failed durable baseline must not leave live writers active.
                    await self._close()
                    raise
            previous=self._policies(self.snapshot)
            server_changed=(snapshot.get('bandwidth')!=self.snapshot.get('bandwidth') or
                            snapshot.get('valid_until')!=self.snapshot.get('valid_until'))
            if server_changed or any(key not in policies or policies[key]!=policy for key,policy in previous.items()):
                # A failed update must not restore revoked users or obsolete limits.
                await self._close()
        wanted={(n['config']['listen_host'],n['config']['listen_port']):n
                for n in snapshot['nodes'] if n['config']['enabled']}
        added={}
        reservations=[]
        activated=False
        self.updating=True
        try:
            public_ports={port for _,port in wanted}
            node_ids={node['id'] for node in wanted.values()}
            ports={ident:port for ident,port in self.inner_ports.items()
                   if ident in node_ids and port not in public_ports}
            used={port for _,port in wanted}
            used.update(ports.values())
            for endpoint,node in wanted.items():
                if node['id'] not in ports:
                    while True:
                        reserve=socket.socket()
                        try: reserve.bind(('127.0.0.1',0))
                        except BaseException:
                            reserve.close(); raise
                        port=reserve.getsockname()[1]
                        reservations.append(reserve)
                        if port not in used: break
                    ports[node['id']]=port
                    used.add(port)
                if endpoint in self.listeners: continue
                entry=Endpoint(node,ports[node['id']])
                entry.server=await asyncio.start_server(lambda r,w,e=entry:self._accept(e,r,w),
                                                        *endpoint,start_serving=False)
                added[endpoint]=entry
            inner=copy.deepcopy(snapshot)
            inner.pop('bandwidth',None)
            inner.pop('valid_until',None)
            for node in inner['nodes']:
                if node['config']['enabled']:
                    node['config']['listen_host']='127.0.0.1'
                    node['config']['listen_port']=ports[node['id']]
                for client in node['clients']:
                    client.pop('bandwidth',None)
                    client.pop('valid_until',None)
                    client.pop('connections',None)
                    client.pop('automatic_bandwidth',None)
                    client.pop('node_bandwidth',None)
                    client.pop('node_connections',None)
                    client.pop('cycle_id',None)
                    client.pop('cycle_budget',None)
                    client.pop('cycle_usage',None)
            inner['digest']=digest({k:v for k,v in inner.items() if k!='digest'})
            # Retry the same desired revision with identical private endpoints,
            # including after a stop or a failed core launch.
            self.inner_ports=ports
            for reserve in reservations: reserve.close()
            await self.core.apply(inner)
            activated=True
            await self._cancel_connections()
            quotas={}
            for (node_id,credential),policy in policies.items():
                user_id,*rest=policy
                cycle_id,budget,used=rest[-3:]
                if cycle_id is not None:
                    key=(user_id,cycle_id)
                    budget=budget or {}; used=used or {}
                    quota=await self._cycle_quota(snapshot,key,budget,used)
                    quotas[key]=quota
                    # A user may be present on multiple nodes; all credentials
                    # share the same cycle allowance.
            self.cycle_quotas=quotas.copy()
            self.quotas=quotas
            for endpoint,entry in list(self.listeners.items()):
                if endpoint not in wanted:
                    entry.server.close(); await entry.server.wait_closed()
            self.listeners={key:entry for key,entry in self.listeners.items() if key in wanted}
            self.listeners.update(added)
            limits=snapshot.get('bandwidth',{})
            self.server_pacers=(BytePacer(limits.get('upload_bps')),BytePacer(limits.get('download_bps')))
            self.pacers={user_id:(BytePacer(limits['upload_bps']),BytePacer(limits['download_bps']))
                         for user_id,limits in user_limits.items()}
            self.node_pacers={(node['id'],client['user_id']):(BytePacer(client['node_bandwidth']['upload_bps']),BytePacer(client['node_bandwidth']['download_bps']))
                              for node in snapshot['nodes'] if node['config']['enabled']
                              for client in node['clients'] if 'node_bandwidth' in client}
            # Loading and atomically pruning automatic-bandwidth state performs
            # JSON/fsync work. Keep it off the event loop and inside the same
            # storage boundary as quota reconciliation; lifecycle lock remains
            # the outer ordering lock, so close never waits while holding the
            # meter lock needed by an in-flight payload.
            await self._storage(self.automatic.configure,snapshot['server_id'],auto_rules)
            for endpoint,entry in self.listeners.items():
                entry.node=wanted[endpoint]
                entry.inner_port=ports[entry.node['id']]
                entry.clients={client['uuid']:client for client in entry.node['clients']}
            for entry in added.values(): await entry.server.start_serving()
            self.snapshot=snapshot
            self.deadlines=user_deadlines(snapshot)
            await self._cancel_expiry()
            if 'valid_until' in snapshot:
                self.expiry_task=asyncio.create_task(self._expire(snapshot['valid_until']))
        except BaseException:
            for entry in added.values():
                entry.server.close(); await entry.server.wait_closed()
            if activated or self.core.snapshot is None: await self._close()
            raise
        finally:
            for reserve in reservations: reserve.close()
            self.updating=False

    def _payload_guard(self):
        """Additional immutable execution constraints, checked after storage."""

    async def _cycle_quota(self,snapshot,key,budget,used):
        if key not in self.cycle_quotas:
            self.cycle_quotas[key]=PersistentCycleQuota(
                QuotaStore(self.quota_path,snapshot['server_id']),key)
        quota=self.cycle_quotas[key]
        await self._storage(quota.reconcile,budget.get('upload_bytes'),budget.get('download_bytes'),
                            used.get('upload_bytes',0),used.get('download_bytes',0))
        return quota

    async def _cancel_expiry(self):
        task=self.expiry_task
        self.expiry_task=None
        if task is not None and task is not asyncio.current_task():
            task.cancel()
            await asyncio.gather(task,return_exceptions=True)

    async def _expire(self, deadline):
        # Legacy snapshots carry only a global deadline. New snapshots isolate
        # expired users while keeping unrelated ingress and core processes alive.
        if not self.deadlines:
            while (remaining:=deadline-time.time())>0:
                await asyncio.sleep(remaining)
            await self.close()
            return
        try:
            for expires in sorted(set(self.deadlines.values())):
                # Timers may wake early; wall clocks may move backwards.
                # Recheck instead of permanently skipping this identity.
                while (remaining:=expires-time.time())>0:
                    await asyncio.sleep(remaining)
                async with self.lifecycle:
                    expired={uid for uid,end in self.deadlines.items() if end<=time.time()}
                    tasks=[task for task,uid in self.task_users.items() if uid in expired]
                    for task in tasks: task.cancel()
                    if tasks: await asyncio.gather(*tasks,return_exceptions=True)
        finally:
            if self.expiry_task is asyncio.current_task(): self.expiry_task=None

    async def close(self):
        async with self.lifecycle:
            await self._close()

    async def _close(self):
        self.updating=True
        await self._cancel_expiry()
        for entry in self.listeners.values(): entry.server.close()
        await self._cancel_connections()
        for entry in self.listeners.values(): await entry.server.wait_closed()
        await self.core.close()
        self.listeners={}
        self.pacers={}
        self.node_pacers={}
        self.quotas={}
        self.cycle_quotas={}
        self.server_pacers=(BytePacer(None),BytePacer(None))
        self.snapshot=None
        self.deadlines={}
