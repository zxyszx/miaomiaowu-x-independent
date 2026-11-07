"""REALITY termination, re-authenticated through the existing paced VLESS gateway.

The TLS core has no direct Internet outbound. Each authenticated user is routed
to its own VLESS outbound using that user's credential and the private gateway.
The private gateway remains responsible for shared limits and expiry.
"""
import asyncio
import copy
from pathlib import Path
import socket
import secrets
import time

from node_config import digest, validate_snapshot, user_deadlines
from vless_runtime import VlessRuntime, usage_only_update
from xray_runtime import XrayRuntime


class RealityRuntime:
    def __init__(self, binary, state_dir, *, local_test_target=None, reality_test_target=None):
        # Preserve the existing CLI state layout for plaintext-only deployments.
        self.paced=VlessRuntime(binary,Path(state_dir),local_test_target=local_test_target)
        # Only constructors used by isolated tests can override the decoy target.
        if reality_test_target is not None:
            host,port=reality_test_target
            if host!='127.0.0.1' or type(port) is not int or not 1<=port<=65535:
                raise ValueError('REALITY 测试目标必须为回环地址和有效端口')
        self.reality_test_target=reality_test_target
        self.front=XrayRuntime(binary,Path(state_dir)/'tls',configuration_factory=self._configuration)
        self.snapshot=None
        self.ports={}
        self.lifecycle=asyncio.Lock()
        self.expiry_task=None

    def _configuration(self,snapshot):
        inbounds=[]; outbounds=[{'tag':'blocked','protocol':'blackhole'}]; rules=[]
        for node in snapshot['nodes']:
            config=node['config']
            if not config['enabled'] or 'reality' not in config: continue
            transport=f'source-transport-{node["id"]}'
            port=self.ports[node['id']]
            outbounds.append({'tag':transport,'protocol':'freedom','settings':{
                'proxyProtocol':2,'userLevel':1,'redirect':f'127.0.0.1:{port}',
                'finalRules':[{'action':'allow','network':'tcp','ip':['127.0.0.1/32'],
                               'port':str(port)}]}})
            reality=config['reality']; tag=f'node-{node["id"]}'
            target=(self.reality_test_target or (reality['target_host'],reality['target_port']))
            inbounds.append({'tag':tag,'listen':config['listen_host'],'port':config['listen_port'],
                'protocol':'vless','settings':{'decryption':'none','clients':[
                    {'id':client['uuid'],'email':f'user-{client["user_id"]}'} for client in node['clients']]},
                'streamSettings':{'network':'raw','security':'reality','realitySettings':{
                    'show':False,'target':f'{target[0]}:{target[1]}','xver':0,
                    'serverNames':[reality['server_name']],'privateKey':reality['private_key'],
                    'shortIds':[reality['short_id']]}}})
            for client in node['clients']:
                outbound=f'gateway-{node["id"]}-{client["user_id"]}'
                outbounds.append({'tag':outbound,'protocol':'vless','settings':{'vnext':[{
                    'address':'127.0.0.1','port':self.ports[node['id']],
                    'users':[{'id':client['uuid'],'encryption':'none'}]}]},
                    'streamSettings':{'network':'raw','security':'none',
                        'sockopt':{'dialerProxy':transport},
                        'rawSettings':{'header':{'type':'http','request':{
                            'version':'1.1','method':'GET',
                            'path':['/'+self.paced.trusted_ingress[node['id']]]},
                            'response':{'version':'1.1','status':'200','reason':'OK'}}}}})
                rules.append({'type':'field','inboundTag':[tag],
                              'user':[f'user-{client["user_id"]}'],'outboundTag':outbound})
        # The pipe transport cannot propagate TCP half-close. Bound its drain
        # period instead of keeping disconnected users admitted for a minute.
        return {'policy':{'levels':{'1':{'downlinkOnly':1,'uplinkOnly':1}}},
                'log':{'loglevel':'none'},'inbounds':inbounds,'outbounds':outbounds,
                'routing':{'rules':rules}}

    def usage_storage_boundary(self):
        return self.paced.usage_storage_boundary()

    def usage_snapshot(self):
        # Both raw nodes and authenticated encrypted ingress pass the same meter.
        return self.paced.usage_snapshot()

    def set_usage_checkpoint(self, checkpoint):
        self.paced.set_usage_checkpoint(checkpoint)

    def set_usage_write_intent(self, callback):
        self.paced.set_usage_write_intent(callback)

    def set_incremental_usage_checkpoint(self, checkpoint):
        self.paced.set_incremental_usage_checkpoint(checkpoint)

    async def apply(self,value):
        async with self.lifecycle:
            snapshot=copy.deepcopy(validate_snapshot(value))
            if any(n['config'].get('protocol')!='vless' for n in snapshot['nodes']):
                raise ValueError('REALITY 执行端不支持 TCP 转发节点')
            if snapshot.get('valid_until',float('inf'))<=time.time():
                raise ValueError('临时配置已到期')
            if self.snapshot:
                if snapshot['server_id']!=self.snapshot['server_id']: raise ValueError('服务器身份变化')
                if snapshot['revision']<self.snapshot['revision']: raise ValueError('拒绝旧配置版本')
                if snapshot['revision']==self.snapshot['revision'] and snapshot['digest']!=self.snapshot['digest']:
                    raise ValueError('相同配置版本内容不一致')
            baseline_only=(usage_only_update(self.snapshot,snapshot) and
                           self.paced.snapshot is not None and
                           self.paced.core.process is not None and
                           self.paced.core.process.returncode is None and
                           self.front.process is not None and self.front.process.returncode is None)
            changed=self.snapshot!=snapshot and not baseline_only
            # Stop encrypted ingress before changing any credential or policy.
            # A failed update must never restore a revoked encrypted ingress.
            if changed: await self.front.close()
            # Fence the desired revision even if launch subsequently fails.
            # Otherwise replaying the previous snapshot could re-enable a user
            # revoked by the failed replacement.
            self.snapshot=snapshot
            reservations=[]
            try:
                reality_ids={n['id'] for n in snapshot['nodes'] if 'reality' in n['config']}
                self.paced.trusted_ingress={k:v for k,v in self.paced.trusted_ingress.items() if k in reality_ids}
                inner=copy.deepcopy(snapshot)
                forbidden={n['config']['listen_port'] for n in snapshot['nodes']}
                for node in inner['nodes']:
                    if 'reality' not in node['config']: continue
                    self.paced.trusted_ingress.setdefault(node['id'],secrets.token_urlsafe(32))
                    if node['id'] not in self.ports or self.ports[node['id']] in forbidden:
                        while True:
                            sock=socket.socket()
                            reservations.append(sock)
                            sock.bind(('127.0.0.1',0)); port=sock.getsockname()[1]
                            if port not in forbidden and port not in self.ports.values(): break
                        self.ports[node['id']]=port
                    node['config'].pop('reality')
                    node['config']['listen_host']='127.0.0.1'
                    node['config']['listen_port']=self.ports[node['id']]
                inner['digest']=digest({k:v for k,v in inner.items() if k!='digest'})
                for sock in reservations: sock.close()
                await self.paced.apply(inner)
                if any(n['config']['enabled'] and 'reality' in n['config'] for n in snapshot['nodes']):
                    if not baseline_only: await self.front.apply(snapshot)
                else:
                    await self.front.close()
                await self._cancel_expiry()
                if 'valid_until' in snapshot and not user_deadlines(snapshot):
                    self.expiry_task=asyncio.create_task(self._expire(snapshot['valid_until']))
            except BaseException:
                await self._close()
                raise
            finally:
                for sock in reservations: sock.close()

    async def _cancel_expiry(self):
        task=self.expiry_task; self.expiry_task=None
        if task is not None and task is not asyncio.current_task():
            task.cancel(); await asyncio.gather(task,return_exceptions=True)

    async def _expire(self,deadline):
        while (remaining:=deadline-time.time())>0:
            await asyncio.sleep(remaining)
        await self.close()

    async def _close(self):
        await self._cancel_expiry()
        await self.front.close()
        await self.paced.close()
        # Keep the last accepted revision as a replay guard after failure/expiry.

    async def close(self):
        async with self.lifecycle: await self._close()
