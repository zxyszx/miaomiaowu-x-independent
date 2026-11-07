"""Own Xray process adapter. Local trusted VLESS transport only at this stage."""
import asyncio
import copy
import json
import ipaddress
import os
import socket
from pathlib import Path
import tempfile
import time
from node_config import validate_snapshot


def configuration(snapshot, local_test_target=None):
    validate_snapshot(snapshot)
    if 'valid_until' in snapshot: raise ValueError('临时配置必须通过 VLESS 执行器验证有效期')
    if 'bandwidth' in snapshot: raise ValueError('服务器限速必须通过独立执行入口应用')
    inbounds = []
    for node in snapshot['nodes']:
        config = node['config']
        if 'reality' in config: raise ValueError('REALITY 必须通过 RealityRuntime 保留授权和限速入口')
        if any('bandwidth' in client or 'valid_until' in client or 'connections' in client or 'automatic_bandwidth' in client or 'node_bandwidth' in client or 'node_connections' in client or 'cycle_id' in client or 'cycle_budget' in client or 'cycle_usage' in client for client in node.get('clients',[])):
            raise ValueError('用户限速必须使用 VlessRuntime 入口，不能直接交给 Xray 核心忽略')
        if config.get('protocol') != 'vless':
            raise ValueError('Xray 执行端不能加载 TCP 转发节点，请为两种执行端使用不同服务器记录')
        if not config['enabled']: continue
        inbounds.append({'tag': f'node-{node["id"]}', 'listen': config['listen_host'],
            'port': config['listen_port'], 'protocol': 'vless',
            'settings': {'decryption': 'none', 'clients': [
                {'id': user['uuid'], 'email': f'user-{user["user_id"]}'} for user in node['clients']]},
            'streamSettings': {'network': 'raw', 'security': 'none'}})
    settings={}
    if local_test_target is not None:
        address,port=local_test_target
        ip=ipaddress.ip_address(address)
        if not ip.is_loopback or type(port) is not int or not 1 <= port <=65535:
            raise ValueError('隔离测试目标必须为回环地址和有效端口')
        settings['finalRules']=[{'action':'allow','network':'tcp','ip':[str(ip)+'/32' if ip.version==4 else str(ip)+'/128'],'port':str(port)}]
    return {'log': {'loglevel': 'none'}, 'inbounds': inbounds,
            'outbounds': [{'tag': 'direct', 'protocol': 'freedom', 'settings': settings}]}


class XrayRuntime:
    def __init__(self, binary, state_dir, *, local_test_target=None, configuration_factory=None):
        self.binary = str(Path(binary).resolve(strict=True))
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.snapshot = None
        self.desired_identity = None
        self.process = None
        self.active_file = None
        self.local_test_target=local_test_target
        self.configuration_factory=configuration_factory or (lambda value: configuration(value,self.local_test_target))

    async def _stop(self, process):
        if process is None or process.returncode is not None: return
        try: process.terminate()
        except ProcessLookupError:
            await process.wait()
            return
        try: await asyncio.wait_for(process.wait(), 3)
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()

    async def _start(self, path, generated):
        if not generated['inbounds']: return None
        # A foreign listener must not satisfy our subsequent readiness probe.
        # Probe all requested binds after stopping the old child, before launch.
        reserved=[]
        try:
            for inbound in generated['inbounds']:
                family=socket.AF_INET6 if ':' in inbound['listen'] else socket.AF_INET
                sock=socket.socket(family,socket.SOCK_STREAM)
                reserved.append(sock)
                sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
                sock.bind((inbound['listen'],inbound['port']))
        except OSError as error:
            raise RuntimeError('Xray 监听地址已被占用') from error
        finally:
            for sock in reserved: sock.close()
        process = await asyncio.create_subprocess_exec(self.binary, 'run', '-config', str(path),
            stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL)
        try:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if process.returncode is not None: raise RuntimeError('Xray 启动失败')
                ready = True
                for inbound in generated['inbounds']:
                    try:
                        _, writer = await asyncio.wait_for(asyncio.open_connection(inbound['listen'], inbound['port']), .2)
                        writer.close(); await writer.wait_closed()
                    except (OSError, asyncio.TimeoutError): ready = False; break
                if ready:
                    await asyncio.sleep(.2)
                    if process.returncode is not None: raise RuntimeError('Xray 启动失败')
                    return process
                await asyncio.sleep(.05)
            raise RuntimeError('Xray 监听启动超时')
        except BaseException:
            await self._stop(process)
            raise

    async def apply(self, value):
        snapshot = copy.deepcopy(validate_snapshot(value))
        generated = self.configuration_factory(snapshot)
        identity=(snapshot['server_id'],snapshot['revision'],snapshot['digest'])
        if self.desired_identity is not None:
            server,revision,checksum=self.desired_identity
            if identity[0]!=server: raise ValueError('服务器身份变化')
            if identity[1]<revision: raise ValueError('拒绝旧配置版本')
            if identity[1]==revision and identity[2]!=checksum:
                raise ValueError('相同配置版本内容不一致')
        # Stopping a child must not forget a newer revocation already received.
        self.desired_identity=identity
        if self.snapshot:
            if snapshot['server_id'] != self.snapshot['server_id']: raise ValueError('服务器身份变化')
            if snapshot['revision'] < self.snapshot['revision']: raise ValueError('拒绝旧配置版本')
            if snapshot['revision'] == self.snapshot['revision']:
                if snapshot['digest'] != self.snapshot['digest']: raise ValueError('相同配置版本内容不一致')
                if not generated['inbounds'] or (self.process and self.process.returncode is None): return
                # A dead child is restarted even if the desired revision has not changed.
        if self.snapshot:
            def permissions(value):
                return {(node['id'],user['uuid']) for node in value['nodes'] if node['config']['enabled']
                        for user in node.get('clients',[])}
            if permissions(self.snapshot)-permissions(snapshot):
                # Never restore revoked credentials if the replacement fails to start.
                await self.close()
        candidate = None
        check = None
        try:
            with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=self.state_dir, suffix='.json', delete=False) as file:
                candidate = Path(file.name)
                json.dump(generated, file, allow_nan=False)
                file.flush(); os.fsync(file.fileno())
            check = await asyncio.create_subprocess_exec(self.binary, 'run', '-test', '-config', str(candidate),
                stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
            if await asyncio.wait_for(check.wait(), 10) != 0: raise ValueError('Xray 配置校验失败')
            old_file, old_snapshot = self.active_file, self.snapshot
            await self._stop(self.process)
            self.process = None
            try:
                self.process = await self._start(candidate, generated)
            except Exception as error:
                if old_file and old_snapshot:
                    try: self.process = await self._start(old_file, self.configuration_factory(old_snapshot))
                    except Exception:
                        self.snapshot = None
                        raise RuntimeError('Xray 更新失败，旧配置恢复也失败；执行端未运行') from error
                    raise RuntimeError('Xray 更新失败，已恢复旧配置') from error
                raise RuntimeError('Xray 启动失败，当前未运行') from error
            self.active_file, self.snapshot = candidate, snapshot
            candidate = None
            if old_file: old_file.unlink(missing_ok=True)
        finally:
            await self._stop(check)
            if candidate: candidate.unlink(missing_ok=True)

    async def close(self):
        await self._stop(self.process)
        self.process = None
        if self.active_file: self.active_file.unlink(missing_ok=True)
        self.active_file, self.snapshot = None, None
