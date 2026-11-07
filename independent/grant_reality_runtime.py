"""Explicit local REALITY runtime for one immutable grant envelope.

The encrypted front end and its private paced VLESS gateway share the same
signed grant and GrantPayloadQuota.  Ordinary snapshots are rejected so a
REALITY listener cannot silently fall back to the normal cycle ledger.
"""
import asyncio
import copy
import secrets
import socket
import time
from pathlib import Path

from grant_agent import validate
from grant_payload import GrantPayloadQuota
from grant_installation import record as record_installation
from node_config import digest, user_deadlines
from reality_runtime import RealityRuntime
from grant_runtime import GrantVlessRuntime


class GrantRealityRuntime(RealityRuntime):
    def __init__(self, binary, state_dir, stage, *, local_test_target=None,
                 reality_test_target=None):
        self.stage = stage
        root = Path(state_dir)
        self.paced = GrantVlessRuntime(binary, root / 'paced', stage,
                                       local_test_target=local_test_target)
        if reality_test_target is not None:
            host, port = reality_test_target
            if host != '127.0.0.1' or type(port) is not int or not 1 <= port <= 65535:
                raise ValueError('REALITY 测试目标必须为回环地址和有效端口')
        self.reality_test_target = reality_test_target
        from xray_runtime import XrayRuntime
        self.front = XrayRuntime(binary, root / 'tls', configuration_factory=self._configuration)
        self.snapshot = None
        self.ports = {}
        self.lifecycle = asyncio.Lock()
        self.expiry_task = None
        self.grant = None

    async def apply(self, value):
        raise ValueError('授予REALITY执行端不接受普通配置，不能回退到旧计量')

    async def install(self, value):
        async with self.lifecycle:
            try:
                value = copy.deepcopy(value)
                content = validate(value, self.stage.server_id, self.stage.session_id)
                if 'reality' not in content['node_config']:
                    raise ValueError('REALITY授予缺少加密入口')
                if self.grant is not None and self.grant != value:
                    raise ValueError('授予执行实例不能替换身份或策略')
                self.grant = value
                self.paced.grant = value
                self.paced.grant_quota = await self.paced._storage(
                    GrantPayloadQuota, self.paced.grant_path, self.stage, value)
                from grant_snapshot import snapshot_for
                snapshot = snapshot_for(content)
                if self.snapshot is not None and self.snapshot != snapshot:
                    raise ValueError('授予REALITY执行实例不能替换配置')
                if self.snapshot is None:
                    self.snapshot = snapshot
                reality_ids = {n['id'] for n in snapshot['nodes'] if 'reality' in n['config']}
                self.paced.trusted_ingress = {k: v for k, v in self.paced.trusted_ingress.items()
                                               if k in reality_ids}
                inner = copy.deepcopy(snapshot)
                forbidden = {n['config']['listen_port'] for n in snapshot['nodes']}
                reservations = []
                for node in inner['nodes']:
                    if 'reality' not in node['config']:
                        continue
                    self.paced.trusted_ingress.setdefault(node['id'], secrets.token_urlsafe(32))
                    if node['id'] not in self.ports or self.ports[node['id']] in forbidden:
                        while True:
                            sock = socket.socket()
                            reservations.append(sock)
                            sock.bind(('127.0.0.1', 0))
                            port = sock.getsockname()[1]
                            if port not in forbidden and port not in self.ports.values():
                                break
                        self.ports[node['id']] = port
                    node['config'].pop('reality')
                    node['config']['listen_host'] = '127.0.0.1'
                    node['config']['listen_port'] = self.ports[node['id']]
                inner['digest'] = digest({k: v for k, v in inner.items() if k != 'digest'})
                for sock in reservations:
                    sock.close()
                await self.paced._apply(inner)
                await self.front.apply(snapshot)
                validate(value, self.stage.server_id, self.stage.session_id)
                if not self.paced.core.process or self.paced.core.process.returncode is not None:
                    raise OSError('授予REALITY内部监听器尚未运行')
                if not self.front.process or self.front.process.returncode is not None:
                    raise OSError('授予REALITY加密监听器尚未运行')
                await self.paced._storage(record_installation, self.stage, value,
                                         snapshot['digest'], 'reality')
                validate(value, self.stage.server_id, self.stage.session_id)
                if (not self.paced.core.process or self.paced.core.process.returncode is not None
                        or not self.front.process or self.front.process.returncode is not None):
                    raise OSError('授予REALITY监听器已停止')
                await self._cancel_expiry()
                if 'valid_until' in snapshot and not user_deadlines(snapshot):
                    self.expiry_task = asyncio.create_task(self._expire(snapshot['valid_until']))
                return dict(grant_id=content['grant_id'], digest=value['digest'],
                    runtime_digest=snapshot['digest'], state='local_reality_runtime_active',
                    ready_for_execution=False)
            except BaseException:
                await self._close()
                raise

    def _payload_guard(self):
        self.paced._payload_guard()

    async def _cycle_quota(self, snapshot, key, budget, used):
        return await self.paced._cycle_quota(snapshot, key, budget, used)
