"""Explicit local VLESS grant execution, not a master applied receipt.

This isolated runtime accepts one immutable grant for its lifetime. It never
falls back to ordinary cycle quotas. No network capability or delivery endpoint
selects it yet; the caller must authenticate the envelope's source separately.
"""
import copy
import time
from pathlib import Path

from grant_agent import validate
from grant_payload import GrantPayloadQuota
from grant_installation import record as record_installation
from vless_runtime import VlessRuntime


class GrantVlessRuntime(VlessRuntime):
    def __init__(self,binary,state_dir,stage,**options):
        super().__init__(binary,state_dir,**options)
        self.stage=stage
        self.grant=None
        self.grant_quota=None
        self.grant_path=Path(state_dir)/'grant-payload.sqlite'

    async def apply(self,value):
        raise ValueError('授予执行端不接受普通配置，不能回退到旧计量')

    async def install(self,value):
        async with self.lifecycle:
            try:
                value=copy.deepcopy(value)
                content=validate(value,self.stage.server_id,self.stage.session_id)
                if 'reality' in content['node_config']:
                    raise ValueError('此执行端尚未接入REALITY授予')
                if self.grant is not None and self.grant!=value:
                    raise ValueError('授予执行实例不能替换身份或策略')
                self.grant=value
                self.grant_quota=await self._storage(
                    GrantPayloadQuota,self.grant_path,self.stage,value)
                from grant_snapshot import snapshot_for
                snapshot=snapshot_for(content)
                await self._apply(snapshot)
                # Startup/storage may have crossed the signed deadline.
                validate(value,self.stage.server_id,self.stage.session_id)
                if not self.core.process or self.core.process.returncode is not None:
                    raise OSError('授予监听器尚未运行')
                await self._storage(record_installation, self.stage, value,
                                    snapshot['digest'], 'vless')
                validate(value,self.stage.server_id,self.stage.session_id)
                if not self.core.process or self.core.process.returncode is not None:
                    raise OSError('授予监听器已停止')
                return dict(grant_id=content['grant_id'],digest=value['digest'],
                    runtime_digest=snapshot['digest'],state='local_runtime_active',
                    ready_for_execution=False)
            except BaseException:
                await self._close()
                raise

    def _payload_guard(self):
        if self.grant is None or not self.grant['issued_at']<=time.time()<self.grant['valid_until']:
            raise OSError('授予执行窗口已失效')

    async def _cycle_quota(self,snapshot,key,budget,used):
        expected=(self.grant['client']['user_id'],self.grant['cycle_id'])
        if key!=expected or self.grant_quota is None:
            raise ValueError('授予执行身份不匹配')
        return self.grant_quota
