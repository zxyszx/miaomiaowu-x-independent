"""Explicit single-grant startup/report/local-stop orchestration.

Does not select a production Agent mode or claim remote closure. The caller
must supply an authenticated immutable envelope and negotiate capabilities.
"""
import asyncio
import copy
from grant_agent import validate
from grant_confirmation import confirm_installed, live
from grant_execution_client import confirm_execution
from grant_worker import GrantUsageWorker
from payload_meter import _stored


class GrantCoordinator:
    def __init__(self, runtime, base, token):
        self.runtime, self.base, self.token = runtime, base, token
        self.lock = asyncio.Lock()
        self.value = None
        self.worker = None
        self.execution = None
        self.stopped = False
        self.closure = None

    def _validate(self, value, *, historical=False):
        options = {'now': value.get('issued_at')} if historical else {}
        return validate(value, self.runtime.stage.server_id,
                        self.runtime.stage.session_id, **options)

    def _check(self):
        validate(self.value, self.runtime.stage.server_id, self.runtime.stage.session_id)
        if self.runtime.grant != self.value:
            raise ValueError('协调器的原授予身份已改变')
        live(self.runtime)

    async def _lifecycle(self, action, states, *, historical=False):
        from agent import request
        response = await _stored(request, self.base, self.token,
            '/agent/grants/lifecycle', dict(action=action,
                grant_id=self.value['grant_id'], session_id=self.value['session_id']))
        if (type(response) is not dict or set(response) != {
                'accepted', 'grant_id', 'state', 'ready_for_execution'}
                or response['accepted'] is not True
                or response['ready_for_execution'] is not False
                or response['grant_id'] != self.value['grant_id']
                or response['state'] not in states):
            raise ValueError('授予生命周期响应身份或状态不匹配')
        self._validate(self.value, historical=historical)
        return response

    async def start(self, value):
        async with self.lock:
            try:
                if self.stopped:
                    raise ValueError('已关闭的协调器不能重新启动')
                value = copy.deepcopy(value)
                validate(value, self.runtime.stage.server_id, self.runtime.stage.session_id)
                if self.value is not None and self.value != value:
                    raise ValueError('协调器不能替换原授予')
                self.value = value
                if self.worker is not None:
                    self._check()
                    return copy.deepcopy(self.execution)
                await self._lifecycle('begin', {'installing', 'installed'})
                await self.runtime.install(value)
                self._check()
                await confirm_installed(self.runtime, self.base, self.token)
                self._check()
                await self._lifecycle('installed', {'installed'})
                self._check()
                response = await confirm_execution(self.runtime, self.base, self.token)
                self._check()
                self.execution = copy.deepcopy(response)
                self.worker = GrantUsageWorker(self.runtime, self.base, self.token)
                return copy.deepcopy(response)
            except BaseException:
                self.worker = None
                await self.runtime.close()
                raise

    async def step(self, **options):
        async with self.lock:
            if self.worker is None or self.stopped:
                raise ValueError('授予协调器尚未完成启动确认')
            try:
                self._check()
                result = await self.worker.step(**options)
                self._check()
                return result
            except BaseException:
                await self.runtime.close()
                self.worker = None
                raise

    async def _finish_stop(self, **options):
        if self.closure is not None:
            return copy.deepcopy(self.closure)
        if self.worker is None:
            raise ValueError('授予协调器没有可关闭的执行记录')
        result, complete = await self.worker.finalize(**options)
        if not complete:
            return result
        response = await self._lifecycle('close', {'closed'}, historical=True)
        self.closure = copy.deepcopy(response)
        self.worker = None
        return copy.deepcopy(response)

    async def stop(self, **options):
        async with self.lock:
            # No meter lock is held while waiting for real connections to stop.
            await self.runtime.close()
            self.stopped = True
            # Retain the worker across capture, delivery, ACK persistence or
            # lifecycle failures so explicit stop can retry exactly.
            return await self._finish_stop(**options)

    async def recover(self, value, **options):
        """Resume only a durable final-report/close tail after process restart."""
        async with self.lock:
            if self.closure is not None:
                return copy.deepcopy(self.closure)
            value = copy.deepcopy(value)
            self._validate(value, historical=True)
            if self.value is not None and self.value != value:
                raise ValueError('协调器不能替换原授予')
            self.value = value
            await self.runtime.close()
            self.stopped = True
            if self.worker is None:
                worker = GrantUsageWorker(self.runtime, self.base, self.token)
                await worker.recover(value)
                self.worker = worker
            return await self._finish_stop(**options)
