"""Explicit production grant-mode selection and single-grant scheduling.

This module only orchestrates existing authenticated grant protocols. It neither
discovers grants nor upgrades legacy configuration receipts into execution proof.
"""
import asyncio
import copy
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import time
import urllib.error

from agent import begin_session, request, send
from agent_liveness import DEADLINE_MARGIN, ONLINE_WINDOW, scheduling_interval
from grant_agent import GrantStageStore, identifier, validate
from grant_capabilities import INSTALLATION, STAGE_IDENTITY, STAGING, negotiate
from grant_coordinator import GrantCoordinator
from grant_transport import FIRST,resolve_ledger
from grant_worker import GrantUsageWorker
from payload_meter import _stored


GRANT_MODE = 'grant'
LEGACY_MODES = frozenset(('legacy-tcp', 'legacy-xray'))
MODES = frozenset((GRANT_MODE, *LEGACY_MODES))
POLL_INTERVAL = 4


def select_mode(value):
    if value not in MODES:
        raise ValueError('Agent执行模式必须显式选择')
    return value


class GrantScheduler:
    def __init__(self, base, token, state, binary, server_id, session_id,
                 grant_id, *, interval=POLL_INTERVAL, runtime_factory=None,
                 coordinator_factory=GrantCoordinator):
        if type(server_id) is not int or server_id <= 0:
            raise ValueError('Agent编号无效')
        identifier(session_id)
        identifier(grant_id)
        scheduling_interval(interval)
        self.base, self.token = base, token
        self.state = Path(state)
        self.binary = Path(binary)
        self.server_id, self.session_id, self.grant_id = (
            server_id, session_id, grant_id)
        self.interval = interval
        self.started = time.monotonic()
        self.stage = GrantStageStore(self.state / 'grant-stage.sqlite',
                                     server_id, session_id)
        self.runtime_factory = runtime_factory or self._runtime
        self.coordinator_factory = coordinator_factory
        self.runtime = None
        self.coordinator = None
        self.value = None
        self.tail = False
        self.closed = False
        self.heartbeat_deadline = None

    def _runtime(self, value):
        root = self.state / 'grant-runtime'
        if 'reality' in value['node_config']:
            from grant_reality_runtime import GrantRealityRuntime
            return GrantRealityRuntime(self.binary, root, self.stage)
        from grant_runtime import GrantVlessRuntime
        return GrantVlessRuntime(self.binary, root, self.stage)

    async def _blocking(self, function, *args):
        worker = asyncio.create_task(asyncio.to_thread(function, *args))
        try:
            if self.heartbeat_deadline is None:
                return await asyncio.shield(worker)
            remaining = self.heartbeat_deadline - time.monotonic()
            done = set()
            if remaining > 0:
                done, _ = await asyncio.wait((worker,), timeout=remaining)
            if done:
                return worker.result()
            await self._close_listener()
            cancelled = await self._drain_thread(worker)
            if cancelled:
                raise asyncio.CancelledError()
            raise asyncio.TimeoutError()
        except asyncio.CancelledError:
            await self._close_listener()
            await self._drain_thread(worker)
            raise

    @staticmethod
    async def _drain_thread(worker):
        """Wait for an uncancellable thread and remember further cancellation."""
        cancelled = False
        while not worker.done():
            try:
                await asyncio.shield(worker)
            except asyncio.CancelledError:
                cancelled = True
            except Exception:
                break
        if worker.done() and not worker.cancelled():
            worker.exception()
        return cancelled

    @staticmethod
    async def _cancel_and_drain(task):
        """Cancel an async stage without leaking its task or its exception."""
        task.cancel()
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                if not task.done():
                    cancelled = True
            except Exception:
                break
        if task.done() and not task.cancelled():
            task.exception()
        return cancelled

    async def _bounded(self, operation):
        """Bound an async stage by the last successful heartbeat deadline."""
        if self.heartbeat_deadline is None:
            return await operation
        task = asyncio.create_task(operation)
        try:
            remaining = self.heartbeat_deadline - time.monotonic()
            done = set()
            if remaining > 0:
                done, _ = await asyncio.wait((task,), timeout=remaining)
            if done:
                return task.result()
            await self._close_listener()
            cancelled = await self._cancel_and_drain(task)
            if cancelled:
                raise asyncio.CancelledError()
            raise asyncio.TimeoutError()
        except asyncio.CancelledError:
            await self._close_listener()
            await self._cancel_and_drain(task)
            raise

    async def _pause(self):
        if self.heartbeat_deadline is None:
            await asyncio.sleep(self.interval)
            return
        remaining = self.heartbeat_deadline - time.monotonic()
        if remaining <= 0:
            await self._close_listener()
            raise asyncio.TimeoutError()
        await asyncio.sleep(min(self.interval, remaining))
        if time.monotonic() >= self.heartbeat_deadline:
            await self._close_listener()
            raise asyncio.TimeoutError()

    async def _close_listener(self):
        if self.runtime is not None:
            await self.runtime.close()

    @staticmethod
    def _execution(value, response):
        from grant_execution import FIELDS, GrantExecution
        fields = {'accepted', 'state', 'execution', 'recorded_at',
                  'ready_for_execution'}
        fact = response.get('execution') if type(response) is dict else None
        protocol = 'reality' if 'reality' in value['node_config'] else 'vless'
        expected = dict(
            grant_id=value['grant_id'], server_id=value['server_id'],
            session_id=value['session_id'], revision=value['revision'],
            digest=value['digest'], valid_until=value['valid_until'],
            protocol=protocol)
        if (type(response) is not dict or set(response) != fields
                or response['accepted'] is not True
                or response['state'] != 'execution_recorded'
                or response['ready_for_execution'] is not False
                or type(fact) is not dict
                or set(fact) != FIELDS
                or any(fact[key] != expected_value
                       for key, expected_value in expected.items())
                or type(response['recorded_at']) not in (int, float)):
            raise ValueError('授予协调器未返回精确执行确认')
        try:
            GrantExecution._validate_fact(fact)
        except Exception:
            raise ValueError('授予协调器未返回精确执行确认') from None
        return response

    def _local_value(self):
        if not self.stage.path.exists():
            return None
        try:
            with closing(sqlite3.connect(
                    self.stage.path.resolve().as_uri() + '?mode=ro', uri=True)) as db:
                exists = db.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='staged_grants'"
                ).fetchone()
                rows = (db.execute('''SELECT ledger_kind,payload,digest
                    FROM staged_grants WHERE grant_id=?''',
                    (self.grant_id,)).fetchall() if exists else [])
            if len(rows)!=1 or rows[0][0]!=FIRST:
                return None
            value = json.loads(rows[0][1])
            value['digest'] = rows[0][2]
            validate(value, self.server_id, self.session_id,
                     now=value.get('issued_at'))
            return value
        except (sqlite3.Error, TypeError, ValueError, json.JSONDecodeError):
            return None

    async def _session(self):
        await self._blocking(begin_session, self.base, self.token,
                             self.session_id, self.server_id)
        heartbeat_started = time.monotonic()
        await self._blocking(send, self.base, self.token,
                             time.monotonic() - self.started, self.session_id,
                             self.server_id)
        # The master commits last_seen no earlier than this local timestamp.
        # Starting here is deliberately conservative; the margin makes listener
        # closure strictly precede the master's exclusive ONLINE_WINDOW boundary.
        self.heartbeat_deadline = (heartbeat_started + ONLINE_WINDOW
                                   - DEADLINE_MARGIN)
        receipt = await self._blocking(
            negotiate, self.base, self.token, self.server_id, self.session_id,
            [STAGING, STAGE_IDENTITY, INSTALLATION])
        if (receipt['selected'] != sorted((STAGING, STAGE_IDENTITY, INSTALLATION))
                or receipt['execution_ready'] is not False):
            raise ValueError('主控未精确选择授予安装协议')

    async def _fetch(self):
        value = await self._blocking(
            request, self.base, self.token, '/agent/grants/configuration',
            dict(grant_id=self.grant_id, session_id=self.session_id))
        validate(value, self.server_id, self.session_id)
        if value['grant_id'] != self.grant_id:
            raise ValueError('主控返回了其他授予')
        identity = await self._blocking(
            resolve_ledger, self.base, self.token, self.session_id,
            self.grant_id, value['digest'], FIRST)
        if (type(identity) is not dict or set(identity)!={
                'accepted','state','ledger_kind','ledger_id','binding_digest',
                'ready_for_execution'} or identity.get('accepted') is not True or
                identity.get('ready_for_execution') is not False or
                identity.get('state')!='ledger_resolved' or
                identity.get('ledger_kind')!=FIRST or
                identity.get('ledger_id')!=self.grant_id or
                identity.get('binding_digest')!=value['digest']):
            raise ValueError('主控授予账本回执不匹配')
        # This receipt remains staging-only. Only coordinator.start can create
        # the installation and immutable execution confirmations used below.
        await _stored(self.stage.stage, value, identity)
        return copy.deepcopy(value)

    async def _status(self):
        response = await self._blocking(
            request, self.base, self.token, '/agent/grants/lifecycle',
            dict(action='status', grant_id=self.grant_id,
                 session_id=self.session_id))
        if (type(response) is not dict or set(response) != {
                'accepted', 'grant_id', 'state', 'ready_for_execution'}
                or response['accepted'] is not True
                or response['grant_id'] != self.grant_id
                or response['state'] != 'installed'
                or response['ready_for_execution'] is not False):
            raise ValueError('授予运行状态与原执行不匹配')

    async def _try_recover(self):
        value = await _stored(self._local_value)
        if value is None:
            return False
        runtime = self.runtime_factory(value)
        coordinator = self.coordinator_factory(runtime, self.base, self.token)
        try:
            # Separate durable proof from network progress. Once this succeeds,
            # later auth/transport failures must remain a close tail and must
            # never fall back to installing the grant again.
            probe = GrantUsageWorker(runtime, self.base, self.token)
            await probe.recover(value)
        except (OSError, ValueError, sqlite3.Error):
            await runtime.close()
            return False
        self.runtime, self.coordinator, self.value = runtime, coordinator, value
        self.tail = True
        try:
            result = await coordinator.recover(value)
        except BaseException:
            await runtime.close()
            raise
        self.tail = type(result) is str
        self.closed = type(result) is dict and result.get('state') == 'closed'
        return True

    async def _start(self):
        await self._session()
        value = await self._fetch()
        runtime = self.runtime_factory(value)
        coordinator = self.coordinator_factory(runtime, self.base, self.token)
        self.runtime, self.coordinator, self.value = runtime, coordinator, value
        try:
            execution = await self._bounded(coordinator.start(value))
            self._execution(value, execution)
        except BaseException:
            await runtime.close()
            self.runtime = self.coordinator = self.value = None
            raise
        return copy.deepcopy(execution)

    async def _maintain(self):
        await self._session()
        validate(self.value, self.server_id, self.session_id)
        await self._status()
        return await self._bounded(self.coordinator.step())

    async def finish(self):
        await self._close_listener()
        if self.coordinator is None or self.closed:
            return None
        try:
            result = await self.coordinator.stop()
        except ValueError:
            # GrantCoordinator closes the listener and drops its worker after a
            # fatal step/storage error. Preserve durable files for a later
            # verified recovery, but never claim the master was closed.
            if self.coordinator.worker is None:
                self.tail = False
                return 'unrecoverable'
            raise
        self.tail = type(result) is str
        if type(result) is dict and result.get('state') == 'closed':
            self.closed = True
            self.tail = False
        return result

    async def step(self):
        if self.closed:
            return 'closed'
        if self.coordinator is None:
            if await self._try_recover():
                return 'closed' if self.closed else 'recovering'
            return await self._start()
        if self.tail:
            return await self.finish()
        return await self._maintain()

    async def run(self, mode=lambda: GRANT_MODE):
        try:
            while select_mode(mode()) == GRANT_MODE and not self.closed:
                try:
                    await self.step()
                except urllib.error.HTTPError as error:
                    try:
                        if error.code in (401, 403):
                            await self.finish()
                        else:
                            await self.finish()
                    finally:
                        error.close()
                    return
                except (OSError, asyncio.TimeoutError, ValueError, TypeError,
                        RuntimeError, sqlite3.Error):
                    await self.finish()
                    return
                if not self.closed:
                    await self._pause()
        finally:
            await self.finish()

