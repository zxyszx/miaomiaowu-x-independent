"""Explicit single-grant automatic usage worker; never selects an Agent mode.

Only starts reporting after a durable exact master installation receipt. This
receipt is historical, not a lease, liveness proof or revocation override.
"""
import asyncio
import copy
from contextlib import closing
import json
import sqlite3
import time
from grant_confirmation import acknowledged
from grant_delivery import GrantDelivery
from payload_meter import _stored


class GrantUsageWorker:
    def __init__(self, runtime, base, token):
        self.runtime = runtime
        self.paced = runtime.paced if hasattr(runtime, 'paced') else runtime
        self.base, self.token = base, token
        self.lock = asyncio.Lock()

    @staticmethod
    def _historical(value, stage):
        from grant_agent import validate
        return validate(value, stage.server_id, stage.session_id,
                        now=value.get('issued_at') if isinstance(value, dict) else None)

    @staticmethod
    def _recovery_evidence(stage, path, value):
        """Reject recovery unless the original execution and frozen report exist."""
        content = GrantUsageWorker._historical(value, stage)
        with closing(sqlite3.connect(stage.path.resolve().as_uri() + '?mode=ro',
                                     uri=True)) as db:
            claim = db.execute(
                'SELECT fact,receipt FROM local_grant_execution_claims WHERE grant_id=?',
                (content['grant_id'],)).fetchone()
        if claim is None or claim[1] is None:
            raise ValueError('恢复关闭缺少原执行确认')
        try:
            fact, receipt = json.loads(claim[0]), json.loads(claim[1])
        except (TypeError, ValueError):
            raise ValueError('恢复关闭的执行确认损坏') from None
        expected = {
            'grant_id': content['grant_id'], 'server_id': content['server_id'],
            'session_id': content['session_id'], 'revision': content['revision'],
            'digest': value['digest'], 'valid_until': content['valid_until']}
        if (any(fact.get(key) != expected_value for key, expected_value in expected.items())
                or type(receipt) is not dict
                or receipt.get('state') != 'execution_recorded'
                or receipt.get('accepted') is not True
                or receipt.get('ready_for_execution') is not False
                or receipt.get('execution') != fact):
            raise ValueError('恢复关闭的执行身份不匹配')
        with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro',
                                     uri=True)) as db:
            owner = db.execute(
                'SELECT scope FROM allocation_quota WHERE allocation_id=?',
                (content['grant_id'],)).fetchone()
            constraints = db.execute(
                'SELECT constraints_json FROM allocation_constraints WHERE allocation_id=?',
                (content['grant_id'],)).fetchone()
            outbox = db.execute(
                'SELECT body FROM grant_usage_outbox WHERE grant_id=?',
                (content['grant_id'],)).fetchone()
        if owner is None or constraints is None or outbox is None:
            raise ValueError('恢复关闭缺少原最终用量报告')
        try:
            scope = json.loads(owner[0])
            limits = json.loads(constraints[0])
            body = json.loads(outbox[0])
        except (TypeError, ValueError):
            raise ValueError('恢复关闭的用量报告损坏') from None
        client = content['client']
        expected_scope = dict(
            allocation_id=content['grant_id'], policy_id=content['policy_id'],
            cycle_id=content['cycle_id'], server_id=content['server_id'],
            session_id=content['session_id'], node_id=content['node_id'],
            user_id=client['user_id'], credential=client['uuid'],
            reserved_bytes=content['reserved_bytes'], issued_at=content['issued_at'],
            valid_until=content['valid_until'])
        keys = ('grant_id', 'policy_id', 'session_id')
        expected_body = (content['grant_id'], content['policy_id'], content['session_id'])
        if (scope != expected_scope
                or tuple(body.get(key) for key in keys) != expected_body):
            raise ValueError('恢复关闭的用量身份不匹配')
        return scope, limits

    @staticmethod
    def _restore_quota(path, scope, constraints):
        from allocation_quota import AllocationQuota
        from grant_payload import GrantPayloadQuota
        quota = GrantPayloadQuota.__new__(GrantPayloadQuota)
        AllocationQuota.__init__(quota, path, scope, constraints=constraints)
        return quota

    async def recover(self, value):
        """Reopen an existing durable quota without starting a listener."""
        value = copy.deepcopy(value)
        path = self.paced.grant_path
        scope, constraints = await self.paced._storage(
            self._recovery_evidence, self.runtime.stage, path, value)
        quota = await self.paced._storage(
            self._restore_quota, path, scope, constraints)
        self.runtime.grant = copy.deepcopy(value)
        self.paced.grant = copy.deepcopy(value)
        self.paced.grant_quota = quota
        return quota

    @staticmethod
    def _all_acknowledged(quota):
        gid = quota._scope['allocation_id']
        with closing(sqlite3.connect(quota.path.resolve().as_uri() + '?mode=ro',
                                     uri=True)) as db:
            row = db.execute(
                'SELECT acknowledged FROM grant_usage_outbox WHERE grant_id=?',
                (gid,)).fetchone()
        return row == (1,)

    async def capture(self):
        value = copy.deepcopy(self.runtime.grant)
        # _storage shares the actual payload meter lock and drains cancelled
        # storage operations before releasing it. Do not acquire it twice.
        await self.paced._storage(acknowledged, self.runtime.stage, value)
        quota = self.paced.grant_quota
        if quota is None or quota._scope['allocation_id'] != value['grant_id']:
            raise ValueError('自动投递缺少原授予计量身份')
        await self.paced._storage(quota.freeze_evidence)
        return quota

    async def step(self, *, now=None, transport=None):
        async with self.lock:
            quota = await self.capture()
            # Network waiting must not hold the payload meter lock. Serialize
            # complete attempts (including cancellation) to preserve sequence.
            delivery = await _stored(GrantDelivery, quota)
            def send():
                args = {} if transport is None else {'transport': transport}
                return delivery.step(self.base, self.token,
                    time.time() if now is None else now, **args)
            return await _stored(send)

    async def finalize(self, *, now=None, transport=None):
        """Attempt one delivery, then prove no newer final snapshot is pending."""
        async with self.lock:
            quota = await self.capture()
            delivery = await _stored(GrantDelivery, quota)
            def send():
                args = {} if transport is None else {'transport': transport}
                return delivery.step(self.base, self.token,
                    time.time() if now is None else now, **args)
            result = await _stored(send)
            # An older pending snapshot may have been ACKed while later bytes were
            # already durable. Freeze again before claiming all reports are done.
            await self.capture()
            complete = await _stored(self._all_acknowledged, quota)
            return result, complete

    async def run(self, interval=5):
        if type(interval) not in (int, float) or not 0 < interval <= 60:
            raise ValueError('自动投递间隔无效')
        while True:
            try:
                await self.step()
            except (OSError, ValueError, sqlite3.Error):
                # Keep original journals. Authentication blocks are durable in
                # GrantDelivery and cannot be cleared by restarting this loop.
                pass
            await asyncio.sleep(interval)

    async def finish(self, task):
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        # Caller must close the actual runtime first; this only freezes bytes,
        # never fabricates closure evidence or sends a final network request.
        async with self.lock:
            await self.capture()
