"""Automatic grant reporting through real listeners and durable journals."""
import asyncio
import sqlite3
import threading
import unittest
from unittest.mock import patch
from grant_ack import fact_for, receipt
from grant_confirmation import acknowledged, confirm_installed
from grant_worker import GrantUsageWorker
from test_xray import BINARY
import test_grant_runtime as fixture


@unittest.skipUnless(BINARY.is_file(), 'Set XRAY_BINARY for real grant execution')
class GrantWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def confirmed(self):
        await self.runtime.install(self.row)
        with patch('agent.request', return_value=receipt(fact_for(self.row))):
            await confirm_installed(self.runtime, 'http://127.0.0.1:1', 'test')
        return GrantUsageWorker(self.runtime, 'http://127.0.0.1:1', 'test')

    def response(self, body):
        count = body['upload_bytes'] + body['download_bytes']
        return dict(accepted=True, duplicate=False, grant_id=body['grant_id'],
                    policy_id=body['policy_id'], sequence=body['sequence'],
                    confirmed_bytes=count, held_bytes=self.runtime.grant_quota._scope['reserved_bytes']-count,
                    ready_for_execution=False)

    async def test_staged_or_local_installation_cannot_report(self):
        await self.runtime.install(self.row)
        worker = GrantUsageWorker(self.runtime, 'http://127.0.0.1:1', 'test')
        with self.assertRaisesRegex(ValueError, '主控安装确认'):
            await worker.step(transport=lambda *args: self.fail('unexpected network'))

    async def test_ack_is_durable_exact_and_idempotent(self):
        await self.confirmed()
        self.assertEqual(acknowledged(self.store, self.row), receipt(fact_for(self.row)))
        with patch('agent.request', return_value=receipt(fact_for(self.row))):
            await confirm_installed(self.runtime, 'http://127.0.0.1:1', 'test')
        with sqlite3.connect(self.store.path) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM local_grant_master_acks').fetchone()[0], 1)
            db.execute("UPDATE local_grant_master_acks SET receipt='{}'")
        with self.assertRaisesRegex(ValueError, '主控安装确认'):
            acknowledged(self.store, self.row)

    async def test_automatic_freeze_one_attempt_and_idle(self):
        worker = await self.confirmed()
        calls = []
        def send(base, token, path, body):
            calls.append((path, body.copy()))
            return self.response(body)
        self.assertEqual(await worker.step(transport=send), 'accepted')
        self.assertEqual(await worker.step(transport=send), 'idle')
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], '/agent/grants/usage')
        self.assertEqual(calls[0][1]['sequence'], 1)
        self.assertEqual(calls[0][1]['upload_bytes'], 0)
        self.assertEqual(calls[0][1]['download_bytes'], 0)

    async def test_lost_response_reopen_preserves_backoff_and_body(self):
        worker = await self.confirmed()
        calls = []
        def lose(*args):
            calls.append(args[3].copy())
            raise OSError('lost response')
        self.assertEqual(await worker.step(now=100, transport=lose), 'retry')
        reopened = GrantUsageWorker(self.runtime, worker.base, worker.token)
        self.assertEqual(await reopened.step(now=104, transport=lose), 'deferred')
        def send(*args):
            calls.append(args[3].copy())
            return self.response(args[3])
        self.assertEqual(await reopened.step(now=105, transport=send), 'accepted')
        self.assertEqual(calls[0], calls[1])

    async def test_concurrent_steps_do_not_duplicate_attempt(self):
        worker = await self.confirmed()
        calls = []
        def send(*args):
            calls.append(args[3].copy())
            return self.response(args[3])
        result = await asyncio.gather(worker.step(transport=send), worker.step(transport=send))
        self.assertEqual(result, ['accepted', 'idle'])
        self.assertEqual(len(calls), 1)

    async def test_repeated_cancel_drains_network_before_releasing_worker(self):
        worker = await self.confirmed()
        entered, release = threading.Event(), threading.Event()
        calls = []
        def send(*args):
            calls.append(args[3].copy())
            entered.set()
            if not release.wait(3):
                raise OSError('test transport release timed out')
            return self.response(args[3])
        task = asyncio.create_task(worker.step(transport=send))
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait, 3))
            task.cancel()
            await asyncio.sleep(.01)
            task.cancel()
            await asyncio.sleep(.01)
            self.assertFalse(task.done())
            self.assertTrue(worker.lock.locked())
        finally:
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(await worker.step(transport=send), 'idle')
        self.assertEqual(len(calls), 1)

    async def test_ack_disk_failure_closes_listener_without_confirmation(self):
        await self.runtime.install(self.row)
        with patch('agent.request', return_value=receipt(fact_for(self.row))), patch(
                'grant_confirmation.remember_ack', side_effect=OSError('disk failure')):
            with self.assertRaisesRegex(OSError, 'disk failure'):
                await confirm_installed(self.runtime, 'http://127.0.0.1:1', 'test')
        self.assertTrue(self.runtime.core.process is None or self.runtime.core.process.returncode is not None)
        with self.assertRaisesRegex(ValueError, '主控安装确认'):
            acknowledged(self.store, self.row)


for name in ('setUp', 'sign', 'asyncSetUp', 'make_runtime', 'asyncTearDown', 'exchange'):
    setattr(GrantWorkerTests, name, getattr(fixture.GrantRuntimeTests, name))
