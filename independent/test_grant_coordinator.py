"""Real listener and authenticated HTTP startup orchestration boundaries."""
import asyncio
import sqlite3
import threading
import unittest
from unittest.mock import patch
from agent import request
from grant_capabilities import STAGING, INSTALLATION
from grant_configuration import GrantConfiguration
from grant_coordinator import GrantCoordinator
from grant_runtime import GrantVlessRuntime
from test_runtime import free_port
from test_xray import BINARY, VLESS
import test_grant_transport as fixture


@unittest.skipUnless(BINARY.is_file(), 'Set XRAY_BINARY for real grant execution')
class GrantCoordinatorTests(unittest.IsolatedAsyncioTestCase):
    async def setup_coordinator(self):
        self.start_http(offer=[STAGING, INSTALLATION])
        self.r.configure_node(self.nid, dict(VLESS, listen_port=free_port()))
        await asyncio.to_thread(self.fetch)
        value = GrantConfiguration(self.r).bind(self.token, self.g['grant_id'])
        runtime = GrantVlessRuntime(BINARY, self.store.path.parent/'coordinator', self.store)
        self.addAsyncCleanup(runtime.close)
        return GrantCoordinator(runtime, self.url, self.token), value

    async def test_real_start_order_report_and_confirmed_stop(self):
        coordinator, value = await self.setup_coordinator()
        paths = []
        def send(*args):
            paths.append((args[2], args[3].get('action')))
            return request(*args)
        with patch('agent.request', side_effect=send):
            result = await coordinator.start(value)
        self.assertEqual(paths, [('/agent/grants/lifecycle', 'begin'),
            ('/agent/grants/installed', None), ('/agent/grants/lifecycle', 'installed'),
            ('/agent/grants/execution', None)])
        self.assertFalse(result['ready_for_execution'])
        self.assertEqual(await coordinator.step(), 'accepted')
        closed = await coordinator.stop()
        self.assertEqual(closed['state'], 'closed')
        self.assertIsNone(coordinator.runtime.core.process)
        with self.r.connect() as db:
            self.assertEqual(db.execute('SELECT state FROM usage_first_total_lifecycle').fetchone()[0], 'closed')
        with self.assertRaises(ValueError):
            await coordinator.step()
        self.assertEqual(await coordinator.stop(), closed)

    async def test_wrong_begin_response_never_installs(self):
        coordinator, value = await self.setup_coordinator()
        with patch('agent.request', return_value=dict(accepted=True,
                grant_id='other', state='installing', ready_for_execution=False)):
            with self.assertRaises(ValueError):
                await coordinator.start(value)
        self.assertIsNone(coordinator.runtime.core.process)
        with self.assertRaises(ValueError):
            await coordinator.step()

    async def test_lost_execution_response_closes_and_reopen_reuses_claim(self):
        coordinator, value = await self.setup_coordinator()
        seen = []
        def lose(*args):
            result = request(*args)
            if args[2] == '/agent/grants/execution':
                seen.append(result)
                raise OSError('lost execution reply')
            return result
        with patch('agent.request', side_effect=lose):
            with self.assertRaises(OSError):
                await coordinator.start(value)
        self.assertIsNone(coordinator.runtime.core.process)
        with self.assertRaises(ValueError):
            await coordinator.step()
        self.assertEqual(await coordinator.start(value), seen[0])
        with self.r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_first_total_execution').fetchone()[0], 1)
        await coordinator.stop()

    async def test_concurrent_start_has_one_installation_sequence(self):
        coordinator, value = await self.setup_coordinator()
        calls = []
        def send(*args):
            calls.append(args[2])
            return request(*args)
        with patch('agent.request', side_effect=send):
            a, b = await asyncio.gather(coordinator.start(value), coordinator.start(value))
        self.assertEqual(a, b)
        self.assertEqual(calls.count('/agent/grants/installed'), 1)
        self.assertEqual(calls.count('/agent/grants/execution'), 1)
        a['state'] = 'mutated'
        self.assertEqual((await coordinator.start(value))['state'], 'execution_recorded')
        await coordinator.stop()

    async def test_closed_coordinator_cannot_restart(self):
        coordinator, value = await self.setup_coordinator()
        await coordinator.start(value)
        await coordinator.stop()
        with patch('agent.request') as send:
            with self.assertRaises(ValueError):
                await coordinator.start(value)
            send.assert_not_called()

    async def test_failed_final_capture_can_retry_without_remote_close(self):
        coordinator, value = await self.setup_coordinator()
        await coordinator.start(value)
        worker = coordinator.worker
        with patch.object(worker, 'capture', side_effect=sqlite3.OperationalError('disk unavailable')):
            with self.assertRaises(sqlite3.OperationalError):
                await coordinator.stop()
        self.assertIsNone(coordinator.runtime.core.process)
        self.assertIs(coordinator.worker, worker)
        with self.assertRaises(ValueError):
            await coordinator.step()
        await coordinator.stop()
        self.assertIsNone(coordinator.worker)

    async def test_lost_final_reply_retries_exact_report_before_close(self):
        coordinator, value = await self.setup_coordinator()
        await coordinator.start(value)
        seen = []
        def lost(*args):
            seen.append(args[-1])
            request(*args)
            raise OSError('lost final usage reply')
        self.assertEqual(await coordinator.stop(now=100, transport=lost), 'retry')
        with self.r.connect() as db:
            self.assertEqual(db.execute(
                'SELECT state FROM usage_first_total_lifecycle').fetchone()[0], 'installed')
        closed = await coordinator.stop(now=105, transport=request)
        self.assertEqual(closed['state'], 'closed')
        self.assertEqual(len(seen), 1)
        with self.r.connect() as db:
            rows = db.execute(
                'SELECT sequence,upload_bytes,download_bytes FROM usage_first_total_meter').fetchall()
        self.assertEqual([tuple(row) for row in rows], [(1, 0, 0)])

    async def test_lost_close_reply_is_idempotent_and_keeps_worker(self):
        coordinator, value = await self.setup_coordinator()
        await coordinator.start(value)
        original = request
        def lost_close(*args):
            response = original(*args)
            if args[2] == '/agent/grants/lifecycle' and args[3]['action'] == 'close':
                raise OSError('lost close reply')
            return response
        with patch('agent.request', side_effect=lost_close):
            with self.assertRaises(OSError):
                await coordinator.stop()
        self.assertIsNotNone(coordinator.worker)
        with self.r.connect() as db:
            self.assertEqual(db.execute(
                'SELECT state FROM usage_first_total_lifecycle').fetchone()[0], 'closed')
        closed = await coordinator.stop()
        self.assertEqual(closed['state'], 'closed')
        self.assertIsNone(coordinator.worker)

    async def test_restart_recovers_frozen_report_without_reinstalling(self):
        coordinator, value = await self.setup_coordinator()
        await coordinator.start(value)
        await coordinator.runtime.close()
        await coordinator.worker.capture()
        snapshot = coordinator.runtime.grant_quota.snapshot()
        runtime = GrantVlessRuntime(
            BINARY, self.store.path.parent/'coordinator', self.store)
        self.addAsyncCleanup(runtime.close)
        recovered = GrantCoordinator(runtime, self.url, self.token)
        with patch.object(runtime, 'install') as install:
            closed = await recovered.recover(value)
        install.assert_not_called()
        self.assertEqual(closed['state'], 'closed')
        self.assertEqual(runtime.grant_quota.snapshot(), snapshot)
        self.assertIsNone(runtime.core.process)
        with self.r.connect() as db:
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_first_total_execution').fetchone()[0], 1)
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_first_total_meter').fetchone()[0], 1)

    async def test_restart_recovery_can_finish_after_execution_window(self):
        coordinator, value = await self.setup_coordinator()
        await coordinator.start(value)
        await coordinator.runtime.close()
        def lost(*args):
            request(*args)
            raise OSError('final report response lost before restart')
        result, complete = await coordinator.worker.finalize(now=0, transport=lost)
        self.assertEqual(result, 'retry')
        self.assertFalse(complete)
        runtime = GrantVlessRuntime(
            BINARY, self.store.path.parent/'coordinator', self.store)
        self.addAsyncCleanup(runtime.close)
        recovered = GrantCoordinator(runtime, self.url, self.token)
        with patch('time.time', return_value=value['valid_until'] + 1):
            closed = await recovered.recover(value, now=value['valid_until'] + 1)
        self.assertEqual(closed['state'], 'closed')
        self.assertIsNone(runtime.core.process)

    async def test_stop_acks_old_and_new_final_sequences_before_close(self):
        coordinator, value = await self.setup_coordinator()
        await coordinator.start(value)
        def lost(*args):
            raise OSError('old report remains pending')
        self.assertEqual(await coordinator.step(now=0, transport=lost), 'retry')
        quota = coordinator.runtime.grant_quota
        allowed, ticket = quota.reserve_write(0, 3)
        self.assertEqual(allowed, 3)
        quota.complete_write(ticket)
        self.assertEqual(await coordinator.stop(now=5, transport=request), 'accepted')
        with self.r.connect() as db:
            self.assertEqual(db.execute(
                'SELECT state FROM usage_first_total_lifecycle').fetchone()[0], 'installed')
        closed = await coordinator.stop(now=10, transport=request)
        self.assertEqual(closed['state'], 'closed')
        with self.r.connect() as db:
            rows = db.execute(
                'SELECT sequence,upload_bytes,download_bytes FROM usage_first_total_meter '
                'ORDER BY sequence').fetchall()
        self.assertEqual([tuple(row) for row in rows], [(1, 0, 0), (2, 3, 0)])

    async def test_repeated_cancel_during_final_delivery_keeps_retry_state(self):
        coordinator, value = await self.setup_coordinator()
        await coordinator.start(value)
        entered, release = threading.Event(), threading.Event()
        def slow(*args):
            result = request(*args)
            entered.set()
            if not release.wait(5):
                raise TimeoutError('test final network release missing')
            return result
        task = asyncio.create_task(coordinator.stop(transport=slow))
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait, 3))
            task.cancel()
            await asyncio.sleep(0.01)
            task.cancel()
            await asyncio.sleep(0.01)
            self.assertFalse(task.done())
            self.assertTrue(coordinator.lock.locked())
        finally:
            release.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(coordinator.lock.locked())
        self.assertIsNotNone(coordinator.worker)
        closed = await coordinator.stop()
        self.assertEqual(closed['state'], 'closed')

    async def test_repeated_concurrent_stop_returns_one_exact_closure(self):
        coordinator, value = await self.setup_coordinator()
        await coordinator.start(value)
        first, second = await asyncio.gather(coordinator.stop(), coordinator.stop())
        self.assertEqual(first, second)
        self.assertEqual(first['state'], 'closed')
        with self.r.connect() as db:
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_first_total_meter').fetchone()[0], 1)

    async def test_repeated_cancel_waits_for_pending_network_before_unlock(self):
        coordinator, value = await self.setup_coordinator()
        entered, release = threading.Event(), threading.Event()
        def slow(*args):
            result = request(*args)
            entered.set()
            if not release.wait(5):
                raise TimeoutError('test network release missing')
            return result
        with patch('agent.request', side_effect=slow):
            task = asyncio.create_task(coordinator.start(value))
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait, 3))
                task.cancel()
                await asyncio.sleep(0.01)
                task.cancel()
                await asyncio.sleep(0.01)
                self.assertFalse(task.done())
                self.assertTrue(coordinator.lock.locked())
            finally:
                release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertFalse(coordinator.lock.locked())
        self.assertIsNone(coordinator.worker)
        self.assertIsNone(coordinator.runtime.core.process)

for name in ('setUp', 'tearDown', 'fixtures', 'fixture', 'setup_total', 'setup_drain',
             'applied', 'report', 'data', 'envelope', 'accept', 'sealed', 'request',
             'prepare', 'start', 'start_http', 'fetch', 'post', 'rejected'):
    setattr(GrantCoordinatorTests, name, getattr(fixture.GrantTransportTests, name))
GrantCoordinatorTests.encode = staticmethod(fixture.GrantTransportTests.encode)
