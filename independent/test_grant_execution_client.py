"""Actual listener checks and durable audit identity across ambiguous replies."""
import json
import sqlite3
import unittest
from unittest.mock import patch

from grant_ack import fact_for, receipt
from grant_confirmation import confirm_installed
from grant_execution_client import confirm_execution, prepare
from test_xray import BINARY
import test_grant_runtime as fixture


@unittest.skipUnless(BINARY.is_file(), 'Set XRAY_BINARY for real grant execution')
class GrantExecutionClientTests(unittest.IsolatedAsyncioTestCase):
    async def install_confirmed(self):
        await self.runtime.install(self.row)
        with patch('agent.request', return_value=receipt(fact_for(self.row))):
            await confirm_installed(self.runtime, 'http://127.0.0.1:1', 'test')

    @staticmethod
    def response(*args):
        return dict(accepted=True, state='execution_recorded', execution=args[-1]['fact'],
                    recorded_at=123.0, ready_for_execution=False)

    def saved(self):
        with sqlite3.connect(self.store.path) as db:
            return db.execute('SELECT fact,receipt FROM local_grant_execution_claims').fetchone()

    async def test_exact_claim_and_receipt_survive_reopen_without_refund(self):
        await self.install_confirmed()
        with patch('agent.request', side_effect=self.response) as send:
            first = await confirm_execution(self.runtime, 'http://127.0.0.1:1', 'test')
        self.assertEqual(send.call_args.args[2], '/agent/grants/execution')
        self.assertEqual(json.loads(self.saved()[1]), first)
        self.assertEqual(await self.exchange(b'abcd'), b'ab')
        await self.runtime.close()
        self.runtime = self.make_runtime()
        await self.runtime.install(self.row)
        with patch('agent.request', side_effect=self.response):
            self.assertEqual(await confirm_execution(self.runtime, 'http://127.0.0.1:1', 'test'), first)
        self.assertEqual(self.runtime.grant_quota.snapshot()['remaining_bytes'], 0)

    async def test_lost_response_keeps_same_claim_on_explicit_retry(self):
        await self.install_confirmed()
        seen = []
        def lost(*args):
            seen.append(args[-1]['fact'])
            raise OSError('response lost')
        with patch('agent.request', side_effect=lost):
            with self.assertRaises(OSError):
                await confirm_execution(self.runtime, 'http://127.0.0.1:1', 'test')
        self.assertIsNone(self.saved()[1])
        self.assertFalse(self.runtime.listeners)
        self.runtime = self.make_runtime()
        await self.runtime.install(self.row)
        with patch('agent.request', side_effect=self.response):
            result = await confirm_execution(self.runtime, 'http://127.0.0.1:1', 'test')
        self.assertEqual(result['execution'], seen[0])

    async def test_no_install_ack_cannot_send(self):
        await self.runtime.install(self.row)
        with patch('agent.request') as send:
            with self.assertRaises(ValueError):
                await confirm_execution(self.runtime, 'http://127.0.0.1:1', 'test')
            send.assert_not_called()
        self.assertFalse(self.runtime.listeners)

    async def test_dead_process_cannot_send_historical_installation(self):
        await self.install_confirmed()
        self.runtime.core.process.kill()
        await self.runtime.core.process.wait()
        with patch('agent.request') as send:
            with self.assertRaises(OSError):
                await confirm_execution(self.runtime, 'http://127.0.0.1:1', 'test')
            send.assert_not_called()

    async def test_bad_response_closes_and_never_persists_confirmation(self):
        await self.install_confirmed()
        def bad(*args):
            value = self.response(*args)
            value['execution'] = dict(value['execution'], runtime_digest='0'*64)
            return value
        with patch('agent.request', side_effect=bad):
            with self.assertRaises(ValueError):
                await confirm_execution(self.runtime, 'http://127.0.0.1:1', 'test')
        self.assertIsNone(self.saved()[1])
        self.assertFalse(self.runtime.listeners)

    async def test_process_stopped_during_request_cannot_save_execution_receipt(self):
        import os
        import signal
        await self.install_confirmed()
        def stopped(*args):
            os.kill(self.runtime.core.process.pid, signal.SIGKILL)
            # Wait for the child to be observed as stopped before returning.
            import time
            for _ in range(100):
                if self.runtime.core.process.returncode is not None:
                    break
                time.sleep(0.01)
            return self.response(*args)
        with patch('agent.request', side_effect=stopped):
            with self.assertRaises(OSError):
                await confirm_execution(self.runtime, 'http://127.0.0.1:1', 'test')
        self.assertIsNone(self.saved()[1])
        self.assertFalse(self.runtime.listeners)

    async def test_receipt_storage_failure_retains_claim_and_closes(self):
        await self.install_confirmed()
        prepare(self.store, self.row)
        with sqlite3.connect(self.store.path) as db:
            db.execute("CREATE TRIGGER disk BEFORE UPDATE ON local_grant_execution_claims "
                       "BEGIN SELECT RAISE(ABORT,'disk'); END")
        with patch('agent.request', side_effect=self.response):
            with self.assertRaises(sqlite3.Error):
                await confirm_execution(self.runtime, 'http://127.0.0.1:1', 'test')
        self.assertIsNone(self.saved()[1])
        self.assertFalse(self.runtime.listeners)

    async def test_invalid_feedback_types_never_persist(self):
        from grant_execution_client import remember
        await self.install_confirmed()
        fact = prepare(self.store, self.row)
        original = self.response(None, {'fact': fact})
        for changes in ({'recorded_at': True}, {'recorded_at': float('nan')},
                        {'accepted': 1}, {'ready_for_execution': 0}, {'extra': True}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                remember(self.store, fact, dict(original, **changes))
        self.assertIsNone(self.saved()[1])

for name in ('setUp', 'sign', 'asyncSetUp', 'make_runtime', 'asyncTearDown', 'exchange'):
    setattr(GrantExecutionClientTests, name, getattr(fixture.GrantRuntimeTests, name))
