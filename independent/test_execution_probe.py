"""Optional probes must not hold or revive execution configuration leases."""
import asyncio
import contextlib
import threading
import unittest
import urllib.error
from unittest import mock

import execution_agent
from test_execution_lease import Runtime


class ExecutionProbeTests(unittest.IsolatedAsyncioTestCase):
    async def test_blocked_probe_does_not_delay_configuration_lease_expiry(self):
        runtime=Runtime(); started=threading.Event(); release=threading.Event(); calls=0
        def blocked(*args):
            started.set(); release.wait(3)
        async def cycle(*args):
            nonlocal calls
            calls+=1
            if calls==1: runtime.active=True; return {}
            await asyncio.Event().wait()
        with mock.patch('execution_agent.cycle',side_effect=cycle), \
             mock.patch('execution_agent.probe_pending',side_effect=blocked), \
             mock.patch('execution_agent.LEASE_SECONDS',.15), \
             mock.patch('execution_agent.SYNC_INTERVAL',.01):
            task=asyncio.create_task(execution_agent.run({'master':'unused','token':'unused'},'unused',runtime))
            try:
                self.assertTrue(await asyncio.to_thread(started.wait,1))
                await asyncio.wait_for(runtime.closed.wait(),.6)
                self.assertFalse(runtime.active)
                self.assertFalse(release.is_set())
            finally:
                release.set(); task.cancel()
                with contextlib.suppress(asyncio.CancelledError): await task

    async def test_slow_probe_does_not_block_cycles_or_overlap_and_is_drained(self):
        runtime=Runtime(); started=threading.Event(); release=threading.Event()
        multiple=asyncio.Event(); calls=0
        def blocked(*args):
            started.set(); release.wait(3)
        async def cycle(*args):
            nonlocal calls
            calls+=1; runtime.active=True
            if calls>=3: multiple.set()
        with mock.patch('execution_agent.cycle',side_effect=cycle), \
             mock.patch('execution_agent.probe_pending',side_effect=blocked) as probe, \
             mock.patch('execution_agent.SYNC_INTERVAL',.01):
            task=asyncio.create_task(execution_agent.run({'master':'unused','token':'unused'},'unused',runtime))
            try:
                self.assertTrue(await asyncio.to_thread(started.wait,1))
                await asyncio.wait_for(multiple.wait(),1)
                self.assertEqual(probe.call_count,1)
                task.cancel()
                await asyncio.wait_for(runtime.closed.wait(),1)
                self.assertFalse(runtime.active)
                self.assertFalse(task.done())
                release.set()
                with self.assertRaises(asyncio.CancelledError): await asyncio.wait_for(task,1)
            finally:
                release.set()
                if not task.done(): task.cancel()
                with contextlib.suppress(asyncio.CancelledError): await task

    async def test_transient_failure_keeps_runtime_and_retries_on_later_cycle(self):
        runtime=Runtime(); retried=threading.Event()
        def probe(*args):
            if not retried.is_set():
                retried.set(); raise OSError('temporary')
        with mock.patch('execution_agent.cycle',return_value={}), \
             mock.patch('execution_agent.probe_pending',side_effect=probe) as pending, \
             mock.patch('execution_agent.SYNC_INTERVAL',.01):
            runtime.active=True
            task=asyncio.create_task(execution_agent.run({'master':'unused','token':'unused'},'unused',runtime))
            try:
                for _ in range(100):
                    if pending.call_count>=2: break
                    await asyncio.sleep(.01)
                self.assertGreaterEqual(pending.call_count,2)
                self.assertTrue(runtime.active)
                self.assertFalse(runtime.closed.is_set())
            finally:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError): await task

    async def test_auth_revocation_closes_again_after_inflight_apply(self):
        for status in (401,403):
            with self.subTest(status=status):
                runtime=Runtime(); second=asyncio.Event(); loop=asyncio.get_running_loop(); calls=0
                def revoked(*args):
                    future=asyncio.run_coroutine_threadsafe(second.wait(),loop)
                    future.result(timeout=2)
                    raise urllib.error.HTTPError('local',status,'revoked',{},None)
                async def cycle(*args):
                    nonlocal calls
                    calls+=1
                    if calls==2:
                        second.set()
                        await runtime.closed.wait()
                    runtime.active=True
                with mock.patch('execution_agent.cycle',side_effect=cycle), \
                     mock.patch('execution_agent.probe_pending',side_effect=revoked), \
                     mock.patch('execution_agent.SYNC_INTERVAL',.01):
                    await asyncio.wait_for(execution_agent.run({'master':'unused','token':'unused'},'unused',runtime),2)
                self.assertEqual(calls,2)
                self.assertFalse(runtime.active)

    async def test_failed_config_never_claims_probe(self):
        runtime=Runtime(); failed=asyncio.Event()
        async def cycle(*args):
            failed.set(); raise OSError('sync failed')
        with mock.patch('execution_agent.cycle',side_effect=cycle), \
             mock.patch('execution_agent.probe_pending') as probe:
            task=asyncio.create_task(execution_agent.run({'master':'unused','token':'unused'},'unused',runtime))
            await asyncio.wait_for(failed.wait(),1)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError): await task
            probe.assert_not_called()
