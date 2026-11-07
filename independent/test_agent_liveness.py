"""Exact Agent liveness, offline status and grant credential revocation."""
import asyncio
import io
from pathlib import Path
import tempfile
import threading
import time
import unittest
import urllib.error
import uuid
from unittest.mock import patch

from agent import begin_session, send
from agent_liveness import (DEADLINE_MARGIN, ONLINE_WINDOW, POST_HEARTBEAT_REQUESTS,
                            REQUEST_TIMEOUT, scheduling_interval)
from core import Error, Registry
from grant_capabilities import INSTALLATION, STAGE_IDENTITY, STAGING
from grant_scheduler import GrantScheduler, POLL_INTERVAL
from test_runtime import free_port
from test_xray import BINARY, VLESS
import test_grant_transport as transport_fixture


class AgentLivenessTests(unittest.TestCase):
    setUp = transport_fixture.GrantTransportTests.setUp
    tearDown = transport_fixture.GrantTransportTests.tearDown
    fixtures = transport_fixture.GrantTransportTests.fixtures
    fixture = transport_fixture.GrantTransportTests.fixture
    setup_total = transport_fixture.GrantTransportTests.setup_total
    setup_drain = transport_fixture.GrantTransportTests.setup_drain
    applied = transport_fixture.GrantTransportTests.applied
    report = transport_fixture.GrantTransportTests.report
    data = transport_fixture.GrantTransportTests.data
    envelope = transport_fixture.GrantTransportTests.envelope
    accept = transport_fixture.GrantTransportTests.accept
    sealed = transport_fixture.GrantTransportTests.sealed
    request = transport_fixture.GrantTransportTests.request
    prepare = transport_fixture.GrantTransportTests.prepare
    start = transport_fixture.GrantTransportTests.start
    encode = staticmethod(transport_fixture.GrantTransportTests.encode)

    def prepare_live(self):
        r = self.start()
        r.begin_session(self.token, {'session_id': self.session})
        r.heartbeat(self.token, dict(hostname='fixture', uptime=1,
                                     session_id=self.session))
        current = r.configuration(self.token)
        receipt = {key: current[key] for key in ('revision', 'digest')}
        receipt['session_id'] = self.session
        r.acknowledge_configuration(self.token, receipt)
        r.acknowledge_runtime(self.token, receipt)
        return r

    def test_online_and_listener_share_exact_exclusive_window(self):
        r = self.prepare_live()
        status = r.agent_status()[0]
        anchor = status['last_seen']
        with patch('core.time.time', return_value=anchor + ONLINE_WINDOW - .001):
            before = r.agent_status()[0]
            self.assertTrue(before['online'])
            self.assertTrue(before['listeners_current'])
        with patch('core.time.time', return_value=anchor + ONLINE_WINDOW):
            boundary = r.agent_status()[0]
            self.assertFalse(boundary['online'])
            self.assertFalse(boundary['listeners_current'])
            self.assertIsNotNone(boundary['applied_revision'])

    def test_master_restart_preserves_offline_not_old_listener_truth(self):
        r = self.prepare_live()
        status = r.agent_status()[0]
        reopened = Registry(r.path)
        with patch('core.time.time', return_value=status['last_seen'] + ONLINE_WINDOW):
            after = reopened.agent_status()[0]
        self.assertFalse(after['online'])
        self.assertFalse(after['listeners_current'])

    def test_scheduler_interval_includes_network_bound_and_is_strict(self):
        maximum = ONLINE_WINDOW - REQUEST_TIMEOUT * POST_HEARTBEAT_REQUESTS
        self.assertEqual(scheduling_interval(maximum - .001), maximum - .001)
        for value in (0, maximum,
                      ONLINE_WINDOW, 60, True):
            with self.subTest(value=value), self.assertRaises(ValueError):
                scheduling_interval(value)
        self.assertEqual(scheduling_interval(POLL_INTERVAL), POLL_INTERVAL)
        self.assertLess(POLL_INTERVAL + POST_HEARTBEAT_REQUESTS * REQUEST_TIMEOUT,
                        ONLINE_WINDOW)

    def test_session_and_heartbeat_receipts_require_exact_identity(self):
        sid, session = 7, str(uuid.uuid4())
        good = dict(accepted=True, server_id=sid, session_id=session)
        with patch('agent.request', return_value=good):
            self.assertEqual(begin_session('', '', session, sid), good)
            self.assertEqual(send('', '', 1, session, sid), good)
        changes = (
            {'accepted': 1}, {'server_id': sid + 1},
            {'session_id': str(uuid.uuid4())}, {'extra': 1})
        for change in changes:
            bad = dict(good, **change)
            for function, args in ((begin_session, ('', '', session, sid)),
                                   (send, ('', '', 1, session, sid))):
                with self.subTest(change=change, function=function.__name__), \
                     patch('agent.request', return_value=bad), \
                     self.assertRaises(ValueError):
                    function(*args)
        for bad in ({'accepted': True},
                    {'accepted': True, 'server_id': sid}, None):
            with patch('agent.request', return_value=bad), \
                 self.assertRaises(ValueError):
                send('', '', 1, session, sid)


class AgentLivenessDeadlineTests(unittest.IsolatedAsyncioTestCase):
    async def test_full_chain_closes_before_deadline_then_drains_next_begin(self):
        events = []
        release = threading.Event()
        second_begin = threading.Event()
        heartbeat_at = []

        class Runtime:
            def __init__(self):
                self.active = True
                self.closed_event = asyncio.Event()
                self.first_closed_at = None
                self.refunds = 0

            async def close(self):
                if self.first_closed_at is None:
                    self.first_closed_at = time.monotonic()
                    events.append('listener.closed')
                self.active = False
                self.closed_event.set()

        class Coordinator:
            def __init__(self):
                self.start_calls = 0
                self.stop_calls = 0

            async def step(self):
                events.append('usage')
                await asyncio.sleep(.01)
                return 'idle'

            async def stop(self):
                self.stop_calls += 1
                events.append('tail')
                return 'blocked'

        calls = 0

        def begin(*args):
            nonlocal calls
            calls += 1
            events.append(f'begin.{calls}')
            if calls == 2:
                second_begin.set()
                release.wait(2)
                events.append('begin.2.drained')
            return None

        def heartbeat(*args):
            heartbeat_at.append(time.monotonic())
            events.append('heartbeat')
            return None

        def capability(*args):
            events.append('negotiate')
            time.sleep(.01)
            return {'selected': sorted((STAGING, STAGE_IDENTITY, INSTALLATION)),
                    'execution_ready': False}

        async def status():
            events.append('status')
            await asyncio.sleep(.01)

        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        scheduler = GrantScheduler(
            '', '', temporary.name, Path(temporary.name) / 'unused', 1,
            str(uuid.uuid4()), str(uuid.uuid4()), interval=.02)
        runtime, coordinator = Runtime(), Coordinator()
        scheduler.runtime = runtime
        scheduler.coordinator = coordinator
        scheduler.value = {'current': True}
        scheduler._status = status

        window, margin = .25, .05
        with patch('grant_scheduler.ONLINE_WINDOW', window), \
             patch('grant_scheduler.DEADLINE_MARGIN', margin), \
             patch('grant_scheduler.begin_session', side_effect=begin), \
             patch('grant_scheduler.send', side_effect=heartbeat), \
             patch('grant_scheduler.negotiate', side_effect=capability), \
             patch('grant_scheduler.validate'):
            task = asyncio.create_task(scheduler.run())
            await asyncio.wait_for(asyncio.to_thread(second_begin.wait), .2)
            await asyncio.wait_for(runtime.closed_event.wait(), .3)

            self.assertLess(runtime.first_closed_at - heartbeat_at[0], window)
            self.assertFalse(runtime.active)
            self.assertFalse(task.done())
            self.assertNotIn('begin.2.drained', events)

            release.set()
            await asyncio.wait_for(task, .3)

        self.assertIn('begin.2.drained', events)
        self.assertLess(events.index('listener.closed'),
                        events.index('begin.2.drained'))
        self.assertEqual(events[:6], [
            'begin.1', 'heartbeat', 'negotiate', 'status', 'usage', 'begin.2'])
        self.assertGreaterEqual(coordinator.stop_calls, 1)
        self.assertEqual(coordinator.start_calls, 0)
        self.assertEqual(runtime.refunds, 0)
        self.assertTrue(scheduler.tail)
        self.assertFalse(scheduler.closed)


@unittest.skipUnless(BINARY.is_file(), 'Set XRAY_BINARY for real grant execution')
class AgentLivenessIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setup_scheduler(self):
        self.start_http(offer=False)
        self.r.configure_node(self.nid, dict(VLESS, listen_port=free_port()))
        state = self.store.path.parent / 'liveness-grant-scheduler'
        scheduler = GrantScheduler(
            self.url, self.token, state, BINARY, self.g['server_id'],
            self.session, self.g['grant_id'], interval=.01)
        return scheduler, state

    async def test_real_revocation_closes_listener_and_preserves_unclosed_tail(self):
        scheduler, state = self.setup_scheduler()
        await scheduler.step()
        self.assertIsNotNone(scheduler.runtime.core.process)
        self.r.revoke_agent(self.g['server_id'])
        await scheduler.run()
        self.assertIsNone(scheduler.runtime.core.process)
        self.assertTrue(scheduler.tail)
        self.assertFalse(scheduler.closed)
        with self.r.connect() as db:
            self.assertEqual(db.execute(
                'SELECT state FROM usage_first_total_lifecycle').fetchone()[0],
                'installed')
        quota = scheduler.runtime.grant_quota.snapshot()
        self.assertEqual(quota['remaining_bytes'],
                         scheduler.value['reserved_bytes'])
        self.assertTrue((state/'grant-runtime'/'grant-payload.sqlite').exists())

    async def test_real_listener_closes_on_scheduler_network_interruption(self):
        scheduler, _ = self.setup_scheduler()
        await scheduler.step()
        self.assertIsNotNone(scheduler.runtime.core.process)
        with patch.object(scheduler, '_session',
                          side_effect=OSError('master unreachable')):
            await scheduler.run()
        self.assertIsNone(scheduler.runtime.core.process)
        self.assertTrue(scheduler.closed)
        with self.r.connect() as db:
            self.assertEqual(db.execute(
                'SELECT state FROM usage_first_total_lifecycle').fetchone()[0],
                'closed')

    async def test_rotation_and_restarts_cannot_take_over_old_grant_or_tail(self):
        scheduler, state = self.setup_scheduler()
        await scheduler.step()
        await scheduler.runtime.close()
        def lost(*args):
            from agent import request
            request(*args)
            raise OSError('tail response lost')
        result, complete = await scheduler.coordinator.worker.finalize(
            now=0, transport=lost)
        self.assertEqual(result, 'retry')
        self.assertFalse(complete)
        old_token = self.token
        replacement = self.r.provision_agent(self.g['server_id'])['token']
        new_session = str(uuid.uuid4())
        reopened_master = Registry(self.r.path)
        with self.assertRaises(Error):
            reopened_master.heartbeat(old_token, dict(
                hostname='old', uptime=2, session_id=self.session))
        restarted = GrantScheduler(
            self.url, replacement, state, BINARY, self.g['server_id'],
            new_session, self.g['grant_id'], interval=.01)
        with patch('grant_scheduler.GrantCoordinator.start') as start:
            with self.assertRaises(urllib.error.HTTPError):
                await restarted.step()
        start.assert_not_called()
        self.assertFalse(restarted.tail)
        self.assertFalse(restarted.closed)
        self.assertIsNone(restarted.runtime)
        with self.r.connect() as db:
            self.assertEqual(db.execute(
                'SELECT state FROM usage_first_total_lifecycle').fetchone()[0],
                'installed')
            meter = db.execute(
                'SELECT count(*),coalesce(sum(upload_bytes+download_bytes),0) '
                'FROM usage_first_total_meter').fetchone()
        self.assertEqual(tuple(meter), (1, 0))


for name in ('setUp', 'tearDown', 'fixtures', 'fixture', 'setup_total',
             'setup_drain', 'applied', 'report', 'data', 'envelope', 'accept',
             'sealed', 'request', 'prepare', 'start', 'start_http', 'fetch',
             'post', 'rejected'):
    setattr(AgentLivenessIntegrationTests, name,
            getattr(transport_fixture.GrantTransportTests, name))
AgentLivenessIntegrationTests.encode = staticmethod(
    transport_fixture.GrantTransportTests.encode)


if __name__ == '__main__':
    unittest.main()
