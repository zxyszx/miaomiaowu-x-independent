"""Formal grant mode selection and fail-closed serial scheduling."""
import asyncio
import io
import json
import sqlite3
import tempfile
import threading
import unittest
import urllib.error
import uuid
from unittest.mock import patch

import grant_scheduler
from agent import request
from test_runtime import free_port
from test_xray import BINARY, VLESS
import test_grant_transport as fixture


SESSION = str(uuid.uuid4())
GRANT = str(uuid.uuid4())
EXECUTION = str(uuid.uuid4())


class Runtime:
    def __init__(self, events):
        self.events = events
        self.closed = 0

    async def close(self):
        self.events.append('runtime.close')
        self.closed += 1


class Coordinator:
    def __init__(self, runtime, base, token, events, *, execution=None,
                 recovery=ValueError('no verified tail'), stopping=None):
        self.runtime = runtime
        self.events = events
        self.execution = execution or dict(
            accepted=True, state='execution_recorded',
            execution=dict(grant_id=GRANT, server_id=1, session_id=SESSION,
                revision=1, digest='a'*64, runtime_digest='b'*64,
                protocol='vless', execution_id=EXECUTION, valid_until=100),
            recorded_at=1,
            ready_for_execution=False)
        self.recovery = recovery
        self.stopping = stopping or dict(state='closed')
        self.worker = object()
        self.starts = self.steps = self.recovers = self.stops = 0

    async def start(self, value):
        self.events.append('coordinator.start')
        self.starts += 1
        return self.execution

    async def step(self):
        self.events.append('coordinator.step')
        self.steps += 1
        return 'idle'

    async def recover(self, value):
        self.events.append('coordinator.recover')
        self.recovers += 1
        if isinstance(self.recovery, BaseException):
            raise self.recovery
        return self.recovery

    async def stop(self):
        self.events.append('coordinator.stop')
        self.stops += 1
        return self.stopping


class GrantSchedulerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.events = []
        self.runtimes = []
        self.coordinators = []
        self.value = dict(grant_id=GRANT, server_id=1, session_id=SESSION,
                          revision=1, digest='a'*64,
                          node_config={'protocol': 'vless'},
                          issued_at=1, valid_until=100)

    def runtime(self, value):
        runtime = Runtime(self.events)
        self.runtimes.append(runtime)
        return runtime

    def factory(self, **options):
        def create(runtime, base, token):
            coordinator = Coordinator(runtime, base, token, self.events, **options)
            self.coordinators.append(coordinator)
            return coordinator
        return create

    def scheduler(self, **options):
        return grant_scheduler.GrantScheduler(
            'http://127.0.0.1', 'token', self._testMethodName, '/xray',
            1, SESSION, GRANT, interval=.01, runtime_factory=self.runtime,
            coordinator_factory=options.pop('coordinator_factory', self.factory(**options)))

    def network(self, path, *args):
        if path == '/agent/grants/configuration':
            return self.value
        if path == '/agent/grants/lifecycle':
            return dict(accepted=True, grant_id=GRANT, state='installed',
                        ready_for_execution=False)
        raise AssertionError(path)

    def patches(self):
        return (
            patch('grant_scheduler.validate'),
            patch('grant_scheduler.begin_session', return_value={'accepted': True}),
            patch('grant_scheduler.send', return_value={'accepted': True}),
            patch('grant_scheduler.negotiate', return_value=dict(
                accepted=True, server_id=1, session_id=SESSION,
                offered=sorted((grant_scheduler.STAGING, grant_scheduler.STAGE_IDENTITY,
                                grant_scheduler.INSTALLATION)),
                selected=sorted((grant_scheduler.STAGING, grant_scheduler.STAGE_IDENTITY,
                                 grant_scheduler.INSTALLATION)),
                execution_ready=False)),
            patch('grant_scheduler.request', side_effect=lambda b, t, p, d: self.network(p, d)),
            patch.object(grant_scheduler.GrantStageStore, 'stage', return_value={
                'state': 'staged', 'ready_for_execution': False}),
            patch('grant_scheduler.resolve_ledger', return_value={
                'accepted':True,'state':'ledger_resolved','ledger_kind':'first_grant',
                'ledger_id':GRANT,'binding_digest':'a'*64,
                'ready_for_execution':False}))

    async def test_explicit_modes_reject_missing_or_unknown_without_fallback(self):
        for mode in grant_scheduler.MODES:
            self.assertEqual(grant_scheduler.select_mode(mode), mode)
        for mode in (None, 'tcp', 'xray', 'staging', 'auto'):
            with self.assertRaises(ValueError):
                grant_scheduler.select_mode(mode)

    async def test_full_start_then_repeated_poll_is_serial_and_idempotent(self):
        scheduler = self.scheduler()
        with self.patches()[0], self.patches()[1], self.patches()[2], \
             self.patches()[3], self.patches()[4], self.patches()[5], self.patches()[6], \
             patch.object(scheduler, '_local_value', return_value=None):
            result = await scheduler.step()
            self.assertEqual(result['state'], 'execution_recorded')
            self.assertFalse(result['ready_for_execution'])
            self.assertEqual(await scheduler.step(), 'idle')
        self.assertEqual(len(self.coordinators), 1)
        self.assertEqual(self.coordinators[0].starts, 1)
        self.assertEqual(self.coordinators[0].steps, 1)

    async def test_staging_or_ordinary_ack_cannot_be_execution_proof(self):
        ordinary = dict(accepted=True, state='staged', revision=1)
        scheduler = self.scheduler(execution=ordinary)
        with self.patches()[0], self.patches()[1], self.patches()[2], \
             self.patches()[3], self.patches()[4], self.patches()[5], self.patches()[6], \
             patch.object(scheduler, '_local_value', return_value=None):
            with self.assertRaises(ValueError):
                await scheduler.step()
        self.assertGreaterEqual(self.runtimes[0].closed, 1)
        self.assertIsNone(scheduler.coordinator)

    async def test_staging_only_protocol_selection_never_creates_runtime(self):
        scheduler = self.scheduler()
        selected = dict(accepted=True, server_id=1, session_id=SESSION,
            offered=[grant_scheduler.STAGING], selected=[grant_scheduler.STAGING],
            execution_ready=False)
        with patch('grant_scheduler.begin_session', return_value={'accepted': True}), \
             patch('grant_scheduler.send', return_value={'accepted': True}), \
             patch('grant_scheduler.negotiate', return_value=selected), \
             patch.object(scheduler, '_local_value', return_value=None):
            with self.assertRaises(ValueError):
                await scheduler.step()
        self.assertEqual(self.runtimes, [])
        self.assertEqual(self.coordinators, [])

    async def test_wrong_grant_identity_never_creates_runtime(self):
        scheduler = self.scheduler()
        wrong = dict(self.value, grant_id=str(uuid.uuid4()))
        with self.patches()[0], self.patches()[1], self.patches()[2], \
             self.patches()[3], self.patches()[5], \
             patch('grant_scheduler.request', return_value=wrong), \
             patch.object(scheduler, '_local_value', return_value=None):
            with self.assertRaises(ValueError):
                await scheduler.step()
        self.assertEqual(self.runtimes, [])
        self.assertEqual(self.coordinators, [])

    async def test_resolver_failures_and_forged_receipts_never_create_runtime(self):
        failures=(OSError('resolver offline'),
                  urllib.error.HTTPError('local',403,'old session',{},io.BytesIO()),
                  ValueError('wrong ledger kind'),
                  ValueError('ledger collision'))
        for failure in failures:
            scheduler=self.scheduler()
            patches=self.patches()
            with self.subTest(failure=str(failure)), patches[0], patches[1], patches[2], \
                 patches[3], patches[4], patches[5], \
                 patch('grant_scheduler.resolve_ledger',side_effect=failure), \
                 patch.object(scheduler,'_local_value',return_value=None):
                with self.assertRaises(type(failure)):
                    await scheduler.step()
            if isinstance(failure,urllib.error.HTTPError): failure.close()
        forged={'accepted':1,'state':'ledger_resolved','ledger_kind':'first_grant',
                'ledger_id':GRANT,'binding_digest':'a'*64,
                'ready_for_execution':0}
        scheduler=self.scheduler()
        patches=self.patches()
        with patches[0],patches[1],patches[2],patches[3],patches[4],patches[5], \
             patch('grant_scheduler.resolve_ledger',return_value=forged), \
             patch.object(scheduler,'_local_value',return_value=None):
            with self.assertRaises(ValueError):
                await scheduler.step()
        self.assertEqual(self.runtimes,[])
        self.assertEqual(self.coordinators,[])

    async def test_existing_multi_conflict_or_untyped_stage_never_creates_runtime(self):
        for attack in ('multi','conflict','untyped'):
            with self.subTest(attack=attack),tempfile.TemporaryDirectory() as root:
                scheduler=grant_scheduler.GrantScheduler(
                    'http://127.0.0.1','token',root,'/xray',1,SESSION,GRANT,
                    interval=.01,runtime_factory=self.runtime,
                    coordinator_factory=self.factory())
                scheduler.stage.path.parent.mkdir(parents=True,exist_ok=True)
                raw=json.dumps(self.value)
                with sqlite3.connect(scheduler.stage.path) as db:
                    if attack=='untyped':
                        db.execute('''CREATE TABLE staged_grants(
                            grant_id TEXT PRIMARY KEY,cycle_id TEXT NOT NULL UNIQUE,
                            payload TEXT NOT NULL,digest TEXT NOT NULL)''')
                        db.execute('INSERT INTO staged_grants VALUES(?,?,?,?)',
                                   (GRANT,str(uuid.uuid4()),raw,'a'*64))
                    else:
                        db.execute('''CREATE TABLE staged_grants(
                            ledger_kind TEXT NOT NULL,grant_id TEXT NOT NULL,
                            cycle_id TEXT NOT NULL,payload TEXT NOT NULL,digest TEXT NOT NULL,
                            PRIMARY KEY(ledger_kind,grant_id))''')
                        db.execute('INSERT INTO staged_grants VALUES(?,?,?,?,?)',
                                   ('multi_allocation',GRANT,str(uuid.uuid4()),raw,'a'*64))
                        if attack=='conflict':
                            db.execute('INSERT INTO staged_grants VALUES(?,?,?,?,?)',
                                       ('first_grant',GRANT,str(uuid.uuid4()),raw,'a'*64))
                patches=self.patches()
                with patches[0],patches[1],patches[2],patches[3],patches[4], \
                     patch('grant_scheduler.resolve_ledger',
                           side_effect=ValueError('not exact first')):
                    with self.assertRaises(ValueError): await scheduler.step()
        self.assertEqual(self.runtimes,[])
        self.assertEqual(self.coordinators,[])

    async def test_verified_tail_recovers_without_start_or_listener(self):
        scheduler = self.scheduler(recovery='retry', stopping=dict(state='closed'))
        with patch('grant_scheduler.validate'), \
             patch('grant_scheduler.GrantUsageWorker.recover', return_value=None), \
             patch.object(scheduler, '_local_value', return_value=self.value):
            self.assertEqual(await scheduler.step(), 'recovering')
            self.assertEqual(await scheduler.step(), dict(state='closed'))
        coordinator = self.coordinators[0]
        self.assertEqual(coordinator.recovers, 1)
        self.assertEqual(coordinator.starts, 0)
        self.assertEqual(coordinator.stops, 1)
        self.assertGreaterEqual(self.runtimes[0].closed, 1)

    async def test_unverified_local_state_runs_full_explicit_start(self):
        scheduler = self.scheduler()
        with self.patches()[0], self.patches()[1], self.patches()[2], \
             self.patches()[3], self.patches()[4], self.patches()[5], self.patches()[6], \
             patch('grant_scheduler.GrantUsageWorker.recover',
                   side_effect=ValueError('no verified tail')), \
             patch.object(scheduler, '_local_value', return_value=self.value):
            result = await scheduler.step()
        self.assertEqual(result['state'], 'execution_recorded')
        self.assertEqual(len(self.coordinators), 2)
        self.assertEqual(self.coordinators[0].recovers, 0)
        self.assertEqual(self.coordinators[0].starts, 0)
        self.assertEqual(self.coordinators[1].starts, 1)
        self.assertGreaterEqual(self.runtimes[0].closed, 1)

    async def test_verified_tail_network_failure_never_falls_back_to_start(self):
        denied = urllib.error.HTTPError('local', 403, 'revoked', {}, io.BytesIO())
        scheduler = self.scheduler(recovery=denied)
        with patch('grant_scheduler.validate'), \
             patch('grant_scheduler.GrantUsageWorker.recover', return_value=None), \
             patch.object(scheduler, '_local_value', return_value=self.value):
            with self.assertRaises(urllib.error.HTTPError):
                await scheduler.step()
        self.assertTrue(scheduler.tail)
        self.assertEqual(len(self.coordinators), 1)
        self.assertEqual(self.coordinators[0].recovers, 1)
        self.assertEqual(self.coordinators[0].starts, 0)

    async def test_mode_switch_closes_listener_before_final_tail(self):
        scheduler = self.scheduler()
        runtime = self.runtime(self.value)
        coordinator = Coordinator(runtime, '', '', self.events)
        scheduler.runtime, scheduler.coordinator, scheduler.value = (
            runtime, coordinator, self.value)
        modes = iter((grant_scheduler.GRANT_MODE, 'legacy-tcp'))
        with patch('grant_scheduler.validate'), \
             patch.object(scheduler, '_session', return_value=None), \
             patch.object(scheduler, '_status', return_value=None):
            await scheduler.run(lambda: next(modes))
        self.assertIn('coordinator.step', self.events)
        close = self.events.index('runtime.close')
        stop = self.events.index('coordinator.stop')
        self.assertLess(close, stop)

    async def test_auth_rejection_closes_before_tail_and_never_falls_back(self):
        scheduler = self.scheduler()
        runtime = self.runtime(self.value)
        coordinator = Coordinator(runtime, '', '', self.events)
        scheduler.runtime, scheduler.coordinator, scheduler.value = (
            runtime, coordinator, self.value)
        error = urllib.error.HTTPError('local', 403, 'revoked', {}, io.BytesIO())
        with patch.object(scheduler, '_session', side_effect=error):
            await scheduler.run()
        self.assertLess(self.events.index('runtime.close'),
                        self.events.index('coordinator.stop'))
        self.assertEqual(coordinator.starts, 0)

    async def test_cancel_closes_listener_before_blocking_auth_worker_drains(self):
        scheduler = self.scheduler()
        runtime = self.runtime(self.value)
        coordinator = Coordinator(runtime, '', '', self.events)
        scheduler.runtime, scheduler.coordinator, scheduler.value = (
            runtime, coordinator, self.value)
        entered, release = threading.Event(), threading.Event()
        def blocked(*args):
            entered.set()
            release.wait(3)
            return {'accepted': True}
        with patch('grant_scheduler.begin_session', side_effect=blocked):
            task = asyncio.create_task(scheduler.run())
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait, 1))
                task.cancel()
                await asyncio.sleep(.02)
                self.assertIn('runtime.close', self.events)
                self.assertFalse(task.done())
            finally:
                release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertLess(self.events.index('runtime.close'),
                        self.events.index('coordinator.stop'))


@unittest.skipUnless(BINARY.is_file(), 'Set XRAY_BINARY for real grant execution')
class GrantSchedulerIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setup_scheduler(self):
        self.start_http(offer=False)
        self.r.configure_node(self.nid, dict(VLESS, listen_port=free_port()))
        state = self.store.path.parent / 'formal-grant-scheduler'
        return grant_scheduler.GrantScheduler(
            self.url, self.token, state, BINARY, self.g['server_id'],
            self.session, self.g['grant_id'], interval=.01)

    async def test_real_full_start_repeated_poll_and_final_close(self):
        scheduler = self.setup_scheduler()
        result = await scheduler.step()
        self.assertEqual(result['state'], 'execution_recorded')
        self.assertFalse(result['ready_for_execution'])
        self.assertEqual(await scheduler.step(), 'accepted')
        closed = await scheduler.finish()
        self.assertEqual(closed['state'], 'closed')
        self.assertIsNone(scheduler.runtime.core.process)
        with self.r.connect() as db:
            self.assertEqual(db.execute(
                'SELECT state FROM usage_first_total_lifecycle').fetchone()[0], 'closed')
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_first_total_execution').fetchone()[0], 1)
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_first_total_meter').fetchone()[0], 1)

    async def test_real_restart_recovers_lost_tail_without_starting_listener(self):
        scheduler = self.setup_scheduler()
        await scheduler.step()
        await scheduler.runtime.close()
        def lost(*args):
            request(*args)
            raise OSError('final usage response lost before scheduler restart')
        result, complete = await scheduler.coordinator.worker.finalize(
            now=0, transport=lost)
        self.assertEqual(result, 'retry')
        self.assertFalse(complete)
        reopened = grant_scheduler.GrantScheduler(
            self.url, self.token, scheduler.state, BINARY, self.g['server_id'],
            self.session, self.g['grant_id'], interval=.01)
        with patch('grant_scheduler.GrantCoordinator.start') as start:
            self.assertEqual(await reopened.step(), 'closed')
        start.assert_not_called()
        self.assertIsNone(reopened.runtime.core.process)
        with self.r.connect() as db:
            self.assertEqual(db.execute(
                'SELECT state FROM usage_first_total_lifecycle').fetchone()[0], 'closed')
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_first_total_meter').fetchone()[0], 1)


for name in ('setUp', 'tearDown', 'fixtures', 'fixture', 'setup_total',
             'setup_drain', 'applied', 'report', 'data', 'envelope', 'accept',
             'sealed', 'request', 'prepare', 'start', 'start_http', 'fetch',
             'post', 'rejected'):
    setattr(GrantSchedulerIntegrationTests, name,
            getattr(fixture.GrantTransportTests, name))
GrantSchedulerIntegrationTests.encode = staticmethod(
    fixture.GrantTransportTests.encode)


if __name__ == '__main__':
    unittest.main()
