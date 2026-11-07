"""Large closure from actual VLESS payload, not synthetic execution rows."""
import asyncio
import json
from pathlib import Path
import socket
import time
import unittest
import uuid
from unittest import mock

from agent import request
from drain_seal import DrainSeal
from execution_agent import cycle
from execution_epochs import ExecutionEpochs
from fragment_delivery import FragmentDelivery
from usage_delivery import UsageDelivery
from usage_migration import UsageMigration
from usage_outbox import UsageOutbox
from vless_runtime import VlessRuntime
import test_usage_settlement as base
import test_xray


@unittest.skipUnless(test_xray.BINARY.is_file(), 'Real Xray required')
class LargePayloadClosureTests(unittest.IsolatedAsyncioTestCase):
    setUp = base.SettlementRealTests.setUp
    tearDown = base.SettlementRealTests.tearDown
    request = base.SettlementRealTests.request
    asyncSetUp = base.SettlementRealTests.asyncSetUp
    asyncTearDown = base.SettlementRealTests.asyncTearDown
    open = base.SettlementRealTests.open
    accepted = base.SettlementRealTests.accepted
    close_writer = base.SettlementRealTests.close_writer

    async def test_65_real_node_identities_close_deliver_restart_and_seal_exact_bytes(self):
        registry = self.server.registry
        identities = [(self.nid, self.credential['uuid'], self.port)]
        # Hold ephemeral ports until all are selected; never accidentally reuse
        # a released free_port() value within this multi-listener fixture.
        sockets = []
        try:
            for index in range(64):
                listener = socket.socket()
                listener.bind(('127.0.0.1', 0))
                sockets.append(listener)
                port = listener.getsockname()[1]
                nid = registry.save('nodes', dict(name=f'closure-{index}', server_id=self.sid))['id']
                registry.configure_node(nid, {**test_xray.VLESS, 'listen_port': port})
                auth = registry.authorize_node_user(nid, self.uid)
                identities.append((nid, auth['uuid'], port))
        finally:
            for listener in sockets:
                listener.close()
        now = time.time()
        with registry.connect() as db:
            db.execute('INSERT INTO usage_cycles(id,user_id,starts_at,ends_at) VALUES(?,?,?,?)',
                       ('large-real', self.uid, now - 1, now + 180))
            from usage_accounting import initialize_cycle
            initialize_cycle(db,'large-real')
        original_session = str(uuid.uuid4())
        epochs = ExecutionEpochs(self.base, self.token, self.state, self.runtime, original_session)
        await cycle(self.base, self.token, self.state, self.runtime, 1, original_session, epochs)
        expected = {}
        started = time.monotonic()
        # Distinct per-node byte counts expose missing identities, swapped
        # counters and duplicated fragments; handshake bytes must not be billed.
        for index, (nid, credential, port) in enumerate(identities):
            reader, writer = await self.open(credential, port)
            await self.accepted(reader)
            payload = bytes([index]) * (33 + index)
            writer.write(payload)
            await writer.drain()
            self.assertEqual(await asyncio.wait_for(reader.readexactly(len(payload)), 3), payload)
            await self.close_writer(writer)
            expected[nid] = 5 + len(payload)
        transfer_seconds = time.monotonic() - started
        await epochs.delivery.capture()
        actual = self.runtime.usage_snapshot()['items']
        self.assertEqual({i['node_id']: (i['upload_bytes'], i['download_bytes']) for i in actual},
                         {nid: (size, size) for nid, size in expected.items()})
        self.assertEqual(len(actual), 65)
        UsageMigration(registry).begin_drain('large-real')
        await self.runtime.close()  # Real listeners/process stop before closure.
        await epochs.delivery.rotate(str(uuid.uuid4()))
        box = epochs.delivery.box
        final = box.closed_epoch(original_session)
        self.assertEqual(len(final['checkpoint']['items']), 65)
        self.assertEqual(final['checkpoint']['uncertain_writes'], [])
        self.assertTrue(box.outstanding_sessions())
        calls = []
        fragment_jobs = []
        lost = False

        def send(base_url, token, path, data):
            nonlocal lost
            calls.append(path)
            if path.endswith('/fragment'):
                # Real old payload must be confirmed before final evidence.
                with registry.connect() as db:
                    self.assertEqual(int(db.execute(
                        'SELECT total_bytes FROM usage_cycle_confirmed_totals WHERE cycle_id=?',
                        ('large-real',)).fetchone()[0]), 2 * sum(expected.values()))
                fragment_jobs.append(json.loads(json.dumps(data)))
            reply = request(base_url, token, path, data)
            if path.endswith('/fragment') and not lost:
                lost = True
                raise OSError('fixture: master committed first part, response lost')
            return reply

        worker = epochs.delivery
        FragmentDelivery(box)  # Create the queue schema; discovery must enqueue it.
        reopened = False
        with mock.patch('usage_delivery.request', side_effect=send):
            for _ in range(32):
                before = len(calls)
                await worker.step()
                self.assertLessEqual(len(calls) - before, 1)
                if lost and not reopened:
                    # A fresh local runtime has no carried cumulative counters.
                    # Reopen the durable Outbox and worker, not just its cursor.
                    self.runtime = VlessRuntime(test_xray.BINARY, Path(self.temp.name) / 'reopened-xray',
                        local_test_target=('127.0.0.1', self.target_port))
                    worker = UsageDelivery(self.base, self.token, self.state, self.runtime, str(uuid.uuid4()))
                    worker.box = UsageOutbox(box.path, self.sid)
                    box = worker.box
                    with box.connect() as db:
                        row = db.execute('SELECT part,status,attempts FROM closed_fragment_deliveries').fetchone()
                        self.assertEqual(tuple(row), (0, 'pending', 1))
                        # Explicit fixture retry time, not production backoff removal.
                        db.execute('UPDATE closed_fragment_deliveries SET next_attempt=0')
                    reopened = True
                with box.connect() as db:
                    row = db.execute('SELECT status FROM closed_fragment_deliveries').fetchone()
                if row and row['status'] == 'accepted':
                    break
        self.assertTrue(reopened)
        self.assertEqual(row['status'], 'accepted')
        self.assertEqual(len(fragment_jobs), 3)
        self.assertEqual(fragment_jobs[0], fragment_jobs[1])
        self.assertEqual([j['fragment']['body']['index'] for j in fragment_jobs], [0, 0, 1])
        self.assertFalse(box.outstanding_sessions())
        self.assertEqual(box.closed_epoch(original_session), final)
        with registry.connect() as db:
            confirmed = db.execute('SELECT node_id,upload_bytes,download_bytes FROM usage_deltas').fetchall()
            self.assertEqual({r['node_id']: (r['upload_bytes'], r['download_bytes']) for r in confirmed},
                             {nid: (size, size) for nid, size in expected.items()})
            self.assertEqual(len(confirmed), 65)
            parts = db.execute('SELECT body FROM usage_closed_parts').fetchall()
            self.assertEqual(sorted(len(json.loads(r['body'])['items']) for r in parts), [1, 64])
        sealed = DrainSeal(registry).seal('large-real')
        self.assertEqual((sealed['final_upload_bytes'], sealed['final_download_bytes']),
                         (sum(expected.values()), sum(expected.values())))
        self.assertFalse(sealed['ready_for_execution'])
        print('LARGE_REAL_CLOSURE ' + json.dumps(dict(identities=65, payload_per_direction=sum(expected.values()),
              transfer_seconds=transfer_seconds, fragment_requests=len(fragment_jobs))), flush=True)


if __name__ == '__main__':
    unittest.main()
