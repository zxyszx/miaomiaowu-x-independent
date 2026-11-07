"""Contract tests for the explicit REALITY grant runtime boundary."""
import asyncio
import base64
import copy
import shutil
import ssl
import sqlite3
from unittest.mock import patch
import test_grant_agent as staging
import test_reality as reality_fixture
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
import uuid

from grant_reality_runtime import GrantRealityRuntime
from node_config import XRAY_SCHEMA
from test_xray import BINARY, VLESS


class GrantRealityContractTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.stage = SimpleNamespace(server_id=1, session_id=str(uuid.uuid4()))
        self.reality = {
            'target_host': 'fixture.example', 'target_port': 443,
            'server_name': 'fixture.example',
            'private_key': base64.urlsafe_b64encode(b'x' * 32).decode().rstrip('='),
            'short_id': '0123456789abcdef',
        }

    def tearDown(self):
        self.temp.cleanup()

    def make_runtime(self):
        return GrantRealityRuntime(BINARY, Path(self.temp.name) / 'runtime', self.stage,
                                   reality_test_target=('127.0.0.1', 443))

    def test_configuration_keeps_public_reality_and_private_gateway(self):
        runtime = self.make_runtime()
        runtime.ports = {7: 34567}
        runtime.paced.trusted_ingress = {7: 'trusted-token'}
        snapshot = {
            'schema': XRAY_SCHEMA, 'server_id': 1,
            'server_name': 'grant-reality-runtime', 'revision': 1,
            'valid_until': 2**31,
            'nodes': [{'id': 7, 'name': 'grant-reality',
                       'config': {**VLESS, 'listen_port': 24500,
                                  'reality': self.reality},
                       'clients': [{'uuid': str(uuid.uuid4()), 'user_id': 1}]}],
        }
        generated = runtime._configuration(snapshot)
        self.assertEqual(generated['inbounds'][0]['streamSettings']['security'], 'reality')
        self.assertEqual(generated['inbounds'][0]['port'], 24500)
        self.assertTrue(any(o['protocol'] == 'freedom' for o in generated['outbounds']))
        freedom = next(o for o in generated['outbounds'] if o['protocol'] == 'freedom')
        self.assertEqual(freedom['settings']['redirect'], '127.0.0.1:34567')
        self.assertEqual(freedom['settings']['finalRules'][0]['action'], 'allow')

    async def test_plain_apply_is_rejected_without_starting_any_runtime(self):
        runtime = self.make_runtime()
        with self.assertRaisesRegex(ValueError, '不能回退到旧计量'):
            await runtime.apply({})
        await runtime.close()

    def test_invalid_reality_probe_target_is_rejected(self):
        with self.assertRaises(ValueError):
            GrantRealityRuntime(BINARY, Path(self.temp.name) / 'bad', self.stage,
                                reality_test_target=('0.0.0.0', 443))



# Reuse the original real TLS decoy/client setup, not its business test cases.


@unittest.skipUnless(BINARY.is_file() and shutil.which('openssl') and ssl.HAS_TLSv1_3,
                     'Real Xray, OpenSSL CLI and Python TLS 1.3 required')
class GrantRealityRealTests(unittest.IsolatedAsyncioTestCase):
    setUp = staging.GrantAgentTests.setUp
    sign = staging.GrantAgentTests.sign
    client = reality_fixture.RealityRealTests.client
    asyncTearDown = reality_fixture.RealityRealTests.asyncTearDown

    async def asyncSetUp(self):
        self.received = bytearray()
        await reality_fixture.RealityRealTests.asyncSetUp(self)
        await self.runtime.close()
        self.row['node_config'] = self.config
        self.row['reserved_bytes'] = 6
        self.row['baseline_bytes'] = 0
        self.row['client'].update(cycle_id=self.row['cycle_id'],
            cycle_budget=dict(upload_bytes=10, download_bytes=10),
            cycle_usage=dict(upload_bytes=0, download_bytes=0))
        self.credential = self.row['client']['uuid']
        self.sign(self.row)
        self.store.stage(self.row,staging.GrantAgentTests.identity(self,self.row))
        self.runtime = self.make_grant_runtime()

    async def echo(self, reader, writer):
        self.writers.add(writer)
        task = asyncio.current_task()
        self.tasks.add(task)
        try:
            while data := await reader.read(65536):
                if writer.get_extra_info('ssl_object') is None:
                    self.received.extend(data)
                writer.write(data)
                await writer.drain()
        except (OSError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass
            self.writers.discard(writer)
            self.tasks.discard(task)

    def make_grant_runtime(self):
        return GrantRealityRuntime(BINARY, self.root/'grant-runtime', self.store,
            local_test_target=('127.0.0.1', self.target_port),
            reality_test_target=('127.0.0.1', self.decoy.sockets[0].getsockname()[1]))

    async def test_nonfirst_or_ambiguous_stage_blocks_before_quota_and_apply(self):
        for attack in ('multi','conflict','missing_kind'):
            with self.subTest(attack=attack):
                with sqlite3.connect(self.store.path) as db:
                    if attack=='multi':
                        db.execute("UPDATE staged_grants SET ledger_kind='multi_allocation'")
                    elif attack=='conflict':
                        db.execute("INSERT INTO staged_grants SELECT 'first_grant',grant_id,cycle_id,payload,digest FROM staged_grants")
                    else:
                        db.execute('ALTER TABLE staged_grants RENAME COLUMN ledger_kind TO unknown_kind')
                with patch('grant_payload.AllocationQuota.__init__') as quota, \
                     patch.object(self.runtime.paced,'_apply') as inner, \
                     patch.object(self.runtime.front,'apply') as front:
                    with self.assertRaises((ValueError,sqlite3.Error)):
                        await self.runtime.install(self.row)
                quota.assert_not_called(); inner.assert_not_called(); front.assert_not_called()
                self.assertIsNone(self.runtime.front.process)
                self.assertIsNone(self.runtime.paced.core.process)

    async def transfer(self, payload):
        reader, writer = await self.client()
        try:
            writer.write(payload)
            await writer.drain()
            return await asyncio.wait_for(reader.read(), 4)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass

    async def test_real_encrypted_total_and_reopen_never_refund(self):
        receipt = await self.runtime.install(self.row)
        self.assertEqual(receipt['state'], 'local_reality_runtime_active')
        self.assertFalse(receipt['ready_for_execution'])
        with sqlite3.connect(self.path) as db:
            installation = db.execute('SELECT fact,first_installed_at FROM local_grant_installations').fetchall()
        import json
        fact = json.loads(installation[0][0])
        self.assertEqual(fact['runtime_digest'], receipt['runtime_digest'])
        self.assertEqual(fact['digest'], receipt['digest'])
        self.assertEqual(fact['protocol'], 'reality')
        self.assertEqual(await self.transfer(b'abcd'), b'ab')
        self.assertEqual(self.received, b'abcd')
        evidence = self.runtime.paced.grant_quota.snapshot()
        self.assertEqual(evidence['successful_upload_bytes'], 4)
        self.assertEqual(evidence['successful_download_bytes'], 2)
        self.assertEqual(evidence['remaining_bytes'], 0)
        await self.runtime.close()
        self.runtime = self.make_grant_runtime()
        await self.runtime.install(self.row)
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('SELECT fact,first_installed_at FROM local_grant_installations').fetchall(), installation)
        self.assertEqual(await self.transfer(b'xx'), b'')
        self.assertEqual(self.received, b'abcd')
        self.assertEqual(self.runtime.paced.grant_quota.snapshot()['remaining_bytes'], 0)

    async def test_repeat_install_keeps_both_processes_and_balance(self):
        first = await self.runtime.install(self.row)
        pids = (self.runtime.front.process.pid, self.runtime.paced.core.process.pid)
        self.assertEqual(await self.runtime.install(self.row), first)
        self.assertEqual((self.runtime.front.process.pid, self.runtime.paced.core.process.pid), pids)
        with self.assertRaises(ValueError):
            await self.runtime.apply(self.runtime.snapshot)
        self.assertEqual(self.runtime.paced.grant_quota.snapshot()['remaining_bytes'], 6)

    async def test_changed_grant_closes_both_layers_without_refund(self):
        await self.runtime.install(self.row)
        changed = copy.deepcopy(self.row)
        changed['reserved_bytes'] += 1
        self.sign(changed)
        with self.assertRaises(ValueError):
            await self.runtime.install(changed)
        self.assertIsNone(self.runtime.front.process)
        self.assertIsNone(self.runtime.paced.core.process)
        self.assertFalse(self.runtime.paced.listeners)
        self.assertEqual(self.runtime.paced.grant_quota.snapshot()['remaining_bytes'], 6)

    async def test_expiry_during_front_startup_cannot_return_active(self):
        original = self.runtime.front.apply
        import time
        clock = unittest.mock.Mock(return_value=time.time())
        async def crossing(snapshot):
            await original(snapshot)
            clock.return_value = self.row['valid_until']
        with patch('grant_agent.time.time', clock), patch.object(self.runtime.front, 'apply', crossing):
            with self.assertRaises(ValueError):
                await self.runtime.install(self.row)
        self.assertIsNone(self.runtime.front.process)
        self.assertIsNone(self.runtime.paced.core.process)
        self.assertFalse(self.runtime.paced.listeners)
        self.assertEqual(self.received, b'')

    async def test_installation_record_failure_closes_both_live_layers(self):
        def fail(*args):
            for process in (self.runtime.front.process, self.runtime.paced.core.process):
                self.assertIsNotNone(process)
                self.assertIsNone(process.returncode)
            raise sqlite3.OperationalError('disk')
        with patch('grant_reality_runtime.record_installation', side_effect=fail):
            with self.assertRaises(sqlite3.OperationalError):
                await self.runtime.install(self.row)
        self.assertIsNone(self.runtime.front.process)
        self.assertIsNone(self.runtime.paced.core.process)
        self.assertFalse(self.runtime.paced.listeners)
        self.assertEqual(self.runtime.paced.grant_quota.snapshot()['remaining_bytes'], 6)


if __name__ == '__main__':
    unittest.main()
