"""Real Xray payloads through the explicitly selected local grant runtime."""
import asyncio
import copy
import sqlite3
import unittest
import uuid
from unittest.mock import patch

from grant_runtime import GrantVlessRuntime
from test_runtime import free_port
from test_xray import BINARY,VLESS
import test_grant_agent as staging


@unittest.skipUnless(BINARY.is_file(),'Set XRAY_BINARY for real grant execution')
class GrantRuntimeTests(unittest.IsolatedAsyncioTestCase):
    setUp=staging.GrantAgentTests.setUp
    sign=staging.GrantAgentTests.sign

    async def asyncSetUp(self):
        self.received=bytearray()
        async def echo(reader,writer):
            try:
                while data:=await reader.read(1024):
                    self.received.extend(data)
                    writer.write(data); await writer.drain()
            except OSError: pass
            finally:
                writer.close()
                try: await writer.wait_closed()
                except OSError: pass
        self.target=await asyncio.start_server(echo,'127.0.0.1',0)
        self.target_port=self.target.sockets[0].getsockname()[1]
        self.port=free_port()
        self.row['node_config']={**VLESS,'listen_port':self.port}
        self.row['reserved_bytes']=6
        self.row['baseline_bytes']=0
        self.row['client'].update(cycle_id=self.row['cycle_id'],
            cycle_budget=dict(upload_bytes=10,download_bytes=10),
            cycle_usage=dict(upload_bytes=0,download_bytes=0))
        self.sign(self.row)
        self.store.stage(self.row,staging.GrantAgentTests.identity(self,self.row))
        self.state=self.path.parent/'runtime'
        self.runtime=self.make_runtime()

    def make_runtime(self):
        return GrantVlessRuntime(BINARY,self.state,self.store,
            local_test_target=('127.0.0.1',self.target_port))

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
                     patch.object(self.runtime,'_apply') as apply:
                    with self.assertRaises((ValueError,sqlite3.Error)):
                        await self.runtime.install(self.row)
                quota.assert_not_called(); apply.assert_not_called()
                self.assertIsNone(self.runtime.core.process)

    async def asyncTearDown(self):
        await self.runtime.close()
        self.target.close(); await self.target.wait_closed()

    async def exchange(self,data,*,rejected=False):
        reader,writer=await asyncio.open_connection('127.0.0.1',self.port)
        header=(b'\0'+uuid.UUID(self.row['client']['uuid']).bytes+b'\0\1'+
            self.target_port.to_bytes(2,'big')+b'\1\x7f\0\0\1')
        try:
            writer.write(header+data); await writer.drain()
            if rejected:
                result=await asyncio.wait_for(reader.read(),3)
                self.assertIn(result,(b'',b'\0\0'))
                return b''
            result=await asyncio.wait_for(reader.readexactly(2),3)
            self.assertEqual(result,b'\0\0')
            return await asyncio.wait_for(reader.read(),3)
        finally:
            writer.close()
            try: await writer.wait_closed()
            except OSError: pass

    async def test_real_bidirectional_total_and_reopen_do_not_refund(self):
        receipt=await self.runtime.install(self.row)
        self.assertEqual(receipt['state'],'local_runtime_active')
        self.assertFalse(receipt['ready_for_execution'])
        with sqlite3.connect(self.path) as db:
            installation = db.execute('SELECT fact,first_installed_at FROM local_grant_installations').fetchall()
        import json
        fact = json.loads(installation[0][0])
        self.assertEqual(fact['runtime_digest'], receipt['runtime_digest'])
        self.assertEqual(fact['digest'], receipt['digest'])
        self.assertEqual(fact['protocol'], 'vless')
        self.assertEqual(await self.exchange(b'abcd'),b'ab')
        self.assertEqual(self.received,b'abcd')
        evidence=self.runtime.grant_quota.snapshot()
        self.assertEqual(evidence['successful_upload_bytes'],4)
        self.assertEqual(evidence['successful_download_bytes'],2)
        self.assertEqual(evidence['remaining_bytes'],0)
        await self.runtime.close()
        self.runtime=self.make_runtime()
        await self.runtime.install(self.row)
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('SELECT fact,first_installed_at FROM local_grant_installations').fetchall(), installation)
        self.assertEqual(await self.exchange(b'xx',rejected=True),b'')
        self.assertEqual(self.received,b'abcd')
        self.assertEqual(self.runtime.grant_quota.snapshot()['remaining_bytes'],0)

    async def test_same_install_idempotent_and_plain_apply_cannot_bypass(self):
        first=await self.runtime.install(self.row)
        pid=self.runtime.core.process.pid
        self.assertEqual(await self.runtime.install(self.row),first)
        self.assertEqual(self.runtime.core.process.pid,pid)
        with self.assertRaises(ValueError): await self.runtime.apply(self.runtime.snapshot)
        self.assertEqual(self.runtime.core.process.pid,pid)

    async def test_changed_policy_fails_closed_without_erasing_balance(self):
        await self.runtime.install(self.row)
        other=copy.deepcopy(self.row); other['reserved_bytes']+=1; self.sign(other)
        with self.assertRaises(ValueError): await self.runtime.install(other)
        self.assertFalse(self.runtime.listeners)
        self.assertIsNone(self.runtime.snapshot)
        self.assertEqual(self.runtime.grant_quota.snapshot()['remaining_bytes'],6)

    async def test_storage_failure_closes_previously_running_listener(self):
        await self.runtime.install(self.row)
        with sqlite3.connect(self.runtime.grant_path) as db:
            db.execute("CREATE TRIGGER disk BEFORE INSERT ON allocation_quota BEGIN SELECT RAISE(ABORT,'disk'); END")
        with self.assertRaises(OSError): await self.runtime.install(self.row)
        self.assertFalse(self.runtime.listeners)
        self.assertIsNone(self.runtime.snapshot)

    async def test_expiry_after_core_startup_returns_no_success(self):
        original=self.runtime._apply
        async def crossing(snapshot):
            await original(snapshot)
            self.clock.return_value=self.row['valid_until']
        import time
        self.clock=unittest.mock.Mock(return_value=time.time())
        with patch('grant_agent.time.time',self.clock),patch.object(self.runtime,'_apply',crossing):
            with self.assertRaises(ValueError): await self.runtime.install(self.row)
        self.assertFalse(self.runtime.listeners)
        self.assertIsNone(self.runtime.snapshot)

    async def test_write_guard_rejects_clock_rollback_and_expiry(self):
        await self.runtime.install(self.row)
        for instant in (self.row['issued_at']-1,self.row['valid_until']):
            with patch('grant_runtime.time.time',return_value=instant):
                with self.assertRaises(OSError): self.runtime._payload_guard()
        self.assertEqual(self.runtime.grant_quota.snapshot()['remaining_bytes'],6)

    async def test_installation_record_failure_closes_live_core(self):
        def fail(*args):
            self.assertIsNotNone(self.runtime.core.process)
            self.assertIsNone(self.runtime.core.process.returncode)
            raise sqlite3.OperationalError('disk')
        with patch('grant_runtime.record_installation', side_effect=fail):
            with self.assertRaises(sqlite3.OperationalError):
                await self.runtime.install(self.row)
        self.assertIsNone(self.runtime.core.process)
        self.assertFalse(self.runtime.listeners)
        self.assertEqual(self.runtime.grant_quota.snapshot()['remaining_bytes'], 6)

    async def test_expiry_after_installation_commit_returns_no_active_receipt(self):
        from grant_installation import record
        import time
        clock = unittest.mock.Mock(return_value=time.time())
        def crossing(*args):
            fact = record(*args)
            clock.return_value = self.row['valid_until']
            return fact
        with patch('grant_agent.time.time', clock), patch('grant_runtime.record_installation', crossing):
            with self.assertRaises(ValueError): await self.runtime.install(self.row)
        self.assertIsNone(self.runtime.core.process)
        self.assertFalse(self.runtime.listeners)
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM local_grant_installations').fetchone()[0], 1)

    async def test_process_exit_during_installation_storage_returns_no_success(self):
        original = self.runtime._storage
        from grant_installation import record
        async def crossing(fn, *args):
            result = await original(fn, *args)
            if fn is record:
                self.runtime.core.process.terminate()
                await self.runtime.core.process.wait()
            return result
        with patch.object(self.runtime, '_storage', crossing):
            with self.assertRaises(OSError): await self.runtime.install(self.row)
        self.assertIsNone(self.runtime.core.process)
        self.assertFalse(self.runtime.listeners)
