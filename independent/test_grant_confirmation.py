"""Real listener installation is required before explicit confirmation."""
import json
import sqlite3
import unittest
from unittest.mock import patch
from grant_ack import fact_for,receipt
from grant_confirmation import confirm_installed
from test_xray import BINARY
import test_grant_runtime as fixture

@unittest.skipUnless(BINARY.is_file(),'Set XRAY_BINARY for real grant execution')
class GrantConfirmationTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_installed_listener_sends_exact_fact(self):
        await self.runtime.install(self.row)
        expected=fact_for(self.row)
        with patch('agent.request',return_value=receipt(expected)) as send:
            result=await confirm_installed(self.runtime,'http://127.0.0.1:1','test-token')
        self.assertEqual(result,receipt(expected))
        self.assertEqual(send.call_args.args[2:],('/agent/grants/installed',expected))
        self.assertIsNone(self.runtime.core.process.returncode)

    async def test_bad_response_closes_listener_preserves_journal(self):
        await self.runtime.install(self.row)
        with patch('agent.request',return_value=dict(receipt(fact_for(self.row)),extra=True)):
            with self.assertRaisesRegex(ValueError,'响应'):await confirm_installed(self.runtime,'http://127.0.0.1:1','test')
        self.assertTrue(self.runtime.core.process is None or self.runtime.core.process.returncode is not None)
        with sqlite3.connect(self.store.path) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM local_grant_installations').fetchone()[0],1)

    async def test_tampered_journal_never_sends_and_closes(self):
        await self.runtime.install(self.row)
        with sqlite3.connect(self.store.path) as db:db.execute("UPDATE local_grant_installations SET fact='{}'")
        with patch('agent.request') as send:
            with self.assertRaisesRegex(ValueError,'安装记录'):await confirm_installed(self.runtime,'http://127.0.0.1:1','test')
            send.assert_not_called()
        self.assertTrue(self.runtime.core.process is None or self.runtime.core.process.returncode is not None)

    async def test_dead_process_not_recovered_from_historical_journal(self):
        await self.runtime.install(self.row)
        self.runtime.core.process.kill();await self.runtime.core.process.wait()
        with patch('agent.request') as send:
            with self.assertRaises(OSError):await confirm_installed(self.runtime,'http://127.0.0.1:1','test')
            send.assert_not_called()

for name in ('setUp','sign','asyncSetUp','make_runtime','asyncTearDown','exchange'):
    setattr(GrantConfirmationTests,name,getattr(fixture.GrantRuntimeTests,name))
