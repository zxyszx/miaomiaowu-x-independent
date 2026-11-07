"""Real VLESS lifecycle races against durable payload storage."""
import asyncio
import copy
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock

from node_config import digest
import test_payload_meter
import test_xray


@unittest.skipUnless(test_xray.BINARY.is_file(),'Set XRAY_BINARY for real lifecycle tests')
class RuntimeStorageTests(unittest.IsolatedAsyncioTestCase):
    setUp=test_payload_meter.PayloadRealTests.setUp
    tearDown=test_payload_meter.PayloadRealTests.tearDown
    asyncSetUp=test_payload_meter.PayloadRealTests.asyncSetUp
    asyncTearDown=test_payload_meter.PayloadRealTests.asyncTearDown
    request=test_payload_meter.PayloadRealTests.request
    open=test_payload_meter.PayloadRealTests.open
    close_writer=test_payload_meter.PayloadRealTests.close_writer

    def blocked_intent(self):
        entered=threading.Event(); release=threading.Event(); committed=[]
        self.addCleanup(release.set)
        def intent(snapshot):
            entered.set()
            if not release.wait(5): raise OSError('fixture storage timeout')
            committed.append(snapshot)
        self.runtime.set_usage_write_intent(intent)
        return entered,release,committed

    async def test_close_drains_blocked_storage_without_holding_meter_lock(self):
        await self.runtime.apply(self.server.registry.configuration(self.token))
        entered,release,committed=self.blocked_intent()
        reader,_=await self.open()
        closing=None
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            closing=asyncio.create_task(self.runtime.close())
            await asyncio.sleep(.03)
            self.assertFalse(closing.done())
            self.assertTrue(self.runtime.updating)
            self.assertEqual(self.runtime.usage_snapshot()['items'],[])
            release.set()
            await asyncio.wait_for(closing,2)
            # Only the protocol response header may precede payload admission.
            self.assertIn(await asyncio.wait_for(reader.read(),2),(b'',b'\0\0'))
            self.assertEqual(len(committed),1)
            self.assertEqual(self.runtime.usage_snapshot()['items'],[])
            self.assertTrue(self.runtime.meter.failed)
            self.assertFalse(self.runtime.meter.io_lock.locked())
            self.assertFalse(self.runtime.tasks)
        finally:
            release.set()
            if closing: await asyncio.gather(closing,return_exceptions=True)

    async def test_expiry_during_storage_wait_rechecks_before_real_transport(self):
        now=time.time(); clock=mock.Mock(return_value=now)
        snapshot=copy.deepcopy(self.server.registry.configuration(self.token))
        snapshot['valid_until']=now+60
        snapshot['digest']=digest({k:v for k,v in snapshot.items() if k!='digest'})
        with mock.patch('vless_runtime.time',SimpleNamespace(time=clock)):
            await self.runtime.apply(snapshot)
            entered,release,committed=self.blocked_intent()
            reader,_=await self.open()
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait,2))
                clock.return_value=now+61
                release.set()
                self.assertIn(await asyncio.wait_for(reader.read(),2),(b'',b'\0\0'))
                self.assertEqual(len(committed),1)
                self.assertEqual(self.runtime.usage_snapshot()['items'],[])
                self.assertTrue(self.runtime.meter.failed)
            finally: release.set()

    async def test_configuration_bind_waits_for_payload_and_close_still_completes(self):
        snapshot=self.server.registry.configuration(self.token)
        await self.runtime.apply(snapshot)
        entered,release,_=self.blocked_intent()
        reader,_=await self.open()
        applying=None; closing=None
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            applying=asyncio.create_task(self.runtime.apply(snapshot))
            await asyncio.sleep(.02)
            self.assertFalse(applying.done())
            closing=asyncio.create_task(self.runtime.close())
            await asyncio.sleep(.02)
            release.set()
            await asyncio.wait_for(asyncio.gather(applying,closing),3)
            self.assertFalse(self.runtime.tasks)
            self.assertFalse(self.runtime.meter.io_lock.locked())
            self.assertIsNone(self.runtime.snapshot)
        finally:
            release.set()
            await asyncio.gather(*(t for t in (applying,closing) if t),return_exceptions=True)
