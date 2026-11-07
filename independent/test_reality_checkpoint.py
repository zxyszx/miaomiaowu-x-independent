"""Encrypted payload checkpoints survive runtime replacement under their own proof."""
import asyncio
from http.server import ThreadingHTTPServer
import shutil
import sqlite3
import ssl
import threading
import unittest
import uuid
from unittest import mock

from execution_agent import cycle
from execution_epochs import ExecutionEpochs
from reality_runtime import RealityRuntime
from server import Handler
from usage_outbox import UsageOutbox
import test_reality
from test_xray import BINARY


@unittest.skipUnless(BINARY.is_file() and shutil.which('openssl') and ssl.HAS_TLSv1_3,
                     'Real Xray, OpenSSL CLI and Python TLS 1.3 required')
class RealityCheckpointTests(unittest.IsolatedAsyncioTestCase):
    echo=test_reality.RealityRealTests.echo
    client=test_reality.RealityRealTests.client
    exchange=test_reality.RealityRealTests.exchange

    async def asyncSetUp(self):
        await test_reality.RealityRealTests.asyncSetUp(self)
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        self.server.registry=self.registry
        self.server.token='fixture-only'
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True)
        self.thread.start()
        self.base=f'http://127.0.0.1:{self.server.server_port}'
        self.state=self.root/'agent-state'
        self.session=str(uuid.uuid4())
        self.epochs=ExecutionEpochs(self.base,self.token,self.state,self.runtime,self.session)
        await cycle(self.base,self.token,self.state,self.runtime,1,self.session,self.epochs)
        self.sid=self.runtime.usage_snapshot()['server_id']

    async def asyncTearDown(self):
        try:
            await test_reality.RealityRealTests.asyncTearDown(self)
        finally:
            await asyncio.to_thread(self.server.shutdown)
            self.server.server_close()
            self.thread.join(timeout=2)

    def report(self,session):
        # Open a new SQLite handle: do not rely on the live meter or worker box.
        return UsageOutbox(self.state/'usage-outbox.sqlite',self.sid).next_report(session)

    async def test_encrypted_write_recovery_retains_old_proof_and_counts_new_epoch_once(self):
        from agent import request
        self.assertEqual(await self.exchange(payload=b'old'),b'old')
        old=self.report(self.session)
        self.assertEqual(old['session_id'],self.session)
        self.assertEqual(old['items'],[dict(node_id=self.nid,user_id=self.uid,
            credential=self.credential,upload_bytes=3,download_bytes=3)])
        # No capture/finish before replacing the runtime and delivery worker.
        await self.runtime.close()
        self.runtime=RealityRuntime(BINARY,self.root/'replacement',
            local_test_target=('127.0.0.1',self.target_port),
            reality_test_target=('127.0.0.1',self.decoy.sockets[0].getsockname()[1]))
        new=str(uuid.uuid4())
        recovered=ExecutionEpochs(self.base,self.token,self.state,self.runtime,new)
        await cycle(self.base,self.token,self.state,self.runtime,2,new,recovered)
        self.assertEqual(await self.exchange(payload=b'fresh'),b'fresh')
        self.assertEqual(self.report(self.session),old)
        self.assertEqual(self.report(new)['items'][0]['upload_bytes'],5)
        self.assertEqual(self.runtime.usage_snapshot()['items'][0]['download_bytes'],5)
        await recovered.delivery.step()
        await recovered.delivery.step()
        # A lost acknowledgement replay must not charge the old epoch again.
        retry=await asyncio.to_thread(request,self.base,self.token,'/agent/usage/settle',old)
        self.assertTrue(retry['duplicate'])
        from usage_ledger import UsageLedger
        total=UsageLedger(self.registry).user(self.uid)
        self.assertEqual((total['upload_bytes'],total['download_bytes']),(8,8))
        with self.registry.connect() as db:
            rows=db.execute('SELECT session_id,sum(upload_bytes),sum(download_bytes) '
                            'FROM usage_deltas GROUP BY session_id').fetchall()
        self.assertEqual({tuple(row) for row in rows},{(self.session,3,3),(new,5,5)})
        self.assertEqual(recovered.delivery.box.outstanding_sessions(),[])

    async def test_encrypted_storage_failure_fences_further_writes_and_preserves_tail(self):
        self.assertEqual(await self.exchange(payload=b'ok'),b'ok')
        box=self.epochs.delivery.box
        with mock.patch.object(box,'capture',side_effect=sqlite3.OperationalError('fixture full')):
            reader,writer=await self.client()
            try:
                writer.write(b'failure'); await writer.drain()
                try: result=await asyncio.wait_for(reader.read(),5)
                except ConnectionResetError: result=b''
                self.assertEqual(result,b'')
                self.assertTrue(self.runtime.paced.meter.failed)
                before=self.runtime.usage_snapshot()
                # A second encrypted connection cannot forward more payload.
                with self.assertRaises(asyncio.IncompleteReadError) as denied:
                    await self.exchange(payload=b'blocked')
                self.assertEqual(denied.exception.partial,b'')
                self.assertEqual(self.runtime.usage_snapshot(),before)
            finally:
                writer.close()
                try: await writer.wait_closed()
                except OSError: pass
        # First write reached transport before the failure, so it is retained
        # in memory for final capture, never invented as a successful download.
        item=before['items'][0]
        self.assertEqual((item['upload_bytes'],item['download_bytes']),(9,2))
        await self.runtime.close()
        await self.epochs.delivery.capture()
        tail=self.report(self.session)['items'][0]
        self.assertEqual((tail['upload_bytes'],tail['download_bytes']),(9,2))
        with self.assertRaises(OSError):
            await self.runtime.apply(self.registry.configuration(self.token))
