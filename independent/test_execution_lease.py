"""Lease expiry must stop forwarding while an HTTP/config worker is stalled."""
import asyncio
import contextlib
import threading
import unittest
from unittest import mock

import execution_agent


class Runtime:
    def __init__(self):
        self.closed=asyncio.Event()
        self.active=False
        self.applied=0
    async def apply(self,value): self.active=True; self.applied+=1
    async def close(self): self.active=False; self.closed.set()


class ExecutionLeaseTests(unittest.IsolatedAsyncioTestCase):
    async def test_stalled_cycle_closes_runtime_at_lease_deadline(self):
        runtime=Runtime(); hanging=asyncio.Event(); calls=0
        async def attempt(*args):
            nonlocal calls
            calls+=1
            if calls==1: runtime.active=True; return {'accepted':True}
            hanging.set()
            await asyncio.Event().wait()
        with mock.patch('execution_agent.cycle',side_effect=attempt), \
             mock.patch('execution_agent.LEASE_SECONDS',.12,create=True), \
             mock.patch('execution_agent.SYNC_INTERVAL',.01,create=True):
            task=asyncio.create_task(execution_agent.run({'master':'unused','token':'unused'},'unused',runtime))
            try:
                await asyncio.wait_for(hanging.wait(),.5)
                await asyncio.wait_for(runtime.closed.wait(),.4)
                self.assertFalse(runtime.active)
            finally:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError): await task

    async def test_cancelled_staging_worker_is_drained_before_retry_or_return(self):
        runtime=Runtime(); runtime.active=True
        started=threading.Event(); release=threading.Event(); finished=threading.Event()
        def blocked_sync(*args):
            started.set()
            release.wait(3)
            finished.set()
        with mock.patch('execution_agent.send',return_value={'accepted':True}), \
             mock.patch('execution_agent.sync',side_effect=blocked_sync):
            task=asyncio.create_task(execution_agent.cycle('unused','unused','unused',runtime,1))
            try:
                self.assertTrue(await asyncio.to_thread(started.wait,1))
                task.cancel()
                await asyncio.wait_for(runtime.closed.wait(),.5)
                self.assertFalse(task.done(),'worker must finish before cycle cancellation returns')
                self.assertFalse(runtime.active)
                self.assertEqual(runtime.applied,0)
                release.set()
                with self.assertRaises(asyncio.CancelledError): await asyncio.wait_for(task,1)
                self.assertTrue(finished.is_set())
            finally:
                release.set()
                if not task.done(): task.cancel()
                with contextlib.suppress(asyncio.CancelledError): await task

    async def test_real_listener_disconnects_on_stall_and_recovers_without_worker_overlap(self):
        import json
        import tempfile
        from pathlib import Path
        from forward_runtime import ForwardRuntime
        from test_runtime import free_port, snapshot

        async def echo(reader,writer):
            try:
                while data:=await reader.read(1024):
                    writer.write(data); await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
        upstream=await asyncio.start_server(echo,'127.0.0.1',0)
        port=free_port()
        value=snapshot(port,upstream.sockets[0].getsockname()[1])
        runtime=ForwardRuntime()
        blocked=threading.Event(); release=threading.Event(); completed=threading.Event()
        recovered=asyncio.Event(); calls=0; receipts=0
        def staging(*args):
            nonlocal calls
            calls+=1
            if calls==2:
                blocked.set(); release.wait(3); completed.set()
            elif calls>2:
                self.assertTrue(completed.is_set(),'retry overlapped old staging worker')
        # request() runs in a worker thread, so dispatch its event via the loop.
        loop=asyncio.get_running_loop()
        def acknowledged(*args):
            nonlocal receipts
            receipts+=1
            if receipts>1: loop.call_soon_threadsafe(recovered.set)
            return dict(accepted=True,state='listening',revision=1)
        writer=None; task=None
        try:
            with tempfile.TemporaryDirectory() as state, \
                 mock.patch('execution_agent.begin_session',return_value={}), \
                 mock.patch('execution_agent.send',return_value={}), \
                 mock.patch('execution_agent.sync',side_effect=staging), \
                 mock.patch('execution_agent.request',side_effect=acknowledged), \
                 mock.patch('execution_agent.LEASE_SECONDS',.3), \
                 mock.patch('execution_agent.SYNC_INTERVAL',.02):
                Path(state,'staged-config.json').write_text(json.dumps(value))
                task=asyncio.create_task(execution_agent.run({'master':'unused','token':'unused'},state,runtime))
                self.assertTrue(await asyncio.to_thread(blocked.wait,1))
                reader,writer=await asyncio.open_connection('127.0.0.1',port)
                writer.write(b'before'); await writer.drain()
                self.assertEqual(await asyncio.wait_for(reader.readexactly(6),1),b'before')
                self.assertEqual(await asyncio.wait_for(reader.read(),1),b'')
                with self.assertRaises(OSError): await asyncio.open_connection('127.0.0.1',port)
                self.assertEqual(calls,2)
                self.assertFalse(completed.is_set())
                release.set()
                await asyncio.wait_for(recovered.wait(),1)
                restored_reader,restored_writer=await asyncio.open_connection('127.0.0.1',port)
                try:
                    restored_writer.write(b'after'); await restored_writer.drain()
                    self.assertEqual(await asyncio.wait_for(restored_reader.readexactly(5),1),b'after')
                finally:
                    restored_writer.close(); await restored_writer.wait_closed()
        finally:
            release.set()
            if task:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError): await task
            if writer: writer.close(); await writer.wait_closed()
            await runtime.close()
            upstream.close(); await upstream.wait_closed()
