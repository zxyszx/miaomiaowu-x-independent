import asyncio
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest

from loop_probe import LoopProbe


class LoopProbeTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_sqlite_busy_wait_is_visible_without_locals(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'sample.sqlite'
            with sqlite3.connect(path) as db: db.execute('CREATE TABLE t(x)')
            held=threading.Event(); release=threading.Event()
            def hold():
                with sqlite3.connect(path) as db:
                    db.execute('BEGIN IMMEDIATE'); held.set()
                    release.wait(2)
            owner=threading.Thread(target=hold); owner.start()
            self.assertTrue(await asyncio.to_thread(held.wait,1))
            timer=threading.Timer(0.16,release.set)
            try:
                with LoopProbe(interval=0.005) as probe:
                    timer.start()
                    secret='must-not-appear-in-diagnostic'
                    start=time.monotonic()
                    with sqlite3.connect(path,timeout=1) as db:
                        db.execute('INSERT INTO t VALUES(1)')
                    samples=probe.between(start,time.monotonic())
                    self.assertTrue(any(any(loc[0]=='test_loop_probe.py' for loc in s['stack']) for s in samples))
                    self.assertNotIn(secret,str(samples))
            finally:
                release.set(); timer.cancel(); owner.join(2)
            self.assertFalse(owner.is_alive())
        self.assertFalse(probe.worker.is_alive())

    async def test_retention_is_bounded_and_exit_stops_sampling(self):
        with LoopProbe(interval=0.001,capacity=3) as probe:
            await asyncio.sleep(0.03)
            self.assertLessEqual(len(probe.samples),3)
            self.assertTrue(probe.samples)
        self.assertFalse(probe.worker.is_alive())
