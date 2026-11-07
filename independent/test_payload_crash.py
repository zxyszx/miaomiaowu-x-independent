"""SIGKILL boundaries around real socket writes and FULL SQLite checkpoints.

Quota reservation and TCP delivery cannot be one atomic transaction. These
checks expose the uncommitted accounting gap instead of treating it as usage.
"""
import os
from pathlib import Path
import select
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import uuid

from payload_meter import PayloadMeter, MeteredWriter, QuotaExceeded
from quota_store import QuotaStore, PersistentCycleQuota
from usage_outbox import UsageOutbox


CHILD=r'''
from contextlib import contextmanager
import os,signal,socket,sys
from pathlib import Path
from payload_meter import PayloadMeter,MeteredWriter
from quota_store import QuotaStore,PersistentCycleQuota
from usage_outbox import UsageOutbox
state,port,phase,session,credential=sys.argv[1:]
state=Path(state)
box=UsageOutbox(state/'outbox.sqlite',1)
meter=PayloadMeter(); meter.bind(1)
quota=PersistentCycleQuota(QuotaStore(state/'quota.sqlite',1),(2,'period')).reconcile(10,100)
connection=socket.create_connection(('127.0.0.1',int(port)),timeout=5)
armed=False
delivered=False

def stop():
    print('READY',flush=True)
    os.kill(os.getpid(),signal.SIGSTOP)
    raise AssertionError('parent must kill stopped child')

class Destination:
    def write(self,data):
        global delivered
        if armed and phase=='before_write': stop()
        connection.sendall(data)
        if armed: delivered=True

def checkpoint(snapshot):
    if armed and phase=='before_checkpoint': stop()
    box.capture(session,snapshot)

meter.set_incremental_checkpoint(checkpoint)
meter.set_write_intent(lambda snapshot:box.prepare_write(session,snapshot['items'][0]))
writer=MeteredWriter(Destination(),meter,(1,2,credential),0,quota)
writer.write(b'hello')
armed=True
if phase=='before_commit':
    original=box.connect
    @contextmanager
    def connect(*args,**kwargs):
        with original(*args,**kwargs) as db:
            db.set_trace_callback(lambda statement:stop() if statement=='COMMIT' and delivered else None)
            yield db
    box.connect=connect
writer.write(b'abcd')
if phase=='after_commit': stop()
raise AssertionError('requested crash phase was not reached')
'''


@unittest.skipUnless(os.name=='posix','requires SIGSTOP/SIGKILL')
class PayloadCrashTests(unittest.TestCase):
    def crash(self,phase,expected_wire,expected_committed):
        with tempfile.TemporaryDirectory() as folder:
            state=Path(folder); session=str(uuid.uuid4()); credential=str(uuid.uuid4())
            with socket.socket() as listener:
                listener.bind(('127.0.0.1',0)); listener.listen(); listener.settimeout(10)
                child=subprocess.Popen([sys.executable,'-c',CHILD,str(state),
                    str(listener.getsockname()[1]),phase,session,credential],
                    cwd=Path(__file__).resolve().parent,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                try:
                    connection,_=listener.accept()
                    with connection:
                        connection.settimeout(5)
                        ready,_,_=select.select([child.stdout],[],[],10)
                        self.assertTrue(ready,'child did not reach crash boundary')
                        self.assertEqual(child.stdout.readline(),b'READY\n')
                        child.kill(); output,error=child.communicate(timeout=5)
                        self.assertEqual(child.returncode,-signal.SIGKILL)
                        self.assertEqual((output,error),(b'',b''))
                        wire=b''
                        while chunk:=connection.recv(65536): wire+=chunk
                    self.assertEqual(wire,expected_wire)
                finally:
                    if child.poll() is None: child.kill()
                    child.communicate(timeout=5)
            for name in ('quota.sqlite','outbox.sqlite'):
                with sqlite3.connect(state/name) as db:
                    self.assertEqual(db.execute('PRAGMA integrity_check').fetchone()[0],'ok')
            # A stale zero baseline cannot give back the reserved four bytes.
            quota=PersistentCycleQuota(QuotaStore(state/'quota.sqlite',1),(2,'period')).reconcile(10,100)
            self.assertEqual((quota.used[0],quota.remaining[0]),(9,1))
            box=UsageOutbox(state/'outbox.sqlite',1)
            old=box.next_report(session)
            self.assertEqual(old['items'][0]['upload_bytes'],expected_committed)
            self.assertEqual(old['items'][0]['download_bytes'],0)
            uncertain=box.uncertain_writes()
            if phase=='after_commit': self.assertEqual(uncertain,[])
            else:
                self.assertEqual(len(uncertain),1)
                self.assertEqual((uncertain[0]['committed_upload'],uncertain[0]['possible_upload']),(5,9))
                self.assertEqual(uncertain[0]['session'],session)
            # Freeze/reopen recovery returns the same old-session report.
            self.assertEqual(UsageOutbox(state/'outbox.sqlite',1).next_report(session),old)
            new_session=str(uuid.uuid4()); meter=PayloadMeter(); meter.bind(1)
            meter.set_incremental_checkpoint(lambda snapshot:box.capture(new_session,snapshot))
            class Destination:
                def __init__(self): self.blocks=[]
                def write(self,data): self.blocks.append(data)
            target=Destination()
            writer=MeteredWriter(target,meter,(1,2,credential),0,quota)
            with self.assertRaises(QuotaExceeded): writer.write(b'hello')
            self.assertEqual(target.blocks,[b'h'])
            self.assertEqual(quota.remaining[0],0)
            self.assertEqual(box.next_report(new_session)['items'][0]['upload_bytes'],1)
            self.assertEqual(box.next_report(session),old)

    def test_kill_after_reservation_before_write_keeps_budget_but_no_phantom_usage(self):
        self.crash('before_write',b'hello',5)

    def test_kill_after_write_before_checkpoint_exposes_uncommitted_gap(self):
        self.crash('before_checkpoint',b'helloabcd',5)

    def test_kill_before_checkpoint_commit_rolls_back_partial_transaction(self):
        self.crash('before_commit',b'helloabcd',5)

    def test_kill_after_checkpoint_commit_recovers_exact_old_session(self):
        self.crash('after_commit',b'helloabcd',9)
