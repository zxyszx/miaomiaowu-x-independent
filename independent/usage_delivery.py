"""Optional, serial usage delivery; never owns the execution configuration lease."""
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import sqlite3
import sys
import time
import urllib.error
from agent import request
from core import Error
from usage_outbox import UsageOutbox
from payload_meter import _stored


INTERVAL=5
FAILURES=(OSError,ValueError,TypeError,KeyError,sqlite3.Error,Error)


async def drained(function,*args):
    # Repeated cancellation cannot leave urllib/SQLite running after its
    # capture/epoch lock is released (including maintenance after capture).
    return await _stored(function,*args)


class UsageDelivery:
    def __init__(self,base,token,state,runtime,session):
        self.base=base; self.token=token; self.state=Path(state)
        self.runtime=runtime; self.session=session; self.box=None
        self.after=None
        self.closed_turn=False
        self.fragment_turn=True
        self.baseline={}
        # Only a successful SQLite commit advances this process-local cache.
        # Values remain cumulative within the delivery epoch, never deltas.
        self.checkpoint_totals={}
        self.capture_lock=asyncio.Lock()
        self.unconfirmed_session=None
        setter=getattr(type(runtime),'set_incremental_usage_checkpoint',None)
        if not callable(setter): setter=getattr(type(runtime),'set_usage_checkpoint',None)
        self.write_checkpoints=callable(setter)
        if self.write_checkpoints: setter(runtime,self._checkpoint)
        intent_setter=getattr(type(runtime),'set_usage_write_intent',None)
        if self.write_checkpoints and callable(intent_setter):
            intent_setter(runtime,self._prepare_write)

    async def capture(self):
        async with self.capture_lock: await self._capture()

    @asynccontextmanager
    async def _storage_boundary(self):
        # Opt in only after a runtime coordinates its writes AND lifecycle on
        # this lock. Legacy synchronous runtimes retain their existing path.
        boundary=getattr(type(self.runtime),'usage_storage_boundary',None)
        if callable(boundary):
            async with boundary(self.runtime): yield True
        else: yield False

    def _snapshot(self,snapshot=None):
        if snapshot is None: snapshot=self.runtime.usage_snapshot()
        if snapshot['server_id'] is None: return None
        snapshot={'server_id':snapshot['server_id'],'items':[dict(item) for item in snapshot['items']]}
        for item in snapshot['items']:
            key=(item['node_id'],item['user_id'],item['credential'])
            for field,base in zip(('upload_bytes','download_bytes'),self.baseline.get(key,(0,0))):
                if item[field]<base: raise ValueError('执行计数回退，必须启动新的执行进程')
                item[field]-=base
        return snapshot

    def _prepare_write(self,snapshot):
        snapshot=self._snapshot(snapshot)
        if snapshot is None: raise ValueError('载荷写入前尚未绑定服务器')
        if self.box is None:
            self.box=UsageOutbox(self.state/'usage-outbox.sqlite',snapshot['server_id'])
        if self.box.server_id!=snapshot['server_id']:
            raise ValueError('流量队列服务器身份不一致')
        for item in snapshot['items']: self.box.prepare_write(self.session,item)

    def _checkpoint(self,snapshot=None):
        # Called synchronously immediately after transport.write(), before the
        # event loop may accept another payload block. Never restore old totals
        # into a fresh process/session: the outbox retains their original proof.
        # Optimized runtimes supply only the changed identity's cumulative
        # snapshot; no-argument calls intentionally capture every identity.
        snapshot=self._snapshot(snapshot)
        if snapshot is None: return
        if self.box is None:
            self.box=UsageOutbox(self.state/'usage-outbox.sqlite',snapshot['server_id'])
        if self.box.server_id!=snapshot['server_id']:
            raise ValueError('流量队列服务器身份不一致')
        changed=[]; totals={}
        for item in snapshot['items']:
            key=(item['node_id'],item['user_id'],item['credential'])
            if key in totals: raise ValueError('流量快照重复身份')
            value=(item['upload_bytes'],item['download_bytes'])
            totals[key]=value
            if self.checkpoint_totals.get(key)!=value: changed.append(item)
        if snapshot['items'] and not changed: return
        snapshot=dict(snapshot,items=changed)
        self.box.capture(self.session,snapshot)
        self.checkpoint_totals.update(totals)

    async def _capture(self):
        async with self._storage_boundary() as coordinated:
            await self._capture_locked(coordinated)

    async def _capture_locked(self,coordinated):
        if self.write_checkpoints:
            # Do not race a stale snapshot worker against a newer write commit.
            if coordinated: await _stored(self._checkpoint)
            else: self._checkpoint()
            if self.box is None: return
        else:
            snapshot=self._snapshot()
            if snapshot is None: return
            if self.box is None:
                self.box=await drained(UsageOutbox,self.state/'usage-outbox.sqlite',snapshot['server_id'])
            await drained(self.box.capture,self.session,snapshot)
        try:
            await drained(self.box.retire_confirmed,self.session)
        except FAILURES:
            # Maintenance cannot suppress a successfully captured batch.
            print('Usage cleanup deferred; delivery continues',file=sys.stderr,flush=True)

    async def rotate(self,session):
        """After listeners close, durably capture the old epoch before rebasing.

        The runtime keeps its cumulative meter and cycle quota high-water marks.
        Only delivery counters are segmented. No network request is required;
        an in-flight frozen report retains its own original session identity.
        """
        UsageOutbox._session_value(session)
        async with self.capture_lock:
            if session==self.session: raise ValueError('周期轮换必须使用新的执行会话')
            async with self._storage_boundary() as coordinated:
                await self._capture_locked(coordinated) # Failure preserves old epoch.
                snapshot=self.runtime.usage_snapshot()
                baseline={(i['node_id'],i['user_id'],i['credential']):
                          (i['upload_bytes'],i['download_bytes']) for i in snapshot['items']}
                if self.box is not None:
                    await _stored(self.box.close_epoch,self.session)
                self.baseline=baseline
                self.session=session
                self.checkpoint_totals={}

    async def step(self):
        await self.capture()
        if self.box is None: return
        sessions=await drained(self.box.ready_sessions,time.time())
        # Capture remains durable while an applied receipt is in flight, but
        # never submit a new epoch before its execution proof is confirmed.
        sessions=[s for s in sessions if s!=self.unconfirmed_session]
        # Share evidence slots with ordinary final delivery and discovery;
        # each fragment attempt returns before any other network operation.
        if self.closed_turn or not sessions:
            from fragment_delivery import FragmentDelivery
            fragments=await drained(FragmentDelivery,self.box)
            job=await drained(fragments.ready,time.time(),(self.session,self.unconfirmed_session))
            if job is not None and self.fragment_turn:
                self.fragment_turn=False
                self.closed_turn=False
                try:
                    reply=await drained(request,self.base,self.token,'/agent/usage/closed/fragment',job)
                    await drained(fragments.acknowledge,job,reply)
                except FAILURES as error:
                    revoked=isinstance(error,urllib.error.HTTPError) and error.code in (401,403)
                    await drained(lambda:fragments.defer(job,time.time(),revoked=revoked))
                    print('Final usage fragment unconfirmed; original evidence retained',file=sys.stderr,flush=True)
                return
            self.fragment_turn=True
        from closed_delivery import ClosedDelivery
        closed=ClosedDelivery(self.box)
        final=await drained(closed.ready,time.time())
        if final is not None and (self.closed_turn or not sessions):
            self.closed_turn=False
            try:
                receipt=await drained(request,self.base,self.token,'/agent/usage/closed',final)
                await drained(closed.acknowledge,final,receipt)
            except FAILURES as error:
                revoked=isinstance(error,urllib.error.HTTPError) and error.code==403
                await drained(lambda:closed.defer(final,time.time(),revoked=revoked))
                print('Final usage evidence unconfirmed; checkpoint retained',file=sys.stderr,flush=True)
            return
        if self.closed_turn or not sessions:
            from closed_discovery import ClosedDiscovery
            discovery=ClosedDiscovery(self.box)
            job=await drained(discovery.ready,time.time(),(self.session,self.unconfirmed_session))
            if job is not None:
                self.closed_turn=False
                try:
                    reply=await drained(request,self.base,self.token,'/agent/usage/drains',job['request'])
                    await drained(discovery.accept,job,reply,time.time())
                except FAILURES as error:
                    revoked=isinstance(error,urllib.error.HTTPError) and error.code in (401,403)
                    await drained(lambda:discovery.defer(job,time.time(),revoked=revoked))
                    print('Drain discovery unconfirmed; closed evidence retained',file=sys.stderr,flush=True)
                return
        if not sessions: return
        self.closed_turn=True
        # Bound each round to one HTTP attempt, rotating through ready sessions.
        # Bad old sessions cannot multiply the capture delay or starve new ones.
        session=next((s for s in sessions if self.after is None or s>self.after),sessions[0])
        self.after=session
        report=await drained(self.box.next_report,session)
        if report is None: return
        try:
            receipt=await drained(request,self.base,self.token,'/agent/usage/settle',report)
            await drained(self.box.acknowledge,report,receipt)
        except FAILURES as error:
            if isinstance(error,urllib.error.HTTPError) and error.code==403:
                await drained(self.box.isolate,report,time.time())
                print('Usage batch isolated; unconfirmed counters retained',file=sys.stderr,flush=True)
                return
            reason=('rejected' if isinstance(error,urllib.error.HTTPError) else
                    'network' if isinstance(error,OSError) else 'receipt')
            await drained(self.box.defer,report,reason,time.time())
            print('Usage batch unconfirmed; preserved with bounded retry',file=sys.stderr,flush=True)

    async def run(self):
        while True:
            try: await self.step()
            except FAILURES:
                print('Usage capture failed; execution sync continues',file=sys.stderr,flush=True)
            await asyncio.sleep(INTERVAL)

    async def finish(self,task):
        if task is not None:
            task.cancel()
            try: await task
            except asyncio.CancelledError: pass
        # Runtime listeners have already closed: capture their final counters,
        # without making shutdown depend on another network request.
        try:
            async with self.capture_lock:
                async with self._storage_boundary() as coordinated:
                    await self._capture_locked(coordinated)
                    if self.box is not None:
                        await _stored(self.box.close_epoch,self.session)
        except FAILURES:
            print('Final usage capture failed; uncaptured bytes are not durable',file=sys.stderr,flush=True)
