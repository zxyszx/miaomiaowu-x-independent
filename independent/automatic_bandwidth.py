"""Per-executor download threshold policy; never changes administrator rules."""
import asyncio
import json
import math
import os
from pathlib import Path
import tempfile
import time

from bandwidth import BytePacer, CombinedPacer


_PENDING_UNSET = object()


class DownloadController:
    def __init__(self, rule, save, saved=None, async_save=None):
        self.rule=dict(rule)
        self.save=save
        self.async_save=async_save
        self.pending_save=_PENDING_UNSET
        self.persist_lock=asyncio.Lock()
        self.start=time.monotonic()
        self.count=0
        self.hot=0
        self.until=0
        self.cap=BytePacer(rule['download_bps'])
        if saved is not None and not isinstance(saved,dict):
            raise ValueError('自动限速恢复状态无效')
        if saved and saved.get('rule')==rule:
            end,recorded=saved.get('until'),saved.get('recorded_at')
            if (type(end) not in (int,float) or type(recorded) not in (int,float)
                    or not math.isfinite(end) or not math.isfinite(recorded)
                    or not 0<=end-recorded<=rule['duration_seconds']):
                raise ValueError('自动限速恢复状态无效')
            # Clock rollback cannot increase the remaining duration beyond policy.
            self.until=self.start+min(rule['duration_seconds'],max(0,end-time.time()))

    def sample(self, defer_persist=False):
        now=time.monotonic()
        if self.until:
            if now<self.until: return self.cap
            self._save(None, defer_persist)
            self.until=0
            self.start=now; self.count=0; self.hot=0
            return None
        elapsed=now-self.start
        if elapsed>=1:
            # An idle or stalled executor must not convert a burst into sustained use.
            self.hot=(self.hot+elapsed if elapsed<=2.5 and self.count/elapsed>=self.rule['threshold_bps'] else 0)
            self.start=now; self.count=0
            if self.hot>=self.rule['sustain_seconds']:
                wall=time.time()
                self._save({'rule':self.rule,'until':wall+self.rule['duration_seconds'],'recorded_at':wall}, defer_persist)
                self.until=now+self.rule['duration_seconds']
                self.hot=0
                return self.cap
        return None

    def _save(self, state, defer_persist=False):
        if self.async_save is None or not defer_persist:
            self.save(state)
        else:
            # The runtime path persists outside the event loop. Keep only the
            # latest transition; write() awaits it before sending bytes.
            self.pending_save=state

    async def persist_pending(self):
        async with self.persist_lock:
            while self.pending_save is not _PENDING_UNSET:
                state=self.pending_save
                # Another connection may sample expiry while this save awaits
                # storage. Never erase that newer transition, including on
                # failure/cancellation; drain it before any waiter sends bytes.
                await self.async_save(state)
                if self.pending_save is state:
                    self.pending_save=_PENDING_UNSET

    def record(self, count):
        self.count+=count

    async def write(self, destination, data, user, server, deadline):
        # All nodes/connections for a user share this controller and cap lock.
        # Sample between bounded blocks; do not pace protocol headers.
        offset=0
        while offset<len(data):
            cap=self.sample(defer_persist=True)
            await self.persist_pending()
            rates=[pacer.rate for pacer in (user,server,cap) if pacer is not None and pacer.rate is not None]
            quantum=16384 if not rates else max(1,min(16384,min(rates)//20))
            block=data[offset:offset+quantum]
            await CombinedPacer(user,server,*([] if cap is None else [cap]),deadline=deadline).write(destination,block)
            self.record(len(block))
            offset+=len(block)


class DownloadStore:
    def __init__(self, path, async_save=None):
        self.path=Path(path)
        self.controllers={}
        self.saved=None
        self.async_save=async_save

    def configure(self, server_id, rules):
        if self.saved is None:
            try: self.saved=json.loads(self.path.read_text())
            except FileNotFoundError: self.saved={'server_id':server_id,'users':{}}
            if (not isinstance(self.saved,dict) or self.saved.get('server_id')!=server_id
                    or not isinstance(self.saved.get('users'),dict)):
                raise ValueError('自动限速状态与服务器身份不一致')
        if self.saved['server_id']!=server_id: raise ValueError('自动限速服务器身份变化')
        current={}
        for uid,rule in rules.items():
            old=self.controllers.get(uid)
            current[uid]=(old if old and old.rule==rule else DownloadController(
                rule,lambda state,ident=uid:self.save(ident,state),self.saved['users'].get(str(uid)),
                async_save=(lambda state,ident=uid:self.async_save(ident,state)) if self.async_save else None))
        self.controllers=current
        # Persisted entries for changed/removed policies cannot revive on restart.
        users={str(uid):state for uid in current if (state:=self.saved['users'].get(str(uid))) and state.get('rule')==rules[uid]}
        if users!=self.saved['users']:
            self.saved['users']=users
            self.flush()

    def save(self, uid, state):
        if state is None: self.saved['users'].pop(str(uid),None)
        else: self.saved['users'][str(uid)]=state
        self.flush()  # Trigger is not committed to memory until durable save succeeds.

    def flush(self):
        self.path.parent.mkdir(parents=True,exist_ok=True)
        fd,name=tempfile.mkstemp(dir=self.path.parent,prefix='.auto-bandwidth-')
        try:
            with os.fdopen(fd,'w') as stream:
                json.dump(self.saved,stream,sort_keys=True,allow_nan=False)
                stream.flush(); os.fsync(stream.fileno())
            os.replace(name,self.path)
        finally:
            if os.path.exists(name): os.unlink(name)
