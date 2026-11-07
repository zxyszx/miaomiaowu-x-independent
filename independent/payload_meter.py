"""Single-event-loop payload counters, measured at successful transport writes.

Counts bytes queued by write(), not TCP acknowledgements or physical wire bytes.
Kept across configuration reload/close. An attached checkpoint persists each
successful write under its execution epoch; fresh processes start fresh counters.
An optional durable pre-write intent preserves uncertainty across hard crashes.
"""
import asyncio


async def _stored(function,*args):
    """Cancellation must not leave a storage worker running past its owner."""
    worker=asyncio.create_task(asyncio.to_thread(function,*args))
    cancelled=False
    while True:
        try:
            result=await asyncio.shield(worker)
            break
        except asyncio.CancelledError:
            cancelled=True
            # Repeated cancellation still must not release the serialization
            # lock while a SQLite transaction can commit in the background.
            if worker.done():
                if not worker.cancelled(): worker.exception()
                raise
        except BaseException:
            if cancelled: raise asyncio.CancelledError()
            raise
    if cancelled: raise asyncio.CancelledError()
    return result


class PayloadMeter:
    def __init__(self):
        self.server_id=None
        self.counters={}
        self.checkpoint=None
        self.incremental_checkpoint=False
        self.failed=False
        self.write_intent=None
        self.io_lock=asyncio.Lock()

    def set_checkpoint(self, checkpoint):
        self.checkpoint=checkpoint
        self.incremental_checkpoint=False
        if self.server_id is not None: self._checkpoint()

    def set_incremental_checkpoint(self, checkpoint):
        self.checkpoint=checkpoint
        self.incremental_checkpoint=True
        if self.server_id is not None: self._checkpoint()

    def set_write_intent(self, callback):
        self.write_intent=callback

    def prepare_write(self,key,direction,count):
        if self.write_intent is None: return
        value=list(self.counters.get(key,(0,0)))
        value[direction]+=count
        snapshot={'server_id':self.server_id,'items':[dict(
            node_id=key[0],user_id=key[1],credential=key[2],
            upload_bytes=value[0],download_bytes=value[1])]}
        try: self.write_intent(snapshot)
        except Exception as error:
            self.failed=True
            raise OSError('载荷写入意图持久化失败，停止转发并检查存储') from error

    def available(self):
        if self.failed: raise OSError('载荷计量持久化失败，停止转发并检查存储')

    def _checkpoint(self, key=None):
        if self.checkpoint is None: return
        try:
            if self.incremental_checkpoint: self.checkpoint(self.snapshot(key))
            else: self.checkpoint()
        except Exception as error:
            self.failed=True
            raise OSError('载荷计量持久化失败，停止转发并检查存储') from error

    def bind(self, server_id):
        if self.server_id is not None and self.server_id!=server_id:
            raise ValueError('流量计量服务器身份变化')
        self.server_id=server_id
        self.available()
        self._checkpoint()

    def record(self, key, direction, count):
        if not count: return
        value=self.counters.setdefault(key,[0,0])
        value[direction]+=count
        self._checkpoint(key)

    def snapshot(self, key=None):
        # A transport write changes exactly one identity. Full snapshots remain
        # available for bind, periodic recovery and execution-epoch rotation.
        entries=sorted(self.counters.items()) if key is None else ((key,self.counters[key]),)
        return {'server_id':self.server_id,'items':[
            dict(node_id=key[0],user_id=key[1],credential=key[2],
                 upload_bytes=value[0],download_bytes=value[1])
            for key,value in entries]}


class QuotaExceeded(Exception):
    """Raised after the final permitted payload bytes have been queued."""


class CycleQuota:
    """Single event-loop byte quota shared by all nodes of one usage cycle."""
    def __init__(self, upload_limit=None, download_limit=None,
                 upload_used=0, download_used=0, *, total_limit=None):
        limits=(upload_limit, download_limit)
        used=(upload_used, download_used)
        if any(x is not None and (type(x) is not int or x < 0) for x in limits+used):
            raise ValueError('周期流量额度无效')
        if any(limit is not None and value > limit for limit, value in zip(limits,used)):
            raise ValueError('周期流量已超过额度')
        if total_limit is not None and (type(total_limit) is not int or total_limit<0):
            raise ValueError('周期合计流量额度无效')
        if total_limit is not None and sum(used)>total_limit:
            raise ValueError('周期流量已超过合计额度')
        self.total_limit=total_limit
        self.used=list(used)
        self.remaining=[None if limit is None else limit-value for limit,value in zip(limits,used)]

    def reconcile(self, upload_limit=None, download_limit=None,
                  upload_used=0, download_used=0, *, total_limit=None):
        """A stale master baseline must never erase locally reserved bytes.

        This is an instance-local high-water mark, not cross-agent settlement.
        A lowered limit may already be exhausted; never make it negative.
        """
        limits=(upload_limit,download_limit); baseline=(upload_used,download_used)
        if any(x is not None and (type(x) is not int or x < 0) for x in limits):
            raise ValueError('周期流量额度无效')
        if any(type(x) is not int or x < 0 for x in baseline):
            raise ValueError('周期流量额度无效')
        if total_limit is not None and (type(total_limit) is not int or total_limit<0):
            raise ValueError('周期合计流量额度无效')
        self.total_limit=total_limit
        self.used=[max(local,remote) for local,remote in zip(self.used,baseline)]
        self.remaining=[None if limit is None else max(0,limit-value)
                        for limit,value in zip(limits,self.used)]
        return self

    @property
    def total_remaining(self):
        return None if self.total_limit is None else max(0,self.total_limit-sum(self.used))

    def reserve(self, direction, count):
        if type(direction) is not int or direction not in (0,1): raise ValueError('流量方向无效')
        if type(count) is not int or count < 0: raise ValueError('流量字节数无效')
        remaining=self.remaining[direction]
        allowed=count if remaining is None else min(remaining,count)
        if self.total_remaining is not None: allowed=min(allowed,self.total_remaining)
        if remaining is not None: self.remaining[direction]-=allowed
        self.used[direction]+=allowed
        return allowed


class MeteredWriter:
    """Wrap only paced application writes, never authentication/bridge headers."""
    def __init__(self, destination, meter, key, direction, quota=None, *, before_write=None):
        self.destination=destination
        self.meter=meter
        self.key=key
        self.direction=direction
        self.quota=quota
        self.before_write=before_write
        # Allocation-scoped adapters must not spend another identity's grant.
        if quota is not None and hasattr(quota, 'validate_binding'):
            quota.validate_binding(meter.server_id, key)

    def write(self, data):
        if self.meter.io_lock.locked():
            raise OSError('异步载荷持久化尚未完成，不能混用同步写入')
        self.meter.available()
        ticket = None
        if self.quota is not None and hasattr(self.quota, 'reserve_write'):
            allowed, ticket = self.quota.reserve_write(self.direction, len(data))
        else:
            allowed=len(data) if self.quota is None else self.quota.reserve(self.direction,len(data))
        if not allowed: raise QuotaExceeded('周期流量额度已耗尽')
        try:
            self.meter.prepare_write(self.key,self.direction,allowed)
            self.destination.write(data[:allowed])
            if ticket is not None: self.quota.complete_write(ticket)
        except Exception:
            # write() can partially queue before raising. Preserve intent as
            # uncertain and fence this process rather than inventing usage.
            if self.meter.write_intent is not None or ticket is not None: self.meter.failed=True
            if ticket is not None: self.quota.abandon_write(ticket)
            raise
        # No await between successful queueing and counting. Cancellation while
        # waiting for drain must not erase a block already handed to transport.
        self.meter.record(self.key,self.direction,allowed)
        if allowed < len(data): raise QuotaExceeded('周期流量额度已耗尽')

    async def awrite(self, data):
        """Await durable stages; transport and counters stay on the loop thread.

        Callers serialize epoch/configuration changes and periodic capture on
        this same meter lock. VLESS and the REALITY pacing bridge use this path.
        Cancellation after an intent keeps uncertainty; it never refunds quota.
        """
        async with self.meter.io_lock:
            self.meter.available()
            ticket=None
            checkpointed=False
            def checkpoint():
                nonlocal checkpointed
                self.meter._checkpoint(self.key)
                checkpointed=True
            try:
                if self.quota is not None and hasattr(self.quota,'reserve_write'):
                    allowed,ticket=await _stored(self.quota.reserve_write,self.direction,len(data))
                else:
                    allowed=(len(data) if self.quota is None else
                             await _stored(self.quota.reserve,self.direction,len(data)))
                if not allowed: raise QuotaExceeded('周期流量额度已耗尽')
                await _stored(self.meter.prepare_write,self.key,self.direction,allowed)
                if self.before_write is not None: self.before_write()
                self.destination.write(data[:allowed])
                if ticket is not None: await _stored(self.quota.complete_write,ticket)
                # No await between successful transport queueing and counting
                # (except the allocation adapter's existing durable completion).
                value=self.meter.counters.setdefault(self.key,[0,0])
                value[self.direction]+=allowed
                await _stored(checkpoint)
            except QuotaExceeded:
                raise
            except BaseException:
                # A cancelled storage call is drained before releasing io_lock.
                # A prepared-but-unwritten intent remains unknown, never usage.
                if not checkpointed:
                    if self.meter.write_intent is not None or ticket is not None:
                        self.meter.failed=True
                    if ticket is not None: await _stored(self.quota.abandon_write,ticket)
                raise
            if allowed<len(data): raise QuotaExceeded('周期流量额度已耗尽')

    def __getattr__(self, name):
        return getattr(self.destination,name)
