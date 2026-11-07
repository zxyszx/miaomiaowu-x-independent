"""Shared, no-idle-credit byte pacing for one listener and one direction."""
import asyncio
import time


async def payload_write(destination, data):
    # Type lookup avoids mistaking a transport proxy's delegated attributes for
    # the durable writer contract (including dynamic Mock attributes in tests).
    asynchronous=getattr(type(destination),'awrite',None)
    if asynchronous is None: destination.write(data)
    else: await asynchronous(destination,data)


class BytePacer:
    def __init__(self, bytes_per_second):
        self.rate = bytes_per_second
        self.lock = asyncio.Lock()

    async def write(self, destination, data):
        if self.rate is None:
            await payload_write(destination,data)
            await destination.drain()
            # A buffered drain may not suspend. Give configuration renewals and
            # other connections a turn even when no rate wait is required.
            await asyncio.sleep(0)
            return
        # Bound each scheduling turn to about 50 ms. All connections share this
        # lock, so opening extra connections does not multiply the allowance.
        quantum = max(1, min(16384, self.rate // 20))
        for offset in range(0, len(data), quantum):
            block = data[offset:offset + quantum]
            async with self.lock:
                # Charge before sending, with no credit accumulated while idle.
                # Cancellation does not leave a reservation for later callers.
                await asyncio.sleep(len(block) / self.rate)
                await payload_write(destination,block)
                await destination.drain()


class CombinedPacer:
    """Charge independent shared allowances concurrently, not add their delays."""
    def __init__(self, *pacers, deadline=None):
        self.deadline=deadline
        self.pacers = [p for p in pacers if p.rate is not None]

    def _check_deadline(self):
        if self.deadline is not None and time.time()>=self.deadline:
            raise asyncio.TimeoutError('临时配置已到期')

    async def write(self, destination, data):
        self._check_deadline()
        if not self.pacers:
            await payload_write(destination,data)
            await destination.drain()
            await asyncio.sleep(0)
            return
        quantum = max(1, min(16384, min(p.rate for p in self.pacers) // 20))
        for offset in range(0, len(data), quantum):
            block = data[offset:offset + quantum]
            async def charge(pacer):
                async with pacer.lock:
                    await asyncio.sleep(len(block) / pacer.rate)
            self._check_deadline()
            if len(self.pacers)==1:
                # No concurrent allowances to coordinate. Avoid a child task
                # and gather future on every block of the common single-rule path.
                await charge(self.pacers[0])
            else:
                await asyncio.gather(*(charge(pacer) for pacer in self.pacers))
            self._check_deadline()
            await payload_write(destination,block)
            await destination.drain()
