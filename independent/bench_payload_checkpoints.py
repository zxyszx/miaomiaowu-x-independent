#!/usr/bin/env python3
"""Local FULL-checkpoint cost diagnostic; simulated transport, no network claim.

Run from any directory with the same Python as run_tests.py. Each case creates
an isolated temporary outbox, seeds all identities, then times 128 cumulative
64 KiB writes round-robin. Does not run quota reservations or HTTP delivery.
Results depend on the host/filesystem; no pass threshold is asserted.
"""
import json
from pathlib import Path
import statistics
import tempfile
import time
import uuid

from payload_meter import MeteredWriter, PayloadMeter
from usage_delivery import UsageDelivery


class Runtime:
    def __init__(self): self.meter=PayloadMeter()
    def usage_snapshot(self): return self.meter.snapshot()
    def set_usage_checkpoint(self, callback): self.meter.set_checkpoint(callback)
    def set_incremental_usage_checkpoint(self, callback): self.meter.set_incremental_checkpoint(callback)


class Target:
    def write(self, data): pass


def measure(identities):
    with tempfile.TemporaryDirectory() as state:
        runtime=Runtime()
        delivery=UsageDelivery('unused','fixture',Path(state),runtime,str(uuid.uuid4()))
        runtime.meter.bind(1)
        writers=[MeteredWriter(Target(),runtime.meter,(1,n+1,str(uuid.uuid4())),0)
                 for n in range(identities)]
        for writer in writers: writer.write(b'x')
        samples=[]; payload=b'x'*65536
        for n in range(128):
            start=time.perf_counter()
            writers[n%identities].write(payload)
            samples.append(time.perf_counter()-start)
        # Keep the delivery object alive while timed writes invoke its callback.
        assert delivery.box is not None
        return dict(identities=identities,writes=128,mib_s=round(8/sum(samples),2),
                    median_ms=round(statistics.median(samples)*1000,3),
                    max_ms=round(max(samples)*1000,3))


if __name__=='__main__':
    for identities in (1,64,256): print(json.dumps(measure(identities)),flush=True)
