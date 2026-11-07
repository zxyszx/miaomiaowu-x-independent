"""Test-only loop stall sampler. Retains code locations, never frame locals."""
import sys
import threading
import time
from collections import deque
from pathlib import Path


class LoopProbe:
    def __init__(self, interval=0.02, capacity=256):
        self.thread_id=threading.get_ident()
        self.interval=interval
        self.samples=deque(maxlen=capacity)
        self.stop=threading.Event()
        self.worker=None

    def __enter__(self):
        self.worker=threading.Thread(target=self._sample,daemon=True)
        self.worker.start()
        return self

    def _sample(self):
        while not self.stop.wait(self.interval):
            frame=sys._current_frames().get(self.thread_id)
            locations=[]
            while frame is not None and len(locations)<12:
                locations.append((Path(frame.f_code.co_filename).name,
                                  frame.f_code.co_name,frame.f_lineno))
                frame=frame.f_back
            # Do not retain frame references, arguments, tokens or payloads.
            del frame
            self.samples.append((time.monotonic(),locations))

    def between(self,start,end):
        return [dict(at=round(at-start,6),stack=stack)
                for at,stack in list(self.samples) if start<=at<=end]

    def __exit__(self,*exc):
        self.stop.set()
        self.worker.join(timeout=1)
