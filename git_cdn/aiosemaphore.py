"""
Add simple asyncio class over multiprocess Semaphore
"""

# Standard Library
import asyncio
import concurrent
from time import time

# Third Party Libraries
from structlog import getLogger
from structlog.contextvars import bind_contextvars

log = getLogger()
executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)


class AioSemaphore:
    def __init__(self, sema):
        self.sema = sema
        self.cancel = False
        self.acquired = False

    def _acquire(self):
        self.sema.acquire()
        self.acquired = True
        if self.cancel:
            self.release()

    async def acquire(self):
        # try non-blocking first
        if self.sema.acquire(False):
            bind_contextvars(semaphore="non-blocking")
            return
        log.info("wait for semaphore")
        start_wait = time()
        p = asyncio.get_event_loop().run_in_executor(executor, self._acquire)
        try:
            await p
            bind_contextvars(semaphore="acquired")
        except asyncio.CancelledError:
            bind_contextvars(semaphore="canceled")
            if self.acquired:
                self.release()
            else:
                self.cancel = True
            raise
        finally:
            bind_contextvars(sema_wait=time() - start_wait)

    def release(self):
        self.sema.release()
        self.acquired = False

    async def __aenter__(self):
        await self.acquire()

    async def __aexit__(self, exc_type, exc, tb):
        self.release()
