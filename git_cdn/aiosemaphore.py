"""
Add simple asyncio class over multiprocess Semaphore
"""

# Standard Library
import asyncio
from time import time

# Third Party Libraries
from structlog import getLogger

log = getLogger()


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
            return
        log.info("wait for semaphore")
        start_wait = time()
        p = asyncio.get_event_loop().run_in_executor(None, self._acquire)
        try:
            await p
            log.info("Semaphore acquired", sema_wait=time() - start_wait)
        except asyncio.CancelledError:
            if self.acquired:
                self.release()
            else:
                self.cancel = True
            raise

    def release(self):
        self.sema.release()
        self.acquired = False

    async def __aenter__(self):
        await self.acquire()

    async def __aexit__(self, exc_type, exc, tb):
        self.release()
