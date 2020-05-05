"""
Add simple asyncio class over multiprocess Semaphore
"""

# Standard Library
import asyncio

# RSWL Dependencies
from logging_configurer import get_logger

log = get_logger()


class AioSemaphore:
    def __init__(self, sema):
        self.sema = sema

    async def acquire(self):
        # try non-blocking first
        if self.sema.acquire(False):
            return
        log.info("wait for semaphore")
        await asyncio.get_event_loop().run_in_executor(None, self.sema.acquire)

    def release(self):
        self.sema.release()

    async def __aenter__(self):
        await self.acquire()

    async def __aexit__(self, exc_type, exc, tb):
        self.release()
