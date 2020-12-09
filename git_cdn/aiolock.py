"""
aiolock works between process, threads(gunicorn uses them), and between coroutines.
We cannot use a single semantic. fcntl flock only works between processes, and
asyncio.Lock() is single threaded.

We use a LockManager singleton which records per process lock holder and then fcntl semantics to
communicate between processes
"""

# Standard Library
import asyncio
import collections
import concurrent
import fcntl
import os
import time
from enum import Enum

from structlog import getLogger

# Third Party Libraries
from git_cdn.util import backoff

log = getLogger()


def succeed(v):
    f = asyncio.Future()
    f.set_result(v)
    return f


class Lock:
    def __init__(self, flock, mode):
        self.flock = flock
        self.mode = mode

    async def acquire(self):
        p = self.flock.acquire(self.mode)
        try:
            await p
        except asyncio.CancelledError:
            if p.cancelled():
                raise
            if p.done():  # in case the task was just about to be scheduled.
                self.release()
            else:
                p.cancel()
            raise

    def release(self):
        return self.flock.release(self.mode)

    async def __aenter__(self):
        await self.acquire()
        # We have no use for the "as ..."  clause in the with
        # statement for locks.
        return None

    async def __aexit__(self, exc_type, exc, tb):
        self.release()


class S(Enum):
    IDLE = 0
    ACQUIRING_EX = 1
    ACQUIRING_SH = 2
    ACQUIRED_EX = 3
    ACQUIRED_SH = 4


class FLock:
    def __init__(self, filename):
        # ensure directory is created, handling for race conditions at creating the dir
        # between processes
        for timeout in backoff(0.1, 10):
            if os.path.isdir(os.path.dirname(filename)):
                break
            try:
                os.makedirs(os.path.dirname(filename))
                break
            except FileExistsError:
                log.warning(
                    (
                        "(Race condition, take care if too often)"
                        " unable to create dir for lock"
                    ),
                    timeout=timeout,
                )
                # At this point the concurring
                # process may have not finish
                # to create the whole directory tree
                time.sleep(timeout)
        self.filename = filename
        self.lock_holder_num = 0
        self.ex_waiters = collections.deque()
        self.sh_waiters = collections.deque()
        self.state = S.IDLE
        self.loop = asyncio.get_event_loop()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.f = None

    def lock(self, mode):
        return Lock(self, mode=mode)

    def acquire(self, mode):
        f = asyncio.Future()
        if mode == fcntl.LOCK_EX:
            self.ex_waiters.append(f)
        else:
            self.sh_waiters.append(f)
        self._try_acquire()
        return f

    def _acquired(self, mode):

        if mode == fcntl.LOCK_EX:
            self.state = S.ACQUIRED_EX
        else:
            self.state = S.ACQUIRED_SH
        self.loop.call_soon_threadsafe(self._try_acquire)

    def _try_acquire_idle(self, mode):
        assert self.f is None
        self.f = open(self.filename, "a+")
        try:
            # First try fast lock
            fcntl.flock(self.f.fileno(), mode | fcntl.LOCK_NB)
            self._acquired(mode)
            return
        except BlockingIOError:
            # then run the flock in blocking mode, but in an executor (thread)
            if mode == fcntl.LOCK_EX:
                self.state = S.ACQUIRING_EX
            else:
                self.state = S.ACQUIRING_SH
            asyncio.get_event_loop().run_in_executor(
                self.executor, self._sync_flock, mode
            )
            return

    def _acquire_ex(self):
        if not self.lock_holder_num:
            while self.ex_waiters and self.ex_waiters[0].cancelled():
                self.ex_waiters.popleft()

            if not self.ex_waiters:
                self._release()
                return

            waiter = self.ex_waiters.popleft()
            waiter.set_result(None)
            self.lock_holder_num += 1

    def _acquire_sh(self):
        # prioritize writers
        # note that this prioritization do not work across process
        if not self.ex_waiters:
            for waiter in self.sh_waiters:
                if not waiter.cancelled():
                    waiter.set_result(None)
                    self.lock_holder_num += 1
            self.sh_waiters.clear()

        # in the case of a ex was asked while waiting for sh, we release the lock
        # or we would deadlock
        if not self.lock_holder_num:
            self._release()

    def _try_acquire(self):
        # log.debug(
        #     "try_acquire",
        #     state=self.state,
        #     lock_holder_num=self.lock_holder_num,
        #     num_ex_waiters=len(self.ex_waiters),
        #     num_sh_waiters=len(self.sh_waiters),
        #     pid=os.getpid())
        if self.state in (S.ACQUIRING_EX, S.ACQUIRING_SH):
            return

        if self.ex_waiters:
            mode = fcntl.LOCK_EX
        elif self.sh_waiters:
            mode = fcntl.LOCK_SH
        else:
            return

        if self.state == S.IDLE:
            self._try_acquire_idle(mode)

        elif self.state == S.ACQUIRED_EX:
            self._acquire_ex()
        elif self.state == S.ACQUIRED_SH:
            self._acquire_sh()

    def _sync_flock(self, mode):
        fcntl.flock(self.f.fileno(), mode)
        self._acquired(mode)

    def maybe_remove_lock_file(self):
        try:
            fcntl.flock(self.f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                os.unlink(self.filename)
            except FileNotFoundError:
                pass
            fcntl.flock(self.f.fileno(), fcntl.LOCK_UN)
        except BlockingIOError:
            # not the last process holding, don't remove the file
            pass

    def release(self, mode):
        assert self.state in (S.ACQUIRED_EX, S.ACQUIRED_SH)
        self.lock_holder_num -= 1
        if self.lock_holder_num:
            return succeed(None)
        return self._release()

    def _release(self):
        fcntl.flock(self.f.fileno(), fcntl.LOCK_UN)
        self.f.close()
        self.f = None
        self.state = S.IDLE
        self._try_acquire()

        # if nobody else is holding the lock, remove it from the manager
        if self.state == S.IDLE:
            manager.remove_lock(self.filename)
        return succeed(None)


class LockManager:
    def __init__(self):
        # locks index by absolute file name
        self.locks = {}

    def remove_lock(self, filename):
        del self.locks[filename]

    def make_lock(self, filename):
        return FLock(filename)

    def get_lock(self, filename) -> FLock:
        filename = os.path.abspath(filename)
        if filename not in self.locks:
            self.locks[filename] = self.make_lock(filename)
        return self.locks[filename]


manager = LockManager()


def lock(filename, mode=fcntl.LOCK_EX) -> Lock:
    return manager.get_lock(filename).lock(mode)


class AsyncIOLockManager(LockManager):
    def make_lock(self, filename):
        lock = asyncio.Lock()

        class FakeLock:
            def lock(self, mode):
                return lock

        return FakeLock()


# manager = AsyncIOLockManager()
