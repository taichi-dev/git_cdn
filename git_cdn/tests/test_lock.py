# Standard Library
import asyncio
import fcntl
import os
import sys
import traceback
from asyncio import Future
from asyncio import gather
from asyncio import sleep

# Third Party Libraries
import pytest

from git_cdn.aiolock import lock

execution = []


def log(s):
    print(s)
    execution.append(s)


def reset_log():
    global execution
    execution = []


async def release_promises(*promises):
    for p in promises:
        p.set_result(None)
        await sleep(0.1)


async def locking_coroutine(filename, mode, done, task_id):
    log("lock requested {}".format(task_id))
    try:
        async with lock(filename, mode=mode):
            log("lock acquired {}".format(task_id))
            await done
            log("promised finished {}".format(task_id))
    except asyncio.CancelledError:
        traceback.print_exc()
        log("cancelled {}".format(task_id))

    log("lock released {}".format(task_id))
    return 1


@pytest.mark.asyncio
async def test_lock(tmpdir, cdn_event_loop):
    reset_log()
    fn = str(tmpdir / "lock.lock")
    p1 = Future()
    task1 = locking_coroutine(fn, fcntl.LOCK_SH, p1, 1)
    await gather(task1, release_promises(p1))


@pytest.mark.asyncio
async def test_lock_ex_sh(tmpdir, cdn_event_loop):
    reset_log()
    fn = str(tmpdir / "lock.lock")
    p1 = Future()
    task1 = locking_coroutine(fn, fcntl.LOCK_EX, p1, 1)

    p2 = Future()
    task2 = locking_coroutine(fn, fcntl.LOCK_SH, p2, 2)

    await gather(task1, task2, release_promises(p1, p2))


@pytest.mark.asyncio
async def test_lock_ex_sh_sh(tmpdir, cdn_event_loop):
    reset_log()
    fn = str(tmpdir / "lock.lock")
    p1 = Future()
    task1 = locking_coroutine(fn, fcntl.LOCK_EX, p1, 1)

    p2 = Future()
    task2 = locking_coroutine(fn, fcntl.LOCK_SH, p2, 2)

    p3 = Future()
    task3 = locking_coroutine(fn, fcntl.LOCK_SH, p3, 3)

    await gather(task1, task2, task3, release_promises(p1, p3, p2))
    print(execution)


@pytest.mark.parametrize("wait_time", [0, 0.1, 0.01, 0.001, 0.0001, 0.00001])
@pytest.mark.asyncio
async def test_lock_ex_sh_cancel_wait(tmpdir, cdn_event_loop, wait_time):
    reset_log()
    fn = str(tmpdir / "lock.lock")
    p1 = Future()
    task1 = locking_coroutine(fn, fcntl.LOCK_EX, p1, 1)
    task1 = asyncio.ensure_future(task1)  # turn into a cancellable task

    p2 = Future()
    task2 = locking_coroutine(fn, fcntl.LOCK_SH, p2, 2)

    async def release_all():
        if wait_time:
            await sleep(wait_time)
        task1.cancel()
        p2.set_result(None)

    await gather(task1, task2, release_all(), return_exceptions=True)
    print(execution)


# @pytest.mark.skip()
@pytest.mark.parametrize("with_cancel_monkey", [True, False])
@pytest.mark.parametrize("num_times", [10])
@pytest.mark.asyncio
async def test_monkey_lock(tmpdir, cdn_event_loop, num_times, with_cancel_monkey):
    monkey = os.path.join(os.path.dirname(__file__), "lock_monkey.py")
    dl = []
    for _ in range(num_times):
        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            monkey,
            str(with_cancel_monkey),
            cwd=str(tmpdir),
            stdin=asyncio.subprocess.PIPE,
        )
        dl.append(proc.wait())
    rets = await asyncio.gather(*dl, return_exceptions=True)
    assert rets == [0] * num_times
