# Standard Library
import asyncio
import random
from asyncio import gather
from multiprocessing import BoundedSemaphore

# Third Party Libraries
from git_cdn.aiosemaphore import AioSemaphore


async def take_semaphore(sema):
    try:
        async with AioSemaphore(sema):
            await asyncio.sleep(0.001)
    except asyncio.CancelledError:
        pass


async def test_semaphore(loop):
    sema = BoundedSemaphore(1)
    sema.acquire()

    task = asyncio.create_task(take_semaphore(sema))

    await asyncio.sleep(0.001)

    assert not task.done()
    sema.release()

    await gather(task)
    assert task.done()


async def test_multi_semaphore(loop):
    sema = BoundedSemaphore(2)

    tasks = [asyncio.create_task(take_semaphore(sema)) for _ in range(100)]

    await gather(*tasks)

    # assert still 2 semaphores can be taken
    assert sema.acquire(False)
    assert sema.acquire(False)
    assert not sema.acquire(False)


async def test_multi_semaphore2(loop):
    sema = BoundedSemaphore(2)
    n_tasks = 1000

    tasks = [asyncio.create_task(take_semaphore(sema)) for _ in range(n_tasks)]

    for _ in range(400):
        await asyncio.sleep(0.001)
        t = random.randint(0, n_tasks - 1)
        tasks[t].cancel()

    await gather(*tasks)

    # Assert there are still 2 semaphore
    assert sema.acquire(False)
    assert sema.acquire(False)
    assert not sema.acquire(False)
