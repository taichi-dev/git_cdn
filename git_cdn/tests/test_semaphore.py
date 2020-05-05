# Standard Library
import asyncio
from asyncio import gather
from multiprocessing import BoundedSemaphore

# Third Party Libraries
from git_cdn.aiosemaphore import AioSemaphore


async def take_semaphore(sema):
    async with sema:
        await asyncio.sleep(0.01)


async def test_semaphore(loop):
    sema = AioSemaphore(BoundedSemaphore(1))
    await sema.acquire()

    task = asyncio.create_task(take_semaphore(sema))

    await asyncio.sleep(0.01)

    assert not task.done()
    sema.release()

    await gather(task)
    assert task.done()


async def test_multi_semaphore(loop):
    sema = AioSemaphore(BoundedSemaphore(2))

    tasks = [asyncio.create_task(take_semaphore(sema)) for _ in range(100)]

    await gather(*tasks)

    # assert still 2 semaphores can be taken
    assert sema.sema.acquire(False)
    assert sema.sema.acquire(False)
    assert not sema.sema.acquire(False)
