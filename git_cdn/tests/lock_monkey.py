# Standard Library
import asyncio
import fcntl
import os
import random
import sys
from asyncio import gather
from asyncio import sleep

# Third Party Libraries
from git_cdn.aiolock import lock

FILENAME = "competition_file.txt"


async def write():
    print("writing")
    try:
        with open(FILENAME, "wb", 0) as f:
            val = random.randint(1, 255)
            for _ in range(val):
                f.write(bytes([val]))
                f.flush()
                await sleep(random.random() / 100000)
    except asyncio.CancelledError:
        print("writing cancelled")
        # write a coherent file
        with open(FILENAME, "wb", 0) as f:
            f.write(bytes([1]))
        raise
    print("end writing")


async def verify():
    buffer = b""
    if not os.path.exists(FILENAME):
        return
    with open(FILENAME, "rb", 0) as f:
        while True:
            chunk = f.read(40)
            if not chunk:
                break
            buffer += chunk
            await sleep(random.random() / 1000)
    val = buffer[0]
    assert len(buffer) == val, "length differ {} {} {}".format(len(buffer), val, buffer)
    for i, c in enumerate(buffer):
        assert c == val, "character at position {} differ {:x} != {:x}".format(
            i, c, val
        )


async def monkey():
    try:
        for _ in range(200):
            await sleep(random.random() / 1000)
            if random.random() > 0.99:
                async with lock("monkey.lock", fcntl.LOCK_EX):
                    await write()
            else:
                async with lock("monkey.lock", fcntl.LOCK_SH):
                    await verify()
    except asyncio.CancelledError:
        pass


async def cancel_monkey(tasks):
    if sys.argv[1] == "False":
        return
    for _ in range(3000):
        await sleep(random.random() / 1000)
        if random.random() > 0.9:
            if tasks:
                t = tasks.pop(random.randint(0, len(tasks) - 1))
                print("cancelling", id(t), len(tasks))
                t.cancel()


async def main():
    random.seed(a=None)
    tasks = [asyncio.ensure_future(monkey()) for i in range(40)]
    await gather(cancel_monkey(tasks), *tasks)


if __name__ == "__main__":
    asyncio.run(main())
