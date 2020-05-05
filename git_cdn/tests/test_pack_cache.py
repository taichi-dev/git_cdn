# Standard Library
import asyncio
import os
from time import time

# Third Party Libraries
from git_cdn.pack_cache import PackCache
from git_cdn.pack_cache import PackCacheCleaner
from git_cdn.tests.test_packet_line import DataReader
from git_cdn.tests.test_upload_pack import FakeStreamWriter


def get_data(filename):
    with open(os.path.join(os.path.dirname(__file__), "packs", filename), "rb") as f:
        return f.read()


async def cache_pack(hash, tmpdir):
    pc = PackCache(hash, workdir=tmpdir)
    fakewrite = FakeStreamWriter()
    fakeread = DataReader(get_data("upload_pack.bin"))

    async with pc.write_lock():
        await pc.cache_pack(fakeread.read)

    async with pc.read_lock():
        await pc.send_pack(fakewrite)

    assert fakewrite.output == get_data("pack_cache.bin")
    assert pc.exists()
    return pc


async def test_pack_cache_create(tmpdir, loop):
    pc = await cache_pack("1234", tmpdir)
    fakewrite = FakeStreamWriter()
    await pc.send_pack(fakewrite)
    assert fakewrite.output == get_data("pack_cache.bin")


async def test_pack_cache_clean(tmpdir, loop):
    # gitlab-ci filesystem has 1 second precision
    mtime_precision = os.environ.get("MTIME_PRECISION", "ns")
    sleep = 0
    if mtime_precision == "s":
        sleep = 1

    pc1 = await cache_pack("11111", tmpdir)
    await asyncio.sleep(sleep)
    pc2 = await cache_pack("22222", tmpdir)
    await asyncio.sleep(sleep)
    pc3 = await cache_pack("33333", tmpdir)
    await asyncio.sleep(sleep)

    # read "11111" so cleaner should remove "22222"
    await pc1.send_pack(FakeStreamWriter())

    cleaner = PackCacheCleaner(workdir=tmpdir)
    assert await cleaner.clean() == 0
    cleaner.max_size = 3000000
    fake_time = (time() - 120, time() - 120)
    os.utime(cleaner.lockfile, fake_time)
    assert await cleaner.clean() == 1
    assert pc1.exists()
    assert not pc2.exists()
    assert pc3.exists()


async def test_pack_cache_abort(tmpdir, loop):
    pc = PackCache("failed", workdir=tmpdir)

    fakeread = DataReader(get_data("upload_pack_trunc.bin"))

    async with pc.write_lock():
        await pc.cache_pack(fakeread.read)

    assert os.path.exists(pc.filename) is False


async def test_corrupt(tmpdir):
    data = get_data("upload_pack_trunc.bin")
    pc = PackCache("fake", workdir=tmpdir)
    with open(pc.filename, "wb") as f:
        f.write(data)
    assert os.path.exists(pc.filename)
    assert not pc.exists()
