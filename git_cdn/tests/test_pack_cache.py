# Standard Library
import asyncio
import os
from time import time

import pytest

# Third Party Libraries
from git_cdn.pack_cache import PackCache
from git_cdn.pack_cache import PackCacheCleaner
from git_cdn.tests.test_packet_line import DataReader
from git_cdn.tests.test_upload_pack import FakeStreamWriter


def get_data(filename):
    with open(os.path.join(os.path.dirname(__file__), "packs", filename), "rb") as f:
        return f.read()


async def cache_pack(hash):
    pc = PackCache(hash)
    fakewrite = FakeStreamWriter()
    fakeread = DataReader(get_data("upload_pack.bin"))

    async with pc.write_lock():
        await pc.cache_pack(fakeread.read)

    async with pc.read_lock():
        await pc.send_pack(fakewrite)

    assert fakewrite.output == get_data("pack_cache.bin")
    assert pc.exists()
    return pc


@pytest.mark.asyncio
async def test_pack_cache_create(tmpworkdir, cdn_event_loop):
    pc = await cache_pack("1234")
    fakewrite = FakeStreamWriter()
    await pc.send_pack(fakewrite)
    assert fakewrite.output == get_data("pack_cache.bin")


@pytest.mark.asyncio
async def test_pack_cache_clean(tmpworkdir, cdn_event_loop):
    # gitlab-ci filesystem has 1 second precision
    sleep = 0
    if "CI_JOB_TOKEN" in os.environ:
        sleep = 1

    pc1 = await cache_pack("11111")
    await asyncio.sleep(sleep)
    pc2 = await cache_pack("22222")
    await asyncio.sleep(sleep)
    pc3 = await cache_pack("33333")
    await asyncio.sleep(sleep)

    # read "11111" so cleaner should remove "22222"
    await pc1.send_pack(FakeStreamWriter())

    cleaner = PackCacheCleaner()
    assert cleaner.clean_task() == 0
    cleaner.max_size = 3000000
    fake_time = (time() - 120, time() - 120)
    os.utime(cleaner.lock.filename, fake_time)
    assert cleaner.clean_task() == 1
    assert pc1.exists()
    assert not pc2.exists()
    assert pc3.exists()


@pytest.mark.asyncio
async def test_pack_cache_abort(tmpworkdir, cdn_event_loop):
    pc = PackCache("failed")

    fakeread = DataReader(get_data("upload_pack_trunc.bin"))

    async with pc.write_lock():
        await pc.cache_pack(fakeread.read)

    assert os.path.exists(pc.filename) is False


@pytest.mark.asyncio
async def test_corrupt(tmpworkdir):
    data = get_data("upload_pack_trunc.bin")
    pc = PackCache("fake")
    with open(pc.filename, "wb") as f:
        f.write(data)
    assert os.path.exists(pc.filename)
    assert not pc.exists()


@pytest.mark.asyncio
async def test_pack_cache_error(tmpworkdir, cdn_event_loop):
    pc = PackCache("error")
    fakewrite = FakeStreamWriter()
    fakeread = DataReader(get_data("upload_pack_error.bin"))

    async with pc.write_lock():
        await pc.cache_pack(fakeread.read, fakewrite)

    assert fakewrite.output == get_data("upload_pack_error.bin")
    assert not pc.exists()
