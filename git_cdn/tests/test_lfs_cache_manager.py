# Standard Library
import asyncio
import fcntl
import hashlib
import json
import os
from copy import deepcopy

# Third Party Libraries
import mock
import pytest
from aiohttp import web
from aiohttp.web_exceptions import HTTPNotFound
from git_cdn.aiolock import lock
from git_cdn.lfs_cache_manager import LFSCacheManager


@pytest.fixture
def cache_manager(tmpdir):
    assert not tmpdir.listdir()
    c = LFSCacheManager(
        str(tmpdir / "workdir"), "https://upstream", "http://base", None
    )
    return c


@pytest.fixture
def mocked_cache_manager(cache_manager, loop):
    cache_manager.download_object = mock.Mock(
        spec=cache_manager.download_object, return_value=asyncio.gather()
    )
    return cache_manager


DOWNLOAD_RESPONSE = {
    "transfer": "basic",
    "objects": [
        {
            "oid": "1111111",
            "size": 123,
            "authenticated": True,
            "actions": {
                "download": {
                    "href": "https://upstream/1111111",
                    "header": {"Key": "value"},
                    "expires_at": "2016-11-10T15:29:07Z",
                }
            },
        }
    ],
}


async def test_hook_lfs_batch(mocked_cache_manager, loop):
    cache_manager = mocked_cache_manager
    content = json.dumps(DOWNLOAD_RESPONSE)
    content = await cache_manager.hook_lfs_batch(content)
    exp = (
        b'{"transfer": "basic", "objects": [{"oid": "1111111", "size": 123,'
        b' "authenticated": true, "actions": {"download": {"href": "http://base/1111111",'
        b' "header": {"Key": "value"}, "expires_at": "2016-11-10T15:29:07Z"}}}]}'
    )
    assert content == exp


async def test_hook_lfs_batch_no_object(mocked_cache_manager, loop):
    cache_manager = mocked_cache_manager
    response = DOWNLOAD_RESPONSE.copy()
    del response["objects"]
    content = json.dumps(response)
    content = await cache_manager.hook_lfs_batch(content)
    assert content == '{"transfer": "basic"}'


async def test_hook_lfs_batch_no_action(mocked_cache_manager, loop):
    cache_manager = mocked_cache_manager
    response = deepcopy(DOWNLOAD_RESPONSE)
    del response["objects"][0]["actions"]
    content = json.dumps(response)
    content = await cache_manager.hook_lfs_batch(content)
    assert content == (
        b'{"transfer": "basic", "objects": [{"oid": "1111111",'
        b' "size": 123, "authenticated": true}]}'
    )


async def test_download_object_with_lock(cache_manager, tmpdir, loop, aiohttp_client):
    TEXT = "Hello, world"

    async def hello(request):
        return web.Response(text=TEXT)

    app = web.Application()

    # build the checksum of our file
    checksum = hashlib.sha256(TEXT.encode()).hexdigest()
    path = f"/{checksum}"
    app.add_routes([web.get(path, hello)])
    client = await aiohttp_client(app)
    cache_manager.session = client
    fn = str(tmpdir / checksum)
    await cache_manager.download_object_with_lock(fn, path, {})
    with open(fn) as f:
        assert f.read() == TEXT


async def test_download_object_bad_checksum(
    cache_manager, tmpdir, loop, aiohttp_client
):
    TEXT = "Hello, world"

    async def hello(request):
        return web.Response(text=TEXT)

    app = web.Application()

    path = "/xx"
    app.add_routes([web.get(path, hello)])
    client = await aiohttp_client(app)
    cache_manager.session = client
    fn = str(tmpdir / "xx")
    with pytest.raises(HTTPNotFound):
        await cache_manager.download_object_with_lock(fn, path, {})
    assert not os.path.exists(fn)


async def test_download_object(cache_manager, tmpdir, loop, aiohttp_client):
    TEXT = "Hello, world"

    async def hello(request):
        return web.Response(text=TEXT)

    app = web.Application()

    # build the checksum of our file
    checksum = hashlib.sha256(TEXT.encode()).hexdigest()
    path = f"/{checksum}"
    app.add_routes([web.get(path, hello)])
    client = await aiohttp_client(app)
    cache_manager.session = client
    fn = await cache_manager.download_object(path, {})
    with open(fn) as f:
        assert f.read() == TEXT


async def test_download_object_cache_hit(cache_manager, tmpdir, loop, aiohttp_client):
    TEXT = "Hello, world"

    async def hello(request):
        # we should not download in that case
        raise Exception("nope")

    app = web.Application()

    # build the checksum of our file
    checksum = hashlib.sha256(TEXT.encode()).hexdigest()
    path = f"/{checksum}"
    app.add_routes([web.get(path, hello)])
    client = await aiohttp_client(app)
    cache_manager.session = client
    fn = await cache_manager.get_cache_path_for_href(path)
    lock(fn + ".lock")
    with open(fn, "wb") as f:
        f.write(TEXT.encode())
    fn = await cache_manager.download_object(path, {})
    with open(fn) as f:
        assert f.read() == TEXT


async def test_download_object_cache_being_written(
    cache_manager, tmpdir, loop, aiohttp_client
):
    TEXT = "Hello, world"

    async def hello(request):
        # we should not download in that case
        raise Exception("nope")

    app = web.Application()

    # build the checksum of our file
    checksum = hashlib.sha256(TEXT.encode()).hexdigest()
    path = f"/{checksum}"
    app.add_routes([web.get(path, hello)])
    client = await aiohttp_client(app)
    cache_manager.session = client
    fn = await cache_manager.get_cache_path_for_href(path)
    async with lock(fn + ".lock", fcntl.LOCK_EX):
        coroutine = cache_manager.download_object(path, {})
        with open(fn, "wb") as f:
            f.write(TEXT.encode())
    # no we have release the lock, we wait for the coroutine
    await coroutine
    with open(fn) as f:
        assert f.read() == TEXT


async def test_download_object_download_error(
    cache_manager, tmpdir, loop, aiohttp_client
):
    TEXT = "Hello, world"

    async def hello(request):
        # we should not download in that case
        raise HTTPNotFound()

    app = web.Application()

    # build the checksum of our file
    checksum = hashlib.sha256(TEXT.encode()).hexdigest()
    path = f"/{checksum}"
    app.add_routes([web.get(path, hello)])
    client = await aiohttp_client(app)
    cache_manager.session = client
    fn = await cache_manager.get_cache_path_for_href(path)
    lock(fn + ".lock")
    with pytest.raises(HTTPNotFound):
        await cache_manager.download_object(path, {})
