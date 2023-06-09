# Standard Library
import gzip
import hashlib
import json
from copy import deepcopy

# Third Party Libraries
import pytest
import pytest_asyncio
from aiohttp import web
from aiohttp.web_exceptions import HTTPNotFound

from git_cdn.lfs_cache_manager import LFSCacheFile
from git_cdn.lfs_cache_manager import LFSCacheManager

# pylint: disable=unused-argument,protected-access,redefined-outer-name,broad-exception-raised


@pytest_asyncio.fixture
def cache_manager(tmpworkdir):
    assert not tmpworkdir.listdir()
    c = LFSCacheManager("https://upstream", "http://base", None)
    return c


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


@pytest.mark.asyncio
async def test_hook_lfs_batch(cache_manager, cdn_event_loop):
    content = json.dumps(DOWNLOAD_RESPONSE)
    content = await cache_manager.hook_lfs_batch(content)
    exp = (
        b'{"transfer": "basic", "objects": [{"oid": "1111111", "size": 123,'
        b' "authenticated": true, "actions": {"download": {"href": "http://base/1111111",'
        b' "header": {"Key": "value"}, "expires_at": "2016-11-10T15:29:07Z"}}}]}'
    )
    assert content == exp


@pytest.mark.asyncio
async def test_hook_lfs_batch_no_object(cache_manager, cdn_event_loop):
    response = DOWNLOAD_RESPONSE.copy()
    del response["objects"]
    content = json.dumps(response)
    content = await cache_manager.hook_lfs_batch(content)
    assert content == '{"transfer": "basic"}'


@pytest.mark.asyncio
async def test_hook_lfs_batch_no_action(cache_manager, cdn_event_loop):
    response = deepcopy(DOWNLOAD_RESPONSE)
    del response["objects"][0]["actions"]
    content = json.dumps(response)
    content = await cache_manager.hook_lfs_batch(content)
    assert content == (
        b'{"transfer": "basic", "objects": [{"oid": "1111111",'
        b' "size": 123, "authenticated": true}]}'
    )


@pytest.mark.asyncio
async def test_download_gzip(cache_manager, tmpworkdir, cdn_event_loop, aiohttp_client):
    TEXT = "Hello, world"
    ZTEXT = gzip.compress(TEXT.encode())

    async def hello(request):
        return web.Response(body=ZTEXT, headers={"Content-Encoding": "gzip"})

    app = web.Application()

    # build the checksum of our file
    checksum = hashlib.sha256(TEXT.encode()).hexdigest()
    path = f"/{checksum}"
    app.add_routes([web.get(path, hello)])
    client = await aiohttp_client(app, auto_decompress=False)
    cache_manager.session = client
    fn = LFSCacheFile(checksum, headers={"Accept-Encoding": "gzip"})
    fn.filename = str(tmpworkdir / checksum)
    fn.hash = checksum
    ctx = {}

    await fn.download(cache_manager.session, ctx)
    with open(fn.filename) as f:
        assert f.read() == TEXT


@pytest.mark.asyncio
async def test_download(cache_manager, tmpworkdir, cdn_event_loop, aiohttp_client):
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
    fn = LFSCacheFile(checksum, headers={})
    fn.filename = str(tmpworkdir / checksum)
    fn.hash = checksum
    ctx = {}

    await fn.download(cache_manager.session, ctx)
    with open(fn.filename) as f:
        assert f.read() == TEXT


@pytest.mark.asyncio
async def test_download_bad_checksum(
    cache_manager, tmpworkdir, cdn_event_loop, aiohttp_client
):
    TEXT = "Hello, world"

    async def hello(request):
        return web.Response(text=TEXT)

    app = web.Application()

    path = "/xx"
    app.add_routes([web.get(path, hello)])
    client = await aiohttp_client(app)
    cache_manager.session = client
    with pytest.raises(HTTPNotFound):
        await cache_manager.get_from_cache(path, {})


@pytest.mark.asyncio
async def test_download_cache_miss(
    cache_manager, tmpworkdir, cdn_event_loop, aiohttp_client
):
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
    resp = await cache_manager.get_from_cache(path, {})
    assert resp.body._value.read().decode() == TEXT


@pytest.mark.asyncio
async def test_download_cache_hit(
    cache_manager, tmpworkdir, cdn_event_loop, aiohttp_client
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

    cache_file = LFSCacheFile(path, headers={})
    async with cache_file.write_lock():
        with open(cache_file.filename, "wb") as f:
            f.write(TEXT.encode())

    resp = await cache_manager.get_from_cache(path, {})

    assert resp.body._value.read().decode() == TEXT


@pytest.mark.asyncio
async def test_download_cache_being_written(
    cache_manager, tmpworkdir, cdn_event_loop, aiohttp_client
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
    cache_file = LFSCacheFile(path, headers={})
    async with cache_file.write_lock():
        coroutine = cache_manager.get_from_cache(path, {})
        with open(cache_file.filename, "wb") as f:
            f.write(TEXT.encode())

    # no we have release the lock, we wait for the coroutine
    await coroutine
    with open(cache_file.filename) as f:
        assert f.read() == TEXT


@pytest.mark.asyncio
async def test_download_error(
    cache_manager, tmpworkdir, cdn_event_loop, aiohttp_client
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
    with pytest.raises(HTTPNotFound):
        await cache_manager.get_from_cache(path, {})
