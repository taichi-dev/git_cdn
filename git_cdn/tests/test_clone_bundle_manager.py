import hashlib

import pytest
from aiohttp import web

import git_cdn.util
from git_cdn.clone_bundle_manager import CloneBundleManager
from git_cdn.clone_bundle_manager import close_bundle_session


async def previous(request):
    if request.method == "POST":
        request.app["value"] = (await request.post())["value"]
        return web.Response(body=b"thanks for the data")
    return web.Response(body="value: {}".format(request.app["value"]).encode("utf-8"))


@pytest.fixture
def cli(event_loop, aiohttp_client):
    app = web.Application()
    app.router.add_get("/", previous)
    app.router.add_post("/", previous)
    return event_loop.run_until_complete(aiohttp_client(app))


async def test_set_value(cli):
    resp = await cli.post("/", data={"value": "foo"})
    assert resp.status == 200
    assert await resp.text() == "thanks for the data"
    assert cli.server.app["value"] == "foo"


def check_md5_and_size(body, bundle_file, md5sum, size):
    assert len(body) == size
    h = hashlib.md5()
    h.update(body)
    assert h.digest() == md5sum

    # read the cache file, and make sure it has the correct data
    with open(bundle_file, "rb") as f:
        body = f.read()
    assert len(body) == size
    h = hashlib.md5()
    h.update(body)
    assert h.digest() == md5sum


@pytest.fixture
async def cbm(tmpworkdir):
    await close_bundle_session()
    yield CloneBundleManager("platform_external_javapoet.git")


@pytest.fixture
def client(event_loop, aiohttp_client, cbm):
    # build a small application, with just our handler
    app = web.Application(loop=event_loop)
    app.router.add_get("/", cbm.handle_clone_bundle)
    return event_loop.run_until_complete(aiohttp_client(app, loop=event_loop))


async def test_integration_basic(client, cbm):
    # this project has the smallest bundle size (1M)
    bundle_file = (
        f"{git_cdn.util.WORKDIR}/bundles/platform_external_javapoet_clone.bundle"
    )

    # get the bundle
    resp = await client.get("/")
    assert resp.status == 200
    md5sum, size = cbm.get_md5sum_and_size(resp)
    body = await resp.content.read()
    check_md5_and_size(body, bundle_file, md5sum, size)

    # second test, cached answer
    resp = await client.get("/")
    assert resp.status == 200
    md5sum, size = cbm.get_md5sum_and_size(resp)
    body = await resp.content.read()
    check_md5_and_size(body, bundle_file, md5sum, size)
    assert cbm.cache_hits == 1

    # third test, corrupted cached answer
    with open(bundle_file, "wb") as f:
        f.write(b"hello")

    resp = await client.get("/")
    assert resp.status == 200
    md5sum, size = cbm.get_md5sum_and_size(resp)
    body = await resp.content.read()
    check_md5_and_size(body, bundle_file, md5sum, size)
    assert cbm.cache_hits == 1

    # corrupted cached answer but with same size
    with open(bundle_file, "wb") as f:
        f.write(b"x" * size)

    resp = await client.get("/")
    assert resp.status == 200
    md5sum, size = cbm.get_md5sum_and_size(resp)
    body = await resp.content.read()
    # in this case the corrupted file is passed to the client, but next read should work
    assert body[:4] == b"xxxx"
    assert cbm.cache_hits == 2

    # recover after corrupted cached
    resp = await client.get("/")
    assert resp.status == 200
    md5sum, size = cbm.get_md5sum_and_size(resp)
    body = await resp.content.read()
    check_md5_and_size(body, bundle_file, md5sum, size)
    assert cbm.cache_hits == 2
