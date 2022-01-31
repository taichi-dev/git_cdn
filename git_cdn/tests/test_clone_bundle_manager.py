# Standard Library
import hashlib

# Third Party Libraries
from aiohttp import web

import git_cdn.util
from git_cdn.clone_bundle_manager import CloneBundleManager


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


async def test_integration_basic(loop, aiohttp_client, tmpworkdir):
    # this project has the smallest bundle size (1M)
    cbm = CloneBundleManager("platform_external_javapoet.git")
    bundle_file = (
        f"{git_cdn.util.WORKDIR}/bundles/platform_external_javapoet_clone.bundle"
    )
    # build a small application, with just our handler
    app = web.Application()
    app.router.add_get("/", cbm.handle_clone_bundle)
    client = await aiohttp_client(app)

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
