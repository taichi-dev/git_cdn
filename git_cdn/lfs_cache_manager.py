# Standard Library
import asyncio
import fcntl
import json
import os
import time
from urllib.parse import urlparse

# Third Party Libraries
from aiohttp.web_exceptions import HTTPNotFound
from structlog import getLogger

from git_cdn.aiolock import lock
from git_cdn.util import check_path
from git_cdn.util import get_subdir

log = getLogger()


class LFSCacheFile:
    def __init__(self, href):
        path = urlparse(href).path.lstrip("/")
        check_path(path)
        self.workdir = get_subdir("lfs")
        self.hash = os.path.basename(path)
        self.filename = os.path.join(
            self.workdir, os.path.dirname(path), self.hash[:2], self.hash
        )

    def read_lock(self):
        return lock(self.filename, mode=fcntl.LOCK_SH)

    def write_lock(self):
        return lock(self.filename, mode=fcntl.LOCK_EX)

    def delete(self):
        os.unlink(self.filename)

    def exists(self):
        return os.path.exists(self.filename) and os.stat(self.filename).st_size > 0

    def utime(self):
        os.utime(self.filename, None)

    async def checksum(self):
        p = await asyncio.create_subprocess_exec(
            "sha256sum",
            self.filename,
            "-b",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
        )
        (stdout_data, _) = await p.communicate()
        cs = stdout_data.decode().split(" ")[0]
        if cs != self.hash:
            log.error(
                "bad checksum",
                lfs_filename=self.filename,
                lfs_expected_checksum=self.hash,
                lfs_actual_checksum=cs,
            )
        return cs == self.hash

    async def is_in_cache(self):
        if self.exists():
            if await self.checksum():
                self.utime()
                log.info("LFS cache hit", lfs_hit=True)
                return True
        return False


class LFSCacheManager:
    """Unit testable LFS cache manager which download LFS objects in parallel

    Design Model: to workaround per connection limitation of the proxy, we download LFS objects in
    parallel.
    We still limit the number of connection via aiohttp client connection pool.

    Because the LFS client might only download a smaller number of objects in parallel,
    we start the parallel download of all the files of a batch as soon as we get the batch
    attempt from the batch API.

    We use aiolock writeonce, read multiple in order to make sure we only download the same object
    once.
    """

    def __init__(self, upstream_url, base_url, session):
        self.upstream_url = upstream_url
        self.base_url = base_url
        self.session = session

    def set_base_url(self, base_url):
        self.base_url = base_url

    async def hook_lfs_batch(self, lfs_result_content):
        """modify the lfs batch result to change the hrefs so that they point to us"""
        js = json.loads(lfs_result_content)
        if "objects" not in js:
            return lfs_result_content
        for o in js["objects"]:
            if "actions" not in o:
                continue

            for action in ["download", "upload", "verify"]:
                if action in o["actions"]:
                    href = o["actions"][action]
                    href["href"] = href["href"].replace(
                        self.upstream_url, self.base_url
                    )

        return json.dumps(js).encode()

    async def download_object(self, cache_file, href, headers):
        t1 = time.time()
        async with self.session.get(href, headers=headers) as request:
            ctx = {
                "lfs_href": href,
                "lfs_content_length": request.headers["Content-Length"],
            }
            if request.status != 200:
                raise HTTPNotFound(body=await request.content.read())

            with open(cache_file.filename, "wb") as f:
                try:
                    while chunk := await request.content.readany():
                        f.write(chunk)

                except Exception:
                    # don't need to raise, if the file is not present, we will try again
                    log.exception("Aborting lfs download", **ctx)
                    cache_file.delete()
                    return

            if await cache_file.checksum():
                log.info(
                    "downloaded LFS", lfs_download_duration=time.time() - t1, **ctx
                )
            else:
                cache_file.delete()

    async def get_from_cache(self, href, headers):
        """@returns: filename where to find the file
        raises: HTTPNotFound in case of impossibility to download the file
        """
        if self.base_url is not None and href.startswith(self.base_url):
            href = href.replace(self.base_url, self.upstream_url)

        cache_file = LFSCacheFile(href)

        async with cache_file.read_lock():
            if await cache_file.is_in_cache():
                return cache_file.filename

        async with cache_file.write_lock():
            if await cache_file.is_in_cache():
                return cache_file.filename
            log.info("LFS cache miss", lfs_hit=False)
            await asyncio.shield(self.download_object(cache_file, href, headers))

            if not cache_file.exists():
                raise HTTPNotFound(body=f"failed to get LFS file")
            return cache_file.filename
