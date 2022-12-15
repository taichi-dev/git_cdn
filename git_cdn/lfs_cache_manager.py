import asyncio
import fcntl
import json
import os
import time
from urllib.parse import urlparse

from aiohttp import web
from aiohttp.web_exceptions import HTTPNotFound
from structlog import getLogger
from structlog.contextvars import bind_contextvars

from git_cdn.lock.aio_lock import lock
from git_cdn.util import check_path
from git_cdn.util import get_subdir

log = getLogger()


class LFSCacheFile:
    def __init__(self, href, headers):
        self.href = href
        self.headers = headers
        self.accept_encoding = ""
        if "Accept-Encoding" in headers:
            self.accept_encoding = headers["Accept-Encoding"]
        path = urlparse(href).path.lstrip("/")
        check_path(path)
        self.workdir = get_subdir("lfs")
        self.hash = os.path.basename(path)
        self.filename = os.path.join(
            self.workdir, os.path.dirname(path), self.hash[:2], self.hash
        )

    @property
    def gzip(self):
        return f"{self.filename}.gzip"

    def read_lock(self):
        return lock(self.filename, mode=fcntl.LOCK_SH)

    def write_lock(self):
        return lock(self.filename, mode=fcntl.LOCK_EX)

    def delete(self):
        if os.path.exists(self.gzip):
            os.unlink(self.gzip)
        if os.path.exists(self.filename):
            os.unlink(self.filename)

    def gzip_exists(self):
        return os.path.exists(self.gzip) and os.stat(self.gzip).st_size > 0

    def raw_exists(self):
        return os.path.exists(self.filename) and os.stat(self.filename).st_size > 0

    def exists(self):
        return (
            "gzip" in self.accept_encoding and self.gzip_in_cache() or self.raw_exists()
        )

    def utime(self):
        os.utime(self.filename, None)

    def gzip_utime(self):
        os.utime(self.gzip, None)

    async def gunzip(self):
        # We do not want to compute the gunzip using python stlib
        # - file may be huge, native gunzip will have better perf
        # - avoid python memory allocations and GC operations
        p = await asyncio.create_subprocess_exec(
            "gunzip",
            "-f",
            "-k",
            "-S",
            ".gzip",
            self.gzip,
            stderr=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
        )
        cmd_stdout, cmd_stderr = await p.communicate()
        r = p.returncode
        if r:
            log.error(
                "gunzip failed",
                lfs_filename=self.filename,
                cmd_stderr=cmd_stderr.decode(),
                cmd_stdout=cmd_stdout.decode(),
                lfs_gzip=self.gzip,
                pid=p.pid,
            )
        return r

    async def checksum(self):
        # We do not want to compute the sha using python stlib
        # - file may be huge, native checksum will have better perf
        # - avoid python memory allocations and GC operations
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
                pid=p.pid,
            )
        return cs == self.hash

    def gzip_in_cache(self):
        if self.gzip_exists():
            self.gzip_utime()
            bind_contextvars(lfs_hit=True)
            return True
        return False

    def raw_in_cache(self):
        if self.raw_exists():
            self.utime()
            bind_contextvars(lfs_hit=True)
            return True
        return False

    def response(self):
        if "gzip" in self.accept_encoding and self.gzip_in_cache():
            bind_contextvars(lfs_served="gzip")
            return web.Response(
                body=open(self.gzip, "rb"), headers={"Content-Encoding": "gzip"}
            )
        if self.raw_in_cache():
            bind_contextvars(lfs_served="raw")
            return web.Response(body=open(self.filename, "rb"))
        return None

    async def download(self, session, ctx):
        t1 = time.time()
        async with session.get(self.href, headers=self.headers) as request:
            if "Content-Length" in request.headers:
                ctx["lfs_content_length"] = request.headers["Content-Length"]
            if request.status != 200:
                raise HTTPNotFound(body=await request.content.read())

            ext = ""
            if (
                "Content-Encoding" in request.headers
                and "gzip" in request.headers["Content-Encoding"]
            ):
                ctx["lfs_content_encoding"] = "gzip"
                ext = ".gzip"

            with open(self.filename + ext, "wb") as f:
                try:
                    while chunk := await request.content.readany():
                        f.write(chunk)

                except Exception:
                    # don't need to raise, if the file is not present, we will try again
                    log.exception("Aborting lfs download", **ctx)
                    self.delete()
                    return

            t2 = time.time()
            ctx["lfs_download_duration"] = t2 - t1
            if ext == ".gzip":
                await self.gunzip()
                ctx["lfs_uncompress_duration"] = time.time() - t2

            if not await self.checksum():
                self.delete()


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
        self.ctx = {}

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

    async def get_from_cache(self, href, headers):
        """@returns: filename where to find the file
        raises: HTTPNotFound in case of impossibility to download the file
        """
        if self.base_url is not None and href.startswith(self.base_url):
            href = href.replace(self.base_url, self.upstream_url)

        cache_file = LFSCacheFile(href, headers)
        self.ctx = {"lfs_href": href, "lfs_content_encoding": "none"}

        async with cache_file.read_lock():
            r = cache_file.response()
            if r is not None:
                return r

        async with cache_file.write_lock():
            r = cache_file.response()
            if r is not None:
                return r
            await asyncio.shield(cache_file.download(self.session, self.ctx))
            bind_contextvars(lfs_hit=False, **self.ctx)

            if not cache_file.exists():
                raise HTTPNotFound(body="failed to get LFS file")
            return cache_file.response()
