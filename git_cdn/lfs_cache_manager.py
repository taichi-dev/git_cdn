# Standard Library
import asyncio
import fcntl
import json
import os
import time
from concurrent.futures._base import CancelledError
from urllib.parse import urlparse

# Third Party Libraries
from aiohttp.client_exceptions import ClientPayloadError
from aiohttp.web_exceptions import HTTPFound
from aiohttp.web_exceptions import HTTPNotFound
from structlog import getLogger

from git_cdn.aiolock import lock
from git_cdn.util import check_path

log = getLogger()


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

    MAX_DOWNLOAD_TRIES = 10

    def __init__(self, workdir, upstream_url, base_url, session):
        self.workdir = os.path.expanduser(workdir)
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

    async def get_cache_path_for_href(self, href):
        path = urlparse(href).path.lstrip("/")
        check_path(path)
        oldpath = os.path.join(self.workdir, "lfs", path)
        bn = os.path.basename(oldpath)
        newdir = os.path.join(os.path.dirname(oldpath), bn[:2])
        newpath = os.path.join(newdir, bn)
        if os.path.exists(oldpath):
            async with lock(newpath + ".lock", fcntl.LOCK_EX):
                os.rename(oldpath, newpath)
                os.unlink(oldpath + ".lock")
        return newpath

    async def _download_object_with_lock_one_try(self, retry, fn, href, headers):
        t1 = time.time()
        cancelled_error = None
        async with self.session.get(href, headers=headers) as request:
            ctx = {
                "lfs_retry": retry,
                "lfs_href": href,
                "lfs_content_length": request.headers["Content-Length"],
            }
            log.debug("downloading lfs file", **ctx)
            if request.status != 200:
                raise HTTPNotFound(body=await request.content.read())
            with open(fn, "wb") as f:
                while True:
                    try:
                        chunk = await request.content.readany()
                    # if download gets too long, git-lfs gets impatient and retries
                    # 30s timeout.
                    # never mind, we continue the download until the end
                    # the lock mechanism will take effect, and the next retry will get
                    # the result
                    except CancelledError as e:
                        log.info(
                            "git-lfs client got impatient. Finishing anyway", **ctx
                        )
                        cancelled_error = e
                        continue
                    if not chunk:
                        break
                    f.write(chunk)

            log.info(
                "downloaded LFS object", lfs_download_duration=time.time() - t1, **ctx
            )

            if cancelled_error is not None:
                # we forward the cancel, as the connection has been closed by client
                # response would raise another exception
                raise cancelled_error  # pylint: disable=E0702

    async def download_object_with_lock(self, fn, href, headers):
        for retry in range(self.MAX_DOWNLOAD_TRIES):
            try:
                await self._download_object_with_lock_one_try(retry, fn, href, headers)
                if await self.checksum_lfs_file(fn):
                    raise HTTPFound(fn)
                os.unlink(fn)
            except HTTPFound:
                return
            except (HTTPNotFound, CancelledError):  # pylint: disable=W0706
                raise
            except ClientPayloadError:
                continue
            except Exception:
                log.exception(
                    "exception while downloading lfs object",
                    lfs_retry=retry,
                    lfs_href=href,
                )
        raise HTTPNotFound(
            body=f"failed to get file after {self.MAX_DOWNLOAD_TRIES} attempts"
        )

    async def checksum_lfs_file(self, fn):
        p = await asyncio.create_subprocess_exec(
            "sha256sum",
            fn,
            "-b",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
        )
        (stdout_data, _) = await p.communicate()
        cs = stdout_data.decode().split(" ")[0]
        exp = os.path.basename(fn)
        if cs != exp:
            log.error(
                "bad checksum",
                lfs_filename=fn,
                lfs_expected_checksum=exp,
                lfs_actual_checksum=cs,
            )
        return cs == exp

    async def download_object(self, href, headers):
        """@returns: filename where to find the file
        raises: HTTPNotFound in case of impossibility to download the file
        """
        if self.base_url is not None and href.startswith(self.base_url):
            href = href.replace(self.base_url, self.upstream_url)
        fn = await self.get_cache_path_for_href(href)
        # try to see if the file has been downloaded
        async with lock(fn + ".lock", fcntl.LOCK_SH):
            # getting the lock in shared mode ensures that the file is not being written
            if os.path.exists(fn):
                if await self.checksum_lfs_file(fn):
                    os.utime(fn, None)
                    return fn
                log.warn("Erasing corrupted file", lfs_filename=fn)
                os.unlink(fn)

        async with lock(fn + ".lock", fcntl.LOCK_EX):
            if os.path.exists(fn):
                return fn
            try:
                await self.download_object_with_lock(fn, href, headers)
            except HTTPFound:
                return fn
        return fn
