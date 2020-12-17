# Standard Library
import base64
import fcntl
import hashlib
import os

# Third Party Libraries
from aiohttp import ClientHttpProxyError
from aiohttp import ClientSession
from aiohttp import ClientTimeout
from aiohttp import TCPConnector
from aiohttp import web
from structlog import getLogger

from git_cdn.aiolock import lock
from git_cdn.util import get_bundle_paths

log = getLogger()

PROXY = os.getenv("BUNDLE_PROXY", None)
MAX_CONNECTIONS = int(os.getenv("MAX_CONNECTIONS", "10"))

# One session shared between all CloneBundleManager Instance
http_session = None


def new_session():
    """automatically create a shared session for http proxying"""
    global http_session

    conn = TCPConnector(
        limit=MAX_CONNECTIONS, verify_ssl=os.getenv("GIT_SSL_NO_VERIFY") is None
    )
    timeout = ClientTimeout(total=0)
    http_session = ClientSession(
        # supports deflate brotli and gzip
        connector=conn,
        timeout=timeout,
        trust_env=(PROXY is not None),
        auto_decompress=False,
    )


async def close_bundle_session():
    global http_session
    if http_session:
        await http_session.close()
        http_session = None


class CheckSumError(ValueError):
    pass


class CloneBundleManager:
    """Testable clone bundle downloader, and forwarder

    Android contains huge repositories, which are very long to bootstrap with git-cdn when the
    git-cdn is far away from the upstream server.
    Google provides clone bundles for most of those repositories, with geo-replication
    Those are "git bundle" files containing a recent version of most branches in AOSP server.
    This is a good boostrap, and allow to speed-up from 3.5 hours to 25min the bootstrap
    of some big repositories.
    """

    CDN_BUNDLE_URL = os.environ.get(
        "CDN_BUNDLE_URL",
        "https://storage.googleapis.com/gerritcodereview/android_{}_clone.bundle",
    )
    CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 32 * 1024))

    def __init__(self, git_path):
        self.bundle_name, self.lock, self.bundle_file = get_bundle_paths(git_path)
        self.cache_hits = 0
        if not http_session:
            new_session()

    @property
    def bundle_url(self):
        return self.CDN_BUNDLE_URL.format(self.bundle_name)

    async def stream_and_md5sum(self, inf, outf, request, writer, md5sum):
        """Compute the checksum while uploading the file
        We don't compute the checksum beforehand as this can be very long to compute
        and a source of DOS attack.
        The case of checksum failure is very rare. We detect it in order to remove
        any corrupted file in the cache.
        In case of corrupted file, one client will however get the corrupted file.
        """
        h = hashlib.md5()
        while True:
            if inf:
                chunk = inf.read(self.CHUNK_SIZE)
            else:
                chunk = await request.content.readany()
            if not chunk:
                break
            # using writer.write instead of response.write saves a few checks
            await writer.write(chunk)
            h.update(chunk)
            if outf:
                outf.write(chunk)
        if h.digest() != md5sum:
            # we unlink the file. It is still open, but this is no problem on unix
            os.unlink(self.bundle_file)

    def get_md5sum_and_size(self, request):
        md5sum = None
        if "x-goog-hash" in request.headers:
            for h in request.headers.getall("x-goog-hash"):
                typ, val = h.split("=", 1)
                if typ == "md5":
                    md5sum = base64.b64decode(val)
        expected_size = None
        if "Content-Length" in request.headers:
            expected_size = request.headers.get("Content-Length")
        return md5sum, int(expected_size)

    async def handle_clone_bundle(self, server_request):
        if not self.CDN_BUNDLE_URL:
            return web.Response(text="bundle unavailable", status=404)

        try:
            # short request to make sure google still serves the data
            async with http_session.head(self.bundle_url, proxy=PROXY) as request:
                md5sum, expected_size = self.get_md5sum_and_size(request)
                if request.status != 200 or md5sum is None or expected_size is None:
                    return web.Response(text="bundle unavailable", status=404)
        except ClientHttpProxyError:
            log.warning("HTTP Proxy error, convert to 404")
            return web.Response(text="bundle unavailable", status=404)

        response = web.StreamResponse(status=200, headers=request.headers)
        writer = await response.prepare(server_request)

        async with lock(self.lock, mode=fcntl.LOCK_SH):
            if os.path.exists(self.bundle_file) and expected_size:
                statinfo = os.stat(self.bundle_file)
                if statinfo.st_size == expected_size:
                    self.cache_hits += 1
                    # if the size differ, this might be that google updated the bundle
                    # or we somehow got corruption or transfer interrupt
                    # so we do not serve the cached version
                    with open(self.bundle_file, "rb") as inf:
                        await self.stream_and_md5sum(inf, None, None, writer, md5sum)
                        return response

        # we use the same lock to make sure we download the bundle before cloning
        async with lock(self.lock, mode=fcntl.LOCK_EX):
            async with http_session.get(self.bundle_url, proxy=PROXY) as request:
                with open(self.bundle_file, "wb") as outf:
                    await self.stream_and_md5sum(None, outf, request, writer, md5sum)

        return response
