import asyncio
import gzip
import logging
import os
import re
import time
import uuid
from concurrent.futures import CancelledError

import aiohttp
from aiohttp import ClientSession
from aiohttp import TCPConnector
from aiohttp import web
from aiohttp.web_exceptions import HTTPBadRequest
from aiohttp.web_exceptions import HTTPPermanentRedirect
from aiohttp.web_exceptions import HTTPUnauthorized
from structlog import getLogger
from structlog.contextvars import bind_contextvars
from structlog.contextvars import clear_contextvars

from git_cdn.client_session import ClientSessionWithRetry
from git_cdn.clone_bundle_manager import CloneBundleManager
from git_cdn.clone_bundle_manager import close_bundle_session
from git_cdn.lfs_cache_manager import LFSCacheManager
from git_cdn.log import enable_console_logs
from git_cdn.log import enable_udp_logs
from git_cdn.upload_pack import UploadPackHandler
from git_cdn.upload_pack_input_parser import UploadPackInputParser
from git_cdn.upload_pack_input_parser_v2 import UploadPackInputParserV2
from git_cdn.util import GITCDN_VERSION
from git_cdn.util import GITLFS_OBJECT_RE
from git_cdn.util import find_gitpath
from git_cdn.util import get_url_creds_from_auth

log = getLogger()
parallel_request = 0
PROTOCOL_VERSION_RE = re.compile(r"^version=(\d+)$")


def fix_response_headers(headers):
    """Remove headers about encoding, which do not make sense to forward in proxy
    This blacklist applies to response headers
    """
    header = "Transfer-Encoding"
    if header in headers:
        del headers[header]


def fix_headers(headers):
    """Remove headers about encoding and hosts, which do not make sense to forward in proxy
    This blacklist applies to request headers
    """
    to_del = (
        "Host",
        "Transfer-Encoding",
        "Content-Length",
        "Content-Encoding",
    )
    for header in to_del:
        if header in headers:
            del headers[header]


def check_auth(request):
    """This method provides a quick way to redirect to correct path and force
    git to authenticate"""
    spl = request.path_qs.split("/")
    if "info" in spl:
        # early redirect paths not ending with git (we avoid a few round trip to the far server)
        i = spl.index("info")
        if not spl[i - 1].endswith(".git") and spl[i + 1].split("?")[0] in (
            "refs",
            "lfs",
        ):
            spl[i - 1] += ".git"
            raise HTTPPermanentRedirect("/".join(spl))
    if request.headers.get("Authorization") is None:
        raise HTTPUnauthorized(headers={"WWW-Authenticate": 'Basic realm="Git Proxy"'})


def redirect_browsers(request, upstream):
    """This method provides a quick way to redirect to correct path and force
    git to authenticate"""
    ua = request.headers.get("User-Agent", "git").lower()
    # "git" will also match "JGit/4.3.0.201604071810-r" UA (matlab is using that)
    if "git" not in ua and "aiohttp" not in ua:
        upstream_url = upstream + request.path.lstrip("/")
        raise HTTPPermanentRedirect(upstream_url)


def extract_headers_to_context(h):
    a = {"request_header": {}}
    for k in (
        "X-CI-INTEG-TEST",
        "X-CI-JOB-URL",
        "X-CI-PROJECT-PATH",
        "X-REPO-JOBURL",
        "X-FORWARDED-FOR",
    ):
        if k in h:
            a["request_header"][k] = h[k]
    bind_contextvars(**a)


def hide_auth_on_headers(h):
    if "Authorization" in h:
        h["Authorization"] = (
            h["Authorization"][0:10] + "XXX" + h["Authorization"][-3:-1]
        )


class GitCDN:
    MAX_CONNECTIONS = int(os.getenv("MAX_CONNECTIONS", "10"))

    def __init__(self, upstream, app, router):
        log_server = os.getenv("LOGGING_SERVER")
        if log_server is not None:
            host, port = log_server.split(":")
            enable_udp_logs(host, int(port), GITCDN_VERSION)
        else:
            enable_console_logs()

        logging.getLogger("gunicorn.access").propagate = True
        app.gitcdn = self
        self.app = app
        self.router = router
        self.upstream = self.app.upstream = upstream
        self.router.add_get("/", self.handle_liveness)
        self.router.add_resource("/{path:.+}").add_route("*", self.routing_handler)
        self.proxysession = None
        self.lfs_manager = None
        self.sema = None
        # for tests
        self.app.served_lfs_objects = 0

        # part on the init that needs to happen in the loop
        async def on_startup(_):
            self.get_session()
            self.lfs_manager = LFSCacheManager(upstream, None, self.proxysession)

        async def on_shutdown(_):
            if self.proxysession is not None:
                await self.proxysession.close()
            await close_bundle_session()

        self.app.on_startup.append(on_startup)
        self.app.on_shutdown.append(on_shutdown)

    def get_session(self):
        """automatically create a shared session for http proxying
        @todo: http_proxy is not automatically handled apparently
        """
        if self.proxysession is None:
            conn = TCPConnector(
                limit=self.MAX_CONNECTIONS,
                ssl=os.getenv("GIT_SSL_NO_VERIFY") is None,
            )
            # session can be indefinitively long, but we need at least some activity every minute
            timeout = aiohttp.ClientTimeout(
                total=None, connect=60, sock_connect=60, sock_read=60
            )
            self.proxysession = ClientSession(
                # supports deflate brotli and gzip
                connector=conn,
                trust_env=True,
                timeout=timeout,
                auto_decompress=False,
            )

        return self.proxysession

    async def handle_lfs_response(self, request, response):
        lfs_json = await response.content.read()
        self.lfs_manager.set_base_url(str(request.url.origin()) + "/")
        headers = response.headers.copy()
        # in case of large response, gitlab can automatically encode to gzip,
        # so we need to fix headers
        fix_response_headers(headers)

        # as we need to change lfs_json, we need to decompress the payload
        if "Content-Encoding" in headers:
            # only gzip supported! no check, as if it is not gzip it will send an exception anyway.
            # headers are already logged before.
            lfs_json = gzip.decompress(lfs_json)

        # we change lfs_json, so Content-Length will be changed
        if "Content-Length" in headers:
            del headers["Content-Length"]
        response = web.StreamResponse(status=response.status, headers=headers)
        writer = await response.prepare(request)
        # change the json data to force pointing to git-cdn url instead of upstream url
        lfs_json = await self.lfs_manager.hook_lfs_batch(lfs_json)
        if "Content-Encoding" in headers:
            lfs_json = gzip.compress(lfs_json)
        await writer.write(lfs_json)
        return response

    async def stream_response(self, request, input_response):
        if request.path.endswith("info/lfs/objects/batch"):
            return await self.handle_lfs_response(request, input_response)
        headers = input_response.headers.copy()
        fix_response_headers(headers)
        response = web.StreamResponse(status=input_response.status, headers=headers)
        writer = await response.prepare(request)
        while True:
            chunk = await input_response.content.readany()
            if not chunk:
                break
            # using writer.write instead of response.write saves a few checks
            await writer.write(chunk)
        return response

    async def _routing_handler(self, request):
        """We implement the routing manually
        because iohttp routing may not handle the requirements"""
        path = request.path
        path = path.lower()
        method = request.method.lower()
        git_path = find_gitpath(path)
        clear_contextvars()
        bind_contextvars(ctx={"uuid": str(uuid.uuid4()), "path": str(git_path)})

        extract_headers_to_context(request.headers)
        h = dict(request.headers)
        hide_auth_on_headers(h)
        log.debug(
            "handling response",
            request_path=request.path,
            path=path,
            git_path=git_path,
            request_headers_dict=h,
            parallel_request=parallel_request,
        )
        # For the case of clone bundle, we don't enforce authentication, and browser redirection
        if method == "get" and path.endswith("/clone.bundle"):
            bind_contextvars(handler="clone-bundle")
            if not git_path:
                raise HTTPBadRequest(reason="bad path: " + path)
            cbm = CloneBundleManager(git_path)
            return await cbm.handle_clone_bundle(request)

        redirect_browsers(request, self.upstream)
        # FIXME: check_auth maybe implementable via middleware
        check_auth(request)

        protocol_version = 1
        git_protocol = h.get("Git-Protocol")
        if git_protocol is not None:
            version = PROTOCOL_VERSION_RE.match(git_protocol)
            if version is not None:
                protocol_version = int(version.group(1))
        bind_contextvars(git_protocol_version=protocol_version)

        if method == "post" and path.endswith("git-upload-pack"):
            bind_contextvars(handler="upload-pack")
            if not git_path:
                raise HTTPBadRequest(reason="bad path: " + path)
            return await self.handle_upload_pack(request, git_path, protocol_version)
        if method in ("post", "put") and path.endswith("git-receive-pack"):
            bind_contextvars(handler="redirect")
            return await self.proxify(request)

        # we skip the authentication step in order to avoid one round trip
        # arguing it is impossible to guess a valid 64 byte oid without having access
        # to the git repo already
        if method == "get" and GITLFS_OBJECT_RE.match(path):
            bind_contextvars(handler="lfs")
            self.lfs_manager.set_base_url(str(request.url.origin()) + "/")
            h = request.headers.copy()
            del h["Host"]
            fn = await self.lfs_manager.get_from_cache(str(request.url), headers=h)
            if os.path.exists(fn):
                self.app.served_lfs_objects += 1
                return web.Response(body=open(fn, "rb"))
        bind_contextvars(handler="redirect")
        return await self.proxify(request)

    async def routing_handler(self, request):
        self.start_time = time.time()
        response = None
        global parallel_request
        try:
            parallel_request += 1
            response = await self._routing_handler(request)
            return response
        except (asyncio.CancelledError, CancelledError):
            bind_contextvars(canceled=True)
            log.warning("request canceled", resp_time=time.time() - self.start_time)
            raise
        finally:
            self.stats(response)
            parallel_request -= 1

    async def proxify(self, request):
        return await self.proxify_with_data(request, request.content)

    async def proxify_with_data(self, request, data):
        """Gitcdn acts as a dumb proxy to simplfy git 'insteadof' configuration."""
        upstream_url = self.upstream + request.path.lstrip("/")
        headers = request.headers.copy()
        fix_headers(headers)
        query = request.query
        try:
            async with ClientSessionWithRetry(
                self.get_session,
                request.method.lower(),
                upstream_url,
                headers=headers,
                params=query,
                allow_redirects=False,
                skip_auto_headers=["Accept-Encoding", "Accept", "User-Agent"],
                # note that request.content is a StreamReader, so the data is streamed
                # and not fully loaded in memory (unlike with python-requests)
                data=data,
            ) as response:
                resp_error = "n/a"
                if response.status >= 400:
                    resp_error = (await response.content.read()).decode(
                        errors="replace"
                    )
                if response.status == 500:
                    log.error(
                        "request leading to err 500",
                        request_content=data,
                        # only dump the first value in multidict
                        request_headers=dict(request.headers),
                    )

                log.debug(
                    "upstream returned",
                    upstream_url=upstream_url,
                    resp_error=resp_error,
                    resp_status=response.status,
                    # only dump the first value in multidict
                    resp_headers=dict(response.headers),
                    resp_time=time.time() - self.start_time,
                )

                if response.status < 400:
                    return await self.stream_response(request, response)

                error_text, error_code = resp_error, response.status
        except aiohttp.ClientConnectionError:
            log.exception(
                "Exception when connecting",
                upstream_url=upstream_url,
                resp_time=time.time() - self.start_time,
            )
            error_text = "Bad gateway"
            error_code = 502
        except (asyncio.CancelledError, CancelledError):
            raise
        except Exception:
            log.exception("Unexpected exception from aiohttp client")
            raise
        return web.Response(text=error_text, status=error_code)

    async def handle_liveness(self, _):
        return web.Response(text="live")

    async def handle_upload_pack(self, request, path, protocol_version):
        """Second part of the git+http protocol. (fetch)
        This part creates the git-pack bundle fully locally if possible.
        The authentication is still re-checked (because we are stateless, we can't assume that the
        client has the rights to access)

        @todo see how we can do fully local by caching the authentication results in a local db
        (redis/sqlite?)
        """
        request_content = await request.content.read()
        if protocol_version == 2:
            parsed_content = UploadPackInputParserV2(request_content)
            if parsed_content.command != b"fetch":
                bind_contextvars(
                    upload_pack_status="direct",
                    command=parsed_content.command.decode()
                    if parsed_content.command is not None
                    else None,
                )
                return await self.proxify_with_data(request, request_content)
            bind_contextvars(command="fetch")
        else:
            parsed_content = UploadPackInputParser(request_content)

        bind_contextvars(upload_pack_status="direct", canceled=False)
        response = None
        try:
            # proxy another info/refs request to the upstream server
            # (forwarding the BasicAuth as well) to check repo existence and credentials
            # previously we used a '/HEAD' request, but gitlab do not support it anymore.
            upstream_url = self.upstream + path + "/info/refs?service=git-upload-pack"
            auth = request.headers["Authorization"]
            headers = {"Authorization": auth}
            async with ClientSessionWithRetry(
                self.get_session,
                "get",
                upstream_url,
                headers=headers,
                allow_redirects=False,
            ) as response:
                await response.content.read()
                if response.status != 200:
                    return web.Response(text=response.reason, status=response.status)

            # read the upload-pack input from http response
            creds = get_url_creds_from_auth(auth)

            # start a streaming the response to the client
            response = web.StreamResponse(
                status=200,
                headers={
                    "Content-Type": "application/x-git-upload-pack-result",
                    "Cache-Control": "no-cache",
                },
            )
            writer = await response.prepare(request)

            # run git-upload-pack
            proc = UploadPackHandler(
                path,
                writer,
                auth=creds,
                upstream=self.upstream,
                sema=self.sema,
                protocol_version=protocol_version,
            )
            await proc.run(parsed_content)
        except (asyncio.CancelledError, CancelledError):
            bind_contextvars(canceled=True)
            raise
        except ConnectionResetError:
            bind_contextvars(conn_reset=True)
            raise
        except HTTPUnauthorized as e:
            log.warning("Unauthorized", unauthorized=e.reason)
            raise
        except Exception:
            bind_contextvars(upload_pack_status="exception")
            log.exception("Exception during UploadPack handling")
            raise
        return response

    def get_sema_count(self):
        if self.sema is not None:
            try:
                return self.sema.get_value()
            except NotImplementedError:
                return 0
        return 0

    def stats(self, response=None):
        response_stats = {}
        if response is not None:
            output_size = 0
            if hasattr(response, "_payload_writer"):
                output_size = getattr(response._payload_writer, "output_size", 0)
            if not output_size:
                output_size = response.content_length
            response_stats = dict(
                response_size=output_size,
                response_status=getattr(response, "status", 500),
            )
        log.info(
            "Response stats",
            **response_stats,
            resp_time=time.time() - self.start_time,
            sema_count=self.get_sema_count(),
        )
        return response
