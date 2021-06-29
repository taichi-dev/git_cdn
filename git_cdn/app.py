# Standard Library
import asyncio
import base64
import gzip
import logging
import os
import re
import time
import urllib.parse
import uuid
from concurrent.futures import CancelledError
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version

# Third Party Libraries
import aiohttp
import sentry_sdk
from aiohttp import ClientSession
from aiohttp import TCPConnector
from aiohttp import helpers
from aiohttp import web
from aiohttp.web_exceptions import HTTPBadRequest
from aiohttp.web_exceptions import HTTPPermanentRedirect
from aiohttp.web_exceptions import HTTPUnauthorized
from sentry_sdk.integrations.aiohttp import AioHttpIntegration
from structlog import getLogger
from structlog.contextvars import bind_contextvars
from structlog.contextvars import clear_contextvars

from git_cdn.clone_bundle_manager import CloneBundleManager
from git_cdn.clone_bundle_manager import close_bundle_session
from git_cdn.lfs_cache_manager import LFSCacheManager
from git_cdn.log import enable_console_logs
from git_cdn.log import enable_udp_logs
from git_cdn.upload_pack import UploadPackHandler
from git_cdn.util import backoff
from git_cdn.util import check_path

try:
    GITCDN_VERSION = version("git_cdn")
except PackageNotFoundError:
    GITCDN_VERSION = "unknown"

sentry_dsn = os.getenv("SENTRY_DSN")
if sentry_dsn:
    sentry_sdk.init(
        sentry_dsn,
        release=GITCDN_VERSION,
        environment=os.getenv("SENTRY_ENV", "dev"),
        integrations=[AioHttpIntegration()],
    )


log = getLogger()
helpers.netrc_from_env = lambda: None
GITLFS_OBJECT_RE = re.compile(r"(?P<path>.*\.git)/gitlab-lfs/objects/[0-9a-f]{64}$")
parallel_request = 0


def fix_headers(headers):
    """Remove headers about encoding and hosts, which do not make sense to forward in proxy
    This blacklist applies to request and response headers

    Also remove Git-Protocol header from the client, which tells git to use the protocol-v2
    which we don't support yet.
    """
    for header in ("Host", "Transfer-Encoding", "Git-Protocol"):
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


def find_gitpath(path):
    """find the git path for this url path
    ensures it ends with ".git", and do not start with "/" and do not contain ../
    """
    path = path.strip("/")
    check_path(path)

    for suffix in (
        ".git/info/refs",
        ".git/git-upload-pack",
        ".git/git-receive-pack",
        "/info/refs",
        "/git-upload-pack",
        "/git-receive-pack",
        ".git/clone.bundle",
        "/clone.bundle",
        "/info/lfs/objects/batch",
    ):
        if path.endswith(suffix):
            return path[: -len(suffix)] + ".git"
    res = GITLFS_OBJECT_RE.match(path)
    if res:
        return res.groupdict()["path"]
    return None


def get_url_creds_from_auth(auth):
    # decode the creds from the auth in
    creds = base64.b64decode(auth.split(" ", 1)[-1]).decode()
    # gitlab token not supposed to require quote, but we still encode.
    # because some people put their email as user and this put an extraneous @ in the url.
    # https://tools.ietf.org/html/rfc7617  -> BasicAuth  Page 4
    # https://tools.ietf.org/html/rfc3986.html -> URLs:  3.2.1.  User Information
    return ":".join([urllib.parse.quote_plus(p) for p in creds.split(":", 1)])


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


class ClientSessionWithRetry:
    REQUEST_MAX_RETRIES = int(os.getenv("REQUEST_MAX_RETRIES", "10"))

    def __init__(self, get_session, *args, **kwargs):
        self.get_session = get_session
        self.args = args
        self.kwargs = kwargs
        self.cm_request = None

    async def __aenter__(self, *args, **kwargs):
        start_time = time.time()
        for retries, timeout in enumerate(backoff(0.1, self.REQUEST_MAX_RETRIES)):
            try:
                self.cm_request = self.get_session().request(*self.args, **self.kwargs)
                return await self.cm_request.__aenter__(*args, **kwargs)
            except aiohttp.ClientConnectionError:
                if retries + 1 >= self.REQUEST_MAX_RETRIES:
                    log.exception(
                        "Max of request retries reached",
                        resp_time=time.time() - start_time,
                        timeout=timeout,
                        request_max_retries=self.REQUEST_MAX_RETRIES,
                        methods=self.args[0],
                        upstream_url=self.args[1],
                    )
                    raise
                log.exception(
                    "Client connection error",
                    resp_time=time.time() - start_time,
                    timeout=timeout,
                    request_max_retries=self.REQUEST_MAX_RETRIES,
                    retries=retries,
                    methods=self.args[0],
                    upstream_url=self.args[1],
                )
                await asyncio.sleep(timeout)

    async def __aexit__(self, *args, **kwargs):
        return await self.cm_request.__aexit__(*args, **kwargs)


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
                verify_ssl=os.getenv("GIT_SSL_NO_VERIFY") is None,
            )
            # session can be indefinitively long, but we need at least some activity every minute
            timeout = aiohttp.ClientTimeout(
                total=0, connect=60, sock_connect=60, sock_read=60
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
        fix_headers(headers)

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
        fix_headers(headers)
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
        """We implement the routing manually because iohttp routing may not handle the requirements"""
        path = request.path
        method = request.method.lower()
        git_path = find_gitpath(request.path)
        clear_contextvars()
        bind_contextvars(ctx={"uuid": str(uuid.uuid4()), "path": str(git_path)})

        extract_headers_to_context(request.headers)
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
        h = dict(request.headers)
        hide_auth_on_headers(h)
        log.debug(
            "handling response",
            request_path=request.path,
            request_headers_dict=h,
            parallel_request=parallel_request,
        )
        if method == "post" and path.endswith("git-upload-pack"):
            bind_contextvars(handler="upload-pack")
            if not git_path:
                raise HTTPBadRequest(reason="bad path: " + path)
            return await self.handle_upload_pack(request, git_path)
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
        start_time = time.time()
        global parallel_request
        try:
            parallel_request += 1
            return await self._routing_handler(request)
        except CancelledError:
            bind_contextvars(canceled=True)
            log.warning("request canceled", resp_time=time.time() - start_time)
            raise
        finally:
            parallel_request -= 1

    async def proxify(self, request):
        """Gitcdn acts as a dumb proxy to simplfy git 'insteadof' configuration. """
        upstream_url = self.upstream + request.path.lstrip("/")
        headers = request.headers.copy()
        fix_headers(headers)
        query = request.query
        start_time = time.time()
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
                data=request.content,
            ) as response:
                resp_error = "n/a"
                if response.status >= 400:
                    resp_error = (await response.content.read()).decode(
                        errors="replace"
                    )
                log.debug(
                    "upstream returned",
                    upstream_url=upstream_url,
                    resp_error=resp_error,
                    resp_status=response.status,
                    # only dump the first value in multidict
                    resp_headers=dict(
                        **{k: response.headers[k] for k in response.headers.keys()}
                    ),
                    resp_time=time.time() - start_time,
                )

                if response.status < 400:
                    return await self.stream_response(request, response)

                error_text, error_code = resp_error, response.status
        except aiohttp.ClientConnectionError:
            log.exception(
                "Exception when connecting",
                upstream_url=upstream_url,
                resp_time=time.time() - start_time,
            )
            error_text = "Bad gateway"
            error_code = 502
        except CancelledError:
            raise
        except Exception:
            log.exception("Unexpected exception from aiohttp client")
            raise
        return web.Response(text=error_text, status=error_code)

    async def handle_liveness(self, _):
        return web.Response(text="live")

    async def handle_upload_pack(self, request, path):
        """Second part of the git+http protocol. (fetch)
        This part creates the git-pack bundle fully locally if possible.
        The authentication is still re-checked (because we are stateless, we can't assume that the
        client has the rights to access)

        @todo see how we can do fully local by caching the authentication results in a local db
        (redis/sqlite?)
        """
        start_time = time.time()
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
            content = await request.content.read()
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
            )
            await proc.run(content)
        except CancelledError:
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
        finally:
            if hasattr(response, "_payload_writer"):
                output_size = getattr(response._payload_writer, "output_size", 0)
            else:
                output_size = 0
            log.info(
                "Response stats",
                response_size=output_size,
                response_status=getattr(response, "status", 500),
                resp_time=time.time() - start_time,
                sema_count=self.get_sema_count(),
            )
        return response

    def get_sema_count(self):
        if self.sema is not None:
            try:
                return self.sema.get_value()
            except NotImplementedError:
                return 0
        return 0


def make_app(upstream):
    app = web.Application()
    GitCDN(upstream, app, app.router)
    return app


if os.getenv("GITSERVER_UPSTREAM") and os.getenv("WORKING_DIRECTORY"):
    app = make_app(os.getenv("GITSERVER_UPSTREAM", None))


def main():
    web.run_app(app, port=os.getenv("PORT", "8000"))


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
