# Standard Library
import ast
import logging
import os

# Third Party Libraries
import sentry_sdk
from aiohttp import helpers
from aiohttp import web
from sentry_sdk.integrations.aiohttp import AioHttpIntegration
from sentry_sdk.integrations.logging import LoggingIntegration
from structlog import getLogger
from structlog.contextvars import get_contextvars
from structlog.threadlocal import get_threadlocal

from git_cdn.git_cdn import GitCDN
from git_cdn.util import GITCDN_VERSION


def before_breadcrumb(event, hint):
    if "log_record" in hint:
        try:
            evt = ast.literal_eval(hint["log_record"].message)
            if "message" in evt:
                event["message"] = evt["message"]
            if "extra" in evt:
                event["data"].update(evt["extra"])
        except Exception:
            pass
    return event


def before_send(event, hint):
    if "log_record" in hint:
        try:
            evt = ast.literal_eval(hint["log_record"].message)
            if "message" in evt:
                event["logentry"]["message"] = evt.pop("message")
            if "extra" in evt:
                event["extra"].update(evt["extra"])
            ctx = get_contextvars()
            if ctx:
                event["extra"].update(ctx)
            tloc = get_threadlocal()
            if tloc:
                event["extra"].update(tloc)
        except Exception:
            pass
    return event


sentry_dsn = os.getenv("SENTRY_DSN")
if sentry_dsn:
    sentry_logging = LoggingIntegration(
        level=logging.DEBUG,  # Capture debug and above as breadcrumbs
        event_level=logging.ERROR,  # Send errors as events
    )
    sentry_sdk.init(
        sentry_dsn,
        integrations=[sentry_logging, AioHttpIntegration()],
        release=GITCDN_VERSION,
        before_breadcrumb=before_breadcrumb,
        before_send=before_send,
        environment=os.getenv("SENTRY_ENV", "dev"),
    )


log = getLogger()
helpers.netrc_from_env = lambda: None


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
