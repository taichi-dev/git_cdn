import asyncio
import os
import time

import aiohttp
from structlog import getLogger

from git_cdn.util import backoff

log = getLogger()


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
