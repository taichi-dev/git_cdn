# -*- coding: utf-8 -*-
# Third Party Libraries
import aiohttp
from aiohttp.helpers import BasicAuth

from git_cdn.conftest import CREDS
from git_cdn.conftest import MANIFEST_PATH


async def test_proxy_connection_issue(make_client, event_loop, app, mocker):
    assert event_loop
    client = await make_client(app)
    session = client.app.gitcdn.get_session()

    class fake_context_manager:
        called = 0

        async def __aenter__(self, *args):
            self.called = 1
            session.request = old_request
            raise aiohttp.ClientConnectionError()

        async def __aexit__(self, *args):
            pass

    old_request = session.request
    fake_ctx = fake_context_manager()
    session.request = lambda *args, **kwargs: fake_ctx

    resp = await client.get(
        f"{MANIFEST_PATH}/info/refs?service=git-upload-pack",
        skip_auto_headers=["Accept-Encoding", "Accept", "User-Agent"],
        auth=BasicAuth(*CREDS.split(":")),
        allow_redirects=False,
    )
    assert resp.status == 200
    assert fake_ctx.called
