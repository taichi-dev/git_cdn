# -*- coding: utf-8 -*-
# Third Party Libraries
import aiohttp
from aiohttp.helpers import BasicAuth

from git_cdn.conftest import CREDS
from git_cdn.conftest import MANIFEST_PATH


async def test_proxy_connection_issue(make_client, cdn_event_loop, app, mocker):
    assert cdn_event_loop
    client = await make_client(app)
    session = client.app.gitcdn.get_session()
    old_request = session.request
    called = 0

    def side_effect(*args, **kwargs):
        nonlocal called
        called += 1
        session.request = old_request
        raise aiohttp.ClientConnectionError()

    session.request = side_effect

    resp = await client.get(
        f"{MANIFEST_PATH}/info/refs?service=git-upload-pack",
        skip_auto_headers=["Accept-Encoding", "Accept", "User-Agent"],
        auth=BasicAuth(*CREDS.split(":")),
        allow_redirects=False,
    )

    assert resp.status == 200
    assert called


async def test_proxy_answer_issue(make_client, cdn_event_loop, app, mocker):
    assert cdn_event_loop
    client = await make_client(app)
    session = client.app.gitcdn.get_session()
    old_request = session.request
    called = 0

    async def side_effect(*args, **kwargs):
        nonlocal called
        called += 1
        session.request = old_request
        mock_request = mocker.AsyncMock()
        mock_request.status = 500
        return mock_request

    session.request = side_effect

    resp = await client.get(
        f"{MANIFEST_PATH}/info/refs?service=git-upload-pack",
        skip_auto_headers=["Accept-Encoding", "Accept", "User-Agent"],
        auth=BasicAuth(*CREDS.split(":")),
        allow_redirects=False,
    )

    assert resp.status == 200
    assert called
