# -*- coding: utf-8 -*-
# Third Party Libraries

import aiohttp
import pytest
from aiohttp.helpers import BasicAuth
from aiohttp.web_exceptions import HTTPBadGateway
from aiohttp.web_exceptions import HTTPBadRequest
from aiohttp.web_exceptions import HTTPPermanentRedirect
from aiohttp.web_exceptions import HTTPUnauthorized

from git_cdn.client_session import ClientSessionWithRetry
from git_cdn.conftest import CREDS
from git_cdn.conftest import MANIFEST_PATH


async def test_proxy_retry_connection_issue(make_client, cdn_event_loop, app, mocker):
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
    assert called == 1


async def test_proxy_retry_answer_issue(make_client, cdn_event_loop, app, mocker):
    assert cdn_event_loop
    client = await make_client(app)
    session = client.app.gitcdn.get_session()
    old_request = session.request
    called = 0

    async def side_effect(*args, **kwargs):
        nonlocal called
        called += 1

        if called == 1:
            raise aiohttp.ClientConnectionError()
        else:
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
    assert called == 2


async def test_proxy_answer_issue(make_client, cdn_event_loop, app, mocker):
    assert cdn_event_loop

    client = await make_client(app)
    session = client.app.gitcdn.get_session()
    called = 0
    mock_request = mocker.AsyncMock()
    mock_request.status = 500
    mock_request.headers = {}
    mock_request.content.read = mocker.AsyncMock(return_value=b"crac")

    async def side_effect(*args, **kwargs):
        nonlocal called
        called += 1
        return mock_request

    session.request = side_effect

    resp = await client.get(
        f"{MANIFEST_PATH}/info/refs?service=git-upload-pack",
        skip_auto_headers=["Accept-Encoding", "Accept", "User-Agent"],
        auth=BasicAuth(*CREDS.split(":")),
        allow_redirects=False,
    )

    assert resp.status == 500
    assert called == ClientSessionWithRetry.REQUEST_MAX_RETRIES


async def test_proxy_connection_issue(make_client, cdn_event_loop, app, mocker):
    assert cdn_event_loop
    client = await make_client(app)
    session = client.app.gitcdn.get_session()
    called = 0

    mocker.patch("asyncio.sleep")

    def side_effect(*args, **kwargs):
        nonlocal called
        called += 1
        raise aiohttp.ClientConnectionError()

    session.request = side_effect

    resp = await client.get(
        f"{MANIFEST_PATH}/info/refs?service=git-upload-pack",
        skip_auto_headers=["Accept-Encoding", "Accept", "User-Agent"],
        auth=BasicAuth(*CREDS.split(":")),
        allow_redirects=False,
    )

    assert called == ClientSessionWithRetry.REQUEST_MAX_RETRIES
    assert resp.status == 502


@pytest.mark.parametrize(
    "http_ex, http_code",
    [
        (HTTPBadRequest, HTTPBadRequest.status_code),
        (HTTPBadGateway, HTTPBadGateway.status_code),
        (HTTPPermanentRedirect, HTTPPermanentRedirect.status_code),
        (Exception, 500),
    ],
)
async def test_exception(
    make_client, cdn_event_loop, app, mocker, monkeypatch, http_ex, http_code
):
    assert cdn_event_loop
    client = await make_client(app)
    session = client.app.gitcdn.get_session()
    called = 0

    monkeypatch.setenv("REQUEST_MAX_RETRIES", "5")

    def side_effect(*args, **kwargs):
        nonlocal called
        called += 1
        if http_ex is HTTPPermanentRedirect:
            raise HTTPPermanentRedirect(location="titi")
        else:
            raise http_ex()

    session.request = side_effect

    resp = await client.get(
        f"{MANIFEST_PATH}/info/refs?service=git-upload-pack",
        skip_auto_headers=["Accept-Encoding", "Accept", "User-Agent"],
        auth=BasicAuth(*CREDS.split(":")),
        allow_redirects=False,
    )

    assert resp.status == http_code
    assert called == 1
