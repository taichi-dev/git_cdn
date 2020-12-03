# -*- coding: utf-8 -*-
# Standard Library
import os

# Third Party Libraries
import aiohttp
import pytest
import yarl

import git_cdn.util
from git_cdn import app as git_cdn_app

GITLAB_REPO_TEST_GROUP = os.getenv("GITLAB_REPO_TEST_GROUP", "grouperenault/repo_test")
GITSERVER_UPSTREAM = os.getenv("GITSERVER_UPSTREAM", "https://gitlab.com/")
MANIFEST_PATH = f"{GITLAB_REPO_TEST_GROUP}/test_git_cdn.git"
CREDS = os.getenv("CREDS", "gitlab-ci-token:{}".format(os.getenv("CI_JOB_TOKEN")))


@pytest.fixture
def tmpworkdir(tmpdir):
    git_cdn.util.WORKDIR = tmpdir
    yield tmpdir


@pytest.fixture
def app(tmpworkdir):
    def _(cached=False):
        return git_cdn_app.make_app(GITSERVER_UPSTREAM)

    yield _


class FakeClient:
    def __init__(self, url, creds):
        self.url = yarl.URL(url)
        user, password = creds.split(":")
        self.url = self.url.with_user(user).with_password(password)
        self.baseurl = str(self.url)

    async def get(self, path, **kw):
        if "auth" in kw:
            del kw["auth"]
        url = self.url.join(yarl.URL(path))
        async with aiohttp.ClientSession() as session:
            async with session.get(url, **kw) as r:
                c = await r.content.read()

                async def fake_read():
                    return c

                r.content.read = fake_read
                return r

    async def post(self, path, **kw):
        if "auth" in kw:
            del kw["auth"]
        url = self.url.join(yarl.URL(path))
        async with aiohttp.ClientSession() as session:
            async with session.post(url, **kw) as r:
                c = await r.content.read()

                async def fake_read():
                    return c

                r.content.read = fake_read
                return r


@pytest.fixture
def header_for_git(request):
    return ["-c", f"http.extraheader=X-CI-INTEG-TEST: {request.node.nodeid}"]


@pytest.fixture
def make_client(aiohttp_client):
    async def ret(app, creds=CREDS):
        if "UNDER_TEST_APP" not in os.environ:
            c = await aiohttp_client(app)
            c.baseurl = "http://{}@localhost:{}".format(creds, c._server.port)
            return c
        return FakeClient(os.environ["UNDER_TEST_APP"], creds)

    yield ret
