import asyncio.subprocess
import datetime
import os

import pytest

from git_cdn.cache_handler.clean_cache import find_older_repo
from git_cdn.cache_handler.common import find_git_repo
from git_cdn.conftest import CREDS
from git_cdn.conftest import GITLAB_REPO_TEST_GROUP
from git_cdn.conftest import GITSERVER_UPSTREAM
from git_cdn.conftest import MANIFEST_PATH
from git_cdn.upload_pack import generate_url

repolist = [
    (f"{GITLAB_REPO_TEST_GROUP}/test_git_cdn.git", "master"),
    (f"{GITLAB_REPO_TEST_GROUP}/platform_external_dnsmasq.git", "pie-release"),
]


@pytest.fixture
async def repocache(make_client, loop, tmpdir, app, header_for_git):
    assert loop

    app = app()
    client = await make_client(app)
    userdir = tmpdir / "user"
    userdir.mkdir()
    userdir.chdir()
    for repo, branch in repolist:
        url = "{}/{}".format(client.baseurl, repo)
        proc = await asyncio.create_subprocess_exec(
            "git",
            *header_for_git,
            "clone",
            url,
            "-b",
            branch,
            stdin=asyncio.subprocess.PIPE,
        )
        assert (await proc.wait()) == 0
    yield tmpdir / "gitCDN"


@pytest.fixture
async def old_atime_repocache(repocache):
    git_cache = repocache / "git"
    now = datetime.datetime.utcnow()
    week_ago = now - datetime.timedelta(days=8)
    os.utime(
        next(find_git_repo(git_cache)).lockfile,
        (int(week_ago.strftime("%s")), int(now.strftime("%s"))),
    )
    yield repocache


@pytest.fixture
async def oldrepocache(repocache):
    git_cache = repocache / "git"
    now = datetime.datetime.utcnow()
    week_ago = now - datetime.timedelta(days=8)
    os.utime(
        next(find_git_repo(git_cache)).lockfile,
        (int(now.strftime("%s")), int(week_ago.strftime("%s"))),
    )
    yield repocache


async def test_find_git_repo(repocache):
    git_cache = repocache / "git"
    list_gits = [i.path for i in find_git_repo(git_cache)]
    assert git_cache / GITLAB_REPO_TEST_GROUP / "test_git_cdn.git" in list_gits
    assert (
        git_cache / GITLAB_REPO_TEST_GROUP / "platform_external_dnsmasq.git"
        in list_gits
    )
    assert len(list_gits) == 2


async def test_find_old(oldrepocache):
    gitcdn_cache = oldrepocache
    gitcdn_cache.chdir()
    assert len(find_older_repo(5)) == 1
    assert not len(find_older_repo(8))


async def test_find_old_atime(old_atime_repocache):
    gitcdn_cache = old_atime_repocache
    gitcdn_cache.chdir()
    assert not len(find_older_repo(5))
    assert not len(find_older_repo(8))
