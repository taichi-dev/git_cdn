import asyncio.subprocess
import datetime
import os
import subprocess
import threading
import time

import pytest
import pytest_asyncio
import yarl

from git_cdn.cache_handler.clean_cache import Cache
from git_cdn.cache_handler.clean_cache import clean_cdn_cache
from git_cdn.cache_handler.clean_cache import scan_cache
from git_cdn.cache_handler.common import find_git_repo
from git_cdn.conftest import GITLAB_REPO_TEST_GROUP
from git_cdn.util import FileLock

repolist = [
    (f"{GITLAB_REPO_TEST_GROUP}/test_git_cdn.git", "master"),
    (f"{GITLAB_REPO_TEST_GROUP}/platform_external_dnsmasq.git", "pie-release"),
]


@pytest_asyncio.fixture
async def repocache(make_client, cdn_event_loop, tmpdir, app, header_for_git):
    assert cdn_event_loop

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


@pytest_asyncio.fixture
async def old_atime_repocache(repocache):
    git_cache = repocache / "git"
    now = datetime.datetime.utcnow()
    week_ago = now - datetime.timedelta(days=8)
    os.utime(
        next(find_git_repo(git_cache)).lockfile,
        (int(week_ago.strftime("%s")), int(now.strftime("%s"))),
    )
    yield repocache


@pytest_asyncio.fixture
async def oldrepocache(repocache):
    git_cache = repocache / "git"
    now = datetime.datetime.utcnow()
    week_ago = now - datetime.timedelta(days=8)
    os.utime(
        next(find_git_repo(git_cache)).lockfile,
        (int(now.strftime("%s")), int(week_ago.strftime("%s"))),
    )
    yield repocache


@pytest.mark.asyncio
async def test_find_git_repo(repocache):
    git_cache = repocache / "git"
    list_gits = [i.path for i in find_git_repo(git_cache)]
    assert git_cache / GITLAB_REPO_TEST_GROUP / "test_git_cdn.git" in list_gits
    assert (
        git_cache / GITLAB_REPO_TEST_GROUP / "platform_external_dnsmasq.git"
        in list_gits
    )
    assert len(list_gits) == 2


@pytest.fixture
def anotherrepocache(tmpdir, header_for_git):
    gitcdn = tmpdir / "gitcdn"
    gitcdn.mkdir()
    gitrepo = gitcdn / "git"
    gitrepo.mkdir()
    gitrepo.chdir()
    for repo, branch in repolist:
        reponame = os.path.basename(repo)[:-4]
        server_url = os.getenv("GITSERVER_UPSTREAM") or os.getenv("CI_SERVER_URL")
        server_url = server_url[:-1] if server_url.endswith("/") else server_url
        url = yarl.URL(server_url) / repo
        if "CREDS" in os.environ:
            user, _, password = os.getenv("CREDS").partition(":")
            url = url.with_user(user).with_password(password)
        elif "CI_JOB_TOKEN" in os.environ:
            url = url.with_user("gitlab-ci-token").with_password(
                os.getenv("CI_JOB_TOKEN")
            )

        proc = subprocess.check_call(
            ["git", *header_for_git, "clone", str(url), "-b", branch]
        )
        with FileLock(gitrepo / reponame / ".git.lock"):
            time.sleep(0.5)
        assert proc == 0
    time.sleep(1)
    yield gitcdn


def test_clean_repocache(mocker, anotherrepocache):
    mocker.patch(
        "git_cdn.cache_handler.clean_cache.must_clean", return_value=[True, True, False]
    )
    anotherrepocache.chdir()

    lockfilename = FileLock(anotherrepocache / "git" / "test_git_cdn" / ".git.lock")
    lockfilename.lock()

    caches = scan_cache(True, False, False)

    t1 = threading.Thread(
        target=clean_cdn_cache,
        kwargs={"caches": caches, "threshold": 0.1, "delete": True},
    )
    t1.start()
    time.sleep(0.5)

    lockfilename.release()
    assert lockfilename.exists
    t1.join()
    assert not lockfilename.exists
