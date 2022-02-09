# Standard Library
import asyncio.subprocess
import json
import os
import uuid

# Third Party Libraries
import pytest
from aiohttp.helpers import BasicAuth

from git_cdn.conftest import CREDS
from git_cdn.conftest import GITLAB_REPO_TEST_GROUP
from git_cdn.conftest import GITSERVER_UPSTREAM
from git_cdn.conftest import MANIFEST_PATH


async def test_bad_url(make_client, cdn_event_loop, app):
    assert cdn_event_loop
    app = app()
    client = await make_client(app)
    resp = await client.get(
        "/does_not_exist", auth=BasicAuth(*CREDS.split(":")), allow_redirects=False
    )
    assert resp.status == 302
    # assert we redirect to the upstream server, and not our own users/sign_in
    assert resp.headers["Location"] == GITSERVER_UPSTREAM + "users/sign_in"


async def test_proxy_no_content_encoding(make_client, cdn_event_loop, app, request):
    assert cdn_event_loop
    app = app()
    client = await make_client(app)
    resp = await client.get(
        f"{MANIFEST_PATH}/info/refs?service=git-upload-pack",
        skip_auto_headers=["Accept-Encoding", "Accept", "User-Agent"],
        auth=BasicAuth(*CREDS.split(":")),
        headers=[("X-CI-INTEG-TEST", request.node.nodeid)],
        allow_redirects=False,
    )
    assert resp.status == 200
    assert resp.headers.get("Content-Encoding") != "gzip"


async def test_git_lfs_low_level(make_client, cdn_event_loop, app, request):
    assert cdn_event_loop
    app = app()
    client = await make_client(app)
    resp = await client.post(
        f"{MANIFEST_PATH}/info/lfs/objects/batch",
        auth=BasicAuth(*CREDS.split(":")),
        allow_redirects=False,
        headers={
            "Accept": "application/vnd.git-lfs+json",
            "Content-Type": "application/vnd.git-lfs+json",
            "X-CI-INTEG-TEST": request.node.nodeid,
        },
        json={
            "operation": "download",
            "transfers": ["basic"],
            "ref": {"name": "refs/heads/lfs"},
            "objects": [
                {
                    "oid": "3ecc0bf8cd58b5bcfe371c55bad3bf72a"
                    "ca9dfce0b8f31a99aa565267d71ae05",
                    "size": 196,
                }
            ],
        },
    )
    assert resp.status == 200
    content = await resp.content.read()
    js = json.loads(content)
    assert len(js["objects"]) == 1
    assert "Authorization" in js["objects"][0]["actions"]["download"]["header"]
    href = js["objects"][0]["actions"]["download"]["href"]
    assert "3ecc0bf8cd58b5bcfe371c55bad3bf72aca9dfce0b8f31a99aa565267d71ae05" in href

    assert GITSERVER_UPSTREAM not in href


async def test_git_lfs_low_level_gzip(make_client, cdn_event_loop, app, request):
    assert cdn_event_loop
    app = app()
    client = await make_client(app)
    resp = await client.post(
        f"{MANIFEST_PATH}/info/lfs/objects/batch",
        auth=BasicAuth(*CREDS.split(":")),
        allow_redirects=False,
        headers={
            "Accept": "application/vnd.git-lfs+json",
            "Content-Type": "application/vnd.git-lfs+json",
            "X-CI-INTEG-TEST": request.node.nodeid,
        },
        # data is "big enough" so that gitlab will compress it
        data=b'{"operation":"download","objects":[{"oid":"3ecc0bf8cd58b5bcfe371c55b'
        b'ad3bf72aca9dfce0b8f31a99aa565267d71ae05","size":196},{"oid":"6502889'
        b'40041906a28f3c26e0fd99b6017476f2717c1ae40916fbaa5b94fb49b","size":36}'
        b',{"oid":"acd252ea60584821744f6f3211abe052f9ad48f7ed5346cc5f8b0ea2b886'
        b'6618","size":36},{"oid":"4cfb45cdd094cdafde8e0a2146dbfc914acbbde3dfc3'
        b'9dbc26ee17fc27f1627d","size":36},{"oid":"6ee5b0aba045410d72d341016f7f'
        b'e033cd310e552f51778826f88cf9de4dff37","size":36},{"oid":"e8a7e21c1967'
        b'b15dab4dc2204e642d9cb456c5398c39f09de3d8194168dedcd6","size":36},'
        b'{"oid":"63b950d565efa7527deb876b0df20da2d8e86357aea8ed035f727dc09d'
        b'9414cf","size":36},{"oid":"9eda34bf588dc7503d46203d2f545809dc046e0'
        b'843d27747103b09f99e488c1d","size":36}],"ref":{"name":"refs/heads/lfs"}}',
    )
    assert resp.status == 200
    content = await resp.content.read()
    js = json.loads(content)
    assert len(js["objects"]) == 8
    assert "Authorization" in js["objects"][0]["actions"]["download"]["header"]
    href = js["objects"][0]["actions"]["download"]["href"]
    assert "3ecc0bf8cd58b5bcfe371c55bad3bf72aca9dfce0b8f31a99aa565267d71ae05" in href


@pytest.mark.parametrize("protocol_version", [1, 2])
async def test_basic(
    make_client, cdn_event_loop, tmpdir, app, header_for_git, protocol_version
):
    assert cdn_event_loop

    app = app()
    client = await make_client(app)
    url = "{}/{}".format(client.baseurl, MANIFEST_PATH)
    tmpdir.chdir()
    protocol = f"protocol.version={protocol_version}"
    proc = await asyncio.create_subprocess_exec(
        "git",
        *header_for_git,
        "-c",
        protocol,
        "clone",
        url,
        stdin=asyncio.subprocess.PIPE,
    )
    assert (await proc.wait()) == 0


@pytest.mark.parametrize("protocol_version", [1, 2])
async def test_huge_branch(
    make_client, cdn_event_loop, tmpdir, app, header_for_git, protocol_version
):
    assert cdn_event_loop

    bigbranch = "I_DONT_CREATE_LONG_BRANCH_NAME" * 50
    app = app()
    client = await make_client(app)
    url = "{}/{}".format(client.baseurl, MANIFEST_PATH)
    tmpdir.chdir()
    protocol = f"protocol.version={protocol_version}"
    proc = await asyncio.create_subprocess_exec(
        "git",
        *header_for_git,
        "-c",
        protocol,
        "clone",
        url,
        "-b",
        bigbranch,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert (await proc.wait()) == 128
    assert (await proc.stdout.read()) == b""
    error = (await proc.stderr.read()).decode(errors="ignore")
    assert "fatal: Remote branch " + bigbranch + " not found" in error


@pytest.mark.parametrize("protocol_version", [1, 2])
async def test_no_ending_dot_git(
    make_client, cdn_event_loop, tmpdir, app, header_for_git, protocol_version
):
    assert cdn_event_loop

    app = app()
    client = await make_client(app)
    url = "{}/{}".format(client.baseurl, MANIFEST_PATH[:-4])
    tmpdir.chdir()
    protocol = f"protocol.version={protocol_version}"
    proc = await asyncio.create_subprocess_exec(
        "git",
        *header_for_git,
        "-c",
        protocol,
        "clone",
        url,
        stdin=asyncio.subprocess.PIPE,
    )
    assert (await proc.wait()) == 0


@pytest.mark.parametrize("protocol_version", [1, 2])
async def test_basic_shallow(
    make_client, cdn_event_loop, tmpdir, app, header_for_git, protocol_version
):
    assert cdn_event_loop

    app = app()
    client = await make_client(app)
    url = "{}/{}".format(client.baseurl, MANIFEST_PATH)
    tmpdir.chdir()
    protocol = f"protocol.version={protocol_version}"
    proc = await asyncio.create_subprocess_exec(
        "git",
        *header_for_git,
        "-c",
        protocol,
        "clone",
        "--depth=1",
        url,
        stdin=asyncio.subprocess.PIPE,
    )
    assert (await proc.wait()) == 0


@pytest.mark.parametrize("protocol_version", [1, 2])
async def test_basic_filter(
    make_client, cdn_event_loop, tmpdir, app, header_for_git, protocol_version
):
    assert cdn_event_loop

    app = app()
    client = await make_client(app)
    url = "{}/{}".format(client.baseurl, MANIFEST_PATH)
    tmpdir.chdir()
    protocol = f"protocol.version={protocol_version}"
    proc = await asyncio.create_subprocess_exec(
        "git",
        *header_for_git,
        "-c",
        protocol,
        "clone",
        "--depth=1",
        "--filter=blob:none",
        url,
        stdin=asyncio.subprocess.PIPE,
    )
    assert (await proc.wait()) == 0


@pytest.mark.parametrize("protocol_version", [1, 2])
async def test_git_lfs(
    make_client,
    cdn_event_loop,
    tmpdir,
    app,
    monkeypatch,
    header_for_git,
    protocol_version,
):
    assert cdn_event_loop

    app = app()
    client = await make_client(app)
    url = "{}/{}".format(client.baseurl, MANIFEST_PATH)
    tmpdir.chdir()
    monkeypatch.setenv("GIT_TRACE", 1)
    protocol = f"protocol.version={protocol_version}"
    proc = await asyncio.create_subprocess_exec(
        "git",
        *header_for_git,
        "-c",
        protocol,
        "clone",
        url,
        "-b",
        "lfs",
        "gitdir",
        stdin=asyncio.subprocess.PIPE,
    )
    assert (await proc.wait()) == 0

    if "UNDER_TEST_APP" not in os.environ:
        # in this case, we don't use app
        assert app.served_lfs_objects == 8

    (tmpdir / "gitdir").chdir()
    with open("Readme.zip", "w") as f:
        f.write(str(uuid.uuid4()))

    # cannot run push tests in public pre-commit CI
    if "PUSH_TESTS" in os.environ:
        # create a commit creating and lfs object, and push it to a test_push branch
        proc = await asyncio.create_subprocess_exec(
            "git",
            *header_for_git,
            "-c",
            "protocol.version=1",
            "-c",
            "user.email=test@bot",
            "-c",
            "user.name=test",
            "commit",
            "-m",
            "[ci skip] test_commit",
            "Readme.zip",
            stdin=asyncio.subprocess.PIPE,
        )
        assert (await proc.wait()) == 0
        os.system("git show")
        proc = await asyncio.create_subprocess_exec(
            "git",
            *header_for_git,
            "-c",
            "protocol.version=1",
            "push",
            url,
            "-f",
            "HEAD:test_push",
            stdin=asyncio.subprocess.PIPE,
        )
        assert (await proc.wait()) == 0


@pytest.mark.parametrize("protocol_version", [1, 2])
async def test_push(
    make_client, cdn_event_loop, tmpdir, protocol_version, app, header_for_git
):
    assert cdn_event_loop
    if "PUSH_TESTS" not in os.environ:
        pytest.skip("cannot run push tests in public pre-commit CI")

    app = app()
    client = await make_client(app)
    url = "{}/{}".format(client.baseurl, MANIFEST_PATH)
    tmpdir.chdir()
    protocol = f"protocol.version={protocol_version}"

    proc = await asyncio.create_subprocess_exec(
        "git",
        *header_for_git,
        "-c",
        protocol,
        "clone",
        url,
        "gitdir",
        stdin=asyncio.subprocess.PIPE,
    )
    assert (await proc.wait()) == 0
    (tmpdir / "gitdir").chdir()
    # create an empty commit, and push it to a test_push branch
    proc = await asyncio.create_subprocess_exec(
        "git",
        *header_for_git,
        "-c",
        protocol,
        "-c",
        "user.email=test@bot",
        "-c",
        "user.name=test",
        "commit",
        "-m",
        "[ci skip] test_commit",
        "--allow-empty",
        stdin=asyncio.subprocess.PIPE,
    )
    assert (await proc.wait()) == 0

    proc = await asyncio.create_subprocess_exec(
        "git",
        *header_for_git,
        "-c",
        protocol,
        "push",
        url,
        "-f",
        "HEAD:test_push",
        stdin=asyncio.subprocess.PIPE,
    )
    assert (await proc.wait()) == 0


@pytest.mark.parametrize("num_times", range(2, 43, 10))
@pytest.mark.parametrize("protocol_version", [1, 2])
async def test_parallel(
    make_client,
    cdn_event_loop,
    tmpdir,
    num_times,
    protocol_version,
    app,
    header_for_git,
):
    """test N access in parallel from 2 12 22 32 42"""

    app = app()
    client = await make_client(app)
    url = "{}/{}".format(client.baseurl, MANIFEST_PATH)
    tmpdir.chdir()
    dl = []
    protocol = f"protocol.version={protocol_version}"
    for i in range(num_times):
        proc = await asyncio.create_subprocess_exec(
            "git",
            *header_for_git,
            "-c",
            protocol,
            "clone",
            url,
            "dir" + str(i),
            stdin=asyncio.subprocess.PIPE,
        )
        dl.append(proc.wait())
    rets = await asyncio.gather(*dl, return_exceptions=True)
    assert rets == [0] * num_times


@pytest.mark.parametrize("num_times", range(2, 43, 10))
@pytest.mark.parametrize("protocol_version", [1, 2])
async def test_parallel_with_pack_cache(
    make_client,
    cdn_event_loop,
    tmpdir,
    num_times,
    protocol_version,
    app,
    monkeypatch,
    header_for_git,
):
    """test N access in parallel from 2 12 22 32 42"""
    # ensure new directory for each test
    tmpdir.join(str(num_times))
    monkeypatch.setenv("WORKING_DIRECTORY", str(tmpdir))

    assert cdn_event_loop
    app = app()
    client = await make_client(app)
    url = "{}/{}".format(client.baseurl, MANIFEST_PATH)

    tmpdir.chdir()
    dl = []
    protocol = f"protocol.version={protocol_version}"
    for i in range(num_times):
        proc = await asyncio.create_subprocess_exec(
            "git",
            *header_for_git,
            "-c",
            protocol,
            "clone",
            url,
            "dir" + str(i),
            "--single-branch",
            "-b",
            "master",
            stdin=asyncio.subprocess.PIPE,
        )
        dl.append(proc.wait())
    rets = await asyncio.gather(*dl, return_exceptions=True)
    assert rets == [0] * num_times


@pytest.mark.parametrize("protocol_version", [1, 2])
async def test_pack_cache_with_depth(
    make_client,
    cdn_event_loop,
    tmpdir,
    protocol_version,
    app,
    monkeypatch,
    header_for_git,
):
    monkeypatch.setenv("WORKING_DIRECTORY", str(tmpdir))
    monkeypatch.setenv("PACK_CACHE_DEPTH", "true")

    assert cdn_event_loop
    app = app()
    client = await make_client(app)
    url = "{}/{}".format(client.baseurl, MANIFEST_PATH)

    tmpdir.chdir()
    protocol = f"protocol.version={protocol_version}"
    proc = await asyncio.create_subprocess_exec(
        "git",
        *header_for_git,
        "-c",
        protocol,
        "clone",
        url,
        "dir",
        "--single-branch",
        "-b",
        "master",
        "--depth=2",
        stdin=asyncio.subprocess.PIPE,
    )
    assert (await proc.wait()) == 0


@pytest.mark.parametrize("protocol_version", [1, 2])
async def test_clone_with_bundle(
    make_client, cdn_event_loop, tmpdir, app, header_for_git, protocol_version
):
    assert cdn_event_loop
    app = app()
    DNSMASQ_PATH = GITLAB_REPO_TEST_GROUP + "/platform_external_dnsmasq.git"
    client = await make_client(app)
    url = "{}/{}".format(client.baseurl, DNSMASQ_PATH)
    tmpdir.chdir()
    protocol = f"protocol.version={protocol_version}"
    proc = await asyncio.create_subprocess_exec(
        "curl", "-O", url + "/clone.bundle", stdin=asyncio.subprocess.PIPE
    )
    assert (await proc.wait()) == 0
    proc = await asyncio.create_subprocess_exec(
        "git",
        *header_for_git,
        "-c",
        protocol,
        "clone",
        "-b",
        "pie-release",
        url,
        stdin=asyncio.subprocess.PIPE,
    )
    assert (await proc.wait()) == 0


@pytest.mark.parametrize("protocol_version", [1, 2])
async def test_clone_with_bundle_but_not_exists(
    make_client, cdn_event_loop, tmpdir, app, header_for_git, protocol_version
):
    assert cdn_event_loop
    app = app()
    DNSMASQ_PATH = GITLAB_REPO_TEST_GROUP + "/404/platform_external_dnsmasq.git"
    client = await make_client(app)
    url = "{}/{}".format(client.baseurl, DNSMASQ_PATH)
    tmpdir.chdir()
    proc = await asyncio.create_subprocess_exec(
        "curl", "-O", url + "/clone.bundle", stdin=asyncio.subprocess.PIPE
    )
    assert (await proc.wait()) == 0
    protocol = f"protocol.version={protocol_version}"
    proc = await asyncio.create_subprocess_exec(
        "git",
        *header_for_git,
        "-c",
        protocol,
        "clone",
        "-b",
        "pie-release",
        url,
        stdin=asyncio.subprocess.PIPE,
    )
    assert (await proc.wait()) == 128


async def test_browser_ua(make_client, cdn_event_loop, app, request):
    assert cdn_event_loop
    app = app()
    client = await make_client(app)
    resp = await client.get(
        "/group",
        allow_redirects=False,
        headers={
            "User-Agent": "Mozilla/5 compatible",
            "X-CI-INTEG-TEST": request.node.nodeid,
        },
    )
    assert resp.status == 308
    # assert we redirect to the upstream server
    assert resp.headers["Location"] == GITSERVER_UPSTREAM + "group"


async def test_clonebundle_404(make_client, cdn_event_loop, app, request):
    assert cdn_event_loop
    app = app()
    client = await make_client(app)
    resp = await client.get(
        "/python/google-repo/clone.bundle",
        allow_redirects=False,
        headers={"User-Agent": "curl", "X-CI-INTEG-TEST": request.node.nodeid},
    )
    assert resp.status == 404


async def test_clonebundle_200(make_client, cdn_event_loop, app, request):
    assert cdn_event_loop
    app = app()
    client = await make_client(app)
    resp = await client.get(
        "/whatever/platform_external_javapoet/clone.bundle",
        allow_redirects=False,
        headers={"User-Agent": "curl", "X-CI-INTEG-TEST": request.node.nodeid},
    )
    assert resp.status == 200
    body = await resp.content.read()
    # real body verification is in test_clone_bundle_manager.py
    assert len(body) > 1024 * 1024
