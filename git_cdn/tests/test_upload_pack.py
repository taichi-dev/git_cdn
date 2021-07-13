# Standard Library
import os

# Third Party Libraries
import pytest
from aiohttp.abc import AbstractStreamWriter

from git_cdn.tests.conftest import CREDS
from git_cdn.tests.conftest import GITLAB_REPO_TEST_GROUP
from git_cdn.tests.conftest import GITSERVER_UPSTREAM
from git_cdn.upload_pack import RepoCache
from git_cdn.upload_pack import UploadPackHandler
from git_cdn.upload_pack import generate_url
from git_cdn.upload_pack_input_parser import UploadPackInputParser
from git_cdn.upload_pack_input_parser_v2 import UploadPackInputParserV2

CLONE_INPUT = (
    b"""0098want 4284b1521b200ba4934ee710a4a538549f1f0f97 multi_ack_detailed no-done """
    b"""side-band-64k thin-pack ofs-delta deepen-since deepen-not agent=git/2.15.1
0032want 8f6312ec029e7290822bed826a05fd81e65b3b7c
00000009done
"""
)
SHALLOW_INPUT = (
    b"""00a4want 4284b1521b200ba4934ee710a4a538549f1f0f97 multi_ack_detailed """
    b"""no-done side-band-64k thin-pack include-tag ofs-delta deepen-since """
    b"""deepen-not agent=git/2.16.1
000cdeepen 10000
"""
)

SHALLOW_INPUT_TRUNC = (
    b"""00a4want 4284b1521b200ba4934ee710a4a538549f1f0f97 multi_ack_detailed """
    b"""no-done side-band-64k thin-pack no-progress ofs-delta deepen-since """
    b"""deepen-not agent=git/2.16.2\n"""
    b"""0034shallow 4284b1521b200ba4934ee710a4a538549f1f0f97000cdeepen 10000"""
)

INPUT_FETCH = (
    b"0011command=fetch0014agent=git/2.25.10001000dthin-pack000dofs-delta"
    b"0032want 8f6312ec029e7290822bed826a05fd81e65b3b7c\n"
    b"0032want 4284b1521b200ba4934ee710a4a538549f1f0f97\n0009done\n0000"
)

MANIFEST_PATH = f"{GITLAB_REPO_TEST_GROUP}/test_git_cdn.git"

PROTOCOL_VERSION = 1


class FakeStreamWriter(AbstractStreamWriter):
    """fake stream writer."""

    buffer_size = 0
    output_size = 0
    length = 0
    _output = b""
    _eof_written = False

    @property
    def output(self):
        # assert self._eof_written
        return self._output

    async def write(self, chunk: bytes) -> None:
        assert not self._eof_written
        self._output += chunk
        self.length += len(chunk)

    async def write_eof(self, chunk: bytes = b"") -> None:
        self._eof_written = True

    async def drain(self) -> None:
        pass

    def enable_compression(self, encoding: str = "deflate") -> None:
        pass

    def enable_chunking(self) -> None:
        pass

    async def write_headers(self, status_line, headers) -> None:
        pass


def assert_upload_ok(data):
    assert data.startswith(b"0008NAK\n")
    assert data.endswith(b"0000")


async def test_basic(tmpdir, loop):
    writer = FakeStreamWriter()
    proc = UploadPackHandler(
        MANIFEST_PATH, writer, CREDS, GITSERVER_UPSTREAM, PROTOCOL_VERSION
    )

    content = UploadPackInputParser(CLONE_INPUT)
    await proc.run(content)
    assert_upload_ok(writer.output)


async def test_huge(tmpdir, loop):
    # we write 1000 times the same want (simulating a repo with tons of branch on the same commit)
    HUGE_CLONE_INPUT = (
        b"0098want 4284b1521b200ba4934ee710a4a538549f1f0f97 multi_ack_detailed no-done "
        b"side-band-64k thin-pack ofs-delta deepen-since deepen-not agent=git/2.15.1\n"
    )
    HUGE_CLONE_INPUT += b"0032want 8f6312ec029e7290822bed826a05fd81e65b3b7c\n" * 2000
    HUGE_CLONE_INPUT += b"00000009done\n"
    writer = FakeStreamWriter()
    proc = UploadPackHandler(
        MANIFEST_PATH, writer, CREDS, GITSERVER_UPSTREAM, PROTOCOL_VERSION
    )
    content = UploadPackInputParser(HUGE_CLONE_INPUT)
    await proc.run(content)
    assert_upload_ok(writer.output)


async def test_huge2(tmpdir, loop):
    # we write 15000 times the same unknow want
    # git upload pack will close stdin before reading all the input,
    # and write the error to stdout (occurs in production on mirrors repo)
    HUGE_CLONE_INPUT = (
        b"0098want 4284b1521b200ba4934ee710a4a538549f1f0f97 multi_ack_detailed no-done "
        b"side-band-64k thin-pack ofs-delta deepen-since deepen-not agent=git/2.15.1\n"
    )
    HUGE_CLONE_INPUT += b"0032want 7f6312ec029e7290822bed826a05fd81e65b3b7c\n" * 15000
    HUGE_CLONE_INPUT += b"00000009done\n"
    writer = FakeStreamWriter()
    proc = UploadPackHandler(
        MANIFEST_PATH, writer, CREDS, GITSERVER_UPSTREAM, PROTOCOL_VERSION
    )
    content = UploadPackInputParser(HUGE_CLONE_INPUT)
    await proc.run(content)
    data = writer.output
    assert b"not our ref" in data


async def test_fetch_needed(tmpdir, loop):
    workdir = tmpdir / "workdir"
    writer = FakeStreamWriter()

    proc = UploadPackHandler(
        MANIFEST_PATH, writer, CREDS, GITSERVER_UPSTREAM, PROTOCOL_VERSION
    )
    # before run(), clone a small part of the repo (no need to bother for async)
    # to simulate the case where we need a fetch
    os.system(
        "git clone --bare {} {} --single-branch --branch initial_commit".format(
            generate_url(proc.upstream, proc.path, proc.auth),
            (workdir / "git" / MANIFEST_PATH),
        )
    )

    content = UploadPackInputParser(CLONE_INPUT)
    await proc.run(content)
    assert_upload_ok(writer.output)


async def test_unknown_want(tmpdir, loop):
    writer = FakeStreamWriter()
    proc = UploadPackHandler(
        MANIFEST_PATH, writer, CREDS, GITSERVER_UPSTREAM, PROTOCOL_VERSION
    )

    content = UploadPackInputParser(
        CLONE_INPUT.replace(
            b"4284b1521b200ba4934ee710a4a538549f1f0f97",
            b"300a8ae00a1b532ed2364437273221e6c696e0c4",
        )
    )
    await proc.run(content)
    full = writer.output
    # if fails the most probable issue comes from git version (must have >= 2.16)
    assert b"ERR upload-pack: not our ref" in full


async def test_unknown_want2(tmpdir, loop):
    writer = FakeStreamWriter()
    proc = UploadPackHandler(
        MANIFEST_PATH, writer, CREDS, GITSERVER_UPSTREAM, PROTOCOL_VERSION
    )
    parsed_input = UploadPackInputParser(
        CLONE_INPUT.replace(
            b"4284b1521b200ba4934ee710a4a538549f1f0f97",
            b"300a8ae00a1b532ed2364437273221e6c696e0c4",
        )
    )

    proc.rcache = RepoCache(proc.path, proc.auth, proc.upstream)

    await proc.rcache.update()
    assert await proc.uploadPack(parsed_input) is True
    assert proc.not_our_ref is True


async def test_unknown_want_cache(tmpdir, loop, monkeypatch):
    monkeypatch.setenv("PACK_CACHE_MULTI", "true")
    writer = FakeStreamWriter()
    proc = UploadPackHandler(
        MANIFEST_PATH, writer, CREDS, GITSERVER_UPSTREAM, PROTOCOL_VERSION
    )

    content = UploadPackInputParser(
        CLONE_INPUT.replace(
            b"4284b1521b200ba4934ee710a4a538549f1f0f97",
            b"300a8ae00a1b532ed2364437273221e6c696e0c4",
        )
    )
    await proc.run(content)
    full = writer.output
    # if fails the most probable issue comes from git version (must have >= 2.16)
    assert b"ERR upload-pack: not our ref" in full


async def test_shallow(tmpdir, loop):
    writer = FakeStreamWriter()
    proc = UploadPackHandler(
        MANIFEST_PATH, writer, CREDS, GITSERVER_UPSTREAM, PROTOCOL_VERSION
    )

    content = UploadPackInputParser(SHALLOW_INPUT)
    await proc.run(content)
    full = writer.output
    assert full.startswith(b"0034shallow ")
    assert full.endswith(b"0000")


async def test_shallow_trunc(tmpdir, loop):
    writer = FakeStreamWriter()
    proc = UploadPackHandler(
        MANIFEST_PATH,
        writer,
        CREDS,
        GITSERVER_UPSTREAM,
        PROTOCOL_VERSION,
    )

    content = UploadPackInputParser(SHALLOW_INPUT_TRUNC)
    await proc.run(content)
    assert writer.output == b"0000"


async def test_shallow_trunc2(tmpdir, loop):
    writer = FakeStreamWriter()
    # make sur the cache is warm
    proc = UploadPackHandler(
        MANIFEST_PATH, writer, CREDS, GITSERVER_UPSTREAM, PROTOCOL_VERSION
    )

    content = UploadPackInputParser(SHALLOW_INPUT)
    await proc.run(content)
    full = writer.output
    assert full
    writer = FakeStreamWriter()
    # give corrupted input to upload-pack
    proc = UploadPackHandler(
        MANIFEST_PATH,
        writer,
        CREDS,
        upstream="fake_url",
        protocol_version=PROTOCOL_VERSION,
    )
    content = UploadPackInputParser(SHALLOW_INPUT_TRUNC[:-1])
    await proc.run(content)
    assert writer.output == b""


@pytest.mark.parametrize(
    "clone_input",
    [
        pytest.param(CLONE_INPUT[:-1] + b"A", id="detected by git"),
        pytest.param(CLONE_INPUT[:-1], id="detected by gitcdn input parser"),
    ],
)
async def test_wrong_input(tmpdir, loop, clone_input):
    writer = FakeStreamWriter()
    proc = UploadPackHandler(
        MANIFEST_PATH, writer, CREDS, GITSERVER_UPSTREAM, PROTOCOL_VERSION
    )

    content = UploadPackInputParser(clone_input)
    await proc.run(content)
    full = writer.output
    if full:
        assert full[4:7] == b"ERR"


async def test_flush_input(tmpdir, loop):
    writer = FakeStreamWriter()
    proc = UploadPackHandler(
        MANIFEST_PATH, writer, CREDS, GITSERVER_UPSTREAM, PROTOCOL_VERSION
    )

    content = UploadPackInputParser(b"0000")
    await proc.run(content)
    assert not writer.output


@pytest.mark.parametrize(
    "ref, in_repo",
    [
        (
            [
                b"8f6312ec029e7290822bed826a05fd81e65b3b7c",
                b"4284b1521b200ba4934ee710a4a538549f1f0f97",
            ],
            True,
        ),
        (
            [
                b"8f6312ec029e7290822bed826a05fd81e65b3b7c",
                b"4284b1521b200ba4934ee710a4a538549f1f0f96",
            ],
            False,
        ),
    ],
)
async def test_check_input_wants(tmpdir, loop, ref, in_repo):
    writer = FakeStreamWriter()
    proc = UploadPackHandler(
        MANIFEST_PATH, writer, CREDS, GITSERVER_UPSTREAM, PROTOCOL_VERSION
    )

    proc.rcache = RepoCache(proc.path, proc.auth, proc.upstream)

    await proc.rcache.update()
    assert (await proc.check_input_wants(ref)) == in_repo


async def test_ensure_input_wants_in_rcache(tmpdir, loop, mocker):
    wants = [
        b"8f6312ec029e7290822bed826a05fd81e65b3b7c",
        b"4284b1521b200ba4934ee710a4a538549f1f0f97",
    ]

    workdir = tmpdir / "workdir"
    path = "{}/git/{}".format(workdir, MANIFEST_PATH)

    writer = FakeStreamWriter()
    proc = UploadPackHandler(
        MANIFEST_PATH, writer, CREDS, GITSERVER_UPSTREAM, PROTOCOL_VERSION
    )
    proc.rcache = RepoCache(path, proc.auth, proc.upstream)

    # before run(), clone a small part of the repo (no need to bother for async)
    # to simulate the case where we have not all refs
    os.system(
        "git clone --bare {} {} --single-branch --branch initial_commit".format(
            generate_url(proc.upstream, proc.path, proc.auth),
            (workdir / "git" / MANIFEST_PATH),
        )
    )

    assert proc.rcache.exists()
    mock_fetch = mocker.patch.object(proc.rcache, "fetch")
    mock_update = mocker.patch.object(proc.rcache, "force_update")

    await proc.ensure_input_wants_in_rcache(wants)
    mock_fetch.assert_called_once()
    mock_update.assert_not_called()


async def execute(proc, parsed_input):
    await proc.ensure_input_wants_in_rcache(parsed_input.wants)
    if not await proc.uploadPack(parsed_input):
        return
    await proc.rcache.update()


async def test_uploadPack_runs_well(tmpdir, loop, mocker):
    """tests that the 'uploadPack' method runs well
    when running 'execute' method with a repo with missing 'wants'
    """
    parsed_input = UploadPackInputParserV2(INPUT_FETCH)

    workdir = tmpdir / "workdir"
    path = "{}/git/{}".format(workdir, MANIFEST_PATH)

    writer = FakeStreamWriter()
    proc = UploadPackHandler(
        MANIFEST_PATH, writer, CREDS, GITSERVER_UPSTREAM, PROTOCOL_VERSION
    )
    proc.rcache = RepoCache(path, proc.auth, proc.upstream)

    # before run(), clone a small part of the repo (no need to bother for async)
    # to simulate the case where we have not all refs
    os.system(
        "git clone --bare {} {} --single-branch --branch initial_commit".format(
            generate_url(proc.upstream, proc.path, proc.auth),
            (workdir / "git" / MANIFEST_PATH),
        )
    )
    assert proc.rcache.exists()
    mock_update = mocker.patch.object(proc.rcache, "update")

    await execute(proc, parsed_input)
    mock_update.assert_not_called()
