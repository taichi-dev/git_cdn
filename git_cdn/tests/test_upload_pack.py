# Standard Library
import asyncio
import os

# Third Party Libraries
import pytest
from aiohttp.abc import AbstractStreamWriter
from git_cdn.tests.conftest import GITLAB_REPO_TEST_GROUP
from git_cdn.upload_pack import RepoCache
from git_cdn.upload_pack import StdOutReader
from git_cdn.upload_pack import UploadPackHandler
from git_cdn.upload_pack import generate_url
from git_cdn.upload_pack_input_parser import UploadPackInputParser

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

CREDS = os.environ["CREDS"]
MANIFEST_PATH = f"{GITLAB_REPO_TEST_GROUP}/test_git_cdn.git"
UPSTREAM = os.environ["GITSERVER_UPSTREAM"]


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
        MANIFEST_PATH, writer, CREDS, workdir=str(tmpdir / "workdir"), upstream=UPSTREAM
    )

    await proc.run(CLONE_INPUT)
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
        MANIFEST_PATH, writer, CREDS, workdir=str(tmpdir / "workdir"), upstream=UPSTREAM
    )
    await proc.run(HUGE_CLONE_INPUT)
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
        MANIFEST_PATH, writer, CREDS, workdir=str(tmpdir / "workdir"), upstream=UPSTREAM
    )
    await proc.run(HUGE_CLONE_INPUT)
    data = writer.output
    assert b"not our ref" in data


async def test_fetch_needed(tmpdir, loop):
    workdir = tmpdir / "workdir"
    writer = FakeStreamWriter()
    proc = UploadPackHandler(
        MANIFEST_PATH, writer, CREDS, workdir=str(workdir), upstream=UPSTREAM
    )
    # before run(), clone a small part of the repo (no need to bother for async)
    # to simulate the case where we need a fetch
    os.system(
        "git clone --bare {} {} --single-branch --branch initial_commit".format(
            generate_url(proc.upstream, proc.path, proc.auth), proc.directory.decode()
        )
    )

    await proc.run(CLONE_INPUT)
    assert_upload_ok(writer.output)


async def test_unknown_want(tmpdir, loop):
    writer = FakeStreamWriter()

    proc = UploadPackHandler(
        MANIFEST_PATH, writer, CREDS, workdir=str(tmpdir / "workdir"), upstream=UPSTREAM
    )

    await proc.run(
        CLONE_INPUT.replace(
            b"4284b1521b200ba4934ee710a4a538549f1f0f97",
            b"300a8ae00a1b532ed2364437273221e6c696e0c4",
        )
    )
    full = writer.output
    # if fails the most probable issue comes from git version (must have >= 2.16)
    assert b"ERR upload-pack: not our ref" in full


async def test_unknown_want2(tmpdir, loop):
    writer = FakeStreamWriter()

    proc = UploadPackHandler(
        MANIFEST_PATH, writer, CREDS, workdir=str(tmpdir / "workdir"), upstream=UPSTREAM
    )
    parsed_input = UploadPackInputParser(
        CLONE_INPUT.replace(
            b"4284b1521b200ba4934ee710a4a538549f1f0f97",
            b"300a8ae00a1b532ed2364437273221e6c696e0c4",
        )
    )

    proc.rcache = RepoCache(proc.workdir, proc.path, proc.auth, proc.upstream)

    await proc.rcache.update()
    assert await proc.uploadPack(parsed_input) is True
    assert proc.not_our_ref is True


async def test_unknown_want_cache(tmpdir, loop, monkeypatch):
    monkeypatch.setenv("PACK_CACHE_MULTI", "true")
    writer = FakeStreamWriter()
    proc = UploadPackHandler(
        MANIFEST_PATH, writer, CREDS, workdir=str(tmpdir / "workdir"), upstream=UPSTREAM
    )

    await proc.run(
        CLONE_INPUT.replace(
            b"4284b1521b200ba4934ee710a4a538549f1f0f97",
            b"300a8ae00a1b532ed2364437273221e6c696e0c4",
        )
    )
    full = writer.output
    # if fails the most probable issue comes from git version (must have >= 2.16)
    assert b"ERR upload-pack: not our ref" in full


async def test_shallow(tmpdir, loop):
    writer = FakeStreamWriter()
    proc = UploadPackHandler(
        MANIFEST_PATH, writer, CREDS, workdir=str(tmpdir / "workdir"), upstream=UPSTREAM
    )

    await proc.run(SHALLOW_INPUT)
    full = writer.output
    assert full.startswith(b"0034shallow ")
    assert full.endswith(b"0000")


async def test_shallow_trunc(tmpdir, loop):
    writer = FakeStreamWriter()
    proc = UploadPackHandler(
        MANIFEST_PATH,
        writer,
        CREDS,
        workdir=os.environ.get("WORKING_DIRECTORY", str(tmpdir / "workdir")),
        upstream=UPSTREAM,
    )

    await proc.run(SHALLOW_INPUT_TRUNC)
    assert writer.output == b"0000"


async def test_shallow_trunc2(tmpdir, loop):
    writer = FakeStreamWriter()
    # make sur the cache is warm
    proc = UploadPackHandler(
        MANIFEST_PATH, writer, CREDS, workdir=str(tmpdir / "workdir"), upstream=UPSTREAM
    )

    await proc.run(SHALLOW_INPUT)
    full = writer.output
    assert full
    writer = FakeStreamWriter()
    # give corrupted input to upload-pack
    proc = UploadPackHandler(
        MANIFEST_PATH,
        writer,
        CREDS,
        workdir=str(tmpdir / "workdir"),
        upstream="fake_url",
    )
    await proc.run(SHALLOW_INPUT_TRUNC[:-1])
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
        MANIFEST_PATH, writer, CREDS, workdir=str(tmpdir / "workdir"), upstream=UPSTREAM
    )

    await proc.run(clone_input)
    full = writer.output
    if full:
        assert full[4:7] == b"ERR"


async def test_flush_input(tmpdir, loop):
    writer = FakeStreamWriter()
    proc = UploadPackHandler(
        MANIFEST_PATH, writer, CREDS, workdir=str(tmpdir / "workdir"), upstream=UPSTREAM
    )

    await proc.run(b"0000")
    assert not writer.output


class FakeReader:
    def __init__(self, sleeptime):
        self.sleeptime = sleeptime
        self._buffer = []

    async def read(self, _):
        await asyncio.sleep(self.sleeptime)
        return b"data"


async def test_firstchunk():

    reader = StdOutReader(FakeReader(0.1))
    fc = await reader.first_chunk(0.2)
    assert fc == b"data"

    reader = StdOutReader(FakeReader(2))
    with pytest.raises(TimeoutError):
        fc = await reader.first_chunk(0.1)