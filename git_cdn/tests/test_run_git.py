import asyncio

import pytest

from git_cdn.repo_cache import RepoCache

# pylint: disable=no-member,unused-argument


def run_git_fake(*args, **kwargs):
    return asyncio.create_subprocess_exec(
        "sleep",
        "0.5",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )


@pytest.mark.asyncio
async def test_run(mocker):
    mocker.patch("git_cdn.repo_cache.exec_git", run_git_fake)
    spycom = mocker.spy(asyncio.subprocess.Process, "communicate")
    rcache = RepoCache("/tmp", "fake", "fake")
    task = asyncio.create_task(rcache.run_git("fake"))
    await task
    assert task.done()
    assert not task.cancelled()
    assert spycom.call_count == 1


@pytest.mark.asyncio
async def test_cancel_run(mocker):
    mocker.patch("git_cdn.repo_cache.exec_git", run_git_fake)
    spycom = mocker.spy(asyncio.subprocess.Process, "communicate")
    rcache = RepoCache("/tmp", "fake", "fake")
    task = asyncio.create_task(rcache.run_git("fake"))
    await asyncio.sleep(0.2)
    task.cancel()
    assert not task.done()
    assert not task.cancelled()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert task.done()
    assert task.cancelled()
    assert spycom.call_count == 2
