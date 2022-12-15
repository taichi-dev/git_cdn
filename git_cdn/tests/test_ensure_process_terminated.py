# Standard Library
import asyncio
import textwrap
from time import time

import pytest

# Third Party Libraries
import git_cdn.util

# pylint: disable=unused-argument

git_cdn.util.KILLED_PROCESS_TIMEOUT = 0.1

SHELLCODE1 = textwrap.dedent(
    """
    sleep 0.1
"""
)

SHELLCODE2 = textwrap.dedent(
    """
    sleep 10
"""
)

SHELLCODE3 = textwrap.dedent(
    """
    trap "echo nope" SIGTERM
    while true;
    do
        sleep 0.1;
    done
"""
)


@pytest.mark.asyncio
async def test_basic(tmpdir, cdn_event_loop):
    proc = await asyncio.create_subprocess_exec(
        "bash",
        "-c",
        SHELLCODE1,
        stdout=asyncio.subprocess.PIPE,
    )
    await git_cdn.util.ensure_proc_terminated(proc, "bash", 0.2)


@pytest.mark.asyncio
async def test_term(tmpdir, cdn_event_loop):
    start_time = time()
    proc = await asyncio.create_subprocess_exec(
        "bash", "-c", SHELLCODE2, stdout=asyncio.subprocess.PIPE
    )
    await git_cdn.util.ensure_proc_terminated(proc, "bash", 0.2)
    elapsed = time() - start_time
    assert elapsed < 2


@pytest.mark.asyncio
async def test_kill(tmpdir, cdn_event_loop):
    start_time = time()
    proc = await asyncio.create_subprocess_exec(
        "bash", "-c", SHELLCODE3, stdout=asyncio.subprocess.PIPE
    )
    await git_cdn.util.ensure_proc_terminated(proc, "bash", 0.2)
    elapsed = time() - start_time
    assert elapsed < 2
