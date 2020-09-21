# Standard Library
import asyncio
import textwrap
from time import time

# Third Party Libraries
import git_cdn.upload_pack
from git_cdn.upload_pack import ensure_proc_terminated

git_cdn.upload_pack.KILLED_PROCESS_TIMEOUT = 0.1

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


async def test_basic(tmpdir, loop):
    proc = await asyncio.create_subprocess_exec(
        "bash", "-c", SHELLCODE1, stdin=asyncio.subprocess.PIPE
    )
    await ensure_proc_terminated(proc, "bash", 0.2)


async def test_term(tmpdir, loop):
    start_time = time()
    proc = await asyncio.create_subprocess_exec(
        "bash", "-c", SHELLCODE2, stdin=asyncio.subprocess.PIPE
    )
    await ensure_proc_terminated(proc, "bash", 0.2)
    elapsed = time() - start_time
    assert elapsed < 2


async def test_kill(tmpdir, loop):
    start_time = time()
    proc = await asyncio.create_subprocess_exec(
        "bash", "-c", SHELLCODE3, stdin=asyncio.subprocess.PIPE
    )
    await ensure_proc_terminated(proc, "bash", 0.2)
    elapsed = time() - start_time
    assert elapsed < 2
