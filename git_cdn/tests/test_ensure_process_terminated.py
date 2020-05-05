# Standard Library
import asyncio
import textwrap

# Third Party Libraries
from git_cdn.upload_pack import TerminateState
from git_cdn.upload_pack import ensure_proc_terminated

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
    ret = await ensure_proc_terminated(proc, "bash", 0.2)
    assert ret == TerminateState.Wait


async def test_term(tmpdir, loop):
    proc = await asyncio.create_subprocess_exec(
        "bash", "-c", SHELLCODE2, stdin=asyncio.subprocess.PIPE
    )
    ret = await ensure_proc_terminated(proc, "bash", 0.2)
    assert ret == TerminateState.Term


async def test_kill(tmpdir, loop):
    proc = await asyncio.create_subprocess_exec(
        "bash", "-c", SHELLCODE3, stdin=asyncio.subprocess.PIPE
    )
    ret = await ensure_proc_terminated(proc, "bash", 0.2)
    assert ret == TerminateState.Kill
