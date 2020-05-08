# Standard Library
import asyncio
import fcntl
import os
import time
from asyncio.subprocess import Process
from concurrent.futures import CancelledError
from enum import Enum

# Third Party Libraries
import psutil
from aiohttp.abc import AbstractStreamWriter
from aiohttp.web_exceptions import HTTPInternalServerError
from aiohttp.web_exceptions import HTTPUnauthorized
from git_cdn.aiolock import lock
from git_cdn.pack_cache import PackCache
from git_cdn.pack_cache import PackCacheCleaner
from git_cdn.packet_line import to_packet
from git_cdn.upload_pack_input_parser import UploadPackInputParser
from git_cdn.util import backoff
from git_cdn.util import find_directory
from git_cdn.util import get_bundle_paths

# RSWL Dependencies
from logging_configurer import context
from logging_configurer import get_logger

log = get_logger()

GIT_PROCESS_WAIT_TIMEOUT = int(os.getenv("GIT_PROCESS_WAIT_TIMEOUT", "2"))
cache_cleaner = PackCacheCleaner()
BACKOFF_START = float(os.getenv("BACKOFF_START", "0.5"))
BACKOFF_COUNT = int(os.getenv("BACKOFF_COUNT", "5"))
parallel_upload_pack = 0


class TerminateState(Enum):
    Wait = 1
    Term = 2
    Kill = 3

    def increment(self):
        if self != TerminateState.Kill:
            return TerminateState(self.value + 1)
        return self


def log_not_wait(state, pid, cmd):
    if state != TerminateState.Wait:
        log.info("command not yet terminated", state=state.name, pid=pid, cmd=cmd)


def log_proc_termination(proc, cmd):
    if proc.returncode:
        cmd_stdout = ""
        cmd_stderr = ""
        if proc.stderr:
            cmd_stderr = repr(proc.stderr._buffer)
        if proc.stdout:
            cmd_stdout = repr(proc.stdout._buffer)

        # Error 128 on upload-pack is a known issue of git upload-pack and shall be ignored on
        # ctx['depth']==True and ctx['done']==False :
        # https://www.mail-archive.com/git@vger.kernel.org/msg90066.html
        log.info(
            "cmd afterall return state",
            cmd=cmd,
            cmd_stderr=cmd_stderr[:128],
            cmd_stdout=cmd_stdout[:128],
            returncode=proc.returncode,
        )


async def ensure_proc_terminated(
    proc: Process, cmd: str, timeout=GIT_PROCESS_WAIT_TIMEOUT
):
    state = TerminateState.Wait
    while proc.returncode is None:
        log_not_wait(state=state, pid=proc.pid, cmd=cmd)
        try:
            if state == TerminateState.Term:
                proc.terminate()
            if state == TerminateState.Kill:
                proc.kill()
            try:
                await asyncio.wait_for(proc.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                state = TerminateState.increment(state)
        except ProcessLookupError:
            pass
    log_proc_termination(proc, cmd)
    return state


def input_to_ctx(dict_input):
    if "wants" in dict_input:
        del dict_input["wants"]
    if "haves" in dict_input:
        del dict_input["haves"]
    if "caps" in dict_input:
        del dict_input["caps"]
    context.update({"input_details": dict_input})


def generate_url(base, path, auth=None):
    url = base + path
    if auth:
        for proto in "http", "https":
            url = url.replace(proto + "://", proto + "://" + auth + "@")
    return url


class RepoCache:
    def __init__(self, workdir, path, auth, upstream):
        self.directory = find_directory(workdir, path).encode()
        self.auth = auth
        self.lock = self.directory + b".lock"
        self.url = generate_url(upstream, path, auth)
        _, self.bundle_lock, self.bundle_file = get_bundle_paths(workdir, path)
        self.prev_mtime = None

    def exists(self):
        return os.path.isdir(self.directory)

    def mtime(self):
        if self.exists():
            return os.path.getmtime(self.directory)
        return None

    def save_mtime(self):
        self.prev_mtime = self.mtime()

    def utime(self):
        os.utime(self.directory, None)

    def read_lock(self):
        return lock(self.lock, mode=fcntl.LOCK_SH)

    def write_lock(self):
        return lock(self.lock, mode=fcntl.LOCK_EX)

    async def run_git(self, *args):
        """utility which runs a git command, and log outputs
        return stdout, stderr, returncode  via deferred
        """
        t1 = time.time()

        def cleanup(a):
            if isinstance(a, bytes):
                a = a.decode()
            a = a.replace(self.auth, "xx")
            return a

        cleaned_args = [cleanup(a) for a in args]

        log.info("git_cmd start", cmd=cleaned_args)
        fetch_proc = await asyncio.create_subprocess_exec(
            "git",
            *args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout_data, stderr_data = await fetch_proc.communicate()
        await ensure_proc_terminated(fetch_proc, "git " + " ".join(cleaned_args))
        # prevent logging of the creds
        stdout_data = stdout_data.replace(self.auth.encode(), b"<XX>")
        stderr_data = stderr_data.replace(self.auth.encode(), b"<XX>")
        if b"HTTP Basic: Access denied" in stderr_data:
            raise HTTPUnauthorized(reason=stderr_data)

        log.info(
            "git_cmd done",
            cmd=cleaned_args,
            stdout_data=stdout_data.decode(),
            stderr_data=stderr_data.decode(),
            rc=fetch_proc.returncode,
            cmd_duration=time.time() - t1,
        )
        return stdout_data, stderr_data, fetch_proc.returncode

    async def fetch(self):
        for timeout in backoff(BACKOFF_START, BACKOFF_COUNT):
            # fetch all refs (including MRs) and tags, and prune if needed
            _, _, returncode = await self.run_git(
                "--git-dir",
                self.directory,
                "fetch",
                "--prune",
                "--force",
                "--tags",
                self.url,
                "+refs/*:refs/remotes/origin/*",
            )
            if returncode == 0:
                break
            log.warning("fetch failed, trying again", timeout=timeout)
            await asyncio.sleep(timeout)
        self.utime()

    async def clone(self):
        for timeout in backoff(BACKOFF_START, BACKOFF_COUNT):
            async with lock(self.bundle_lock, mode=fcntl.LOCK_SH):
                if os.path.exists(self.bundle_file):
                    # try to clone the bundle file instead
                    _, stderr, returncode = await self.run_git(
                        "clone", "--bare", self.bundle_file, self.directory
                    )
                    if returncode == 0:
                        break
                    # didn't work? erase that file and retry the clone
                    os.unlink(self.bundle_file)

            rm_proc = await asyncio.create_subprocess_exec(
                "rm", "-rf", self.directory, stdin=asyncio.subprocess.PIPE,
            )
            await ensure_proc_terminated(rm_proc, "rm", timeout=3600)
            _, stderr, returncode = await self.run_git(
                "clone", "--bare", self.url, self.directory
            )
            if returncode == 0:
                break
            log.warning("clone failed, trying again", timeout=timeout)
            await asyncio.sleep(timeout)
        if returncode != 0:
            raise HTTPInternalServerError(reason=stderr)

    async def update(self):
        async with self.write_lock():
            if not self.exists():
                await self.clone()
                await self.fetch()
            elif self.prev_mtime == self.mtime():
                await self.fetch()
            self.save_mtime()

    async def force_update(self):
        async with self.write_lock():
            await self.clone()
            await self.fetch()
            self.save_mtime()


class StdOutReader:
    CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 32 * 1024))

    def __init__(self, stdout):
        self.stdout = stdout
        self.firstchunk = None

    async def first_chunk(self, timeout=60):
        for loop in range(10):
            try:
                self.firstchunk = await asyncio.wait_for(
                    self.stdout.read(8), timeout=timeout
                )
                return self.firstchunk
            except asyncio.TimeoutError:
                log.exception(
                    "Firstchunk timeout",
                    loop=loop,
                    buflen=len(self.stdout._buffer),
                    tmpbuf=self.stdout._buffer[:8],
                )
        log.error("Timeout when reading first chunk")
        raise TimeoutError

    async def next_chunk(self):
        return await self.stdout.read(self.CHUNK_SIZE)

    async def read_chunk(self):
        """Read the remaining output of uploadpack, and forward it to the http client
        """
        self.firstchunk, firstchunk = None, self.firstchunk
        if firstchunk:
            return firstchunk
        return await self.next_chunk()

    async def read(self, size):
        self.firstchunk, firstchunk = None, self.firstchunk
        if firstchunk:
            if size > len(firstchunk):
                size -= len(firstchunk)
                data = await self.stdout.readexactly(size)
                return firstchunk + data
            if size == len(firstchunk):
                return firstchunk
            asked = firstchunk[:size]
            self.firstchunk = firstchunk[size:]
            return asked
        return await self.stdout.readexactly(size)


class UploadPackHandler:
    """Unit testable upload-pack handler which automatically call git fetch to update the local copy
    """

    def __init__(
        self, path, writer: AbstractStreamWriter, auth, workdir, upstream, sema=None
    ):
        self.workdir = workdir
        self.upstream = upstream
        self.auth = auth
        self.path = path
        self.sema = sema
        self.directory = find_directory(workdir, path).encode()
        self.writer = writer
        self.rcache = None
        self.pcache = None
        self.not_our_ref = False

    @staticmethod
    async def write_input(proc, input):
        try:
            proc.stdin.write(input)
            await proc.stdin.drain()
        except RuntimeError:
            log.exception("exception while writing to upload-pack stdin")
        except BrokenPipeError:
            # This occur with large input, and upload pack return an early error
            # like "not our ref"
            log.warning("Ignoring BrokenPipeError, while writing to stdin")
        finally:
            proc.stdin.close()

    async def doUploadPack(self, input, forward_error=False):
        global parallel_upload_pack
        t1 = time.time()
        self.not_our_ref = False
        parallel_upload_pack += 1
        cpu_percent = psutil.cpu_percent()
        log.info(
            "Starting upload pack",
            cpu_percent=cpu_percent,
            parallel_upload_pack=parallel_upload_pack,
            upload_pack=1,
        )
        proc = await asyncio.create_subprocess_exec(
            "git-upload-pack",
            "--stateless-rpc",
            self.directory,
            stdout=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            await self.write_input(proc, input.input)
            reader = StdOutReader(proc.stdout)

            firstchunk = await reader.first_chunk()
            error = firstchunk[4:7] == b"ERR"
            log.info(
                "firstchunk from local upload-pack",
                firstchunk=firstchunk,
                uperror=error,
                input=input.input.decode()[:128],
                cmd_duration=time.time() - t1,
            )

            if error:
                if forward_error:
                    await self.flush_to_writer(reader.read_chunk)
                elif b"not our ref" in await reader.next_chunk():
                    self.not_our_ref = True
                return error
            if self.pcache:
                await asyncio.shield(self.pcache.cache_pack(reader.read))
            else:
                await self.flush_to_writer(reader.read_chunk)
        except (CancelledError, ConnectionResetError):
            context.update({"canceled": True})
            log.warning("Client disconnected during upload-pack")
            raise
        except Exception:
            log.exception("upload pack failure")
        finally:
            await ensure_proc_terminated(proc, "git upload-pack")
            parallel_upload_pack -= 1
            log.info(
                "Upload pack done",
                parallel_upload_pack=parallel_upload_pack,
                upload_pack=-1,
            )

    async def write_pack_error(self, error: str):
        log.error("Upload pack, sending error to client", pack_error=error)
        pkt = to_packet(("ERR " + error).encode())
        await self.writer.write(pkt)

    async def flush_to_writer(self, read_func):
        while True:
            chunk = await read_func()
            if not chunk:
                break
            await self.writer.write(chunk)

    async def run_with_cache(self, parsed_input):
        self.pcache = PackCache(parsed_input.hash)
        async with self.pcache.read_lock():
            if self.pcache.exists():
                await self.pcache.send_pack(self.writer)
                return

        async with self.pcache.write_lock():
            # In case 2 threads race for write lock, check again if it has been added in the cache
            if not self.pcache.exists():
                await self.execute(parsed_input)
                # ensure cache size doesn't grow in another coroutine
                asyncio.create_task(cache_cleaner.clean())

        async with self.pcache.read_lock():
            if self.pcache.exists():
                await self.pcache.send_pack(self.writer)
                return
        # In case of error, the error is already forwarded to client in doUploadPack().
        log.warning("Run with cache failed")

    async def run(self, input: bytes):
        """Run the whole process of upload pack, including sending the result to the writer
        """
        parsed_input = UploadPackInputParser(input)
        dict_input = parsed_input.as_dict.copy()
        log.debug("parsed input", input_details=dict_input)
        input_to_ctx(dict_input)
        if parsed_input.parse_error:
            await self.write_pack_error(f"Wrong upload pack input: {input[:128]}")
            return
        if not parsed_input.wants:
            log.warning("Request without wants")
            return
        if parsed_input.can_be_cached():
            await self.run_with_cache(parsed_input)
        else:
            await self.execute(parsed_input)

    async def uploadPack(self, parsed_input, forward_error=False):
        async with self.rcache.read_lock():
            self.rcache.save_mtime()
            if self.rcache.exists():
                if not self.sema:
                    return await self.doUploadPack(parsed_input, forward_error)
                async with self.sema:
                    return await self.doUploadPack(parsed_input, forward_error)
        return True

    async def execute(self, parsed_input):
        """Start the process upload-pack process optimistically.
        Fetch the first line of result to see if there is an error.
        If there is no error, the process output is forwarded to the http client.
        If there is an error, git fetch or git clone are done.
        """
        self.rcache = RepoCache(self.workdir, self.path, self.auth, self.upstream)

        for loop in range(2):
            if not await self.uploadPack(parsed_input):
                return
            log.warning(
                "Upload pack failed, retry", loop=loop, missing_ref=self.not_our_ref
            )
            await self.rcache.update()

        # in case of "not our ref" error, do not clone the whole repo again
        # go directly to forward the error to client
        if not self.not_our_ref:
            if not await self.uploadPack(parsed_input):
                return

            log.warning("Last Try, remove repo, and clone", loop=2, repo_path=self.path)
            await self.rcache.force_update()

        await self.uploadPack(parsed_input, forward_error=True)