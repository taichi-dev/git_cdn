# Standard Library
import asyncio
import fcntl
import os
import time
from asyncio.subprocess import Process
from concurrent.futures import CancelledError

# Third Party Libraries
from aiohttp.abc import AbstractStreamWriter
from aiohttp.web_exceptions import HTTPInternalServerError
from aiohttp.web_exceptions import HTTPUnauthorized
from structlog import getLogger
from structlog.contextvars import bind_contextvars

from git_cdn.aiolock import lock
from git_cdn.aiosemaphore import AioSemaphore
from git_cdn.pack_cache import PackCache
from git_cdn.pack_cache import PackCacheCleaner
from git_cdn.packet_line import to_packet
from git_cdn.upload_pack_input_parser import UploadPackInputParser
from git_cdn.util import backoff
from git_cdn.util import get_bundle_paths
from git_cdn.util import get_subdir

log = getLogger()

GIT_PROCESS_WAIT_TIMEOUT = int(os.getenv("GIT_PROCESS_WAIT_TIMEOUT", "2"))
KILLED_PROCESS_TIMEOUT = 30
cache_cleaner = PackCacheCleaner()
BACKOFF_START = float(os.getenv("BACKOFF_START", "0.5"))
BACKOFF_COUNT = int(os.getenv("BACKOFF_COUNT", "5"))


def log_proc_if_error(proc, cmd):
    if not proc.returncode:
        return
    cmd_stderr = proc.stderr._buffer.decode() if proc.stderr else ""
    try:
        # we might be in the middle of upload-pack so the stdout might be binary
        cmd_stdout = proc.stdout._buffer.decode() if proc.stdout else ""
    except UnicodeDecodeError:
        cmd_stdout = "<binary>"

    # Error 128 on upload-pack is a known issue of git upload-pack and shall be ignored on
    # ctx['depth']==True and ctx['done']==False :
    # https://www.mail-archive.com/git@vger.kernel.org/msg90066.html
    log.info(
        "subprocess return an error",
        cmd=cmd,
        cmd_stderr=cmd_stderr[:128],
        cmd_stdout=cmd_stdout[:128],
        pid=proc.pid,
        returncode=proc.returncode,
    )


async def wait_proc(proc, cmd, timeout):
    try:
        await asyncio.wait_for(proc.wait(), timeout=timeout)
        log_proc_if_error(proc, cmd)
        return True
    except asyncio.TimeoutError:
        pass
    return False


async def ensure_proc_terminated(
    proc: Process, cmd: str, timeout=GIT_PROCESS_WAIT_TIMEOUT
):
    if proc.returncode is not None:
        log_proc_if_error(proc, cmd)
        return
    if await wait_proc(proc, cmd, timeout):
        return
    log.error(
        "process didn't exit, terminate it", cmd=cmd, pid=proc.pid, timeout=timeout
    )
    proc.terminate()
    if await wait_proc(proc, cmd, KILLED_PROCESS_TIMEOUT):
        return
    log.error("process didn't exit, kill it", cmd=cmd, pid=proc.pid, timeout=timeout)
    proc.kill()
    if await wait_proc(proc, cmd, KILLED_PROCESS_TIMEOUT):
        return
    log.error("Process didn't exit after kill", cmd=cmd, pid=proc.pid, timeout=timeout)


def input_to_ctx(dict_input):
    if "wants" in dict_input:
        del dict_input["wants"]
    if "haves" in dict_input:
        del dict_input["haves"]
    if "caps" in dict_input:
        del dict_input["caps"]
    bind_contextvars(input_details=dict_input)


def generate_url(base, path, auth=None):
    url = base + path
    if auth:
        for proto in "http", "https":
            url = url.replace(proto + "://", proto + "://" + auth + "@")
    return url


class RepoCache:
    def __init__(self, path, auth, upstream):
        git_cache_dir = get_subdir("git")
        self.directory = os.path.join(git_cache_dir, path).encode()
        self.auth = auth
        self.lock = self.directory + b".lock"
        self.url = generate_url(upstream, path, auth)
        self.path = path
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

        log.debug("git_cmd start", cmd=args)
        fetch_proc = await asyncio.create_subprocess_exec(
            "git",
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout_data, stderr_data = await fetch_proc.communicate()
        await ensure_proc_terminated(fetch_proc, str(args))
        # prevent logging of the creds
        stdout_data = stdout_data.replace(self.auth.encode(), b"<XX>")
        stderr_data = stderr_data.replace(self.auth.encode(), b"<XX>")
        if b"HTTP Basic: Access denied" in stderr_data:
            raise HTTPUnauthorized(reason=stderr_data)

        log.debug(
            "git_cmd done",
            cmd=args,
            stdout_data=stdout_data.decode(errors="replace"),
            stderr_data=stderr_data.decode(errors="replace"),
            rc=fetch_proc.returncode,
            pid=fetch_proc.pid,
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
        _, bundle_lock, bundle_file = get_bundle_paths(self.path)
        for timeout in backoff(BACKOFF_START, BACKOFF_COUNT):
            if os.path.exists(bundle_file):
                async with lock(bundle_lock, mode=fcntl.LOCK_SH):
                    # try to clone the bundle file instead
                    _, stderr, returncode = await self.run_git(
                        "clone", "--bare", bundle_file, self.directory
                    )
                    if returncode == 0:
                        break
                    # didn't work? erase that file and retry the clone
                    os.unlink(bundle_file)

            if self.exists():
                rm_proc = await asyncio.create_subprocess_exec(
                    "rm",
                    "-rf",
                    self.directory,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                await ensure_proc_terminated(
                    rm_proc, f"rm -rf {self.directory}", timeout=3600
                )
            _, stderr, returncode = await self.run_git(
                "clone", "--bare", self.url, self.directory
            )
            if returncode == 0:
                break
            log.warning("clone failed, trying again", timeout=timeout)
            await asyncio.sleep(timeout)
        if returncode != 0:
            raise HTTPInternalServerError(reason=stderr.decode())

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

    async def first_chunk(self):
        self.firstchunk = await self.stdout.read(8)
        return self.firstchunk

    async def next_chunk(self):
        return await self.stdout.read(self.CHUNK_SIZE)

    async def read_chunk(self):
        """Read the remaining output of uploadpack, and forward it to the http client"""
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
    """Unit testable upload-pack handler which automatically call git fetch to update the local copy"""

    def __init__(self, path, writer: AbstractStreamWriter, auth, upstream, sema=None):
        self.upstream = upstream
        self.auth = auth
        self.path = path
        self.sema = sema
        self.writer = writer
        self.rcache = None
        self.pcache = None
        self.not_our_ref = False
        self.forward_error = False

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
            log.warning(
                "Ignoring BrokenPipeError, while writing to stdin", pid=proc.pid
            )
        finally:
            proc.stdin.close()

    async def check_firstchunk(self, reader, input, start_time):
        firstchunk = await reader.first_chunk()
        error = firstchunk[4:7] == b"ERR"
        log.debug(
            "firstchunk from local upload-pack",
            firstchunk=firstchunk,
            uperror=error,
            input=input.input.decode()[:128],
            cmd_duration=time.time() - start_time,
        )
        if error:
            if self.forward_error:
                await self.flush_to_writer(reader.read_chunk)
            elif b"not our ref" in await reader.next_chunk():
                self.not_our_ref = True
        return error

    async def doUploadPack(self, input):
        t1 = time.time()
        self.not_our_ref = False
        proc = await asyncio.create_subprocess_exec(
            "git-upload-pack",
            "--stateless-rpc",
            self.rcache.directory,
            stdout=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            await self.write_input(proc, input.input)
            reader = StdOutReader(proc.stdout)
            if await self.check_firstchunk(reader, input, t1):
                return True
            if self.pcache:
                await asyncio.shield(self.pcache.cache_pack(reader.read))
            else:
                await self.flush_to_writer(reader.read_chunk)
        except (CancelledError, ConnectionResetError):
            bind_contextvars(canceled=True)
            log.warning("Client disconnected during upload-pack")
            raise
        except Exception:
            log.exception("upload pack failure")
        finally:
            # Wait 10 min, for the shielded upload pack to terminate
            # or 2s if not caching, as the process is useless now
            timeout = 10 * 60 if self.pcache else GIT_PROCESS_WAIT_TIMEOUT
            await ensure_proc_terminated(proc, "git upload-pack", timeout)
            log.debug("Upload pack done", pid=proc.pid)

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
                # ensure cache size doesn't grow in a background task
                cache_cleaner.clean()

        async with self.pcache.read_lock():
            if self.pcache.exists():
                await self.pcache.send_pack(self.writer)
                return
        # In case of error, the error is already forwarded to client in doUploadPack().
        log.warning("Run with cache failed")

    async def run(self, input: bytes):
        """Run the whole process of upload pack, including sending the result to the writer"""
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

    async def uploadPack(self, parsed_input):
        async with self.rcache.read_lock():
            self.rcache.save_mtime()
            if self.rcache.exists():
                if not self.sema:
                    return await self.doUploadPack(parsed_input)
                async with AioSemaphore(self.sema):
                    return await self.doUploadPack(parsed_input)
        return True

    async def execute(self, parsed_input):
        """Start the process upload-pack process optimistically.
        Fetch the first line of result to see if there is an error.
        If there is no error, the process output is forwarded to the http client.
        If there is an error, git fetch or git clone are done.
        """
        self.rcache = RepoCache(self.path, self.auth, self.upstream)

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

        # Last try, forward all errors to the client
        self.forward_error = True
        await self.uploadPack(parsed_input)
