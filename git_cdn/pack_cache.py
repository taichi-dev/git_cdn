# Standard Library
import fcntl
import os
import uuid
from datetime import datetime
from time import time

# Third Party Libraries
from git_cdn.aiolock import lock
from git_cdn.packet_line import PacketLineChunkParser
from git_cdn.util import find_directory
from structlog import getLogger
from structlog.contextvars import bind_contextvars

log = getLogger()

# chunk size when reading the cache file
CHUNK_SIZE = 64 * 1024


class PackCache:
    """ Upload pack cache
    when using a local cached repository, git upload-pack will recompress the whole repository,
    which is very CPU intensive.
    cache the binary pack content to disk
    """

    def __init__(self, hash, workdir=None):
        workdir = workdir or os.getenv("WORKING_DIRECTORY", "")
        self.hash = hash
        self.filename = find_directory(
            workdir, os.path.join("pack_cache2", self.hash[:2], self.hash)
        )
        self.hit = True

    def read_lock(self):
        return lock(self.filename, mode=fcntl.LOCK_SH)

    def write_lock(self):
        return lock(self.filename, mode=fcntl.LOCK_EX)

    def delete(self):
        log.info("deleting file", hash=self.hash)
        os.unlink(self.filename)

    def exists(self):
        if os.path.exists(self.filename) and os.stat(self.filename).st_size > 0:
            with open(self.filename, "rb") as f:
                f.seek(-4, os.SEEK_END)
                last_chunk = f.read(4)
                if last_chunk == b"0000":
                    return True
                log.warning("File in cache is corrupted", hash=self.hash)
        return False

    def size(self):
        return os.stat(self.filename).st_size

    async def send_pack(self, writer):
        status = "hit" if self.hit else "miss"
        bind_contextvars(
            upload_pack_status=status,
            cache={"size": self.size(), "filename": self.filename, "hit": self.hit},
        )
        # We always send the pack from the cache, even on cache Miss
        log.info("Serving from pack cache", hash=self.hash, pack_hit=self.hit)
        with open(self.filename, "rb") as f:
            count = 0
            while True:
                data = f.read(CHUNK_SIZE)
                count += len(data)

                bind_contextvars(
                    upload_pack_progress={
                        "date": datetime.now().isoformat(),
                        "sent": count,
                    }
                )
                if not data:
                    break
                try:
                    await writer.write(data)
                except ConnectionResetError:
                    log.warning("connection reset while serving pack cache")
                    break
        # update mtime for LRU
        os.utime(self.filename, None)

    async def cache_pack(self, read_func):
        log.info("Cache Miss, create new cache entry", hash=self.hash)
        self.hit = False
        pkt_parser = PacketLineChunkParser(read_func)
        with open(self.filename, "wb") as f:
            try:
                async for data in pkt_parser:
                    f.write(data)
            except Exception:
                # don't need to raise, if the file is not present, we will try again
                log.exception(
                    "Aborting cache_pack", hash=self.hash, filename=self.filename
                )
                os.unlink(self.filename)


class PackCacheCleaner:
    def __init__(self, workdir=None, max_size=None):
        self.workdir = workdir or os.path.expanduser(os.getenv("WORKING_DIRECTORY", ""))
        self.cachedir = os.path.join(self.workdir, "pack_cache2")
        self.max_size = os.getenv("PACK_CACHE_SIZE_GB", "20")
        # Use cache size minus 512MB, to avoid exceeding the cache size too much.
        self.max_size = (int(self.max_size) * 1024 - 512) * 1024 * 1024
        self.lockfile = find_directory(self.cachedir, "clean.lock")

    def lock(self):
        return lock(self.lockfile, mode=fcntl.LOCK_EX)

    async def _clean(self):
        # When using os.scandir, DirEntry.stat() are cached (on Linux) and calling it
        # doesn't go through syscall
        subdirs = [d for d in os.scandir(self.cachedir) if d.is_dir()]
        subdirs = [os.path.join(self.cachedir, sub) for sub in subdirs]
        all_files = [f for sub in subdirs for f in os.scandir(sub) if f.is_file()]
        total_size = sum(f.stat().st_size for f in all_files)
        log.info(
            "Pack Cache size is",
            size=total_size,
            max_size=self.max_size,
            n_entry=len(all_files),
        )

        if total_size < self.max_size:
            return 0

        all_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)

        rm_size = 0
        to_delete = []
        while total_size - rm_size >= self.max_size:
            f = all_files.pop()
            rm_size += f.stat().st_size
            to_delete.append(f)

        cache_duration = datetime.now() - datetime.fromtimestamp(
            to_delete[-1].stat().st_mtime
        )
        log.info(
            "Pack cache cleaning",
            size=total_size,
            max_size=self.max_size,
            rm_size=rm_size,
            rm_files=len(to_delete),
            cache_duration=cache_duration.total_seconds(),
        )

        for f in to_delete:
            cache = PackCache(f.name, workdir=self.workdir)
            async with cache.write_lock():
                cache.delete()
        return len(to_delete)

    async def clean(self):
        # cleanup is done in another task, so change the ctx uuid
        bind_contextvars(ctx={"uuid": str(uuid.uuid4())})
        # only clean once per minute
        if (
            os.path.exists(self.lockfile)
            and (time() - os.stat(self.lockfile).st_mtime) < 60
        ):
            log.debug("No need to cleanup")
            return

        async with self.lock():
            os.utime(self.lockfile, None)
            return await self._clean()
