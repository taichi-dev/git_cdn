import fcntl
import os


class FileLock:
    """Synchrone use of flock, do not use it on gitcdn main thread.
    currently used on pack_cache_cleaner threadpool and on clean_cache synchrone script.
    use git_cdn.aiolock on async context
    """

    def __init__(self, filename):
        self.filename = filename
        self._f = None

    @property
    def exists(self):
        return os.path.exists(self.filename)

    @property
    def mtime(self):
        return os.stat(self.filename).st_mtime

    def lock(self):
        self._f = open(self.filename, "a+")
        fcntl.flock(self._f.fileno(), fcntl.LOCK_EX)
        os.utime(self.filename, None)

    def release(self):
        if self._f:
            fcntl.flock(self._f.fileno(), fcntl.LOCK_UN)
            self._f.close()
        self._f = None

    def __enter__(self):
        self.lock()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()

    def delete(self):
        if self.exists:
            os.unlink(self.filename)
