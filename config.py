# Configuration
# Standard Library
import logging
import os

workers = int(os.getenv("GUNICORN_WORKER", "8"))
# set a big timeout to avoid worker being killed, and leaking semaphore
timeout = 3600
# gitCDN requests take can be very long, so try to finish them before killing.
graceful_timeout = 60 * 5

# you can try different worker class
# - aiohttp.worker.GunicornWebWorker (default)
# - aiohttp.worker.GunicornUVLoopWebWorker
worker_class = os.getenv("GUNICORN_WORKER_CLASS", "aiohttp.worker.GunicornWebWorker")

errorlog = "-"
loglevel = "debug"

# if None, there won't be any log to structlog, so push it to /dev/null instead
accesslog = "/dev/null"
access_log_format = '%a "%r" %s %b "%{User-Agent}i" "%{X-FORWARDED-FOR}i" "%{X-CI-JOB-URL}i" "%{X-CI-PROJECT-PATH}i" "%{X-REPO-JOB-URL}i" %D'

log = logging.getLogger()
log.setLevel(logging.DEBUG)
# Upload pack Limit with Semaphores

# Add logs when workers are killed
def worker_int(worker):
    log.error("worker received INT or QUIT signal")

    ## get traceback info
    import sys
    import threading
    import traceback

    id2name = {th.ident: th.name for th in threading.enumerate()}
    code = []
    for threadId, stack in sys._current_frames().items():
        code.append("\n# Thread: %s(%d)" % (id2name.get(threadId, ""), threadId))
        for filename, lineno, name, line in traceback.extract_stack(stack):
            code.append('File: "%s", line %d, in %s' % (filename, lineno, name))
            if line:
                code.append("  %s" % (line.strip()))
    log.warning("\n".join(code))


def worker_abort(worker):
    log.error("worker received SIGABRT signal")


def worker_exit(server, worker):
    log.warning("Worker Exiting")


def child_exit(server, worker):
    log.warning("Child Worker exiting")
