# Configuration
# Standard Library
import os
from multiprocessing import BoundedSemaphore
from multiprocessing import cpu_count

# RSWL Dependencies
from logging_configurer import get_logger

workers = int(os.getenv("NUM_WORKER", "8"))
timeout = 30
# gitCDN requests take can be very long, so try to finish them before killing.
graceful_timeout = 60 * 5
worker_class = "aiohttp.worker.GunicornWebWorker"

errorlog = "-"
loglevel = "debug"
accesslog = "-"
access_log_format = '%a "%r" %s %b "%{User-Agent}i" "%{X-FORWARDED-FOR}i" "%{X-CI-JOB-URL}i" "%{X-CI-PROJECT-PATH}i" "%{X-REPO-JOB-URL}i" %D'



log = get_logger()

# Upload pack Limit with Semaphores


max_semaphore = int(os.getenv("MAX_GIT_UPLOAD_PACK", cpu_count()))
upack_sema = BoundedSemaphore(max_semaphore)


def post_worker_init(worker):
    # Add shared semaphore to gitcdn app
    worker.app.callable.gitcdn.sema = upack_sema


# Add logs when workers are killed
def worker_int(worker):
    log.error("worker received INT or QUIT signal")

    ## get traceback info
    import threading, sys, traceback

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
