# Standard Library
import logging
import socket
import sys
import uuid
from logging.handlers import DatagramHandler

# Third Party Libraries
import structlog
import ujson
from structlog.threadlocal import bind_threadlocal
from structlog.threadlocal import clear_threadlocal


# Workaround for sentry, sentry has a hook on logging, and uses internally str(record.msg)
# if record.msg is a dict, it is unreadable, so return only the message instead
class StructDict(dict):
    def __str__(self):
        if "message" in self:
            return self["message"]
        return super().__str__()


# Move all event_dict fields into extra, and rename event to message for vector.dev
def extra_field(logger, method_name, event_dict):
    message = event_dict.pop("event")
    newdict = StructDict()
    newdict["message"] = message
    if event_dict:
        newdict["extra"] = event_dict
    return newdict


class UdpJsonHandler(DatagramHandler):
    @staticmethod
    def basedict(record):
        return {
            "facility": record.name,
            # "file": record.pathname,
            "line": record.lineno,
            "function": record.funcName,
            "pid": record.process,
            # "thread_name": record.threadName,
            "levelname": record.levelname.lower(),
        }

    def makePickle(self, record):
        msg_dict = self.basedict(record)
        msg_dict = structlog.contextvars.merge_contextvars(None, None, msg_dict)
        msg_dict = structlog.threadlocal.merge_threadlocal(None, None, msg_dict)
        if isinstance(record.msg, dict):
            msg_dict.update(record.msg)
        else:
            msg_dict["message"] = record.getMessage()
        json_msg = (
            ujson.dumps(msg_dict, escape_forward_slashes=False, reject_bytes=False)
            + "\n"
        )
        # Truncate message if too big, UDP has a limit at 64k,
        # take some margin for protocol/wrappers
        if len(json_msg) > 60000:
            trunc_dict = self.basedict(record)
            trunc_dict["message"] = msg_dict["message"][:10000]
            trunc_dict["truncated"] = True
            json_msg = (
                ujson.dumps(
                    trunc_dict, escape_forward_slashes=False, reject_bytes=False
                )
                + "\n"
            )
        return json_msg.encode()


def enable_udp_logs(host="127.0.0.1", port=3465, version=None):
    rlog = logging.getLogger()
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            extra_field,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    udpJsonHandler = UdpJsonHandler(host=host, port=port)
    rlog.addHandler(udpJsonHandler)

    # Add uuid for thread
    clear_threadlocal()
    bind_threadlocal(uuid=str(uuid.uuid4()), application_name="gitcdn")
    host = socket.gethostname()
    if host:
        bind_threadlocal(host=host)
    if version:
        bind_threadlocal(application_version=version)


def enable_console_logs():
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.contextvars.merge_contextvars,
            structlog.processors.TimeStamper(fmt="%H:%M.%S"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    rlog = logging.getLogger()
    out_handler = logging.StreamHandler(sys.stdout)
    rlog.addHandler(out_handler)
