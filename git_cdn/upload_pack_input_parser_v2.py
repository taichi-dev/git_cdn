# Standard Library
import hashlib
import os
import uuid

from structlog import getLogger

# Third Party Libraries
from git_cdn.packet_line import DELIM_PKT
from git_cdn.packet_line import FLUSH_PKT
from git_cdn.packet_line import RESPONSE_END_PKT
from git_cdn.packet_line import PacketLineParser

log = getLogger()

GIT_CAPS = {  # https://git-scm.com/docs/protocol-v2/en#_capabilities
    # without commands
    b"agent",
    b"server-option",
    b"object-format",
    b"session-id",
}

PROXY_COMMANDS = {
    b"ls-refs",
    b"object-info",
    # + empty request that will return no response (None)
    b"empty request",
}

FEATURES = {
    b"unborn",
    b"shallow",
    b"filter",
    b"ref-in-want",
    b"sideband-all",
    b"packfile-uris",
    b"wait-for-done",
}

ARGS = {
    b"want",
    b"have",
    b"done",
    b"thin-pack",
    b"no-progress",
    b"include-tag",
    b"ofs-delta",
    # if feature shallow
    b"shallow",
    b"deepen",
    b"deepen-relative",
    b"deepen-since",
    b"deepen-not",
    # end if feature shallow
    b"filter",  # if feature filter
    b"want-ref",  # if feature ref-in-want
    b"sideband-all",  # if feature sideband-all
    b"packfile-uris",  # if feature packfile-uris
    b"wait-for-done",  # if feature wait-for-done
}


class UploadPackInputParserV2:
    """implements gramar as per spec in
    https://www.git-scm.com/docs/protocol-v2#_command_request
    """

    class InputParserError(Exception):
        pass

    def __init__(self, input):
        assert isinstance(input, bytes)
        self.input = input
        self.command = b""
        self.caps = {}

        # for 'fetch' command
        self.haves = set()
        self.wants = set()
        self.args = {}  # args without haves and wants
        self.done = False
        self.filter = False
        self.depth = False
        self.depth_lines = []

        self.parse_error = True
        try:
            self.parser = iter(PacketLineParser(input))
            self.parse_caps()

            if self.command == b"":
                raise self.InputParserError("Missing keyword 'command'")
            if self.command in PROXY_COMMANDS:
                return
            if self.command != b"fetch":
                # if we do not know the command, we assume it exists anyway
                # we log a warning to inform us about a potential new command
                log.warning("Unknown command", command=self.command)
                return

            self.parse_args()

            # make sure that the FLUSH_PKT has been found at the end of the input
            if self.parser.i != len(input):
                raise self.InputParserError(
                    "Parser not empty when ending flush packet occured"
                )

            if b"filter" in self.args:
                self.filter = True

            hash = hashlib.sha256()
            hash.update(b"caps")
            for i in sorted(self.caps):
                hash.update(i)
            hash.update(b"haves")
            for i in sorted(self.haves):
                hash.update(i)
            hash.update(b"wants")
            for i in sorted(self.wants):
                hash.update(i)
            hash.update(b"args")
            for i in sorted(self.args):
                hash.update(i)
            for i in sorted(self.depth_lines):
                hash.update(i)
            if self.done:
                hash.update(b"done")
            self.hash = hash.hexdigest()

            self.as_dict = {
                # decoded data to be stored in logstash for analysis
                "caps": b" ".join(sorted(self.caps)).decode(),
                "hash": self.hash[:8],
                "agent": self.caps.get(b"agent", b"na").decode(),
                "haves": b" ".join([x[:8] for x in sorted(self.haves)]).decode(),
                "wants": b" ".join([x[:8] for x in sorted(self.wants)]).decode(),
                "num_haves": len(self.haves),
                "num_wants": len(self.wants),
                "args": b" ".join(sorted(self.args)).decode(),
                "clone": len(self.haves) == 0,
                "single_branch": len(self.wants) == 1,
                "done": self.done,
                "filter": self.filter,
                "depth": self.depth,
                "parse_error": False,
            }
            self.parse_error = False
        except Exception:
            # if we get any error on parsing, we don't fail the rest
            log.exception("while parsing input", bad_input=input.decode())
            self.hash = str(uuid.uuid4())  # get random hash to avoid cashing
            self.parse_error = True
            self.as_dict = {
                "input": input.decode(),
                "parse_error": True,
                "hash": self.hash[:5],
            }

    def parse_caps(self):
        self.caps = {}
        self.command = b""

        pkt = next(self.parser)
        if pkt == FLUSH_PKT:
            self.command = b"empty request"
            return

        while pkt not in (
            FLUSH_PKT,
            DELIM_PKT,
        ):
            if pkt == RESPONSE_END_PKT:
                raise self.InputParserError(
                    "Found RESPONSE_END_PKT during caps parsing"
                )

            line = pkt.rstrip(b"\n")
            line = line.lower()
            if b"=" in line:
                k, v = line.split(b"=", 1)
            else:
                k, v = line, True

            # parsing caps and command at the same time
            # because some clients send the command in the middle of the caps
            # even if it is not documented like that
            if k == b"command":
                self.command = v
            else:
                if k not in GIT_CAPS:
                    log.warning("unknown cap: %r", k)
                self.caps[k] = v
            pkt = next(self.parser)

    def parse_args(self):
        self.args = {}

        pkt = next(self.parser)
        while pkt != FLUSH_PKT:
            if pkt in (DELIM_PKT, RESPONSE_END_PKT):
                raise self.InputParserError(f"Found {pkt} during args parsing")

            line = pkt.rstrip(b"\n")
            line = line.lower()
            if b" " in line:
                k, v = line.split(b" ", 1)

                if k == b"have":
                    self.haves.add(v)
                elif k == b"want":
                    self.wants.add(v)
                else:
                    self.args[k] = v
            else:
                k = line
                self.args[k] = True

                if k == b"done":
                    self.done = True
                if b"deep" in k:
                    self.depth = True
                    self.depth_lines.append(k)

            if k not in ARGS:
                log.warning("unknown arg: %r", k)
            pkt = next(self.parser)

    def __hash__(self):
        return int(self.hash, 16)

    def __repr_(self):
        return "UploadPackInputV2(command={}, caps={}, hash='{}', haves=[{}], wants=[{}], args={}, depth={})".format(
            self.command,
            ",".join(k + ":" + v for k, v in self.caps.items()),
            self.hash,
            ",".join(self.haves),
            ",".join(self.wants),
            ",".join(k + ":" + v for k, v in self.args.items()),
            self.depth,
        )

    def can_be_cached(self):
        """
        by default, cache only for git clone with single branch, and no depth
        multibranch and depth can be enabled with environment variable.
        fetches (with haves > 0) won't benefit from a cache.
        also only cache if self.done=True
        """
        if len(self.haves) != 0 or not self.done:
            return False
        if self.filter:
            return False
        multi = os.getenv("PACK_CACHE_MULTI", "false").lower() in ["true", "1"]
        if not multi and len(self.wants) > 1:
            return False
        depth = os.getenv("PACK_CACHE_DEPTH", "false").lower() in ["true", "1"]
        if not depth and self.depth:
            return False
        return True
