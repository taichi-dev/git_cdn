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
    # + None (empty request) that will return no response (None)
    None,
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

    def __init__(self, input_data):
        assert isinstance(input_data, bytes)
        self.input = input_data
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
            self.parser = iter(PacketLineParser(input_data))
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
            if self.parser.i != len(input_data):
                raise self.InputParserError(
                    "Parser not empty when ending flush packet occured"
                )

            if b"filter" in self.args:
                self.filter = True

            self.hash_update()
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
            log.exception("while parsing input", bad_input=input_data.decode())
            self.hash = str(uuid.uuid4())  # get random hash to avoid cashing
            self.parse_error = True
            self.as_dict = {
                "input": input_data.decode(),
                "parse_error": True,
                "hash": self.hash[:5],
            }

    def hash_update(self):
        # pylint: disable=duplicate-code
        computed_hash = hashlib.sha256()
        computed_hash.update(b"caps")
        for i in sorted(self.caps):
            computed_hash.update(i)
        computed_hash.update(b"haves")
        for i in sorted(self.haves):
            computed_hash.update(i)
        computed_hash.update(b"wants")
        for i in sorted(self.wants):
            computed_hash.update(i)
        computed_hash.update(b"args")
        for i in sorted(self.args):
            computed_hash.update(i)
        for i in sorted(self.depth_lines):
            computed_hash.update(i)
        if self.done:
            computed_hash.update(b"done")
        self.hash = computed_hash.hexdigest()
        # pylint: enable=duplicate-code

    def parse_caps(self):
        self.caps = {}
        self.command = b""

        pkt = next(self.parser)
        if pkt == FLUSH_PKT:
            self.command = None
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
                if self.command != b"":
                    cmd_decoded = self.command.decode()
                    raise self.InputParserError(
                        f"Found two commands ({cmd_decoded} and {v.decode()}) instead of one"
                    )
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
                elif b"deep" in k:
                    self.depth = True
                    self.depth_lines.append(line)
                else:
                    self.args[k] = v
            else:
                k = line
                self.args[k] = True

                if k == b"done":
                    self.done = True

            if k not in ARGS:
                log.warning(f"unknown arg: {k!r}")
            pkt = next(self.parser)

    def __hash__(self):
        return int(self.hash, 16)

    def __repr__(self):
        if self.command in (b"", None):
            return f"UploadPackInputV2(command={self.command})"

        caps = b",".join(k + b":" + v for k, v in self.caps.items()).decode()

        if self.command in PROXY_COMMANDS:
            return f"UploadPackInputV2(command={self.command}, caps={caps})"

        haves = ",".join(self.haves)
        wants = (b",".join(list(self.wants)).decode(),)
        args = ",".join(k.decode() + ":" + str(v) for k, v in self.args.items())
        return (
            f"UploadPackInputV2(command={self.command,}, caps={caps}, hash='{self.hash}', "
            f"haves=[{haves}], wants=[{wants}], args={args}, depth={self.depth})"
        )

    def can_be_cached(self):
        """
        by default, cache only for git clone with single branch, and no depth
        multibranch and depth can be enabled with environment variable.
        fetches (with haves > 0) won't benefit from a cache.
        also only cache if self.done=True
        """
        # pylint: disable=duplicate-code
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
        # pylint: enable=duplicate-code
