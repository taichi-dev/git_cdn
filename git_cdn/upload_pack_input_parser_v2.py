# Standard Library
import hashlib
import uuid

from structlog import getLogger

# Third Party Libraries
from git_cdn.packet_line import DELIM_PKT
from git_cdn.packet_line import FLUSH_PKT
from git_cdn.packet_line import RESPONSE_END_PKT
from git_cdn.packet_line import PacketLineParser

log = getLogger()

GIT_CAPS = {  # https://git-scm.com/docs/protocol-v2/en#_capabilities
    b"agent",
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
    # fetch command
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

    def __init__(self, input):
        assert isinstance(input, bytes)
        self.input = input
        self.command = b""
        self.caps = {}

        # for 'fetch' command
        self.haves = []
        self.wants = []
        self.args = {}  # args without haves and wants
        self.done = False
        self.filter = False
        self.depth = False
        self.depth_lines = []

        self.parse_error = True
        try:
            self.parser = iter(PacketLineParser(input))
            self.parse_command()
            self.parse_caps()
            assert self.command == b"fetch"

            self.parse_args()
            # make sure that the FLUSH_PKT has been found at the end of the input
            assert self.parser.i == len(input)

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
                "haves": b" ".join([x[:8] for x in self.haves]).decode(),
                "wants": b" ".join([x[:8] for x in self.wants]).decode(),
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

    def parse_command(self):
        self.command = b""

        pkt = next(self.parser)
        if pkt == FLUSH_PKT:
            return

        if pkt[-1] == 10:
            line = pkt[:-1]
        else:
            line = pkt
        line_split = line.split(b"=")
        assert line_split[0].lower() == b"command"
        self.command = line_split[1]

    def parse_caps(self):
        self.caps = {}

        pkt = next(self.parser)
        while pkt not in (
            FLUSH_PKT,
            DELIM_PKT,
            RESPONSE_END_PKT,
        ):
            if pkt[-1] == 10:
                line = pkt[:-1]
            else:
                line = pkt

            if b"=" in line:
                k, v = line.split(b"=", 1)
            else:
                k, v = line, True
            if k not in GIT_CAPS:
                log.warning("unknown cap: %r", k)
                continue
            self.caps[k] = v
            pkt = next(self.parser)
        # RESPONSE_END_PKT should not appear here
        assert pkt in (FLUSH_PKT, DELIM_PKT)

    def parse_args(self):
        self.args = {}

        pkt = next(self.parser)
        while pkt not in (
            FLUSH_PKT,
            DELIM_PKT,
            RESPONSE_END_PKT,
        ):
            if pkt[-1] == 10:
                line = pkt[:-1]
            else:
                line = pkt

            if b" " in line:
                k, v = line.split(b" ", 1)

                if k.lower() == b"have":
                    if v not in self.haves:
                        self.haves.append(v)
                elif k.lower() == b"want":
                    if v not in self.wants:
                        self.wants.append(v)
                else:
                    self.args[k] = v
            else:
                self.args[line] = True

                if line.lower() == b"done":
                    self.done = True
                if b"deep" in line.lower():
                    self.depth = True
                    self.depth_lines.append(line)

                k = line

            if k not in ARGS:
                log.warning("unknown arg: %r", k)
                continue
            pkt = next(self.parser)
        # only FLUSH_PKT should appear here
        assert pkt == FLUSH_PKT

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
        for now we return False
        until we analyze logs and needs and decide what to cache
        """
        return False
