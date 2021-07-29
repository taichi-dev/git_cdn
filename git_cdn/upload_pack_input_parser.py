# Standard Library
import hashlib
import os
import uuid

from structlog import getLogger

# Third Party Libraries
from git_cdn.packet_line import FLUSH_PKT
from git_cdn.packet_line import PacketLineParser

log = getLogger()

GIT_CAPS = {
    b"ofs-delta",
    b"side-band-64k",
    b"multi_ack",
    b"multi_ack_detailed",
    b"no-done",
    b"thin-pack",
    b"side-band",
    b"agent",
    b"symref",
    b"shallow",
    b"deepen-since",
    b"deepen-not",
    b"deepen-relative",
    b"no-progress",
    b"include-tag",
    b"report-status",
    b"delete-refs",
    b"quiet",
    b"atomic",
    b"push-options",
    b"allow-tip-sha1-in-want",
    b"allow-reachable-sha1-in-want",
    b"push-cert",
    b"filter",
}


class UploadPackInputParser:
    """implements gramar as per spec in http-protocol.txt:

    compute_request   =  want_list
                         have_list
                         request_end
    request_end       =  "0000" / "done"
    want_list         =  PKT-LINE(want SP cap_list LF)
                *(want_pkt)
    want_pkt          =  PKT-LINE(want LF)
    want              =  "want" SP id
    cap_list          =  capability *(SP capability)

    have_list         =  *PKT-LINE("have" SP id LF)
    """

    def __init__(self, input):
        assert isinstance(input, bytes)
        self.input = input
        self.wants = set()
        self.haves = set()
        self.caps = {}
        self.depth = False
        self.depth_lines = []
        self.done = False
        self.parse_error = True
        self.filter = False
        try:
            self.parser = iter(PacketLineParser(input))
            self.parse_header()
            self.parse_lists()
            if b"filter" in self.caps:
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
            for i in sorted(self.depth_lines):
                hash.update(i)
            if self.done:
                hash.update(b"done")
            self.hash = hash.hexdigest()
            self.as_dict = {
                # decoded data to be stored in logstash for analysis
                "haves": b" ".join([x[:8] for x in self.haves]).decode(),
                "wants": b" ".join([x[:8] for x in self.wants]).decode(),
                "caps": b" ".join(sorted(self.caps)).decode(),
                "hash": self.hash[:8],
                "agent": self.caps.get(b"agent", b"na").decode(),
                "num_haves": len(self.haves),
                "num_wants": len(self.wants),
                "clone": len(self.haves) == 0,
                "single_branch": len(self.wants) == 1,
                "parse_error": False,
                "depth": self.depth,
                "done": self.done,
                "filter": self.filter,
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

    def parse_header(self):
        pkt = next(self.parser)
        if pkt == FLUSH_PKT:
            return
        assert pkt[-1] == 10  # \n
        line = pkt[:-1]
        line_split = line.split(b" ")
        assert line_split[0].lower() == b"want"
        self.wants = {line_split[1]}
        self.caps = {}
        for cap in line_split[2:]:
            if b"=" in cap:
                k, v = cap.split(b"=", 1)
            else:
                k, v = cap, True
            if k not in GIT_CAPS:
                log.warning("unknown cap: %r", k)
                continue
            self.caps[k] = v

    def parse_lists(self):
        for pkt in self.parser:
            if pkt == FLUSH_PKT:
                continue

            line = pkt.rstrip(b"\n")
            line_split = line.split(b" ")
            if line_split[0].lower() == b"want":
                self.wants.add(line_split[1])
            if line_split[0].lower() == b"done":
                self.done = True
            if line_split[0].lower() == b"have":
                self.haves.add(line_split[1])
            if b"deep" in line_split[0].lower():
                self.depth = True
                self.depth_lines.append(line)

    def __hash__(self):
        return int(self.hash, 16)

    def __repr__(self):
        return "UploadPackInput(wants=[{}], haves=[{}], caps={}, hash='{}', depth={})".format(
            ",".join(self.wants),
            ",".join(self.haves),
            ",".join(k + ":" + v for k, v in self.caps.items()),
            self.hash,
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
        if b"side-band" not in self.caps and b"side-band-64k" not in self.caps:
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
