# RSWL Dependencies
# Standard Library
import asyncio

from logging_configurer import get_logger

log = get_logger()


def to_packet(data, channel=None):
    chan = bytes([channel]) if channel else b""
    size = 4 + len(chan) + len(data)
    header = "{:04x}".format(size).encode()
    return header + chan + data


class __FlushPkt:
    """Marker Class for Flush Packets"""

    def __repr__(self):
        return "FLUSH_PKT"


FLUSH_PKT = __FlushPkt()


class PacketLineParser:
    """a packet line parser inplemented as an iterator"""

    def __init__(self, input):
        assert isinstance(input, bytes)
        self.input = input
        self.i = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.i + 4 > len(self.input):
            raise StopIteration()

        length = self.input[self.i : self.i + 4]
        length = int(length.decode(), 16)
        if length == 0:
            self.i += 4
            return FLUSH_PKT

        if self.i + length > len(self.input):
            raise ValueError(
                "at {} pkt line length {} goes outside buffer".format(self.i, length)
            )

        payload = self.input[self.i + 4 : self.i + length]
        self.i += length
        return payload


class PacketLineChunkParser:
    """Parse git packets on the fly.
        Drop all sideband 2 packet, as they are not meaningful inside the cache
        Replace first sideband 2 packet with a message telling git-cdn is using the cache
    """

    class ParseError(Exception):
        pass

    def __init__(self, read_func):
        self.read = read_func
        self.first_sideband = True

    async def read_header(self):
        hdr = b""
        try:
            hdr = await self.read(4)
            if not hdr:
                return hdr
            if len(hdr) < 4:
                raise self.ParseError(f"Invalid packet header {hdr}")
        except asyncio.IncompleteReadError as e:
            if e.partial:
                raise self.ParseError(f"Invalid packet header {e.partial}")
        return hdr

    async def process_chunks(self):
        endflush = False
        while True:
            hdr = await self.read_header()
            if not hdr:
                break

            pkt_len = int(hdr.decode(), 16)
            if pkt_len == 0:
                yield hdr
                endflush = True
                continue

            if pkt_len < 5:
                raise self.ParseError(f"Invalid packet length {pkt_len}")

            endflush = False
            pkt = await self.read(pkt_len - 4)

            if pkt[0] != 2:
                yield hdr
                yield pkt
            elif self.first_sideband:
                self.first_sideband = False
                yield to_packet(b"git-cdn, using cached pack\n", channel=2)

        if not endflush:
            raise self.ParseError("Missing ending Flush packet")

    def __aiter__(self):
        return self.process_chunks()