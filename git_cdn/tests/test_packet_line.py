# Third Party Libraries
# Standard Library
import os

import pytest
from git_cdn.packet_line import FLUSH_PKT
from git_cdn.packet_line import PacketLineChunkParser
from git_cdn.packet_line import PacketLineParser

BASE_INPUT = (
    b"00a4want 7bc80fd0ada7602695c7819e0105431e3262ad0c multi_ack_detailed "
    b"no-done side-band-64k thin-pack no-progress ofs-delta deepen-since "
    b"deepen-not agent=git/2.20.1\n00000009done\n"
)


def test_parse_pkt_line():
    results = list(PacketLineParser(BASE_INPUT))
    assert results == [
        b"want 7bc80fd0ada7602695c7819e0105431e3262ad0c multi_ack_detailed no-done "
        b"side-band-64k thin-pack no-progress ofs-delta deepen-since deepen-not "
        b"agent=git/2.20.1\n",
        FLUSH_PKT,
        b"done\n",
    ]


def get_data(filename):
    with open(os.path.join(os.path.dirname(__file__), "packs", filename), "rb") as f:
        return f.read()


class DataReader:
    def __init__(self, data):
        self.data = data
        self.offset = 0

    async def read(self, size):
        ret = self.data[self.offset : self.offset + size]
        self.offset += size
        return ret


async def parse(filename):
    data = get_data(filename + ".bin")

    reader = DataReader(data)
    plcp = PacketLineChunkParser(reader.read)

    chunks = [chunk async for chunk in plcp]
    assert b"".join(chunks) == get_data(filename + "_parsed.bin")
    return plcp


async def test_parse_pkt_chunk(loop):
    await parse("pack1")


async def test_parse_pkt_chunk2(loop):
    with pytest.raises(PacketLineChunkParser.ParseError):
        await parse("pack2")


data = get_data("upload_pack.bin")


async def bench_chunk_parser():
    reader = DataReader(data)
    plcp = PacketLineChunkParser(reader.read)
    chunks = [chunk async for chunk in plcp]
    assert chunks


def sync_bench(loop):
    loop.run_until_complete(bench_chunk_parser())


def test_benchmark_chunk_parser(loop, benchmark):
    benchmark(sync_bench, loop)


if __name__ == "__main__":
    # for use with profiling tools
    import asyncio

    for i in range(100):
        asyncio.run(bench_chunk_parser())
