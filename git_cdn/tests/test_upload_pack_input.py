# Standard Library
import json
import os

# Third Party Libraries
import pytest

from git_cdn.upload_pack_input_parser import PacketLineParser
from git_cdn.upload_pack_input_parser import UploadPackInputParser

from .any import ANYBOOL
from .any import ANYINT
from .any import ANYSTRING

BASE_INPUT = (
    b"00a4want 7bc80fd0ada7602695c7819e0105431e3262ad0c multi_ack_detailed "
    b"no-done side-band-64k thin-pack no-progress ofs-delta deepen-since "
    b"deepen-not agent=git/2.20.1\n00000009done\n"
)

INPUT_WITH_HAVE = (
    b"00a4want 3ff9e763a0b11f0c51101b5cb204a12d233f5f65 multi_ack_detailed "
    b"no-done side-band-64k thin-pack no-progress ofs-delta deepen-since "
    b"deepen-not agent=git/2.23.0\n000cdeepen 300000032have "
    b"3ff9e763a0b11f0c51101b5cb204a12d233f5f65\n0009done\n"
)

with open(os.path.join(os.path.dirname(__file__), "upload_pack_inputs.json")) as f:
    _upload_pack_inputs = json.load(f)

TEST_BATCH_NUM = 1


@pytest.fixture(params=range(len(_upload_pack_inputs) // TEST_BATCH_NUM))
def upload_pack_inputs(request):
    return [
        x.encode()
        for x in _upload_pack_inputs[request.param : request.param + TEST_BATCH_NUM]
    ]


def test_parse_pkt_all_input(upload_pack_inputs):
    for i in upload_pack_inputs:
        list(PacketLineParser(i))


def test_parse_upload_pack_input_nodone():
    parser = UploadPackInputParser(BASE_INPUT.replace(b"0009done\n", b""))
    assert not parser.done


def test_parse_upload_pack_input():
    parser = UploadPackInputParser(BASE_INPUT)
    assert parser.wants == [b"7bc80fd0ada7602695c7819e0105431e3262ad0c"]
    assert parser.haves == []
    assert parser.done
    assert parser.caps == {
        b"multi_ack_detailed": True,
        b"no-done": True,
        b"thin-pack": True,
        b"no-progress": True,
        b"side-band-64k": True,
        b"ofs-delta": True,
        b"deepen-since": True,
        b"deepen-not": True,
        b"agent": b"git/2.20.1",
    }
    assert parser.as_dict == {
        "haves": "",
        "wants": "7bc80fd0",
        "hash": "a82bd210",
        "num_haves": 0,
        "num_wants": 1,
        "clone": True,
        "caps": "agent deepen-not deepen-since multi_ack_detailed no-done "
        "no-progress ofs-delta side-band-64k thin-pack",
        "agent": "git/2.20.1",
        "single_branch": True,
        "depth": False,
        "parse_error": False,
        "done": True,
        "filter": False,
    }
    BASE_HASH = "a82bd2107a5a0d8eb001000874a3e1ae1721a9797b5e95ce7522c27ebed0cf23"
    assert parser.hash == BASE_HASH
    # change git version shouldn't change the hash
    parser = UploadPackInputParser(
        BASE_INPUT.replace(b"agent=git/2.20.1", b"agent=git/2.29.7")
    )
    assert parser.hash == BASE_HASH
    # change want version must change the hash
    parser = UploadPackInputParser(
        BASE_INPUT.replace(
            b"7bc80fd0ada7602695c7819e0105431e3262ad0c",
            b"7bc80fd0ada7602695c7519e0105431e3262ad0c",
        )
    )
    assert parser.hash != BASE_HASH
    assert parser.can_be_cached() is True


def test_parse_upload_pack_input_with_have():
    parser = UploadPackInputParser(INPUT_WITH_HAVE)
    assert parser.wants == [b"3ff9e763a0b11f0c51101b5cb204a12d233f5f65"]
    assert parser.haves == [b"3ff9e763a0b11f0c51101b5cb204a12d233f5f65"]
    assert parser.caps == {
        b"agent": b"git/2.23.0",
        b"deepen-not": True,
        b"deepen-since": True,
        b"multi_ack_detailed": True,
        b"no-done": True,
        b"no-progress": True,
        b"ofs-delta": True,
        b"side-band-64k": True,
        b"thin-pack": True,
    }
    assert parser.as_dict == {
        "haves": "3ff9e763",
        "wants": "3ff9e763",
        "hash": "56a6d151",
        "num_haves": 1,
        "num_wants": 1,
        "clone": False,
        "caps": "agent deepen-not deepen-since multi_ack_detailed no-done "
        "no-progress ofs-delta side-band-64k thin-pack",
        "agent": "git/2.23.0",
        "single_branch": True,
        "parse_error": False,
        "depth": True,
        "done": True,
        "filter": False,
    }
    BASE_HASH = "56a6d15154546d03da99bd83b5483b260decffddaf6dd00b41f3e063c45dc021"
    assert parser.hash == BASE_HASH
    assert parser.can_be_cached() is False


def test_parse_upload_pack_all_input(upload_pack_inputs):
    for i in upload_pack_inputs:
        parser = UploadPackInputParser(i)
        assert parser.as_dict == {
            "haves": ANYSTRING,
            "wants": ANYSTRING,
            "hash": ANYSTRING,
            "num_haves": ANYINT,
            "num_wants": ANYINT,
            "clone": ANYBOOL,
            "depth": ANYBOOL,
            "caps": ANYSTRING,
            "agent": ANYSTRING,
            "single_branch": ANYBOOL,
            "parse_error": ANYBOOL,
            "done": ANYBOOL,
            "filter": ANYBOOL,
        }


def test_parse_upload_pack_input_error():
    input = BASE_INPUT.replace(b"00a4", b"01a4")
    parser = UploadPackInputParser(input)
    assert parser.wants == []
    assert parser.haves == []
    assert parser.caps == {}
    assert parser.as_dict == {
        "parse_error": True,
        "input": input.decode(),
        "hash": ANYSTRING,
    }


@pytest.mark.parametrize("i", [b"0000", b"0000" + BASE_INPUT])
def test_parse_pkt_line_with_flush_before_header(i):
    parser = UploadPackInputParser(i)
    assert parser.parse_error is False
