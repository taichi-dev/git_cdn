# Third Party Libraries
import pytest

from git_cdn.upload_pack_input_parser_v2 import UploadPackInputParserV2

from .any import ANYSTRING

INPUT_FETCH = (
    b"0011command=fetch0014agent=git/2.25.10001000dthin-pack000dofs-delta"
    b"0032want fcd062d2d06d00fc2a1bf3c8432effccbd186a08\n"
    b"0032want 44667f210351a1a425a6463a204f32279d3b24f3\n0009done\n0000"
)
HASH_FETCH = "1e95621aee9bfc6f9d7eae5aaa9e31c6d8e482f7542b4ce1145e08d0328c9ea8"

FETCH_WITH_HAVE = (
    b"0011command=fetch0014agent=git/2.25.10001000dthin-pack000dofs-delta"
    b"0032want fcd062d2d06d00fc2a1bf3c8432effccbd186a08\n"
    b"0032want 44667f210351a1a425a6463a204f32279d3b24f3\n"
    b"0032have 7bc80fd0ada7602695c7819e0105431e3262ad0c\n0009done\n0000"
)
HASH_WITH_HAVE = "264287a5a069953bfa9e72256674b4a9c857d4908458473171ff7e2100f47acb"

FETCH_WITH_ALL_BASIC_ARGS = (
    b"0011command=fetch0014agent=git/2.25.10001"
    b"000dthin-pack000fno-progress000finclude-tag000dofs-delta"
    b"0032want fcd062d2d06d00fc2a1bf3c8432effccbd186a08\n"
    b"0032want 44667f210351a1a425a6463a204f32279d3b24f3\n0009done\n0000"
)
HASH_WITH_ALL_BASIC_ARGS = (
    "0caff59b65c8f2bab7acf514dce99edb24bf49672d5eecce0642cd4d4bbe0960"
)


def test_parse_input_with_fetch():
    parser = UploadPackInputParserV2(INPUT_FETCH)

    assert parser.command == b"fetch"

    assert parser.haves == []
    assert parser.wants == [
        b"fcd062d2d06d00fc2a1bf3c8432effccbd186a08",
        b"44667f210351a1a425a6463a204f32279d3b24f3",
    ]
    assert parser.done
    assert not parser.filter
    assert not parser.depth
    assert parser.depth_lines == []

    assert parser.caps == {b"agent": b"git/2.25.1"}

    assert parser.hash == HASH_FETCH
    assert parser.as_dict == {
        "caps": "agent",
        "hash": "1e95621a",
        "agent": "git/2.25.1",
        "parse_error": False,
        "haves": "",
        "wants": "fcd062d2 44667f21",
        "num_haves": 0,
        "num_wants": 2,
        "args": "done ofs-delta thin-pack",
        "clone": True,
        "single_branch": False,
        "done": True,
        "filter": False,
        "depth": False,
    }


def test_parse_input_with_haves():
    parser = UploadPackInputParserV2(FETCH_WITH_HAVE)

    assert parser.command == b"fetch"

    assert parser.haves == [b"7bc80fd0ada7602695c7819e0105431e3262ad0c"]
    assert parser.wants == [
        b"fcd062d2d06d00fc2a1bf3c8432effccbd186a08",
        b"44667f210351a1a425a6463a204f32279d3b24f3",
    ]
    assert parser.done
    assert not parser.filter
    assert not parser.depth
    assert parser.depth_lines == []

    assert parser.caps == {b"agent": b"git/2.25.1"}

    assert parser.hash == HASH_WITH_HAVE
    assert parser.as_dict == {
        "caps": "agent",
        "hash": "264287a5",
        "agent": "git/2.25.1",
        "parse_error": False,
        "haves": "7bc80fd0",
        "wants": "fcd062d2 44667f21",
        "num_haves": 1,
        "num_wants": 2,
        "args": "done ofs-delta thin-pack",
        "clone": False,
        "single_branch": False,
        "done": True,
        "filter": False,
        "depth": False,
    }


def test_parse_input_with_all_basic_args():
    parser = UploadPackInputParserV2(FETCH_WITH_ALL_BASIC_ARGS)

    assert parser.command == b"fetch"

    assert parser.haves == []
    assert parser.wants == [
        b"fcd062d2d06d00fc2a1bf3c8432effccbd186a08",
        b"44667f210351a1a425a6463a204f32279d3b24f3",
    ]
    assert parser.done
    assert not parser.filter
    assert not parser.depth
    assert parser.depth_lines == []

    assert parser.caps == {b"agent": b"git/2.25.1"}

    assert parser.hash == HASH_WITH_ALL_BASIC_ARGS
    assert parser.as_dict == {
        "caps": "agent",
        "hash": "0caff59b",
        "agent": "git/2.25.1",
        "parse_error": False,
        "haves": "",
        "wants": "fcd062d2 44667f21",
        "num_haves": 0,
        "num_wants": 2,
        "args": "done include-tag no-progress ofs-delta thin-pack",
        "clone": True,
        "single_branch": False,
        "done": True,
        "filter": False,
        "depth": False,
    }


def test_parse_input_with_duplicated_wants():
    """duplicated haves or wants should not affect parser"""
    FETCH_WITH_DUPLICATED_WANTS = (
        b"0011command=fetch0014agent=git/2.25.10001000dthin-pack000dofs-delta"
        b"0032want fcd062d2d06d00fc2a1bf3c8432effccbd186a08\n"
        b"0032want 44667f210351a1a425a6463a204f32279d3b24f3\n"
        b"0032want fcd062d2d06d00fc2a1bf3c8432effccbd186a08\n0009done\n0000"
    )

    parser = UploadPackInputParserV2(FETCH_WITH_DUPLICATED_WANTS)
    assert parser.wants == [
        b"fcd062d2d06d00fc2a1bf3c8432effccbd186a08",
        b"44667f210351a1a425a6463a204f32279d3b24f3",
    ]
    assert parser.hash == HASH_FETCH


def test_parse_upload_pack_input_error():
    input = INPUT_FETCH.replace(b"0011", b"0111")
    parser = UploadPackInputParserV2(input)
    assert parser.wants == []
    assert parser.haves == []
    assert parser.caps == {}
    assert parser.as_dict == {
        "parse_error": True,
        "input": input.decode(),
        "hash": ANYSTRING,
    }


def test_parse_upload_pack_input_error_2():
    """FETCH_PKT too soon"""
    WRONG_INPUT = (
        b"0011command=fetch0014agent=git/2.25.10001000dthin-pack000dofs-delta"
        b"0032want fcd062d2d06d00fc2a1bf3c8432effccbd186a08\n0000"
        b"0032want 44667f210351a1a425a6463a204f32279d3b24f3\n0009done\n"
    )

    parser = UploadPackInputParserV2(WRONG_INPUT)
    assert parser.as_dict == {
        "parse_error": True,
        "input": WRONG_INPUT.decode(),
        "hash": ANYSTRING,
    }


@pytest.mark.parametrize(
    "current",
    [
        pytest.param(b"0001"),
        pytest.param(b"0009"),
    ],
)
def test_input_with_response_end_pkt(current):
    input = INPUT_FETCH.replace(current, b"0002")
    parser = UploadPackInputParserV2(input)
    assert parser.as_dict == {
        "parse_error": True,
        "input": input.decode(),
        "hash": ANYSTRING,
    }


def test_input_with_two_delim_pkt():
    input = INPUT_FETCH.replace(b"0009", b"0001")
    parser = UploadPackInputParserV2(input)
    assert parser.as_dict == {
        "parse_error": True,
        "input": input.decode(),
        "hash": ANYSTRING,
    }
