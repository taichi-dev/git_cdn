# Third Party Libraries
import pytest

from git_cdn.upload_pack_input_parser_v2 import UploadPackInputParserV2

from .any import ANYSTRING

# pylint: disable = duplicate-code


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

FETCH_WITH_OBJECT_FORMAT = (
    b"0016object-format=sha10011command=fetch001eagent=git/2.29.2.windows.20001"
    b"000dthin-pack000fno-progress000finclude-tag000dofs-delta"
    b"0032want fcd062d2d06d00fc2a1bf3c8432effccbd186a08\n"
    b"0032want 44667f210351a1a425a6463a204f32279d3b24f3\n0009done\n0000"
)
HASH_WITH_OBJECT_FORMAT = (
    "2a46d98d04e4867c7e4d40efb7919ec117319c55ff2322bb9bb00daa44201089"
)

INPUT_WITH_DEPTH = (
    b"0016object-format=sha10011command=fetch0014agent=git/2.28.00001"
    b"000dthin-pack000fno-progress000finclude-tag000dofs-delta0"
    b"00cdeepen 10032want 2b05463665b52df44b052c02e05c21fa8eab0f60\n"
    b"0032want 2b05463665b52df44b052c02e05c21fa8eab0f60\n0009done\n0000"
)


def test_parse_input_with_depth():
    parser = UploadPackInputParserV2(INPUT_WITH_DEPTH)

    assert parser.command == b"fetch"

    assert parser.haves == set()
    assert parser.wants == {
        b"2b05463665b52df44b052c02e05c21fa8eab0f60",
    }
    assert parser.done
    assert not parser.filter
    assert parser.depth
    assert parser.depth_lines == [b"deepen 1"]

    assert parser.caps == {b"agent": b"git/2.28.0", b"object-format": b"sha1"}

    # assert parser.hash == HASH_FETCH
    assert parser.as_dict == {
        "caps": "agent object-format",
        "hash": "1fbd1e5a",
        "agent": "git/2.28.0",
        "parse_error": False,
        "haves": "",
        "wants": "2b054636",
        "num_haves": 0,
        "num_wants": 1,
        "args": "done include-tag no-progress ofs-delta thin-pack",
        "clone": True,
        "single_branch": True,
        "done": True,
        "filter": False,
        "depth": True,
    }


def test_parse_input_with_fetch():
    parser = UploadPackInputParserV2(INPUT_FETCH)

    assert parser.command == b"fetch"

    assert parser.haves == set()
    assert parser.wants == {
        b"44667f210351a1a425a6463a204f32279d3b24f3",
        b"fcd062d2d06d00fc2a1bf3c8432effccbd186a08",
    }
    assert parser.done
    assert not parser.filter
    assert not parser.depth
    assert not parser.depth_lines

    assert parser.caps == {b"agent": b"git/2.25.1"}

    assert parser.hash == HASH_FETCH
    assert parser.as_dict == {
        "caps": "agent",
        "hash": "1e95621a",
        "agent": "git/2.25.1",
        "parse_error": False,
        "haves": "",
        "wants": "44667f21 fcd062d2",
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

    assert parser.haves == {b"7bc80fd0ada7602695c7819e0105431e3262ad0c"}
    assert parser.wants == {
        b"44667f210351a1a425a6463a204f32279d3b24f3",
        b"fcd062d2d06d00fc2a1bf3c8432effccbd186a08",
    }
    assert parser.done
    assert not parser.filter
    assert not parser.depth
    assert not parser.depth_lines

    assert parser.caps == {b"agent": b"git/2.25.1"}

    assert parser.hash == HASH_WITH_HAVE
    assert parser.as_dict == {
        "caps": "agent",
        "hash": "264287a5",
        "agent": "git/2.25.1",
        "parse_error": False,
        "haves": "7bc80fd0",
        "wants": "44667f21 fcd062d2",
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

    assert parser.haves == set()
    assert parser.wants == {
        b"44667f210351a1a425a6463a204f32279d3b24f3",
        b"fcd062d2d06d00fc2a1bf3c8432effccbd186a08",
    }
    assert parser.done
    assert not parser.filter
    assert not parser.depth
    assert not parser.depth_lines

    assert parser.caps == {b"agent": b"git/2.25.1"}

    assert parser.hash == HASH_WITH_ALL_BASIC_ARGS
    assert parser.as_dict == {
        "caps": "agent",
        "hash": "0caff59b",
        "agent": "git/2.25.1",
        "parse_error": False,
        "haves": "",
        "wants": "44667f21 fcd062d2",
        "num_haves": 0,
        "num_wants": 2,
        "args": "done include-tag no-progress ofs-delta thin-pack",
        "clone": True,
        "single_branch": False,
        "done": True,
        "filter": False,
        "depth": False,
    }


def test_parse_input_with_object_format():
    parser = UploadPackInputParserV2(FETCH_WITH_OBJECT_FORMAT)

    assert parser.command == b"fetch"

    assert parser.haves == set()
    assert parser.wants == {
        b"44667f210351a1a425a6463a204f32279d3b24f3",
        b"fcd062d2d06d00fc2a1bf3c8432effccbd186a08",
    }
    assert parser.done
    assert not parser.filter
    assert not parser.depth
    assert not parser.depth_lines

    assert parser.caps == {b"object-format": b"sha1", b"agent": b"git/2.29.2.windows.2"}

    assert parser.hash == HASH_WITH_OBJECT_FORMAT
    assert parser.as_dict == {
        "caps": "agent object-format",
        "hash": "2a46d98d",
        "agent": "git/2.29.2.windows.2",
        "parse_error": False,
        "haves": "",
        "wants": "44667f21 fcd062d2",
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
    assert parser.wants == {
        b"44667f210351a1a425a6463a204f32279d3b24f3",
        b"fcd062d2d06d00fc2a1bf3c8432effccbd186a08",
    }
    assert parser.hash == HASH_FETCH


def test_parse_upload_pack_input_error():
    data = INPUT_FETCH.replace(b"0011", b"0111")
    parser = UploadPackInputParserV2(data)
    assert parser.wants == set()
    assert parser.haves == set()
    assert not parser.caps
    assert parser.as_dict == {
        "parse_error": True,
        "input": data.decode(),
        "hash": ANYSTRING,
    }


def test_parse_input_fetch_pkt_too_soon():
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


def test_parse_input_without_command():
    data = INPUT_FETCH.replace(b"0011command=fetch", b"")
    parser = UploadPackInputParserV2(data)
    assert parser.as_dict == {
        "parse_error": True,
        "input": data.decode(),
        "hash": ANYSTRING,
    }


def test_parse_input_with_unknown_cap():
    data = INPUT_FETCH.replace(b"agent", b"abcde")
    parser = UploadPackInputParserV2(data)
    assert not parser.as_dict == {
        "parse_error": True,
        "input": data.decode(),
        "hash": ANYSTRING,
    }


def test_parse_input_with_unknown_arg():
    data = INPUT_FETCH.replace(b"want", b"abcd")
    parser = UploadPackInputParserV2(data)
    assert not parser.as_dict == {
        "parse_error": True,
        "input": data.decode(),
        "hash": ANYSTRING,
    }


def test_parse_input_without_flush_pkt():
    """should finish anyway :
    pkt = next(self.parser) will raise a StopIteration exception"""
    data = INPUT_FETCH.replace(b"0000", b"")
    parser = UploadPackInputParserV2(data)
    assert parser.as_dict == {
        "parse_error": True,
        "input": data.decode(),
        "hash": ANYSTRING,
    }


@pytest.mark.parametrize(
    "current",
    [
        b"0001",
        b"0009",
    ],
)
def test_input_with_response_end_pkt(current):
    data = INPUT_FETCH.replace(current, b"0002")
    parser = UploadPackInputParserV2(data)
    assert parser.as_dict == {
        "parse_error": True,
        "input": data.decode(),
        "hash": ANYSTRING,
    }


def test_input_with_two_delim_pkt():
    data = INPUT_FETCH.replace(b"0009", b"0001")
    parser = UploadPackInputParserV2(data)
    assert parser.as_dict == {
        "parse_error": True,
        "input": data.decode(),
        "hash": ANYSTRING,
    }


def test_input_with_two_commands():
    data = INPUT_FETCH.replace(
        b"0011command=fetch", b"0011command=fetch0014command=ls-refs"
    )
    parser = UploadPackInputParserV2(data)
    assert parser.as_dict == {
        "parse_error": True,
        "input": data.decode(),
        "hash": ANYSTRING,
    }
