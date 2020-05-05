# Standard Library
import base64

# Third Party Libraries
from git_cdn.app import get_url_creds_from_auth


def basic(l, p):
    return "Basic " + base64.b64encode((l + ":" + p).encode()).decode()


def test_get_url_creds_from_auth():
    assert get_url_creds_from_auth(basic("me", "pass")) == "me:pass"
    assert (
        get_url_creds_from_auth(basic("m@example.com", "pass"))
        == "m%40example.com:pass"
    )
    assert get_url_creds_from_auth(basic("m.com", "pa:ss")) == "m.com:pa%3Ass"
