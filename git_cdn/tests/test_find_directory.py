# Third Party Libraries
from git_cdn.util import find_gitpath


def test_find_gitpath():
    assert find_gitpath("/repo_test/manifest/info/refs") == "repo_test/manifest.git"
    assert find_gitpath("/repo_test/manifest.git/info/refs") == "repo_test/manifest.git"
    assert (
        find_gitpath("/repo_test/manifest.git/git-upload-pack")
        == "repo_test/manifest.git"
    )
