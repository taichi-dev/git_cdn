# Standard Library
import os

# Third Party Libraries
from aiohttp.web_exceptions import HTTPBadRequest

WORKDIR = os.path.expanduser(os.getenv("WORKING_DIRECTORY", "/tmp/workdir"))


def check_path(path):
    if path.startswith("/"):
        raise HTTPBadRequest(reason="bad path: " + path)
    if "/../" in path or path.startswith("../"):
        raise HTTPBadRequest(reason="bad path: " + path)


def get_subdir(subpath):
    """find or create the working directory of the repository path"""
    d = os.path.join(WORKDIR, subpath)
    if not os.path.isdir(d):
        os.makedirs(d, exist_ok=True)
    return d


def get_bundle_paths(git_path):
    """compute the locks and bundle paths"""
    git_path = git_path.rstrip("/")
    assert git_path.endswith(".git")
    bundle_dir = get_subdir("bundles")
    bundle_name = os.path.basename(git_path)[:-4]  # remove ending '.git'
    lock = os.path.join(bundle_dir, bundle_name + ".lock")
    bundle_file = os.path.join(bundle_dir, bundle_name + "_clone.bundle")
    return bundle_name, lock, bundle_file


def backoff(start, count):
    """
    Return generator of backoff retry with factor of 2

    >>> list(backoff(0.1, 5))
    [0.1, 0.2, 0.4, 0.8, 1.6]
    """
    for x in range(count):
        yield start * 2 ** x
