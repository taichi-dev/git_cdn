# Standard Library
import os

# Third Party Libraries
from aiohttp.web_exceptions import HTTPBadRequest


def check_path(path):
    if path.startswith("/"):
        raise HTTPBadRequest(reason="bad path: " + path)
    if "/../" in path or path.startswith("../"):
        raise HTTPBadRequest(reason="bad path: " + path)


def find_directory(workdir, path):
    """find or create the working directory of the repository path"""
    workdir = os.path.expanduser(workdir)
    d = os.path.join(workdir, path)
    if not os.path.isdir(os.path.dirname(d)):
        os.makedirs(os.path.dirname(d), exist_ok=True)
    return d


def get_bundle_paths(workdir, git_path):
    """compute the locks and bundle paths"""
    workdir = os.path.expanduser(workdir)
    git_path = git_path.rstrip("/")
    assert git_path.endswith(".git")
    workdir = os.path.join(workdir, "bundles")
    if not os.path.isdir(workdir):
        os.makedirs(workdir, exist_ok=True)
    bundle_name = os.path.basename(git_path)[:-4]  # remove ending '.git'
    lock = os.path.join(workdir, bundle_name + ".lock")
    bundle_file = os.path.join(workdir, bundle_name + "_clone.bundle")
    return bundle_name, lock, bundle_file


def backoff(start, count):
    """
    Return generator of backoff retry with factor of 2

    >>> list(backoff(0.1, 5))
    [0.1, 0.2, 0.4, 0.8, 1.6]
    """
    for x in range(count):
        yield start * 2 ** x
