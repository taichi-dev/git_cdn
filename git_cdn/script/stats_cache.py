# Standard Library
import argparse
import os
from datetime import datetime
from shutil import rmtree
import pandas as pd
import numpy as np

NOW = datetime.now()


def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, "Yi", suffix)


# using scandir recursively is faster than using os.walk()
def dir_size(directory):
    size = 0
    for entry in os.scandir(directory):
        if entry.is_symlink():
            continue
        elif entry.is_dir():
            size += dir_size(entry.path)
        elif entry.is_file():
            size += entry.stat().st_size
    return size


class BasePrune:
    def __str__(self):
        return f"{self.path:100}\t{self.age} days\t{self.size_fmt}"

    def __repr__(self):
        return self.__str__()

    @property
    def mtime(self):
        raise NotImplemented

    @property
    def size(self):
        raise NotImplemented

    def to_dict(self):
        return {"mtime":self.mtime, "size":self.size}

    @property
    def age(self):
        return (NOW - self.mtime).days

    @property
    def size_fmt(self):
        return sizeof_fmt(self.size)


class GitRepo(BasePrune):
    def __init__(self, directory):
        self.lockfile = directory.path + ".lock"
        self.path = directory.path
        self._mtime = None
        self._size = None

    @property
    def mtime(self):
        mtime = os.stat(self.lockfile).st_mtime
        return datetime.fromtimestamp(mtime)

    @property
    def size(self):
        if self._size is None:
            self._size = dir_size(self.path)
        return self._size

def find_git_repo(s):
    dir_entries = [e for e in os.scandir(s) if e.is_dir()]
    subgroups = [d for d in dir_entries if not d.name.endswith(".git")]
    repos = [d for d in dir_entries if d.name.endswith(".git")]
    git_repos = [GitRepo(d) for d in repos]
    for subgroup in subgroups:
        yield from find_git_repo(subgroup)
    for g in git_repos:
        yield g


class LfsFile(BasePrune):
    def __init__(self, file):
        self.file = file
        self.path = file.path
        self.lock = file.path + ".lock"
        self._mtime = None

    @property
    def mtime(self):
        if self._mtime is None:
            self._mtime = datetime.fromtimestamp(self.file.stat().st_mtime)
        return self._mtime

    @property
    def size(self):
        return self.file.stat().st_size


def find_lfs(s):
    dir_entries = [e for e in os.scandir(s) if e.is_dir()]
    lfs = [
        LfsFile(f)
        for f in os.scandir(s)
        if f.is_file() and not f.name.endswith(".lock")
    ]

    for directory in dir_entries:
        for g in find_lfs(directory):
            yield g
    for f in lfs:
        yield f


class BundleFile(BasePrune):
    def __init__(self, file):
        self.file = file
        self.path = file.path
        # remove "_clone.bundle" and add ".lock"
        self.lock = file.path[:-13] + ".lock"
        self._mtime = None

    @property
    def mtime(self):
        if self._mtime is None:
            self._mtime = datetime.fromtimestamp(self.file.stat().st_mtime)
        return self._mtime

    @property
    def size(self):
        return self.file.stat().st_size

def find_bundle(s):
    for f in os.scandir(s):
        if f.is_file() and f.path.endswith(".bundle"):
            yield BundleFile(f)


def stats(finder):
    df = pd.DataFrame([g.to_dict() for g in finder])
    ts = df.set_index("mtime") # convert DataFrame to TimeSerie
    print(f"Number of git repos:", ts.count())
    print(ts.resample("1D").agg([np.sum, np.count_nonzero]))
    print(ts.resample("1H").agg([np.sum, np.count_nonzero])[:48])


def stats_cdn_cache():
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--lfs", help="also clean LFS files", action="store_true")
    parser.add_argument(
        "-b", "--bundle", help="also clean Bundle files", action="store_true"
    )
    parser.add_argument(
        "-a", "--all", help="Clean all caches: git repos and LFS", action="store_true"
    )

    args = parser.parse_args()
    workdir = os.path.expanduser(os.getenv("WORKING_DIRECTORY", ""))
    os.chdir(workdir)

    if not args.lfs and not args.bundle:
        stats(find_git_repo("git"))

    if args.lfs or args.all:
        stats(find_lfs("lfs"))

    if args.bundle or args.all:
        stats(find_bundle("bundles"))


if __name__ == "__main__":
    stats_cdn_cache()
