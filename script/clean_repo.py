# Standard Library
import argparse
import os
from datetime import datetime
from shutil import rmtree

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


class GitRepo:
    def __init__(self, directory):
        self.lockfile = directory.path + ".lock"
        self.directory = directory
        self._mtime = None
        self._size = None

    def __str__(self):
        return f"{self.directory.path:100}\t{self.age} days"

    def __repr__(self):
        return self.__str__()

    @property
    def mtime(self):
        if self._mtime is None:
            self._mtime = datetime.fromtimestamp(self.directory.stat().st_mtime)
        return self._mtime

    @property
    def age(self):
        return (NOW - self.mtime).days

    @property
    def size(self):
        if self._size is None:
            self._size = dir_size(self.directory.path)
        return self._size

    @property
    def size_fmt(self):
        return sizeof_fmt(self.size)

    def print(self, with_size=False):
        s = self.__str__()
        if with_size:
            print(s + f"\t{self.size_fmt}")
        else:
            print(s)

    def delete(self):
        print(f"Delete {self.directory.path}", end="")
        rmtree(self.directory.path, ignore_errors=True)
        try:
            os.unlink(self.lockfile)
        except FileNotFoundError:
            pass
        print("\t\t[OK]")


def find_git_repo(s):
    dir_entries = [e for e in os.scandir(s) if e.is_dir()]
    subgroups = [d for d in dir_entries if not d.name.endswith(".git")]
    repos = [d for d in dir_entries if d.name.endswith(".git")]
    git_repos = [GitRepo(d) for d in repos]
    for subgroup in subgroups:
        for g in find_git_repo(subgroup):
            yield g
    for g in git_repos:
        yield g


def mtime(g):
    return g.mtime


def clean_git():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s", "--size", help="display size of each git repository", action="store_true"
    )
    parser.add_argument(
        "-d", "--delete", help="delete old repository", action="store_true"
    )
    parser.add_argument(
        "-o",
        "--older-than",
        help="delete repository, not accessed for more than OLDER_THAN days",
        default=100,
        type=int,
    )
    args = parser.parse_args()

    workdir = os.path.expanduser(os.getenv("WORKING_DIRECTORY", ""))
    os.chdir(os.path.join(workdir, "git"))
    git_dirs = [g for g in find_git_repo(".") if g.age > args.older_than]

    git_dirs.sort(key=mtime)

    for g in git_dirs:
        g.print(args.size)

    if args.size:
        total_size = sum([g.size for g in git_dirs])
        print(f"Total size that would be deleted {sizeof_fmt(total_size)}")

    if args.delete:
        for g in git_dirs:
            g.delete()


if __name__ == "__main__":
    clean_git()
