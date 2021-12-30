# Standard Library
import argparse
import os
from datetime import datetime
from shutil import rmtree

from git_cdn.cache_handler.common import find_bundle
from git_cdn.cache_handler.common import find_git_repo
from git_cdn.cache_handler.common import find_lfs
from git_cdn.cache_handler.common import sizeof_fmt


def mtime(g):
    return g.mtime


def find_older_repo(older_than):
    git_dirs = [g for g in find_git_repo("git") if g.age > older_than]
    git_dirs.sort(key=mtime)
    return git_dirs


def clean_git_repo(older_than, verbose, delete):
    git_dirs = find_older_repo(older_than)

    if verbose:
        for g in git_dirs:
            print(g)

    print(f"Number of older git repos: {len(git_dirs)}")
    if verbose:
        # calculate the size only on verbose mode
        total_size = sum([g.size for g in git_dirs])
        print(f"Total size that would be deleted {sizeof_fmt(total_size)}")

    if delete:
        for g in git_dirs:
            g.delete()


def clean_lfs(older_than, verbose, delete):
    lfs_files = [f for f in find_lfs("lfs") if f.age > older_than]

    lfs_files.sort(key=mtime)

    if verbose:
        for f in lfs_files:
            print(f)

    total_size = sum([f.size for f in lfs_files])
    print(f"Number of lfs files to be cleaned: {len(lfs_files)}")
    print(f"Total size that would be deleted {sizeof_fmt(total_size)}")

    if delete:
        for f in lfs_files:
            f.delete()


def clean_bundle(older_than, verbose, delete):
    bundles = [f for f in find_bundle() if f.age > older_than]
    bundles.sort(key=mtime)

    if verbose:
        for f in bundles:
            print(f)

    total_size = sum([f.size for f in bundles])
    print(f"Number of bundle files to be cleaned: {len(bundles)}")
    print(f"Total size that would be deleted {sizeof_fmt(total_size)}")

    if delete:
        for f in bundles:
            f.delete()


def clean_cdn_cache():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-v",
        "--verbose",
        help="list each file/directory that would be deleted",
        action="store_true",
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
    parser.add_argument(
        "-t",
        "--lfs-older-than",
        help="delete repository, not accessed for more than OLDER_THAN days",
        default=60,
        type=int,
    )
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
        clean_git_repo(args.older_than, args.verbose, args.delete)

    if args.lfs or args.all:
        clean_lfs(args.lfs_older_than, args.verbose, args.delete)

    if args.bundle or args.all:
        clean_bundle(args.lfs_older_than, args.verbose, args.delete)


if __name__ == "__main__":
    clean_cdn_cache()
