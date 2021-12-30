# Standard Library
import argparse
import logging
import os
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version
from shutil import rmtree

import sentry_sdk
from structlog import getLogger

from git_cdn.cache_handler.common import find_bundle
from git_cdn.cache_handler.common import find_git_repo
from git_cdn.cache_handler.common import find_lfs
from git_cdn.cache_handler.common import sizeof_fmt
from git_cdn.log import enable_console_logs
from git_cdn.log import enable_udp_logs

log = getLogger()

try:
    GITCDN_VERSION = version("git_cdn")
except PackageNotFoundError:
    GITCDN_VERSION = "unknown"

sentry_dsn = os.getenv("SENTRY_DSN")
if sentry_dsn:
    sentry_sdk.init(
        sentry_dsn,
        release=GITCDN_VERSION,
        environment=os.getenv("SENTRY_ENV", "dev"),
    )


def mtime(g):
    return g.mtime


def find_older_repo(older_than):
    git_dirs = []
    for g in find_git_repo("git"):
        if g.age > older_than:
            git_dirs.append(g)
        log.info("git repo", **g.to_dict())

    git_dirs.sort(key=mtime)
    return git_dirs


def clean_git_repo(older_than, verbose, delete):
    git_dirs = find_older_repo(older_than)

    if verbose:
        for g in git_dirs:
            print(g)

    print(f"Number of older git repos: {len(git_dirs)}")
    total_size = sum([g.size for g in git_dirs])
    log.info("clean_git_repo stats", clean_size=total_size, clean_files=len(git_dirs))
    if verbose:
        print(f"Total size that would be deleted {sizeof_fmt(total_size)}")

    if delete:
        for g in git_dirs:
            g.delete()


def clean_lfs(older_than, verbose, delete):
    lfs_files = []
    for f in find_lfs("lfs"):
        if f.age > older_than:
            lfs_files.append(f)
        log.info("lfs file", **f.to_dict())

    lfs_files.sort(key=mtime)

    if verbose:
        for f in lfs_files:
            print(f)

    total_size = sum([f.size for f in lfs_files])
    print(f"Number of lfs files to be cleaned: {len(lfs_files)}")
    print(f"Total size that would be deleted {sizeof_fmt(total_size)}")
    log.info("clean_lfs stats", clean_size=total_size, clean_files=len(lfs_files))

    if delete:
        for f in lfs_files:
            f.delete()


def clean_bundle(older_than, verbose, delete):
    bundles = []
    for b in find_bundle("bundles"):
        if b.age > older_than:
            bundles.append(b)
        log.info("bundle file", **b.to_dict())

    bundles.sort(key=mtime)

    if verbose:
        for f in bundles:
            print(f)

    total_size = sum([f.size for f in bundles])
    print(f"Number of bundle files to be cleaned: {len(bundles)}")
    print(f"Total size that would be deleted {sizeof_fmt(total_size)}")
    log.info("clean_bundles stats", clean_size=total_size, clean_files=len(bundles))

    if delete:
        for f in bundles:
            f.delete()


def setup_logging():
    logging.basicConfig(level=logging.DEBUG)
    log_server = os.getenv("LOGGING_SERVER")
    if log_server is not None:
        print("LOGGING TO VECTOR")
        host, port = log_server.split(":")
        enable_udp_logs(host, int(port), GITCDN_VERSION)
    else:
        print("LOGGING TO CONSOLE")
        enable_console_logs()


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


def main():
    setup_logging()
    clean_cdn_cache()


if __name__ == "__main__":
    main()
