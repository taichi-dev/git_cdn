# Standard Library
import argparse
import ast
import logging
import os
from dataclasses import dataclass
from dataclasses import field
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version
from typing import List

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
from structlog import getLogger

from git_cdn.cache_handler.common import find_bundle
from git_cdn.cache_handler.common import find_git_repo
from git_cdn.cache_handler.common import find_lfs
from git_cdn.cache_handler.common import sizeof_fmt
from git_cdn.log import before_breadcrumb
from git_cdn.log import enable_console_logs
from git_cdn.log import enable_udp_logs

log = getLogger()

try:
    GITCDN_VERSION = version("git_cdn")
except PackageNotFoundError:
    GITCDN_VERSION = "unknown"


# pylint: disable=duplicate-code
def before_send(event, hint):
    if "log_record" in hint:
        try:
            evt = ast.literal_eval(hint["log_record"].message)
            if "message" in evt:
                event["logentry"]["message"] = evt.pop("message")
            if "extra" in evt:
                event["extra"].update(evt["extra"])
        except Exception:
            pass
    return event


sentry_dsn = os.getenv("SENTRY_DSN")
if sentry_dsn:
    sentry_logging = LoggingIntegration(
        level=logging.DEBUG,  # Capture debug and above as breadcrumbs
        event_level=logging.ERROR,  # Send errors as events
    )
    sentry_sdk.init(
        sentry_dsn,
        integrations=[sentry_logging],
        release=GITCDN_VERSION,
        before_breadcrumb=before_breadcrumb,
        before_send=before_send,
        environment=os.getenv("SENTRY_ENV", "dev"),
    )
# pylint: enable=duplicate-code


def setup_logging(verbose=False):
    logging.getLogger("urllib3.connectionpool").setLevel(logging.INFO)
    log_server = os.getenv("LOGGING_SERVER")
    if log_server is not None:
        host, port = log_server.split(":")
        enable_udp_logs(host, int(port), GITCDN_VERSION)
    if verbose:
        enable_console_logs()
    else:
        enable_console_logs(logging.INFO)


def mtime(g):
    return g.mtime


@dataclass
class Cache:
    path: str = ""
    items: List = field(default_factory=list)


def set_parser():
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
        "-t",
        "--threshold",
        help="disk free threshold to reach (in GiB)",
        default=100,
        type=int,
    )
    parser.add_argument("-l", "--lfs", help="also clean LFS files", action="store_true")
    parser.add_argument(
        "-b", "--bundle", help="also clean Bundle files", action="store_true"
    )
    parser.add_argument(
        "-a", "--all", help="Clean all caches: git repos and LFS", action="store_true"
    )
    return parser


def scan_cache(git, lfs, bundle):
    caches = {}
    if git:
        path = "git"
        git_dirs = list(find_git_repo(path))
        fs = os.statvfs(path)
        fsid = fs.f_fsid
        caches.setdefault(fsid, Cache())
        caches[fsid].path = path
        caches[fsid].items += git_dirs

    if lfs:
        path = "lfs"
        lfs_files = list(find_lfs(path))
        fs = os.statvfs(path)
        fsid = fs.f_fsid
        caches.setdefault(fsid, Cache())
        caches[fsid].path = path
        caches[fsid].items += lfs_files

    if bundle:
        path = "bundles"
        bundles = list(find_bundle(path))
        fs = os.statvfs(path)
        fsid = fs.f_fsid
        caches.setdefault(fsid, Cache())
        caches[fsid].path = path
        caches[fsid].items += bundles
    return caches


def get_olders(caches, item_types):
    """
    input: item_types is a dict with key of all types and values are None
    output: item_types with feed values if possible

    """
    for found_cache in caches.values():
        for i in found_cache.items:
            if i.type in item_types and item_types[i.type] is None:
                print(f"Older {i.type}: {i}")
                item_types[i.type] = i
            # optimization: if older items are found for all asked types, we can exit loops
            if None not in item_types.values():
                return


def disk_free(path):
    disk_stat = os.statvfs(path)
    return disk_stat.f_bavail * disk_stat.f_frsize


def disk_size(path):
    disk_stat = os.statvfs(path)
    return disk_stat.f_blocks * disk_stat.f_bsize


def must_clean(path, threshold, total_clean_size, delete):
    if delete:
        df = disk_free(path)
    else:
        df = disk_free(path) + total_clean_size
    return df < threshold


def clean_cdn_cache(caches, threshold, delete):
    threshold = threshold * 1024**3
    total_clean_size = 0
    cleaned_files = []
    for found_cache in caches.values():
        found_cache.items.sort(key=mtime)
        while must_clean(found_cache.path, threshold, total_clean_size, delete):
            try:
                g = found_cache.items.pop(0)
            except IndexError:
                print(
                    "The whole cache has been removed a the threshold has not been reached"
                )
                break
            total_clean_size += g.size
            cleaned_files.append(g)
            print(f"removing {g}")
            if delete:
                g.delete()

    print(f"Number of cleaned cache items: {len(cleaned_files)}")
    infos = {}
    items = {"GitRepo": None, "LfsFile": None, "BundleFile": None}
    get_olders(caches, items)
    older_git_repo = items["GitRepo"]
    if older_git_repo is not None:
        infos["git_repo_disk_size"] = disk_size(older_git_repo.path)
        infos["git_repo_disk_free"] = disk_free(older_git_repo.path)
        infos["git_repo_cache_duration"] = older_git_repo.age_sec
    older_lfs = items["LfsFile"]
    if older_lfs is not None:
        infos["lfs_disk_size"] = disk_size(older_lfs.path)
        infos["lfs_disk_free"] = disk_free(older_lfs.path)
        infos["lfs_cache_duration"] = older_lfs.age_sec
    older_bundle = items["BundleFile"]
    if older_bundle is not None:
        infos["bundle_disk_size"] = disk_size(older_bundle.path)
        infos["bundle_disk_free"] = disk_free(older_bundle.path)
        infos["bundle_cache_duration"] = older_bundle.age_sec
    log.info(
        "clean_cache stats",
        tryrun=not delete,
        clean_size=total_clean_size,
        clean_files=len(cleaned_files),
        threshold=threshold,
        **infos,
    )
    print(f"Total size that would be deleted {sizeof_fmt(total_clean_size)}")

    return cleaned_files


def main():
    parser = set_parser()
    args = parser.parse_args()
    setup_logging(args.verbose)
    workdir = os.path.expanduser(os.getenv("WORKING_DIRECTORY", ""))
    os.chdir(workdir)
    scan_git = True if args.all else not (args.lfs or args.bundle)
    scan_lfs = True if args.all else args.lfs
    scan_bundle = True if args.all else args.bundle
    caches = scan_cache(scan_git, scan_lfs, scan_bundle)
    clean_cdn_cache(caches, args.threshold, args.delete)


if __name__ == "__main__":
    main()
