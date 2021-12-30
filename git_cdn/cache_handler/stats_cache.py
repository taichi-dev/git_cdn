# Standard Library
import argparse
import os

import numpy as np
import pandas as pd

from git_cdn.cache_handler.common import find_bundle
from git_cdn.cache_handler.common import find_git_repo
from git_cdn.cache_handler.common import find_lfs
from git_cdn.cache_handler.common import sizeof_fmt


def stats(finder):
    df = pd.DataFrame([g.to_dict() for g in finder])
    ts = df.set_index("mtime")  # convert DataFrame to TimeSerie
    print("Number of git repos:", ts.count())
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
