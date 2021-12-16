# Standard Library
import argparse
import os
from pathlib import Path

import numpy as np
import pandas as pd

from git_cdn.cache_handler.common import find_bundle
from git_cdn.cache_handler.common import find_git_repo
from git_cdn.cache_handler.common import find_lfs
from git_cdn.cache_handler.common import sizeof_fmt


def stats(finder, use_pickles=False, verbose=False):
    pd.set_option("display.max_rows", 1000)
    pkl = f"/tmp/{finder.__name__}.pkl"
    if use_pickles and Path(pkl).exists():
        print("From pickle")
        df = pd.read_pickle(pkl)
    else:
        print("Scanning...")
        df = pd.DataFrame([g.to_dict() for g in finder])
        df.to_pickle(f"/tmp/{finder.__name__}.pkl")
    df["index_age"] = pd.to_timedelta(df.age, unit="s")
    ts = df.set_index("index_age")  # convert DataFrame to TimeSerie

    basenames = (
        ts.groupby("basename")
        .agg(
            count=("size", "count"),
            mean_size=("size", "mean"),
            sum_size=("size", "sum"),
            min_age=("age", "min"),
            max_age=("age", "max"),
            mean_age=("age", "mean"),
        )
        .sort_values(["count", "sum_size"], ascending=False)
    )
    basenames["mean_size_fmt"] = basenames["mean_size"].map(sizeof_fmt)
    basenames["sum_size_fmt"] = basenames["sum_size"].map(sizeof_fmt)
    basenames["oversize"] = basenames["sum_size"] - basenames["mean_size"]
    basenames["oversize_fmt"] = basenames["oversize"].map(sizeof_fmt)

    if verbose:
        print(basenames)
        print(
            basenames.sort_values(["oversize", "count", "mean_size"], ascending=False)[
                [
                    "oversize_fmt",
                    "count",
                    "mean_size_fmt",
                    "min_age",
                    "max_age",
                    "mean_age",
                ]
            ][:100]
        )
    print()
    print("Number of items:", int(ts["size"].count()))
    print("Total used space:", sizeof_fmt(int(ts["size"].sum())))
    print(
        "Total oversize (based on duplicates repo names):",
        sizeof_fmt(basenames["oversize"].sum()),
    )
    for a in [
        ts.resample("1D").agg({"size": "sum", "path": "count"}),
        ts.resample("2H").agg({"size": "sum", "path": "count"}),
    ]:
        print("-" * 50)
        a["size_fmt"] = a["size"].map(sizeof_fmt)
        print(
            a[["size_fmt", "path"]]
            .rename_axis("")
            .rename(columns={"size_fmt": "size", "path": "nb items"})
        )


def stats_cdn_cache():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p",
        "--use-pickles",
        help="Use previously pickled cache stats instead of rescanning file system",
        action="store_true",
    )
    parser.add_argument("-v", "--verbose", help="Verbose", action="store_true")
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
        print("-" * 25)
        print("Git repo stats")
        print("-" * 25)
        stats(find_git_repo("git"), use_pickles=args.use_pickles, verbose=args.verbose)

    if args.lfs or args.all:
        print("-" * 25)
        print("lfs stats")
        print("-" * 25)
        stats(find_lfs("lfs"), use_pickles=args.use_pickles, verbose=args.verbose)

    if args.bundle or args.all:
        print("-" * 25)
        print("Bundle stats")
        print("-" * 25)
        stats(
            find_bundle("bundles"), use_pickles=args.use_pickles, verbose=args.verbose
        )


if __name__ == "__main__":
    stats_cdn_cache()
