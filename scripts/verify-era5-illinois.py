#!/usr/bin/env python3
"""
verify-era5-illinois.py

Open the **Icechunk** ERA5-Land/Illinois repository written by era5-illinois.py
and verify what landed in the object store. Prints a structural summary (dims,
variables, time coverage, chunking, clip footprint) and writes a couple of
diagnostic plots.

Why this differs from a plain-Zarr verifier
--------------------------------------------
era5-illinois.py now writes a Zarr **v3** store managed by Icechunk. It is NOT
plain-Zarr readable -- you cannot `xr.open_zarr("s3://...")` it. You open the
repo through the `icechunk` package and read its `main` branch:

    repo = icechunk.Repository.open(make_icechunk_storage(prefix))
    ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)

The writer also **pre-allocates a full hourly axis** (1950 -> a configurable
end) and region-writes real months into their slots; every un-ingested hour is
all-NaN. So `time=0` (1950-01-01) is almost always empty. This script therefore
detects the *populated* time range and uses it for the clip footprint and the
plots, while still reporting the full pre-allocated axis.

Run
---
  # Full verification (matplotlib isn't a project dependency, so pull it in just
  # for this run):
  uv run --with matplotlib scripts/verify-era5-illinois.py verify

  # options

  # Just the real-data date range (start/end of populated timesteps). Assumes all
  # variables share the same populated range, so it inspects a single --var:
  uv run scripts/verify-era5-illinois.py dates --var t2m

  # Storage size of the Icechunk repo (object-store footprint + chunk stats):
  uv run scripts/verify-era5-illinois.py size

S3/Ceph connection is read from .env exactly like the writer script.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import icechunk

DEFAULT_ZARR_PREFIX = "era5-land/icechunk/IL"


# --------------------------------------------------------------------------- #
# Icechunk storage / repo (mirrors era5-illinois.py so the two stay in sync)
# --------------------------------------------------------------------------- #
def make_icechunk_storage(prefix: str) -> icechunk.Storage:
    """Icechunk S3 storage pointed at the S3 endpoint from .env.

    Ceph compatibility is handled here: force_path_style mirrors s3fs's
    addressing_style="path", and endpoint_url points at that endpoint, not AWS.
    Icechunk's Rust S3 client ignores the botocore AWS_*_CHECKSUM_* env vars, so
    none are set on this path.
    """
    import icechunk

    endpoint = os.environ["AWS_ENDPOINT_URL"]
    return icechunk.s3_storage(
        bucket=os.environ["BUCKET_NAME"],
        prefix=prefix,
        endpoint_url=endpoint,
        region=os.environ.get("S3_REGION", "us-east-1"),
        access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        force_path_style=True,  # Ceph RGW path addressing
        allow_http=endpoint.lower().startswith("http://"),
    )


def open_store(prefix: str, branch: str = "main"):
    """Open the Icechunk repo's branch read-only as an xarray Dataset."""
    import icechunk
    import xarray as xr

    storage = make_icechunk_storage(prefix)
    repo = icechunk.Repository.open(storage)
    session = repo.readonly_session(branch)
    ds = xr.open_zarr(session.store, consolidated=False, decode_timedelta=True)
    return repo, ds


def human_bytes(n: int) -> str:
    """Format a byte count as a human-readable binary-unit string (e.g. 1.23 GiB)."""
    size = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if size < 1024 or unit == "PiB":
            return f"{size:.2f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{size:.2f} PiB"  # unreachable, keeps type-checkers happy


# --------------------------------------------------------------------------- #
# Populated time range (the pre-allocated axis is mostly all-NaN)
# --------------------------------------------------------------------------- #
def populated_time_range(ds, var: str):
    """Return (i0, i1, times) bounding the timesteps that actually hold data.

    The store's `time` axis is pre-allocated from 1950 and only the ingested
    months carry finite values. We find which timesteps have ANY finite cell via
    a lazy dask reduction -- un-ingested regions resolve to the NaN fill value
    without an S3 fetch, so this stays cheap relative to the populated fraction.

    Returns (None, None, mask) with an all-False mask if nothing is populated.
    """
    import numpy as np

    if var not in ds.data_vars:
        return None, None, None

    # Reduce over space to a 1-D per-timestep "has any data" mask, then realize.
    mask = ds[var].notnull().any(dim=("latitude", "longitude")).compute().values
    idx = np.flatnonzero(mask)
    if idx.size == 0:
        return None, None, mask
    return int(idx[0]), int(idx[-1]), mask


def summarize(ds, repo, branch: str) -> None:
    import numpy as np

    print("\n" + "=" * 70)
    print("DATASET SUMMARY")
    print("=" * 70)
    print(ds)

    # Icechunk-specific: how many commits are on this branch.
    try:
        ancestry = list(repo.ancestry(branch=branch))
        print("\n" + "-" * 70)
        print("ICECHUNK HISTORY")
        print("-" * 70)
        print(f"  branch        : {branch}")
        print(f"  commits       : {len(ancestry)}")
        print(f"  latest commit : {ancestry[0].message!r}")
        print(f"  snapshot id   : {ancestry[0].id}")
    except Exception as e:  # noqa: BLE001 -- ancestry API drift shouldn't kill the summary
        print(f"\n  (could not read Icechunk ancestry: {e})")

    print("\n" + "-" * 70)
    print("DIMENSIONS")
    print("-" * 70)
    for dim, n in ds.sizes.items():
        print(f"  {dim:12s} {n}")

    if "time" in ds.coords:
        t = ds["time"].values
        print("\n" + "-" * 70)
        print("PRE-ALLOCATED TIME AXIS")
        print("-" * 70)
        print(f"  start : {t[0]}")
        print(f"  end   : {t[-1]}")
        print(f"  steps : {len(t)}")
        if len(t) > 1:
            deltas = np.diff(t).astype("timedelta64[h]").astype(int)
            uniq, counts = np.unique(deltas, return_counts=True)
            spacing = ", ".join(f"{u}h x{c}" for u, c in zip(uniq, counts, strict=True))
            print(f"  step spacing (hours): {spacing}")
            # Flag any gaps (anything other than the dominant 1h cadence).
            gaps = int((deltas != 1).sum())
            print(f"  non-1h steps: {gaps}")

    if "latitude" in ds.coords and "longitude" in ds.coords:
        lat, lon = ds["latitude"].values, ds["longitude"].values
        print("\n" + "-" * 70)
        print("SPATIAL EXTENT (EPSG:4326)")
        print("-" * 70)
        print(f"  latitude : {lat.min():.3f} .. {lat.max():.3f}  ({len(lat)} cells)")
        print(f"  longitude: {lon.min():.3f} .. {lon.max():.3f}  ({len(lon)} cells)")

    print("\n" + "-" * 70)
    print("DATA VARIABLES")
    print("-" * 70)
    for name, var in ds.data_vars.items():
        chunks = var.chunks
        chunk_str = (
            ", ".join(f"{d}:{c[0]}" for d, c in zip(var.dims, chunks, strict=True))
            if chunks
            else "unchunked"
        )
        units = var.attrs.get("units", "?")
        long_name = var.attrs.get("long_name", "")
        print(f"  {name}")
        print(f"      dims   : {dict(zip(var.dims, var.shape, strict=True))}")
        print(f"      chunks : {chunk_str}")
        print(f"      units  : {units}    {long_name}")


def report_populated(ds, var: str, i0, i1, mask) -> None:
    """Report which slice of the pre-allocated axis actually holds data."""
    if var not in ds.data_vars:
        print(
            f"\nSkipping populated-range report: '{var}' not in store. "
            f"Available: {list(ds.data_vars)}"
        )
        return
    print("\n" + "-" * 70)
    print(f"POPULATED TIME RANGE (var={var})")
    print("-" * 70)
    if i0 is None:
        print("  WARNING: no populated timesteps -- store appears empty for this var.")
        return
    t = ds["time"].values
    n_pop = int(mask.sum())
    print(f"  first populated : {t[i0]}  (index {i0})")
    print(f"  last  populated : {t[i1]}  (index {i1})")
    print(
        f"  populated steps : {n_pop} / {len(t)}  "
        f"({100 * n_pop / len(t):.2f}% of the pre-allocated axis)"
    )
    # Within the populated span, flag any all-NaN gaps (un-ingested months).
    span = i1 - i0 + 1
    holes = span - n_pop
    if holes:
        print(
            f"  gaps inside span: {holes} timesteps "
            f"(un-ingested months between {t[i0]} and {t[i1]})"
        )


def report_dates(ds, var: str, i0, i1) -> None:
    """Print only the start/end timestamps of real (populated) data."""
    if var not in ds.data_vars:
        print(f"No such variable '{var}' in store. Available: {list(ds.data_vars)}")
        return
    if i0 is None:
        print(f"WARNING: no populated timesteps for '{var}' -- store appears empty.")
        return
    t = ds["time"].values
    print(f"start : {t[i0]}")
    print(f"end   : {t[i1]}")


def nan_footprint(ds, var: str, i0) -> None:
    """Report the clip footprint: how much of the bbox is NaN (outside IL).

    Uses the FIRST POPULATED timestep -- on the pre-allocated axis, time=0 is
    1950 and almost always empty, which would falsely look like a broken clip.
    """
    import numpy as np

    if var not in ds.data_vars or i0 is None:
        return
    da = ds[var].isel(time=i0)
    total = da.size
    valid = int(np.isfinite(da.values).sum())
    print("\n" + "-" * 70)
    print(f"CLIP FOOTPRINT (var={var}, first populated timestep {ds.time.values[i0]})")
    print("-" * 70)
    print(f"  valid cells : {valid} / {total}  ({100 * valid / total:.1f}% inside clip)")
    if valid == 0:
        print("  WARNING: no valid cells -- clip may have removed everything.")


def make_plots(ds, var: str, i0, i1, out_dir: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    if var not in ds.data_vars:
        print(f"\nSkipping plots: '{var}' not in store. Available: {list(ds.data_vars)}")
        return
    if i0 is None:
        print(f"\nSkipping plots: '{var}' has no populated timesteps.")
        return

    out_dir.mkdir(parents=True, exist_ok=True)
    # Restrict to the populated span so we never average over the empty 1950..
    # pre-allocated tail (skipna handles any interior gaps).
    da = ds[var].isel(time=slice(i0, i1 + 1))
    units = da.attrs.get("units", "")
    long_name = da.attrs.get("long_name", var)

    # ------------------------------------------------------------------ #
    # Plot 1: time-mean spatial map (shows the Illinois clip footprint).
    # ------------------------------------------------------------------ #
    field = da.mean("time", keep_attrs=True)
    fig, ax = plt.subplots(figsize=(6, 8))
    field.plot(ax=ax, cmap="viridis", add_colorbar=True, cbar_kwargs={"label": f"{var} [{units}]"})
    ax.set_title(
        f"Time-mean {long_name}\n{str(da.time.values[0])[:13]} .. {str(da.time.values[-1])[:13]}"
    )
    ax.set_aspect("equal")
    fig.tight_layout()
    map_path = out_dir / f"{var}_timemean_map.png"
    fig.savefig(map_path, dpi=120)
    plt.close(fig)
    print(f"  wrote {map_path}")

    # ------------------------------------------------------------------ #
    # Plot 2: domain-mean time series (spatial average over IL per step).
    # ------------------------------------------------------------------ #
    series = da.mean(("latitude", "longitude"), keep_attrs=True)
    fig, ax = plt.subplots(figsize=(11, 4))
    ax.plot(da.time.values, series.values, lw=0.8)
    ax.set_xlabel("time")
    ax.set_ylabel(f"{var} [{units}]")
    ax.set_title(f"Illinois domain-mean {long_name}")
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()
    fig.tight_layout()
    ts_path = out_dir / f"{var}_domainmean_timeseries.png"
    fig.savefig(ts_path, dpi=120)
    plt.close(fig)
    print(f"  wrote {ts_path}")


def _load_env_and_check(p) -> None:
    """Load .env and verify the icechunk dependency is importable."""
    from dotenv import load_dotenv

    load_dotenv()

    try:
        import icechunk  # noqa: F401
    except ImportError as e:
        p.error(f"Missing dependency: {e}. Run `uv sync`.")


def cmd_verify(args, p) -> int:
    """Full verification: summary, populated range, clip footprint, and plots."""
    _load_env_and_check(p)

    print(
        f"Opening Icechunk repo s3://{os.environ['BUCKET_NAME']}/{args.prefix} "
        f"(branch={args.branch})"
    )
    repo, ds = open_store(args.prefix, args.branch)

    summarize(ds, repo, args.branch)

    print("\nScanning the pre-allocated axis for populated timesteps...")
    i0, i1, mask = populated_time_range(ds, args.var)
    report_populated(ds, args.var, i0, i1, mask)
    nan_footprint(ds, args.var, i0)

    if not args.no_plots:
        out_dir = Path(args.out_dir) if args.out_dir else Path("era5_verify")
        print("\n" + "-" * 70)
        print("DIAGNOSTIC PLOTS")
        print("-" * 70)
        make_plots(ds, args.var, i0, i1, out_dir)

    ds.close()
    print("\nDone.")
    return 0


def cmd_dates(args, p) -> int:
    """Print only the start/end timestamps of real (populated) data."""
    _load_env_and_check(p)

    print(
        f"Opening Icechunk repo s3://{os.environ['BUCKET_NAME']}/{args.prefix} "
        f"(branch={args.branch})"
    )
    repo, ds = open_store(args.prefix, args.branch)

    print("\nScanning the pre-allocated axis for populated timesteps...")
    i0, i1, _mask = populated_time_range(ds, args.var)
    print("\n" + "-" * 70)
    print(f"REAL-DATA DATE RANGE (var={args.var})")
    print("-" * 70)
    report_dates(ds, args.var, i0, i1)

    ds.close()
    return 0


def report_size(repo, storage) -> None:
    """Report the Icechunk repo's storage size: object-store footprint + chunk stats."""
    # Icechunk's logical chunk accounting (native/virtual/inlined).
    print("\n" + "-" * 70)
    print("CHUNK STORAGE STATS (Icechunk accounting)")
    print("-" * 70)
    try:
        stats = repo.chunk_storage_stats()
        print(f"  native  : {human_bytes(stats.native_bytes)}")
        print(f"  virtual : {human_bytes(stats.virtual_bytes)}  (references to external data)")
        print(f"  inlined : {human_bytes(stats.inlined_bytes)}")
        print(f"  total   : {human_bytes(stats.total_bytes())}")
    except Exception as e:  # noqa: BLE001 -- API drift shouldn't kill the report
        print(f"  (could not read chunk_storage_stats: {e})")

    # Physical object-store footprint: sum every object under the repo prefix.
    print("\n" + "-" * 70)
    print("OBJECT-STORE FOOTPRINT (physical bytes in the bucket)")
    print("-" * 70)
    objs = storage.list_objects_metadata()
    categories: dict[str, list[int]] = {}
    grand_total = 0
    for o in objs:
        grand_total += o.size_bytes
        category = o.key.split("/", 1)[0] if "/" in o.key else "other"
        bucket = categories.setdefault(category, [0, 0])
        bucket[0] += o.size_bytes
        bucket[1] += 1
    for category in sorted(categories):
        total_bytes, count = categories[category]
        print(f"  {category:18s} {human_bytes(total_bytes):>12s}  ({count} objects)")
    print("-" * 70)
    print(f"  {'TOTAL':18s} {human_bytes(grand_total):>12s}  ({len(objs)} objects)")


def cmd_size(args, p) -> int:
    """Report the storage size of the Icechunk repo."""
    import icechunk

    _load_env_and_check(p)

    print(f"Opening Icechunk repo s3://{os.environ['BUCKET_NAME']}/{args.prefix}")
    storage = make_icechunk_storage(args.prefix)
    repo = icechunk.Repository.open(storage)

    report_size(repo, storage)

    print("\nDone.")
    return 0


def _add_common_args(sp) -> None:
    """Args shared by every subcommand."""
    sp.add_argument(
        "--var",
        default="t2m",
        help="Variable to inspect (CF short name, e.g. t2m, tp, sp, d2m, u10, v10).",
    )
    sp.add_argument(
        "--prefix", default=DEFAULT_ZARR_PREFIX, help="Icechunk repo prefix inside the bucket."
    )
    sp.add_argument("--branch", default="main", help="Icechunk branch to read (default: main).")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = p.add_subparsers(dest="command", required=True)

    p_verify = sub.add_parser(
        "verify",
        help="Full verification: summary, populated range, clip footprint, and diagnostic plots.",
    )
    _add_common_args(p_verify)
    p_verify.add_argument(
        "--out-dir", default=None, help="Where to write diagnostic plots (default: ./era5_verify)."
    )
    p_verify.add_argument(
        "--no-plots", action="store_true", help="Print the summary only; skip plotting."
    )
    p_verify.set_defaults(func=cmd_verify)

    p_dates = sub.add_parser(
        "dates",
        help="Print only the start/end dates of real (populated) data. "
        "Assumes all variables share the same range.",
    )
    _add_common_args(p_dates)
    p_dates.set_defaults(func=cmd_dates)

    p_size = sub.add_parser(
        "size",
        help="Report the storage size of the Icechunk repo (object-store footprint + chunk stats).",
    )
    p_size.add_argument(
        "--prefix", default=DEFAULT_ZARR_PREFIX, help="Icechunk repo prefix inside the bucket."
    )
    p_size.add_argument("--branch", default="main", help="Icechunk branch (default: main).")
    p_size.set_defaults(func=cmd_size)

    args = p.parse_args(argv)
    return args.func(args, p)


if __name__ == "__main__":
    sys.exit(main())
