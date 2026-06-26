#!/usr/bin/env python3
"""
verify-era5-illinois.py

Open the ARCO ERA5-Land Zarr store written by era5-illinois.py and verify what
landed in the object store. Prints a structural summary (dims, variables, time
coverage, chunking, clip footprint) and writes a couple of diagnostic plots.

Run
---
  # matplotlib isn't a project dependency, so pull it in just for this run:
  uv run --with matplotlib scripts/verify-era5-illinois.py

  # options
  uv run --with matplotlib scripts/verify-era5-illinois.py \
      --var 2m_temperature --out-dir scratch/era5_check

S3/Ceph connection is read from .env exactly like the writer script.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

DEFAULT_ZARR_PREFIX = "era5/STATE=IL"


def build_storage_options() -> dict:
    """S3/Ceph storage options sourced from .env (loaded via python-dotenv).

    Accept either AWS_ENDPOINT_URL (what's in this project's .env) or
    S3_ENDPOINT_URL (what the writer script documents).
    """
    endpoint = os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("S3_ENDPOINT_URL")
    return {
        "key": os.environ["AWS_ACCESS_KEY_ID"],
        "secret": os.environ["AWS_SECRET_ACCESS_KEY"],
        "client_kwargs": {"endpoint_url": endpoint},
        # Ceph RGW: address buckets as a URL path segment, not a DNS subdomain.
        "config_kwargs": {"s3": {"addressing_style": "path"}},
    }


def open_store(zarr_uri: str, storage_options: dict):
    import xarray as xr

    return xr.open_zarr(
        zarr_uri,
        storage_options=storage_options,
        consolidated=True,
        decode_timedelta=True,
    )


def summarize(ds) -> None:
    import numpy as np

    print("\n" + "=" * 70)
    print("DATASET SUMMARY")
    print("=" * 70)
    print(ds)

    print("\n" + "-" * 70)
    print("DIMENSIONS")
    print("-" * 70)
    for dim, n in ds.sizes.items():
        print(f"  {dim:12s} {n}")

    if "time" in ds.coords:
        t = ds["time"].values
        print("\n" + "-" * 70)
        print("TIME COVERAGE")
        print("-" * 70)
        print(f"  start : {t[0]}")
        print(f"  end   : {t[-1]}")
        print(f"  steps : {len(t)}")
        if len(t) > 1:
            deltas = np.diff(t).astype("timedelta64[h]").astype(int)
            uniq, counts = np.unique(deltas, return_counts=True)
            spacing = ", ".join(f"{u}h x{c}" for u, c in zip(uniq, counts))
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
            ", ".join(f"{d}:{c[0]}" for d, c in zip(var.dims, chunks))
            if chunks
            else "unchunked"
        )
        units = var.attrs.get("units", "?")
        long_name = var.attrs.get("long_name", "")
        print(f"  {name}")
        print(f"      dims   : {dict(zip(var.dims, var.shape))}")
        print(f"      chunks : {chunk_str}")
        print(f"      units  : {units}    {long_name}")


def nan_footprint(ds, var: str) -> None:
    """Report the clip footprint: how much of the bbox is NaN (outside IL)."""
    import numpy as np

    if var not in ds.data_vars:
        return
    da = ds[var].isel(time=0)
    total = da.size
    valid = int(np.isfinite(da.values).sum())
    print("\n" + "-" * 70)
    print(f"CLIP FOOTPRINT (var={var}, first timestep)")
    print("-" * 70)
    print(f"  valid cells : {valid} / {total}  ({100 * valid / total:.1f}% inside clip)")
    if valid == 0:
        print("  WARNING: no valid cells — clip may have removed everything.")


def make_plots(ds, var: str, out_dir: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    if var not in ds.data_vars:
        print(f"\nSkipping plots: '{var}' not in store. "
              f"Available: {list(ds.data_vars)}")
        return

    out_dir.mkdir(parents=True, exist_ok=True)
    da = ds[var]
    units = da.attrs.get("units", "")
    long_name = da.attrs.get("long_name", var)

    # ------------------------------------------------------------------ #
    # Plot 1: time-mean spatial map (shows the Illinois clip footprint).
    # ------------------------------------------------------------------ #
    field = da.mean("time", keep_attrs=True)
    fig, ax = plt.subplots(figsize=(6, 8))
    im = field.plot(ax=ax, cmap="viridis", add_colorbar=True,
                    cbar_kwargs={"label": f"{var} [{units}]"})
    ax.set_title(f"Time-mean {long_name}\n{str(ds.time.values[0])[:13]} .. "
                 f"{str(ds.time.values[-1])[:13]}")
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
    ax.plot(ds.time.values, series.values, lw=0.8)
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


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--var", default="t2m",
                   help="Variable to plot / inspect (CF short name, e.g. t2m, "
                        "tp, sp, d2m, u10, v10).")
    p.add_argument("--prefix", default=DEFAULT_ZARR_PREFIX,
                   help="Zarr prefix inside the bucket.")
    p.add_argument("--out-dir", default=None,
                   help="Where to write diagnostic plots (default: ./era5_verify).")
    p.add_argument("--no-plots", action="store_true",
                   help="Print the summary only; skip plotting.")
    args = p.parse_args(argv)

    from dotenv import load_dotenv
    load_dotenv()

    # Match the writer: OSN/Ceph rejects the checksums newer botocore adds.
    os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"] = "when_required"
    os.environ["AWS_RESPONSE_CHECKSUM_VALIDATION"] = "when_required"

    zarr_uri = f"s3://{os.environ['BUCKET_NAME']}/{args.prefix}"
    print(f"Opening {zarr_uri}")

    storage_options = build_storage_options()
    ds = open_store(zarr_uri, storage_options)

    summarize(ds)
    nan_footprint(ds, args.var)

    if not args.no_plots:
        out_dir = Path(args.out_dir) if args.out_dir else Path("era5_verify")
        print("\n" + "-" * 70)
        print("DIAGNOSTIC PLOTS")
        print("-" * 70)
        make_plots(ds, args.var, out_dir)

    ds.close()
    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())