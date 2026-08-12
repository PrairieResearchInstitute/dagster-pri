#!/usr/bin/env python3
"""
validate-tp-hourly.py

Validate the derived ``tp_hourly`` array in the ERA5-Land/Illinois Icechunk store
against the raw ``tp`` accumulation it was computed from.

The invariant
-------------
``tp`` accumulates since 00:00 UTC and resets each day, so the value at 00:00 on
day D+1 is the whole of day D. ``tp_hourly`` (see
``dagster_pri.era5.accumulation``) is built as::

    hourly(01:00)      = tp(01:00)
    hourly(02:00..00:00) = tp(t) - tp(t-1)

Summing that telescoping series over the 24 steps from **01:00 on day D through
00:00 on day D+1** collapses to ``tp(00:00 on D+1)``::

    tp(01) + [tp(02)-tp(01)] + ... + [tp(00 D+1) - tp(23 D)]  ==  tp(00 D+1)

So for every grid cell and every day::

    sum(tp_hourly[D 01:00 .. D+1 00:00])  ==  tp[D+1 00:00]

Note the window is a *precipitation day*, NOT a calendar day: it starts at 01:00
and ends at the following midnight. Summing ``tp_hourly`` over 00:00..23:00 of a
calendar day does NOT reproduce anything meaningful, and is the most common way
to "disprove" a perfectly good de-accumulation.

Two things legitimately break exact equality:

* **Clamping.** ``hourly_increment`` clips increments at 0. Where the raw
  accumulation dips (float noise in the archive), the clamped sum comes out
  slightly *above* the raw daily total. These show as tiny positive residuals.
* **Float32 differencing.** ``tp`` is stored as float32 in metres; 24 differences
  accumulate rounding on the order of 1e-9 m (a nanometre of rain).

Hence the tolerance check rather than exact equality. Residuals are reported in
**mm** so the numbers are interpretable: anything under ~1e-4 mm is noise.

Windows that touch an un-ingested hour are *incomplete* (any NaN among the 24
hourly steps, or a NaN closing accumulation) and are counted separately rather
than failed -- the first 00:00 of an ingested block has no predecessor, so
``tp_hourly`` there is NaN by design and the preceding day cannot be checked.

Run
---
  # Check a month (every cell, every day)
  uv run scripts/validate-tp-hourly.py check --start 2024-06-01 --end 2024-06-30

  # Loosen/tighten the pass threshold (mm)
  uv run scripts/validate-tp-hourly.py check --start 2024-06-01 --end 2024-06-30 \
      --tol-mm 1e-4

  # Show the hour-by-hour arithmetic for one day at one grid cell (the receipt)
  uv run scripts/validate-tp-hourly.py trace --day 2024-06-17 --lat 40.1 --lon -88.2

S3/Ceph connection is read from .env exactly like the writer/verifier scripts.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import icechunk

DEFAULT_ZARR_PREFIX = "era5-land/icechunk/IL"

V_TP = "tp"
V_TP_HOURLY = "tp_hourly"

# tp is metres of water equivalent; residuals are far easier to judge in mm.
M_TO_MM = 1000.0
# Default pass threshold. float32 differencing over 24 steps plus the clamp lands
# well inside 1e-5 mm (1e-8 m) for realistic precip magnitudes.
DEFAULT_TOL_MM = 1e-4


# --------------------------------------------------------------------------- #
# Icechunk storage / repo (mirrors era5-illinois.py so the scripts stay in sync)
# --------------------------------------------------------------------------- #
def make_icechunk_storage(prefix: str) -> icechunk.Storage:
    """Icechunk S3 storage pointed at the S3 endpoint from .env.

    Ceph compatibility is handled here: force_path_style mirrors s3fs's
    addressing_style="path", and endpoint_url points at that endpoint, not AWS.
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

    repo = icechunk.Repository.open(make_icechunk_storage(prefix))
    session = repo.readonly_session(branch)
    return xr.open_zarr(session.store, consolidated=False, decode_timedelta=True)


def _load_env_and_check(p) -> None:
    """Load .env and verify the icechunk dependency is importable."""
    from dotenv import load_dotenv

    load_dotenv()

    try:
        import icechunk  # noqa: F401
    except ImportError as e:
        p.error(f"Missing dependency: {e}. Run `uv sync`.")


def _require_vars(ds, p) -> None:
    missing = [v for v in (V_TP, V_TP_HOURLY) if v not in ds.data_vars]
    if missing:
        p.error(
            f"store is missing {missing}. Available: {sorted(ds.data_vars)}. "
            f"(tp_hourly only exists for months ingested after de-accumulation "
            f"was added; re-init + re-ingest if the array is absent entirely.)"
        )


# --------------------------------------------------------------------------- #
# The invariant
# --------------------------------------------------------------------------- #
def precip_day_window(start: str, end: str):
    """Time bounds covering the precipitation days from ``start`` to ``end``.

    A precipitation day D spans 01:00 on D through 00:00 on D+1, so validating
    D..E needs hours [D 01:00, E+1 00:00] -- one step past the end date.
    """
    import pandas as pd

    d0 = pd.Timestamp(start).normalize()
    d1 = pd.Timestamp(end).normalize()
    if d1 < d0:
        raise ValueError(f"--end {end} precedes --start {start}")
    return d0 + pd.Timedelta(hours=1), d1 + pd.Timedelta(days=1)


def compare(ds, start: str, end: str):
    """Per-cell, per-day residual between summed tp_hourly and the raw total.

    Returns a Dataset on dims (precip_day, latitude, longitude) with:
      * ``hourly_sum_mm`` -- sum of tp_hourly over the 24-step window
      * ``raw_total_mm``  -- tp at the window's closing midnight
      * ``residual_mm``   -- hourly_sum - raw_total (signed; clamping biases it +)
      * ``n_hours``       -- finite hourly steps in the window (24 == complete)
      * ``complete``      -- n_hours == 24 and the closing tp is finite
    """
    import numpy as np
    import xarray as xr

    t0, t1 = precip_day_window(start, end)
    sub = ds[[V_TP, V_TP_HOURLY]].sel(time=slice(t0, t1))
    if sub.sizes["time"] == 0:
        raise ValueError(
            f"no timesteps in {t0} .. {t1}. The store's axis runs "
            f"{ds.time.values[0]} .. {ds.time.values[-1]}."
        )

    # Label each step with the precipitation day it belongs to: shifting back an
    # hour maps 01:00 D -> 00:00 D and 00:00 D+1 -> 23:00 D, so a floor to the
    # day groups exactly the 24 steps of the telescoping sum.
    day = (sub.time - np.timedelta64(1, "h")).dt.floor("D")
    sub = sub.assign_coords(precip_day=("time", day.values))

    hourly = sub[V_TP_HOURLY]
    grouped = hourly.groupby("precip_day")
    # skipna=False so an un-ingested hour poisons the sum to NaN instead of
    # silently producing a too-small total that would read as a real failure.
    hourly_sum = grouped.sum(skipna=False)
    n_hours = grouped.count()

    # The reference: tp at 00:00 of D+1, i.e. the LAST step of each window. Its
    # precip_day label is already D (the shift-back-an-hour above put it there),
    # so it just needs re-indexing onto that dim to line up with the sums.
    closing = sub[V_TP].isel(time=(sub.time.dt.hour == 0).values)
    closing = closing.swap_dims(time="precip_day").drop_vars("time")

    # A day whose closing midnight lies past the end of the axis has no reference
    # value at all -- it drops out of the inner join rather than failing. Track it
    # so the report can say so instead of silently checking fewer days.
    requested = set(hourly_sum["precip_day"].values.tolist())
    hourly_sum, closing = xr.align(hourly_sum, closing, join="inner")
    n_hours = n_hours.sel(precip_day=hourly_sum["precip_day"])
    unclosed = sorted(requested - set(hourly_sum["precip_day"].values.tolist()))

    hourly_mm = hourly_sum * M_TO_MM
    raw_mm = closing * M_TO_MM
    out = xr.Dataset(
        {
            "hourly_sum_mm": hourly_mm,
            "raw_total_mm": raw_mm,
            "residual_mm": hourly_mm - raw_mm,
            "n_hours": n_hours,
            "complete": (n_hours == 24) & raw_mm.notnull(),
        }
    ).compute()
    out.attrs["unclosed_days"] = [str(np.datetime64(d, "D")) for d in unclosed]
    return out


def report(cmp, tol_mm: float, top: int) -> bool:
    """Print the verdict. Returns True if every complete window passed."""
    import numpy as np

    n_windows = int(cmp["complete"].size)
    complete = cmp["complete"].values
    n_complete = int(complete.sum())
    unclosed = cmp.attrs.get("unclosed_days", [])

    print("\n" + "=" * 70)
    print("tp_hourly VALIDATION")
    print("=" * 70)
    days = cmp["precip_day"].values
    if len(days) == 0:
        print(
            "  Nothing to check: no requested day has a closing 00Z sample in the "
            "store.\n  A day D is validated against tp at 00:00 on D+1, so the "
            "store must extend one\n  hour past the range."
        )
        if unclosed:
            print(f"  unvalidatable days: {unclosed[0]} .. {unclosed[-1]} ({len(unclosed)})")
        return False
    print(f"  precip days   : {str(days[0])[:10]} .. {str(days[-1])[:10]}  ({len(days)} days)")
    if unclosed:
        print(
            f"  no closing 00Z: {len(unclosed)} day(s) skipped "
            f"({unclosed[0]}{f' .. {unclosed[-1]}' if len(unclosed) > 1 else ''}) "
            f"-- beyond the axis"
        )
    print(f"  grid cells    : {cmp.sizes.get('latitude', 1)} x {cmp.sizes.get('longitude', 1)}")
    print(f"  windows       : {n_windows} (day x cell)")
    print(
        f"  complete      : {n_complete}  "
        f"({100 * n_complete / n_windows:.2f}%)  -- 24 finite hours + finite closing tp"
    )
    incomplete = n_windows - n_complete
    if incomplete:
        # Overwhelmingly cells outside the Illinois clip (NaN everywhere) plus the
        # day before each ingested block's first 00:00.
        print(f"  skipped       : {incomplete} incomplete windows (NaN hours / outside clip)")

    if n_complete == 0:
        print("\n  WARNING: nothing to check -- no complete windows in this range.")
        return False

    resid = np.where(complete, np.abs(cmp["residual_mm"].values), np.nan)
    failed = resid > tol_mm
    n_failed = int(np.nansum(failed))

    print("\n" + "-" * 70)
    print(f"RESIDUALS |sum(tp_hourly) - tp(next 00Z)|   tolerance {tol_mm:g} mm")
    print("-" * 70)
    print(f"  max    : {np.nanmax(resid):.3e} mm")
    print(f"  mean   : {np.nanmean(resid):.3e} mm")
    print(f"  p99.9  : {np.nanpercentile(resid, 99.9):.3e} mm")
    signed = np.where(complete, cmp["residual_mm"].values, np.nan)
    print(f"  signed range : {np.nanmin(signed):+.3e} .. {np.nanmax(signed):+.3e} mm")
    print(
        "  (a small POSITIVE bias is expected: increments are clamped at 0, so "
        "noise dips in the raw\n   accumulation are absorbed into the hourly sum.)"
    )

    if n_failed == 0:
        print(f"\n  PASS: all {n_complete} complete windows agree within {tol_mm:g} mm.")
        return True

    print(f"\n  FAIL: {n_failed} / {n_complete} windows exceed {tol_mm:g} mm.")
    print("\n" + "-" * 70)
    print(f"WORST {min(top, n_failed)} OFFENDERS")
    print("-" * 70)
    # Rank by residual, but stop at the failures -- padding to `top` with passing
    # windows would read as more breakage than there is.
    flat = np.argsort(np.nan_to_num(resid, nan=-1.0).ravel())[::-1][: min(top, n_failed)]
    for pos in flat:
        idx = np.unravel_index(pos, resid.shape)
        sel = {d: cmp[d].values[i] for d, i in zip(cmp["residual_mm"].dims, idx, strict=True)}
        day = str(sel.pop("precip_day"))[:10]
        where = "  ".join(f"{k}={v:.3f}" for k, v in sel.items())
        print(
            f"  {day}  {where}  "
            f"hourly_sum={cmp['hourly_sum_mm'].values[idx]:.6f} mm  "
            f"raw={cmp['raw_total_mm'].values[idx]:.6f} mm  "
            f"resid={cmp['residual_mm'].values[idx]:+.3e} mm"
        )
    print("\n  Reproduce one with: trace --day <day> --lat <lat> --lon <lon>")
    return False


def cmd_check(args, p) -> int:
    """Validate the sum-of-increments identity across a date range."""
    _load_env_and_check(p)

    print(
        f"Opening Icechunk repo s3://{os.environ['BUCKET_NAME']}/{args.prefix} "
        f"(branch={args.branch})"
    )
    ds = open_store(args.prefix, args.branch)
    _require_vars(ds, p)

    print(f"Comparing precipitation days {args.start} .. {args.end} ...")
    try:
        cmp = compare(ds, args.start, args.end)
    except ValueError as e:
        p.error(str(e))

    ok = report(cmp, args.tol_mm, args.top)
    ds.close()
    print("\nDone.")
    return 0 if ok else 1


# --------------------------------------------------------------------------- #
# trace: the hour-by-hour receipt for one day at one cell
# --------------------------------------------------------------------------- #
def cmd_trace(args, p) -> int:
    """Print the full 24-step arithmetic for one precipitation day at one cell."""
    import numpy as np
    import pandas as pd

    _load_env_and_check(p)

    ds = open_store(args.prefix, args.branch)
    _require_vars(ds, p)

    day = pd.Timestamp(args.day).normalize()
    t0 = day + pd.Timedelta(hours=1)
    t1 = day + pd.Timedelta(days=1)
    cell = (
        ds[[V_TP, V_TP_HOURLY]]
        .sel(latitude=args.lat, longitude=args.lon, method="nearest")
        .sel(time=slice(t0, t1))
    )
    cell = cell.compute()

    lat = float(cell.latitude), float(cell.longitude)
    print("\n" + "=" * 70)
    print(f"TRACE  precip day {day.date()}  (01:00 .. next 00:00 UTC)")
    print("=" * 70)
    print(f"  nearest cell : latitude={lat[0]:.3f}  longitude={lat[1]:.3f}")
    print(f"  requested    : latitude={args.lat}  longitude={args.lon}")
    print("\n" + "-" * 70)
    print(f"  {'time (UTC)':20s} {'tp (mm)':>14s} {'tp_hourly (mm)':>16s} {'expected diff':>16s}")
    print("-" * 70)

    tp = cell[V_TP].values * M_TO_MM
    hourly = cell[V_TP_HOURLY].values * M_TO_MM
    times = cell.time.values
    for i, t in enumerate(times):
        hour = pd.Timestamp(t).hour
        # Hour 01 is the raw value (the reset makes a diff meaningless); every
        # other step should be the difference from the previous hour, clamped.
        expected = tp[i] if hour == 1 else (max(tp[i] - tp[i - 1], 0.0) if i > 0 else np.nan)
        flag = (
            "" if np.isclose(hourly[i], expected, atol=1e-6, equal_nan=True) else "  <-- MISMATCH"
        )
        print(f"  {str(t)[:19]:20s} {tp[i]:14.6f} {hourly[i]:16.6f} {expected:16.6f}{flag}")

    total = np.nansum(hourly)
    closing = tp[-1] if pd.Timestamp(times[-1]).hour == 0 else np.nan
    print("-" * 70)
    print(f"  {'sum(tp_hourly)':20s} {'':14s} {total:16.6f} mm")
    print(f"  {'tp at next 00Z':20s} {'':14s} {closing:16.6f} mm")
    print(f"  {'residual':20s} {'':14s} {total - closing:+16.3e} mm")
    if len(times) != 24:
        print(f"\n  WARNING: window has {len(times)} steps, not 24 -- partially outside the axis.")

    ds.close()
    return 0


def _add_common_args(sp) -> None:
    sp.add_argument(
        "--prefix", default=DEFAULT_ZARR_PREFIX, help="Icechunk repo prefix inside the bucket."
    )
    sp.add_argument("--branch", default="main", help="Icechunk branch to read (default: main).")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = p.add_subparsers(dest="command", required=True)

    p_check = sub.add_parser(
        "check",
        help="Verify sum(tp_hourly over 01:00..next 00:00) == tp(next 00:00) "
        "for every cell and day in a range.",
    )
    _add_common_args(p_check)
    p_check.add_argument("--start", required=True, help="First precipitation day (YYYY-MM-DD).")
    p_check.add_argument("--end", required=True, help="Last precipitation day (YYYY-MM-DD).")
    p_check.add_argument(
        "--tol-mm",
        type=float,
        default=DEFAULT_TOL_MM,
        help=f"Pass threshold on |residual| in mm (default: {DEFAULT_TOL_MM:g}).",
    )
    p_check.add_argument(
        "--top", type=int, default=10, help="How many worst offenders to list on failure."
    )
    p_check.set_defaults(func=cmd_check)

    p_trace = sub.add_parser(
        "trace", help="Print the hour-by-hour tp / tp_hourly arithmetic for one day at one cell."
    )
    _add_common_args(p_trace)
    p_trace.add_argument("--day", required=True, help="Precipitation day (YYYY-MM-DD).")
    p_trace.add_argument("--lat", type=float, required=True, help="Latitude (nearest cell).")
    p_trace.add_argument("--lon", type=float, required=True, help="Longitude (nearest cell).")
    p_trace.set_defaults(func=cmd_trace)

    args = p.parse_args(argv)
    return args.func(args, p)


if __name__ == "__main__":
    sys.exit(main())
