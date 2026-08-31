#!/usr/bin/env python3
"""
era5-stations.py

**Independently validate the daily station report** (the parquet written by the
``daily_station_readings`` asset) for three IVA rain-gauge stations.

The point of this script is to be a *second opinion*, so it deliberately shares no
code with the asset: it imports nothing from ``dagster_pri``, pulls the hourly
station series out of the Icechunk store itself, and re-derives the daily numbers
with plain pandas. If the two agree, the asset's xarray ``groupby`` path and this
script's pandas path both have to be wrong in the same way for the report to be
wrong.

What gets recomputed, per station, per **local (Central) calendar day**:

  * ``precip_total_mm``  -- sum of the store's ``tp_hourly`` increments, x1000 (m -> mm)
  * ``t2m_max_c`` / ``t2m_min_c`` / ``t2m_mean_c`` -- daily max/min/mean 2m temperature
  * ``d2m_mean_c``       -- daily mean 2m dewpoint

Precipitation: tp_hourly, not the accumulation
----------------------------------------------
Raw ERA5-Land ``tp`` accumulates since 00:00 UTC and **resets at 00:00 UTC**, so it
cannot be summed against a *local* day at all -- and a naive
``resample("1D").sum()`` over it is wrong even for UTC days. The ingest already
backed the accumulation out into per-hour increments and stored them as
``tp_hourly``, and those are day-definition agnostic, so a local-day total is a
plain sum. This script reads ``tp_hourly`` and never sums the accumulation.

The raw ``tp`` is still read, for two things that need it:

  * the telescoping cross-check (``sum(tp_hourly over D 01:00..D+1 00:00) ==
    tp(D+1 00:00)``), which validates the increments themselves at these stations;
  * the hour-by-hour receipt plot, where seeing the accumulation climb and reset
    next to the increments is the whole point.

The month-edge NaN (expected, not a bug)
----------------------------------------
The ingest de-accumulates one month's block at a time, so ``tp_hourly`` is NaN at
00:00 UTC on the 1st of every month -- that step has no predecessor inside its own
block. A Central day ends at 06:00 UTC (05:00 during CDT), so the **last local day
of the month** reaches into that NaN hour and its ``precip_total_mm`` comes out
NaN. Both the report and this script produce NaN there and the comparison counts it
as agreement. The summary reports what the total *would* have been with the NaN
hour skipped, so you can see the magnitude of what is missing.

Outputs (into ``--outdir``)
---------------------------
  * ``comparison.csv``          -- tidy report vs recomputed vs difference, every row
  * ``daily_<id>.png``          -- per station: daily precip + difference, daily
                                   temperatures + difference, with the tolerance band
  * ``agreement_scatter.png``   -- report vs recomputed 1:1 for all five columns
  * ``hourly_<id>_<date>.png``  -- the receipt: hour-by-hour increments and the raw
                                   accumulation for that station's worst precip day

Exit status is 0 when every compared value agrees within tolerance, 1 otherwise.

Run
---
  uv run scripts/era5-stations.py --year 2000 --month 1
  uv run scripts/era5-stations.py --year 2000 --month 1 --outdir scratch/jan2000
  uv run scripts/era5-stations.py --year 2000 --month 1 --report ./data.parquet
  uv run scripts/era5-stations.py --year 2000 --month 1 --no-plots

S3/Ceph connection is read from .env exactly like the writer/verifier scripts.
"""

from __future__ import annotations

import argparse
import calendar
import os
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    import icechunk

DEFAULT_STATE = "IL"
ZARR_PREFIX_TEMPLATE = "era5-land/icechunk/{state}"
REPORT_KEY_TEMPLATE = "era5-land/parquet/STATE={state}/YEAR={year}/MONTH={month:02d}/data.parquet"
DEFAULT_OUTDIR = "scratch/validate-daily-stations"

# ERA5-Land variable names *as stored*. The CDS NetCDF carries short names and the
# ingest path does not rename data vars, so the store holds t2m / tp / d2m (not the
# CDS long names "2m_temperature" etc.). tp_hourly is the ingest's derived
# per-hour increment of the tp accumulation.
V_T2M = "t2m"
V_TP = "tp"
V_TP_HOURLY = "tp_hourly"
V_D2M = "d2m"
SOURCE_VARS = [V_T2M, V_D2M, V_TP_HOURLY, V_TP]

# Local day definition. ERA5-Land is UTC; the stations are Central.
DEFAULT_TZ = "America/Chicago"

KELVIN = 273.15
M_TO_MM = 1000.0

# Report columns, with the unit and default tolerance each is judged against. The
# report is rounded to 3 decimals and stored as float32, so ~1e-3 of slack is
# inherent; anything at 1e-2 is a real disagreement, not formatting.
TEMP_COLS = ["t2m_max_c", "t2m_min_c", "t2m_mean_c", "d2m_mean_c"]
PRECIP_COL = "precip_total_mm"
VALUE_COLS = [*TEMP_COLS, PRECIP_COL]


class Station(NamedTuple):
    id: str  # str, not int: the report's station_id is a string (the CSV mixes RG37 etc.)
    name: str
    lat: float
    lon: float
    group: str


# Embedded IVA rain-gauge stations (central Illinois). These three are the
# validation sample; the asset itself summarises every station in the CSV.
STATIONS: list[Station] = [
    Station("2", "IVA rain gage", 40.477878, -89.765486, "IVA"),
    Station("3", "IVA rain gage", 40.482442, -89.625972, "IVA"),
    Station("4", "IVA rain gage", 40.408206, -89.911394, "IVA"),
]


# --------------------------------------------------------------------------- #
# Plot palette: validated categorical slots (all-pairs safe at three series) plus
# a blue<->red diverging pair with a neutral gray midpoint for signed residuals.
# --------------------------------------------------------------------------- #
C_SERIES = ["#2a78d6", "#eb6834", "#1baf7a"]  # blue, orange, aqua
C_NEG = "#2a78d6"  # recomputed above report
C_POS = "#e34948"  # report above recomputed
C_SURFACE = "#fcfcfb"
C_INK = "#0b0b0b"
C_INK_2 = "#52514e"
C_GRID = "#e5e4e0"
C_BAND = "#f0efec"
# A light step of the same blue ramp: same quantity as C_SERIES[0], de-emphasised.
C_OUTSIDE = "#b7d3f6"


# --------------------------------------------------------------------------- #
# Storage (mirrors era5-illinois.py / verify-era5-illinois.py)
# --------------------------------------------------------------------------- #
def make_icechunk_storage(prefix: str) -> icechunk.Storage:
    """Icechunk S3 storage pointed at the S3 endpoint from .env."""
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


def s3_filesystem():
    """s3fs on the same endpoint, for the plain parquet read of the report.

    ``request_checksum_calculation="when_required"`` and path addressing mirror the
    resource the asset writes with, so Ceph RGW is addressed the same way.
    """
    import s3fs

    endpoint = os.environ["AWS_ENDPOINT_URL"]
    return s3fs.S3FileSystem(
        key=os.environ["AWS_ACCESS_KEY_ID"],
        secret=os.environ["AWS_SECRET_ACCESS_KEY"],
        use_ssl=endpoint.lower().startswith("https://"),
        client_kwargs={
            "endpoint_url": endpoint,
            "region_name": os.environ.get("S3_REGION", "us-east-1"),
        },
        config_kwargs={
            "s3": {"addressing_style": "path"},
            "request_checksum_calculation": "when_required",
            "response_checksum_validation": "when_required",
        },
    )


# --------------------------------------------------------------------------- #
# The report under test
# --------------------------------------------------------------------------- #
def load_report(path: str, station_ids: list[str]):
    """Read the daily-station parquet into a DataFrame, restricted to our stations.

    ``path`` is either a local file or a ``bucket/key`` object path; local wins if
    the file exists, so ``--report`` can point at a downloaded copy.
    """
    import pandas as pd

    local = Path(path)
    try:
        if local.exists():
            df = pd.read_parquet(local)
        else:
            with s3_filesystem().open(path, "rb") as f:
                df = pd.read_parquet(f)
    except FileNotFoundError as e:
        raise SystemExit(
            f"no report to validate at {path!r} ({e}). `--report` takes either a local "
            f"file or a bucket/key object path; the default is the asset's own output "
            f"path, so materialize `daily_station_readings` for this state/month first "
            f"(`daily_station_readings_job` runs it against an already-ingested month)."
        ) from e

    df["station_id"] = df["station_id"].astype(str)
    df = df[df["station_id"].isin(station_ids)].copy()
    df["date"] = pd.to_datetime(df["date"])
    keep = ["station_id", "station_name", "date", *VALUE_COLS]
    # float64 so the differencing below isn't done in the report's float32.
    for col in VALUE_COLS:
        df[col] = df[col].astype("float64")
    return df[keep].sort_values(["station_id", "date"]).reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Hourly station series, straight out of the store
# --------------------------------------------------------------------------- #
def padded_utc_bounds(year: int, month: int):
    """UTC hour bounds covering every local day that overlaps the month.

    Local-day boundaries do not line up with UTC ones (a Central day starts at
    05:00/06:00 UTC), so the month's first and last local days reach into the
    neighbouring UTC day. One padding day on each side covers a tz offset of
    either sign; the padding days are trimmed after aggregation.
    """
    import pandas as pd

    last = calendar.monthrange(year, month)[1]
    start = pd.Timestamp(year, month, 1) - pd.Timedelta(days=1)
    end = pd.Timestamp(year, month, last, 23) + pd.Timedelta(days=1)
    return start, end


def hourly_frames(ds, stations: list[Station], year: int, month: int, workers: int):
    """Per-station hourly DataFrames (UTC-indexed) for the padded month.

    All stations come out of one vectorized pointwise selection, so each spatial
    chunk is read once no matter how many stations there are (the store keeps the
    whole state grid in a single spatial chunk, so a per-station loop would re-read
    the same bytes once per station). Returns ``(frames, grid_cells)``.
    """
    import pandas as pd
    import xarray as xr

    missing = [v for v in SOURCE_VARS if v not in ds.data_vars]
    if missing:
        raise SystemExit(
            f"store is missing {missing}; it holds {sorted(ds.data_vars)}. "
            f"`tp_hourly` exists only for months ingested after de-accumulation was "
            f"added -- re-run era5_init + era5_iceberg if it is absent entirely."
        )

    start, end = padded_utc_bounds(year, month)
    sub = ds[SOURCE_VARS].sel(time=slice(start, end))
    if sub.sizes.get("time", 0) == 0:
        axis = ds["time"].values
        raise SystemExit(
            f"no timesteps in {start} .. {end}. The store's axis runs "
            f"{pd.Timestamp(axis[0])} .. {pd.Timestamp(axis[-1])}."
        )

    ids = [s.id for s in stations]
    sel_lat = xr.DataArray([s.lat for s in stations], dims="station", coords={"station": ids})
    sel_lon = xr.DataArray([s.lon for s in stations], dims="station", coords={"station": ids})
    pts = sub.sel(latitude=sel_lat, longitude=sel_lon, method="nearest")

    print(
        f"Reading {pts.sizes['time']} hourly steps x {len(stations)} stations "
        f"({workers} dask threads) ..."
    )
    pts = pts.compute(scheduler="threads", num_workers=workers)

    grid_cells = {
        sid: (float(lat), float(lon))
        for sid, lat, lon in zip(
            pts["station"].values.tolist(),
            pts["latitude"].values.tolist(),
            pts["longitude"].values.tolist(),
            strict=True,
        )
    }

    frames = {}
    for st in stations:
        one = pts.sel(station=st.id)
        df = pd.DataFrame(
            {v: one[v].values.astype("float64") for v in SOURCE_VARS},
            index=pd.DatetimeIndex(one["time"].values, name="time_utc"),
        )
        frames[st.id] = df
    return frames, grid_cells


# --------------------------------------------------------------------------- #
# Independent daily aggregation (pure pandas)
# --------------------------------------------------------------------------- #
def _nan_aware_sum(s):
    """Sum that propagates NaN, unlike pandas' skipna-by-default sum.

    A local day that reaches into a NaN ``tp_hourly`` hour has no honest total, and
    silently skipping the hour would report a too-small number as if it were
    complete. numpy's sum over the raw values gives NaN, which is what the report
    does too (``skipna=False``).
    """
    return s.to_numpy(dtype="float64").sum()


def recompute_daily(hrs, year: int, month: int, tz: str):
    """Daily summary for one station's hourly frame, by local calendar day.

    Deliberately plain pandas -- label every UTC timestep with its calendar day in
    ``tz`` and group on that label. Temperature and dewpoint are instantaneous, so
    max/min/mean over the day's hours; precipitation is the sum of the ``tp_hourly``
    increments (see the module docstring on why the raw accumulation is not summed).
    """
    import pandas as pd

    local_day = hrs.index.tz_localize("UTC").tz_convert(tz).normalize().tz_localize(None)
    g = hrs.assign(local_day=local_day).groupby("local_day")

    out = pd.DataFrame(
        {
            "t2m_max_c": g[V_T2M].max() - KELVIN,
            "t2m_min_c": g[V_T2M].min() - KELVIN,
            "t2m_mean_c": g[V_T2M].mean() - KELVIN,
            "d2m_mean_c": g[V_D2M].mean() - KELVIN,
            PRECIP_COL: g[V_TP_HOURLY].agg(_nan_aware_sum) * M_TO_MM,
        }
    )
    # Diagnostics, not part of the comparison: how many hours the day actually got,
    # how many of them have no tp_hourly (the month-edge NaN), and what the total
    # would have been with those hours skipped.
    out["n_hours"] = g[V_T2M].size()
    out["n_nan_tp_hours"] = g[V_TP_HOURLY].apply(lambda s: int(s.isna().sum()))
    out["precip_skipna_mm"] = g[V_TP_HOURLY].sum() * M_TO_MM

    # Drop the padding days: keep only local days inside the requested month.
    days = pd.DatetimeIndex(out.index)
    out = out[(days.year == year) & (days.month == month)]
    return out.rename_axis("date").reset_index()


def telescoping_check(hrs, year: int, month: int):
    """Per-station check of ``tp_hourly`` itself against the raw accumulation.

    ``tp`` accumulates from 00:00 UTC and resets daily, so summing the increments
    over the 24 steps from 01:00 on day D through 00:00 on D+1 telescopes exactly to
    ``tp(00:00 on D+1)``. That window is a *precipitation day*, not a calendar day.
    Returns a DataFrame on the precip day with both totals in mm and the residual;
    days touching a NaN hour come out NaN and are excluded by the caller.
    """
    import numpy as np
    import pandas as pd

    # Shift back an hour so 01:00 D -> 00:00 D and 00:00 D+1 -> 23:00 D; flooring
    # then groups exactly the 24 steps of one telescoping sum.
    precip_day = (hrs.index - pd.Timedelta(hours=1)).floor("D")
    g = hrs.assign(precip_day=precip_day).groupby("precip_day")

    hourly_sum = g[V_TP_HOURLY].agg(_nan_aware_sum) * M_TO_MM
    n_hours = g[V_TP_HOURLY].size()
    # The reference is tp at the window's closing midnight -- the last step of each
    # window, whose precip_day label the shift above already set to D.
    closing = hrs.loc[hrs.index.hour == 0, V_TP] * M_TO_MM
    closing.index = (closing.index - pd.Timedelta(hours=1)).floor("D")

    out = pd.DataFrame({"hourly_sum_mm": hourly_sum, "n_hours": n_hours}).join(
        closing.rename("raw_total_mm"), how="inner"
    )
    out["residual_mm"] = out["hourly_sum_mm"] - out["raw_total_mm"]
    out = out[out["n_hours"] == 24]
    days = pd.DatetimeIndex(out.index)
    out = out[(days.year == year) & (days.month == month)]
    return out.replace([np.inf, -np.inf], np.nan)


# --------------------------------------------------------------------------- #
# Comparison
# --------------------------------------------------------------------------- #
def build_comparison(recomputed: dict, report, stations: list[Station]):
    """Join recomputed against report on (station, date); add signed differences.

    An outer join on purpose: a date present on only one side is a finding, not a
    row to drop.
    """
    import pandas as pd

    mine = []
    for st in stations:
        one = recomputed[st.id].copy()
        one.insert(0, "station_id", st.id)
        mine.append(one)
    mine = pd.concat(mine, ignore_index=True)

    cmp = mine.merge(
        report.drop(columns=["station_name"]),
        on=["station_id", "date"],
        how="outer",
        suffixes=("_mine", "_report"),
        indicator="present_in",
    )
    for col in VALUE_COLS:
        cmp[f"{col}_diff"] = cmp[f"{col}_report"] - cmp[f"{col}_mine"]
    cmp["present_in"] = cmp["present_in"].map(
        {"both": "both", "left_only": "recomputed only", "right_only": "report only"}
    )
    return cmp.sort_values(["station_id", "date"]).reset_index(drop=True)


def column_stats(cmp, col: str, tol: float):
    """Agreement stats for one value column: counts, worst case, RMSE."""
    import numpy as np

    mine = cmp[f"{col}_mine"].to_numpy(dtype="float64")
    rep = cmp[f"{col}_report"].to_numpy(dtype="float64")
    both_nan = np.isnan(mine) & np.isnan(rep)
    one_nan = np.isnan(mine) ^ np.isnan(rep)
    ok = ~np.isnan(mine) & ~np.isnan(rep)

    diff = np.where(ok, rep - mine, np.nan)
    absdiff = np.abs(diff)
    n_over = int(np.nansum(absdiff > tol))
    worst_i = int(np.nanargmax(absdiff)) if ok.any() else None
    return {
        "col": col,
        "n_compared": int(ok.sum()),
        "n_both_nan": int(both_nan.sum()),
        "n_one_nan": int(one_nan.sum()),
        "max_abs": float(np.nanmax(absdiff)) if ok.any() else float("nan"),
        "rmse": float(np.sqrt(np.nanmean(diff[ok] ** 2))) if ok.any() else float("nan"),
        "n_over_tol": n_over,
        "worst_row": None if worst_i is None else cmp.iloc[worst_i],
    }


def print_report(cmp, stations, grid_cells, tele, tol_c: float, tol_mm: float, tz: str) -> bool:
    """Print the verdict. Returns True when everything agrees within tolerance."""
    import numpy as np

    bar = "=" * 78
    print("\n" + bar)
    print("DAILY STATION REPORT VALIDATION  (report vs independent recomputation)")
    print(bar)
    print(f"  local day        : {tz}")
    print(f"  precip source    : {V_TP_HOURLY} (per-hour increments), summed by local day")
    print(f"  tolerance        : {tol_c:g} degC (temperature/dewpoint), {tol_mm:g} mm (precip)")
    print(f"  rows             : {len(cmp)}  ({cmp['station_id'].nunique()} stations)")

    print("\n" + "-" * 78)
    print("STATIONS")
    print("-" * 78)
    for st in stations:
        glat, glon = grid_cells[st.id]
        n = int((cmp["station_id"] == st.id).sum())
        print(
            f"  {st.id:>4s}  ({st.lat:.4f}, {st.lon:.4f}) -> grid cell "
            f"({glat:.3f}, {glon:.3f})  {n} days"
        )

    # A date on only one side is a structural mismatch: say so loudly.
    lopsided = cmp[cmp["present_in"] != "both"]
    if len(lopsided):
        print("\n  MISSING ROWS (present on one side only):")
        for _, r in lopsided.iterrows():
            print(f"    {r['station_id']:>4s}  {r['date'].date()}  {r['present_in']}")

    print("\n" + "-" * 78)
    print("AGREEMENT  (difference = report - recomputed)")
    print("-" * 78)
    print(
        f"  {'column':16s} {'n':>5s} {'max|diff|':>12s} {'rmse':>12s} "
        f"{'>tol':>5s} {'NaN=NaN':>8s} {'NaN mism':>9s}"
    )
    all_stats = []
    for col in VALUE_COLS:
        tol = tol_mm if col == PRECIP_COL else tol_c
        s = column_stats(cmp, col, tol)
        all_stats.append(s)
        print(
            f"  {col:16s} {s['n_compared']:5d} {s['max_abs']:12.3e} {s['rmse']:12.3e} "
            f"{s['n_over_tol']:5d} {s['n_both_nan']:8d} {s['n_one_nan']:9d}"
        )

    for s in all_stats:
        if s["worst_row"] is not None and s["n_compared"]:
            r = s["worst_row"]
            print(
                f"    worst {s['col']:16s} station {r['station_id']:>4s} {r['date'].date()}  "
                f"report {r[s['col'] + '_report']:.4f}  recomputed {r[s['col'] + '_mine']:.4f}"
            )

    # The expected month-edge NaN: report it as an explanation, not a failure.
    nan_days = cmp[cmp["n_nan_tp_hours"].fillna(0) > 0]
    if len(nan_days):
        print("\n" + "-" * 78)
        print("EXPECTED NaN PRECIP DAYS  (tp_hourly is NaN at 00:00 UTC on the 1st)")
        print("-" * 78)
        print("  The ingest de-accumulates one month at a time, so the first 00:00 UTC of")
        print("  each block has no predecessor. A local day reaching into it has no honest")
        print("  total. Both sides give NaN. 'skipna' is what you would get by ignoring the")
        print("  missing hour -- a lower bound on the real total, shown for scale only.")
        for _, r in nan_days.iterrows():
            agree = (
                "both NaN"
                if (np.isnan(r[PRECIP_COL + "_mine"]) and np.isnan(r[PRECIP_COL + "_report"]))
                else "MISMATCH"
            )
            print(
                f"    {r['station_id']:>4s}  {r['date'].date()}  "
                f"{int(r['n_nan_tp_hours'])} NaN hour(s) of {int(r['n_hours'])}  "
                f"skipna {r['precip_skipna_mm']:8.3f} mm   {agree}"
            )

    # Independent anchor for the increments themselves.
    print("\n" + "-" * 78)
    print("tp_hourly vs RAW ACCUMULATION  (per precip day D: 01:00 D .. 00:00 D+1)")
    print("-" * 78)
    print("  sum(tp_hourly) must telescope to tp at the closing midnight. This checks")
    print("  the ingest's de-accumulation at these stations, independent of the report.")
    tele_ok = True
    for st in stations:
        t = tele[st.id].dropna(subset=["residual_mm"])
        if not len(t):
            print(f"  {st.id:>4s}  no complete 24-hour windows in the month")
            continue
        worst = t["residual_mm"].abs().max()
        tele_ok = tele_ok and bool(worst <= tol_mm)
        print(
            f"  {st.id:>4s}  {len(t)} complete days  "
            f"increments {t['hourly_sum_mm'].sum():9.3f} mm  "
            f"raw {t['raw_total_mm'].sum():9.3f} mm  "
            f"max|residual| {worst:.3e} mm"
        )
    print("  (a tiny POSITIVE bias is expected: increments are clamped at 0, so noise")
    print("   dips in the raw accumulation are absorbed into the hourly sum.)")

    n_over = sum(s["n_over_tol"] for s in all_stats)
    n_nan_mismatch = sum(s["n_one_nan"] for s in all_stats)
    passed = n_over == 0 and n_nan_mismatch == 0 and not len(lopsided) and tele_ok

    print("\n" + bar)
    if passed:
        n = sum(s["n_compared"] for s in all_stats)
        print(f"  PASS -- all {n} compared values agree within tolerance.")
    else:
        print(
            f"  FAIL -- {n_over} value(s) over tolerance, {n_nan_mismatch} NaN mismatch(es), "
            f"{len(lopsided)} missing row(s)"
            f"{', de-accumulation residual over tolerance' if not tele_ok else ''}."
        )
    print(bar)
    return passed


# --------------------------------------------------------------------------- #
# Diagnostic plots
# --------------------------------------------------------------------------- #
def _style():
    """Recessive grid/axes, light surface, ink from the text tokens."""
    import matplotlib as mpl

    mpl.rcParams.update(
        {
            "figure.facecolor": C_SURFACE,
            "axes.facecolor": C_SURFACE,
            "savefig.facecolor": C_SURFACE,
            "axes.edgecolor": C_GRID,
            "axes.labelcolor": C_INK_2,
            "axes.titlecolor": C_INK,
            "axes.titlesize": 11,
            "axes.titleweight": "bold",
            "axes.labelsize": 9,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "text.color": C_INK,
            "xtick.color": C_INK_2,
            "ytick.color": C_INK_2,
            "xtick.labelsize": 8,
            "ytick.labelsize": 8,
            "grid.color": C_GRID,
            "grid.linewidth": 0.8,
            "legend.frameon": False,
            "legend.fontsize": 8,
            "lines.linewidth": 2.0,
            "figure.dpi": 150,
        }
    )


def _grid(ax, axis: str = "both"):
    ax.grid(True, axis=axis, zorder=0)
    ax.set_axisbelow(True)


def _headroom(ax, factor: float = 1.32):
    """Leave room above the marks so an upper-left legend never sits on the data."""
    lo, hi = ax.get_ylim()
    ax.set_ylim(lo, lo + (hi - lo) * factor)


def _residual_axis(ax, values, tol: float, unit: str, compact: bool = False) -> str:
    """Scale a residual panel to its own residuals; return a note on the tolerance.

    Residuals here are normally ~1e-4 against a 1e-2 tolerance, so drawing the
    tolerance band to scale would flatten every mark onto the zero line and the panel
    would show nothing. Scale to the residuals instead and, when the band is off
    scale, say in words how much headroom there is -- that number is the finding.
    """
    import numpy as np

    finite = values[np.isfinite(values)]
    worst = float(np.abs(finite).max()) if finite.size else 0.0
    if worst > 0 and tol <= worst * 1.3:
        lim = max(tol, worst) * 1.35
        ax.axhspan(-tol, tol, color=C_BAND, zorder=0)
        note = f"tol +/-{tol:g} {unit} shaded"
    elif worst > 0:
        lim = max(worst * 1.4, tol * 0.02)
        note = (
            f"tol +/-{tol:g} {unit} = {tol / worst:.0f}x this panel"
            if compact
            else f"worst {worst:.2e} {unit} -- the +/-{tol:g} {unit} tolerance is "
            f"{tol / worst:.0f}x wider than this panel"
        )
    else:
        lim = max(worst * 1.4, tol * 0.02)
        note = f"all residuals zero (tol +/-{tol:g} {unit})"
    ax.set_ylim(-lim, lim)
    return note


def plot_station_daily(cmp, st: Station, grid_cell, tol_c: float, tol_mm: float, out: Path):
    """Four panels for one station: precip, precip residual, temps, temp residuals.

    Every panel is single-axis and single-unit -- the residual panels exist precisely
    so the comparison never needs a second y-scale on top of the values.
    """
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    import numpy as np

    d = cmp[cmp["station_id"] == st.id].sort_values("date")
    x = d["date"].to_numpy()
    fig, axes = plt.subplots(4, 1, figsize=(11, 12), sharex=True)

    # 1. Daily precipitation: recomputed as bars, report as markers on top. A marker
    #    sitting on its bar top *is* agreement, so disagreement is visible directly.
    ax = axes[0]
    mine = d[PRECIP_COL + "_mine"].to_numpy(dtype="float64")
    rep = d[PRECIP_COL + "_report"].to_numpy(dtype="float64")
    ax.bar(x, np.nan_to_num(mine), width=0.62, color=C_SERIES[0], label="recomputed (tp_hourly)")
    ax.plot(
        x,
        rep,
        marker="o",
        markersize=4.5,
        linestyle="none",
        color=C_SERIES[1],
        markeredgecolor=C_SURFACE,
        markeredgewidth=1.0,
        label="daily station report",
    )
    for xi in x[np.isnan(mine) & np.isnan(rep)]:
        ax.annotate(
            "NaN",
            (xi, 0),
            textcoords="offset points",
            xytext=(0, 6),
            ha="center",
            fontsize=7,
            color=C_INK_2,
            rotation=90,
        )
    ax.set_ylabel("precipitation (mm/day)")
    ax.set_title(
        f"Station {st.id} -- daily precipitation, grid cell "
        f"({grid_cell[0]:.3f}, {grid_cell[1]:.3f})"
    )
    _grid(ax)
    _headroom(ax)
    ax.legend(loc="upper left", ncol=2)

    # 2. Precip residual, diverging by sign around a zero baseline.
    ax = axes[1]
    diff = d[PRECIP_COL + "_diff"].to_numpy(dtype="float64")
    note = _residual_axis(ax, diff, tol_mm, "mm")
    ax.bar(x, np.nan_to_num(diff), width=0.62, color=np.where(diff >= 0, C_POS, C_NEG), zorder=2)
    ax.axhline(0, color=C_INK_2, linewidth=0.8, zorder=3)
    ax.set_ylabel("report - recomputed (mm)")
    ax.set_title("Precipitation difference -- red: report higher, blue: recomputed higher\n" + note)
    _grid(ax)

    # 3. Daily temperatures, recomputed. Warm hue for the max, cool for the min.
    ax = axes[2]
    for col, color, label in zip(
        ["t2m_max_c", "t2m_mean_c", "t2m_min_c", "d2m_mean_c"],
        [C_SERIES[1], C_SERIES[2], C_SERIES[0], C_INK_2],
        ["t2m max", "t2m mean", "t2m min", "d2m mean"],
        strict=True,
    ):
        ax.plot(
            x,
            d[col + "_mine"],
            color=color,
            label=label,
            linestyle="--" if col == "d2m_mean_c" else "-",
        )
    ax.set_ylabel("degrees C")
    ax.set_title("Recomputed daily temperature and dewpoint")
    _grid(ax)
    _headroom(ax)
    ax.legend(loc="upper left", ncol=4)

    # 4. Temperature/dewpoint residuals, all four on one degC axis.
    ax = axes[3]
    note = _residual_axis(
        ax, d[[c + "_diff" for c in TEMP_COLS]].to_numpy(dtype="float64"), tol_c, "degC"
    )
    for col, color, label in zip(
        TEMP_COLS,
        [C_SERIES[1], C_SERIES[0], C_SERIES[2], C_INK_2],
        ["t2m max", "t2m min", "t2m mean", "d2m mean"],
        strict=True,
    ):
        ax.plot(
            x,
            d[col + "_diff"],
            color=color,
            marker="o",
            markersize=3.0,
            markeredgecolor=C_SURFACE,
            markeredgewidth=0.6,
            label=label,
            zorder=2,
        )
    ax.axhline(0, color=C_INK_2, linewidth=0.8, zorder=3)
    ax.set_ylabel("report - recomputed (degC)")
    ax.set_title("Temperature / dewpoint difference\n" + note)
    _grid(ax)
    _headroom(ax)
    ax.legend(loc="upper left", ncol=4)

    ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    fig.autofmt_xdate(rotation=45, ha="right")
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)


def plot_agreement_scatter(cmp, stations: list[Station], tol_c: float, tol_mm: float, out: Path):
    """Two views per column: report vs recomputed on 1:1, and residual vs magnitude.

    The 1:1 row is the gut check. The residual row is the one that can actually show
    a defect: a bias that grows with the value (float32 quantization, a unit slip, a
    boundary effect) is a slope there and invisible on the 1:1 line. Three stations,
    so the categorical slots stay inside the all-pairs-safe set that scatter needs.
    """
    import matplotlib.pyplot as plt
    import numpy as np

    ncol = len(VALUE_COLS)
    fig, axes = plt.subplots(2, ncol, figsize=(3.5 * ncol, 8.0))
    color_of = {st.id: C_SERIES[i % len(C_SERIES)] for i, st in enumerate(stations)}

    for j, col in enumerate(VALUE_COLS):
        tol = tol_mm if col == PRECIP_COL else tol_c
        unit = "mm" if col == PRECIP_COL else "degC"
        top, bot = axes[0, j], axes[1, j]

        for st in stations:
            d = cmp[cmp["station_id"] == st.id]
            # Unfilled markers: the two series agree to ~1e-4, so filled ones would
            # simply hide whichever station is drawn first.
            top.plot(
                d[col + "_mine"],
                d[col + "_report"],
                marker="o",
                markersize=4.5,
                linestyle="none",
                markerfacecolor="none",
                markeredgecolor=color_of[st.id],
                markeredgewidth=1.2,
                label=f"station {st.id}",
            )
            bot.plot(
                d[col + "_mine"],
                d[col + "_diff"],
                marker="o",
                markersize=4.5,
                linestyle="none",
                markerfacecolor="none",
                markeredgecolor=color_of[st.id],
                markeredgewidth=1.2,
                label=f"station {st.id}",
            )

        mine = cmp[col + "_mine"].to_numpy(dtype="float64")
        rep = cmp[col + "_report"].to_numpy(dtype="float64")
        ok = np.isfinite(mine) & np.isfinite(rep)
        if ok.any():
            lo = float(min(mine[ok].min(), rep[ok].min()))
            hi = float(max(mine[ok].max(), rep[ok].max()))
            pad = 0.05 * ((hi - lo) or 1.0)
            top.plot(
                [lo - pad, hi + pad],
                [lo - pad, hi + pad],
                color=C_INK_2,
                linewidth=0.9,
                linestyle=":",
                zorder=1,
                label="1:1",
            )
            worst = float(np.abs(rep[ok] - mine[ok]).max())
            top.set_title(f"{col}\nmax|diff| {worst:.2e} {unit}  (tol {tol:g})", fontsize=9)

        top.set_xlabel(f"recomputed ({unit})")
        top.set_ylabel(f"report ({unit})")
        top.set_aspect("equal", adjustable="datalim")
        _grid(top)

        note = _residual_axis(
            bot, cmp[col + "_diff"].to_numpy(dtype="float64"), tol, unit, compact=True
        )
        bot.axhline(0, color=C_INK_2, linewidth=0.8, zorder=3)
        bot.set_xlabel(f"recomputed ({unit})")
        bot.set_ylabel(f"report - recomputed ({unit})")
        bot.set_title(note, fontsize=8)
        _grid(bot)

    axes[0, 0].legend(loc="upper left")
    fig.suptitle(
        "Daily station report vs independent recomputation", fontsize=12, fontweight="bold"
    )
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)


def plot_hourly_receipt(hrs, st: Station, day, tz: str, totals: dict, out: Path):
    """The hour-by-hour receipt for one local day at one station.

    Top: the ``tp_hourly`` increments that get summed, in local time, with the local
    day shaded. Bottom: their running sum against the raw ``tp`` accumulation, both
    in mm on one axis -- the accumulation's 00 UTC reset is exactly why the raw
    series cannot be summed against a local day, and the running sum is what the
    report's number actually is.
    """
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd

    day = pd.Timestamp(day)
    lo = day.tz_localize(tz) - pd.Timedelta(hours=3)
    hi = (day + pd.Timedelta(days=1)).tz_localize(tz) + pd.Timedelta(hours=3)
    local = hrs.index.tz_localize("UTC").tz_convert(tz)
    win = hrs[(local >= lo) & (local <= hi)].copy()
    win.index = local[(local >= lo) & (local <= hi)]
    in_day = (win.index >= day.tz_localize(tz)) & (
        win.index < (day + pd.Timedelta(days=1)).tz_localize(tz)
    )

    fig, axes = plt.subplots(2, 1, figsize=(11, 7), sharex=True)
    x = win.index.tz_localize(None).to_numpy()

    ax = axes[0]
    inc = win[V_TP_HOURLY].to_numpy(dtype="float64") * M_TO_MM
    ax.axvspan(
        day, day + pd.Timedelta(days=1), color=C_BAND, zorder=0, label=f"local day {day.date()}"
    )
    ax.bar(
        x[in_day],
        np.nan_to_num(inc[in_day]),
        width=0.030,
        color=C_SERIES[0],
        zorder=2,
        label="tp_hourly, inside the day (summed)",
    )
    ax.bar(
        x[~in_day],
        np.nan_to_num(inc[~in_day]),
        width=0.030,
        color=C_OUTSIDE,
        zorder=2,
        label="tp_hourly, outside the day",
    )
    for xi in x[np.isnan(inc)]:
        ax.axvline(xi, color=C_POS, linewidth=1.4, linestyle="--", zorder=3)
    if np.isnan(inc).any():
        ax.plot(
            [],
            [],
            color=C_POS,
            linewidth=1.4,
            linestyle="--",
            label="tp_hourly is NaN (no predecessor in the ingest block)",
        )
    ax.set_ylabel("hourly increment (mm)")
    ax.set_title(f"Station {st.id} -- hourly precipitation increments, {day.date()} local ({tz})")
    ax.legend(loc="upper left")
    _grid(ax)

    ax = axes[1]
    ax.axvspan(day, day + pd.Timedelta(days=1), color=C_BAND, zorder=0)
    running = np.nancumsum(np.where(in_day, np.nan_to_num(inc), 0.0))
    ax.plot(x, running, color=C_SERIES[0], label="running sum of tp_hourly over the local day")
    ax.plot(
        x,
        win[V_TP].to_numpy(dtype="float64") * M_TO_MM,
        color=C_SERIES[1],
        linestyle="--",
        label="raw tp accumulation (resets at 00 UTC)",
    )
    for label, value, color in [
        ("recomputed total", totals.get("mine"), C_SERIES[0]),
        ("report total", totals.get("report"), C_SERIES[2]),
    ]:
        if value is not None and not np.isnan(value):
            ax.axhline(
                value, color=color, linewidth=1.2, linestyle=":", label=f"{label} {value:.3f} mm"
            )
    ax.set_ylabel("millimetres")
    ax.set_title("Increments telescope to the day total; the raw accumulation resets mid-day")
    _grid(ax)
    _headroom(ax, 1.45)
    ax.legend(loc="upper left")

    ax.xaxis.set_major_locator(mdates.HourLocator(interval=3))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    fig.autofmt_xdate(rotation=45, ha="right")
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)


def worst_precip_day(cmp, station_id: str, tol_mm: float):
    """The day the receipt plot should show for a station.

    A difference over tolerance is the thing to look at; below tolerance every
    "worst" day is just float noise, and the receipt is far more informative on the
    day that actually rained.
    """
    d = cmp[cmp["station_id"] == station_id]
    if not len(d):
        return None, "none"
    diff = d[PRECIP_COL + "_diff"].abs()
    if diff.notna().any() and float(diff.max()) > tol_mm:
        return d.loc[diff.idxmax(), "date"], "largest precip difference (over tolerance)"
    wet = d[PRECIP_COL + "_mine"]
    if wet.notna().any():
        return d.loc[wet.idxmax(), "date"], "wettest day (no disagreement to show)"
    return d["date"].iloc[0], "first day"


def make_plots(
    cmp, frames, stations, grid_cells, outdir: Path, tz: str, tol_c: float, tol_mm: float
) -> list[Path]:
    import matplotlib

    matplotlib.use("Agg")
    _style()

    written = []
    for st in stations:
        p = outdir / f"daily_{st.id}.png"
        plot_station_daily(cmp, st, grid_cells[st.id], tol_c, tol_mm, p)
        written.append(p)

    p = outdir / "agreement_scatter.png"
    plot_agreement_scatter(cmp, stations, tol_c, tol_mm, p)
    written.append(p)

    for st in stations:
        day, why = worst_precip_day(cmp, st.id, tol_mm)
        if day is None or day != day:  # None / NaT
            continue
        row = cmp[(cmp["station_id"] == st.id) & (cmp["date"] == day)].iloc[0]
        totals = {
            "mine": float(row[PRECIP_COL + "_mine"]),
            "report": float(row[PRECIP_COL + "_report"]),
        }
        p = outdir / f"hourly_{st.id}_{day.date()}.png"
        plot_hourly_receipt(frames[st.id], st, day, tz, totals, p)
        print(f"  station {st.id}: receipt day {day.date()} ({why})")
        written.append(p)
    return written


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def _load_env_and_deps(parser: argparse.ArgumentParser, want_plots: bool) -> None:
    from dotenv import load_dotenv

    load_dotenv()
    try:
        import icechunk  # noqa: F401
        import pandas  # noqa: F401
        import pyarrow  # noqa: F401
        import s3fs  # noqa: F401
        import xarray  # noqa: F401
    except ImportError as e:
        parser.error(f"Missing dependency: {e}. Run `uv sync`.")
    if want_plots:
        try:
            import matplotlib  # noqa: F401
        except ImportError:
            parser.error(
                "matplotlib is needed for the diagnostic plots. Run `uv sync` (it is a "
                "dev dependency), or pass --no-plots for the text report only."
            )


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--state", default=DEFAULT_STATE, help="USPS state code of the store.")
    p.add_argument("--year", type=int, required=True, help="Year of the report under test.")
    p.add_argument("--month", type=int, required=True, help="Month (1-12) of the report.")
    p.add_argument("--prefix", default=None, help="Icechunk bucket prefix (default: per state).")
    p.add_argument(
        "--report",
        default=None,
        help="Report parquet: a local path or bucket/key (default: the asset's output path).",
    )
    p.add_argument("--outdir", default=DEFAULT_OUTDIR, help="Directory for the CSV and PNGs.")
    p.add_argument("--tz", default=DEFAULT_TZ, help="Local day definition.")
    p.add_argument("--tol-c", type=float, default=0.02, help="Tolerance in degC.")
    p.add_argument("--tol-mm", type=float, default=0.02, help="Tolerance in mm.")
    p.add_argument("--no-plots", action="store_true", help="Text report and CSV only.")
    p.add_argument(
        "--workers",
        type=int,
        default=32,
        help="Dask threads for parallel S3 reads. The work is I/O-bound, so more "
        "threads than cores helps hide object-store latency.",
    )
    args = p.parse_args()
    if not 1 <= args.month <= 12:
        p.error(f"--month must be 1-12, got {args.month}")

    _load_env_and_deps(p, want_plots=not args.no_plots)

    state = args.state.strip().upper()
    prefix = args.prefix or ZARR_PREFIX_TEMPLATE.format(state=state)
    report_path = args.report or (
        f"{os.environ['BUCKET_NAME']}/"
        + REPORT_KEY_TEMPLATE.format(state=state, year=args.year, month=args.month)
    )
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    print(f"Report under test : {report_path}")
    print(f"Store             : prefix {prefix!r}")
    report = load_report(report_path, [s.id for s in STATIONS])
    if report.empty:
        p.error(
            f"the report at {report_path} has no rows for stations "
            f"{[s.id for s in STATIONS]}. Wrong month, or those stations were dropped "
            f"as all-NaN (outside the clip)."
        )
    print(f"  {len(report)} report rows for {report['station_id'].nunique()} stations")

    ds = open_store(prefix)
    frames, grid_cells = hourly_frames(ds, STATIONS, args.year, args.month, args.workers)

    for st in STATIONS:
        if frames[st.id][V_T2M].isna().all():
            glat, glon = grid_cells[st.id]
            p.error(
                f"station {st.id}: every hour of {args.year}-{args.month:02d} is NaN at "
                f"grid cell ({glat:.3f}, {glon:.3f}). Either that month is not ingested "
                f"or the station is outside the clip footprint."
            )

    recomputed = {
        st.id: recompute_daily(frames[st.id], args.year, args.month, args.tz) for st in STATIONS
    }
    tele = {st.id: telescoping_check(frames[st.id], args.year, args.month) for st in STATIONS}

    cmp = build_comparison(recomputed, report, STATIONS)
    passed = print_report(cmp, STATIONS, grid_cells, tele, args.tol_c, args.tol_mm, args.tz)

    csv_path = outdir / "comparison.csv"
    cols = ["station_id", "date", "present_in", "n_hours", "n_nan_tp_hours", "precip_skipna_mm"]
    for col in VALUE_COLS:
        cols += [f"{col}_report", f"{col}_mine", f"{col}_diff"]
    out = cmp[cols].copy()
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    out.to_csv(csv_path, index=False)
    print(f"\nWrote {csv_path}")

    if not args.no_plots:
        print("Plotting ...")
        for path in make_plots(
            cmp, frames, STATIONS, grid_cells, outdir, args.tz, args.tol_c, args.tol_mm
        ):
            print(f"  {path}")

    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
