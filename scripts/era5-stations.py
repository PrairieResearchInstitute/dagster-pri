#!/usr/bin/env python3
"""
era5-stations.py

Read the ERA5-Land/Illinois Icechunk store written by era5-illinois.py (and the
``era5_iceberg`` Dagster asset) and compute **daily** weather summaries at a set
of station (rain gauge) locations, then write them to a CSV.

Per station, per UTC day:
  * precip_total_mm  -- daily total precipitation
  * t2m_max_c, t2m_min_c, t2m_mean_c -- daily max / min / mean 2m temperature
  * d2m_mean_c       -- daily mean 2m dewpoint

The stations ("IVA" -- a local name for a rain-gauge cluster; the coordinates are
in central Illinois) are embedded below.

ERA5-Land precipitation note
----------------------------
``total_precipitation`` is an accumulation **since 00 UTC that resets daily**, so
a naive ``resample("1D").sum()`` is wrong. The full-day total for day D is the
value at **00:00 UTC of day D+1** (the accumulation just before the reset). We
select the hour-00 timesteps, attribute each to the *previous* day, and convert
m -> mm. The final day in the store has no following 00 UTC sample and is dropped.

Temperature and dewpoint are instantaneous, so they use straightforward daily
max/min/mean over the hourly values within each UTC calendar day.

Run
---
  uv run scripts/era5-stations.py
  uv run scripts/era5-stations.py --out scratch/era5_station_daily.csv
  uv run scripts/era5-stations.py --start 2024-06-01 --end 2024-06-30

S3/Ceph connection is read from .env exactly like the writer/verifier scripts.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    import icechunk

DEFAULT_ZARR_PREFIX = "era5-land/icechunk/IL"
DEFAULT_OUT = "scratch/era5_station_daily.csv"

# ERA5-Land variable names as stored. The CDS NetCDF carries short names and the
# ingest path (era5/transform.py) does not rename data vars, so the store holds
# t2m / tp / d2m (not the CDS long names "2m_temperature" etc.).
V_T2M = "t2m"
V_TP = "tp"
V_D2M = "d2m"


class Station(NamedTuple):
    id: int
    name: str
    lat: float
    lon: float
    group: str


# Embedded IVA rain-gauge stations (central Illinois).
STATIONS: list[Station] = [
    Station(2, "IVA rain gage", 40.477878, -89.765486, "IVA"),
    Station(3, "IVA rain gage", 40.482442, -89.625972, "IVA"),
    Station(4, "IVA rain gage", 40.408206, -89.911394, "IVA"),
]


# --------------------------------------------------------------------------- #
# Icechunk storage / repo (mirrors era5-illinois.py / verify-era5-illinois.py)
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

    storage = make_icechunk_storage(prefix)
    repo = icechunk.Repository.open(storage)
    session = repo.readonly_session(branch)
    ds = xr.open_zarr(session.store, consolidated=False, decode_timedelta=True)
    return repo, ds


# --------------------------------------------------------------------------- #
# Populated time range (the pre-allocated axis is mostly all-NaN)
# --------------------------------------------------------------------------- #
def populated_time_range(ds, var: str):
    """Return (i0, i1) bounding the timesteps that actually hold data.

    The store's `time` axis is pre-allocated from 1950 and only ingested months
    carry finite values. Reduce over space to a per-timestep "has any data" mask.
    Returns (None, None) if nothing is populated.
    """
    import numpy as np

    if var not in ds.data_vars:
        return None, None

    mask = ds[var].notnull().any(dim=("latitude", "longitude")).compute().values
    idx = np.flatnonzero(mask)
    if idx.size == 0:
        return None, None
    return int(idx[0]), int(idx[-1])


VALUE_COLS = ["precip_total_mm", "t2m_max_c", "t2m_min_c", "t2m_mean_c", "d2m_mean_c"]


# --------------------------------------------------------------------------- #
# Vectorized extraction + daily aggregation (all stations at once)
# --------------------------------------------------------------------------- #
def extract_points(ds, stations: list[Station]):
    """Nearest-cell extraction for ALL stations in one shot -> dims (time, station).

    Vectorized (pointwise) indexing reads each spatial chunk once and pulls every
    station out of it in memory, so the read cost is independent of the station
    count. (The store keeps the whole state grid in a single spatial chunk, so a
    per-station loop would otherwise re-read the same bytes once per station.)
    """
    import xarray as xr

    ids = [s.id for s in stations]
    sel_lat = xr.DataArray([s.lat for s in stations], dims="station", coords={"station": ids})
    sel_lon = xr.DataArray([s.lon for s in stations], dims="station", coords={"station": ids})
    pts = ds[[V_T2M, V_TP, V_D2M]].sel(latitude=sel_lat, longitude=sel_lon, method="nearest")
    return pts


def daily_summary(pts):
    """Lazy daily aggregation over a (time, station) point dataset.

    Returns an xarray Dataset on dims (time=day, station). Nothing is read here;
    the dask graph is built and realized by a single .compute() in the caller so
    reads and aggregation pipeline across all time chunks in parallel.
    """
    import numpy as np
    import xarray as xr

    # Temperature / dewpoint: instantaneous -> daily stats, then K -> C.
    t2m = pts[V_T2M].resample(time="1D")
    d2m_mean = pts[V_D2M].resample(time="1D").mean()
    out = xr.Dataset(
        {
            "t2m_max_c": t2m.max() - 273.15,
            "t2m_min_c": t2m.min() - 273.15,
            "t2m_mean_c": t2m.mean() - 273.15,
            "d2m_mean_c": d2m_mean - 273.15,
        }
    )

    # Precipitation: ERA5-Land tp accumulates since 00 UTC and resets daily, so the
    # 00:00 sample of day D+1 holds day D's full-day total. Select the hour-00
    # samples, relabel each to the previous day, and convert m -> mm. xarray aligns
    # this to `out`'s daily index on assignment (trailing day with no following
    # 00 UTC -> NaN; the leading pre-window day -> dropped).
    tp = pts[V_TP]
    at_midnight = tp.isel(time=(tp.time.dt.hour == 0))
    at_midnight = at_midnight.assign_coords(time=at_midnight.time - np.timedelta64(1, "D"))
    out["precip_total_mm"] = at_midnight * 1000.0
    return out


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def _load_env_and_deps(parser: argparse.ArgumentParser) -> None:
    from dotenv import load_dotenv

    load_dotenv()
    try:
        import icechunk  # noqa: F401
        import pandas  # noqa: F401
        import xarray  # noqa: F401
    except ImportError as e:
        parser.error(f"Missing dependency: {e}. Run `uv sync`.")


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--prefix", default=DEFAULT_ZARR_PREFIX, help="Icechunk bucket prefix.")
    p.add_argument("--out", default=DEFAULT_OUT, help="Output CSV path.")
    p.add_argument("--start", default=None, help="Optional start date (UTC), e.g. 2024-01-01.")
    p.add_argument("--end", default=None, help="Optional end date (UTC), e.g. 2024-12-31.")
    p.add_argument(
        "--workers",
        type=int,
        default=32,
        help="Dask threads for parallel S3 reads. The work is I/O-bound, so more "
        "threads than cores helps hide object-store latency.",
    )
    args = p.parse_args()

    _load_env_and_deps(p)

    import numpy as np
    import pandas as pd

    print(f"Opening store at prefix {args.prefix!r} ...")
    _repo, ds = open_store(args.prefix)

    # Restrict to the populated span so we don't drag the all-NaN 1950+ axis
    # through the computation.
    i0, i1 = populated_time_range(ds, V_T2M)
    if i0 is None:
        p.error(f"No populated timesteps for {V_T2M!r} in {args.prefix!r}.")
    ds = ds.isel(time=slice(i0, i1 + 1))
    times = ds.time.values
    print(
        f"Populated span: {pd.Timestamp(times[0])} .. {pd.Timestamp(times[-1])} UTC "
        f"({len(times)} hourly steps)"
    )

    # Optional user bounds on top of the populated span.
    if args.start or args.end:
        ds = ds.sel(time=slice(args.start, args.end))
        if ds.sizes.get("time", 0) == 0:
            p.error(f"No timesteps in requested window [{args.start}, {args.end}].")
        print(f"Restricted to [{args.start}, {args.end}] -> {ds.sizes['time']} steps")

    # Vectorized nearest-cell extraction for every station, then one lazy daily
    # aggregation realized by a single parallel compute.
    pts = extract_points(ds, STATIONS)
    grid_lat = dict(zip(pts.station.values.tolist(), pts.latitude.values.tolist(), strict=True))
    grid_lon = dict(zip(pts.station.values.tolist(), pts.longitude.values.tolist(), strict=True))
    for st in STATIONS:
        print(
            f"  station {st.id} ({st.lat:.4f}, {st.lon:.4f}) -> "
            f"grid cell ({grid_lat[st.id]:.3f}, {grid_lon[st.id]:.3f})"
        )

    daily = daily_summary(pts)
    print(f"Reading + aggregating with dask ({args.workers} threads) ...")
    daily = daily.compute(scheduler="threads", num_workers=args.workers)

    # Warn about stations whose nearest cell is entirely NaN (outside the clip).
    allnan = np.isnan(daily["t2m_mean_c"]).all(dim="time")
    for st in STATIONS:
        if bool(allnan.sel(station=st.id).values):
            print(
                f"  WARNING station {st.id}: nearest grid cell "
                f"({grid_lat[st.id]:.3f}, {grid_lon[st.id]:.3f}) is all-NaN -- "
                "station may be outside the clip footprint.",
                file=sys.stderr,
            )

    # (day, station) Dataset -> tidy long-format DataFrame.
    out = daily[VALUE_COLS].to_dataframe().reset_index()
    out = out.rename(columns={"time": "date", "station": "station_id"})
    out = out.dropna(how="all", subset=VALUE_COLS)

    meta = {s.id: s for s in STATIONS}
    out["station_name"] = out["station_id"].map(lambda i: meta[i].name)
    out["station_lat"] = out["station_id"].map(lambda i: meta[i].lat)
    out["station_lon"] = out["station_id"].map(lambda i: meta[i].lon)
    out["grid_lat"] = out["station_id"].map(grid_lat).round(6)
    out["grid_lon"] = out["station_id"].map(grid_lon).round(6)

    out = out[
        [
            "station_id",
            "station_name",
            "station_lat",
            "station_lon",
            "grid_lat",
            "grid_lon",
            "date",
            *VALUE_COLS,
        ]
    ]
    out = out.sort_values(["station_id", "date"]).reset_index(drop=True)
    out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
    for col in VALUE_COLS:
        out[col] = out[col].round(3)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    print(f"\nWrote {len(out)} rows ({len(STATIONS)} stations) to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
