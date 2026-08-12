"""Per-station daily summaries from the ERA5-Land Icechunk store (Dagster-free).

Reads station coordinates (a CSV in the object store), pulls each station's
nearest grid cell out of an opened ERA5-Land Dataset, and aggregates the hourly
values to **local Central-time calendar days**:

  * t2m_max_c, t2m_min_c, t2m_mean_c -- daily max / min / mean 2m temperature (degC)
  * d2m_mean_c                        -- daily mean 2m dewpoint (degC)
  * precip_total_mm                   -- daily total precipitation (mm)

This mirrors the throwaway ``scripts/era5-stations.py`` (the reference impl) but
buckets by Central day instead of UTC day, which changes how precipitation must
be handled -- see :func:`daily_summary_local`.
"""

from __future__ import annotations

import calendar
from typing import TYPE_CHECKING, NamedTuple

from dagster_pri.era5.accumulation import hourly_increment

if TYPE_CHECKING:
    import pandas as pd
    import xarray as xr

# ERA5-Land variable names *as stored*. The CDS NetCDF carries short names and the
# ingest path (era5/transform.py) does not rename data vars, so the store holds
# t2m / tp / d2m (not the CDS long names "2m_temperature" etc.).
V_T2M = "t2m"
V_TP = "tp"
V_D2M = "d2m"

# Default local day definition. ERA5-Land is UTC; Illinois stations are Central.
DEFAULT_TZ = "America/Chicago"

# Output value columns, in display order.
VALUE_COLS = ["t2m_max_c", "t2m_min_c", "t2m_mean_c", "d2m_mean_c", "precip_total_mm"]

# Hourly output value columns, in native ERA5 units as stored (t2m/d2m in Kelvin,
# tp the raw accumulation-since-00Z in metres). Kept raw and unconverted.
HOURLY_VALUE_COLS = [V_T2M, V_TP, V_D2M]


class Station(NamedTuple):
    id: str
    name: str
    lat: float
    lon: float


def load_stations(fs, bucket: str, key: str) -> list[Station]:
    """Read the stations CSV from the object store into ``Station`` rows.

    Expects the columns of ``data/station_coords_updated.csv`` (``ID``,
    ``Site Name``, ``Lat``, ``Lon``, ``Project``). Only ID/name/lat/lon are kept;
    rows with a missing lat or lon (e.g. ``RG37``) are dropped. ``ID`` is read as a
    string since the file mixes numeric and alphanumeric ids.
    """
    import pandas as pd

    with fs.open(f"{bucket}/{key}", "rb") as f:
        df = pd.read_csv(f, dtype={"ID": str})
    df = df.dropna(subset=["Lat", "Lon"])
    return [
        Station(id=str(r["ID"]), name=str(r["Site Name"]), lat=float(r["Lat"]), lon=float(r["Lon"]))
        for _, r in df.iterrows()
    ]


def padded_utc_slice(ds: xr.Dataset, year: int, month: int) -> xr.Dataset:
    """Slice ``ds`` to the requested month padded by +/- 1 UTC day.

    Central-day boundaries fall at 05/06 UTC, so a one-day pad on each side fully
    covers every Central day that overlaps the month and gives the precipitation
    de-accumulation its neighbouring hours. The padding days are trimmed back out
    in :func:`daily_summary_local`.
    """
    import pandas as pd

    last_day = calendar.monthrange(year, month)[1]
    start = pd.Timestamp(year, month, 1) - pd.Timedelta(days=1)
    end = pd.Timestamp(year, month, last_day, 23) + pd.Timedelta(days=1)
    return ds.sel(time=slice(start, end))


def utc_month_slice(ds: xr.Dataset, year: int, month: int) -> xr.Dataset:
    """Slice ``ds`` to exactly the requested UTC calendar month (no padding).

    The store's ``time`` axis is hourly UTC, so an unpadded slice from ``00:00`` of
    the first day through ``23:00`` of the last day is precisely the month. Unlike
    :func:`padded_utc_slice` there is no local-day conversion here, so no neighbouring
    hours are needed.
    """
    import pandas as pd

    last_day = calendar.monthrange(year, month)[1]
    start = pd.Timestamp(year, month, 1)
    end = pd.Timestamp(year, month, last_day, 23)
    return ds.sel(time=slice(start, end))


def extract_points(ds: xr.Dataset, stations: list[Station]) -> xr.Dataset:
    """Nearest-cell extraction for ALL stations at once -> dims (time, station).

    Vectorized (pointwise) indexing reads each spatial chunk once and pulls every
    station out of it in memory, so the read cost is independent of the station
    count. (The store keeps the whole state grid in a single spatial chunk, so a
    per-station loop would otherwise re-read the same bytes once per station.)
    """
    import xarray as xr

    ids = [s.id for s in stations]
    sel_lat = xr.DataArray([s.lat for s in stations], dims="station", coords={"station": ids})
    sel_lon = xr.DataArray([s.lon for s in stations], dims="station", coords={"station": ids})
    return ds[[V_T2M, V_TP, V_D2M]].sel(latitude=sel_lat, longitude=sel_lon, method="nearest")


def daily_summary_local(pts: xr.Dataset, year: int, month: int, tz: str = DEFAULT_TZ) -> xr.Dataset:
    """Lazy daily aggregation over a (time, station) point dataset, by local day.

    Builds (but does not realize) the dask graph; the caller runs a single
    ``.compute()`` so reads and aggregation pipeline across all time chunks. The
    result is on dims (local_day, station), trimmed to the requested month.

    Local day
    ---------
    The store's ``time`` axis is hourly UTC. We label each timestep with its
    calendar day in ``tz`` and ``groupby`` that label (``resample`` is UTC-bound).

    Precipitation
    -------------
    ERA5-Land ``tp`` is an accumulation since 00 UTC that **resets at 00 UTC**, so
    a naive resample/sum is wrong, and the UTC-only "value at 00:00 of D+1" trick
    does not align to Central days. Instead we de-accumulate to per-hour increments
    (:func:`dagster_pri.era5.accumulation.hourly_increment`) and sum those by local
    day, which is day-definition agnostic. This is the same kernel the ingest uses
    to derive the store's ``tp_hourly`` array.
    """
    import numpy as np
    import pandas as pd
    import xarray as xr

    # Per-timestep local-day label, attached as a coordinate over the time dim.
    t = pd.DatetimeIndex(pts["time"].values)
    local_day = t.tz_localize("UTC").tz_convert(tz).normalize().tz_localize(None)
    pts = pts.assign_coords(local_day=("time", local_day.values))

    # Temperature / dewpoint: instantaneous -> daily stats, then K -> degC.
    t2m = pts[V_T2M].groupby("local_day")
    out = xr.Dataset(
        {
            "t2m_max_c": t2m.max() - 273.15,
            "t2m_min_c": t2m.min() - 273.15,
            "t2m_mean_c": t2m.mean() - 273.15,
            "d2m_mean_c": pts[V_D2M].groupby("local_day").mean() - 273.15,
        }
    )

    # Precipitation: de-accumulate to hourly increments, then sum by local day.
    inc = hourly_increment(pts[V_TP])
    # skipna=False so a station whose nearest cell is all-NaN (outside the clip)
    # yields NaN (dropped downstream) rather than a misleading 0.
    out["precip_total_mm"] = inc.groupby("local_day").sum(skipna=False) * 1000.0

    # Drop the padding days: keep only local days in the requested month.
    days = pd.DatetimeIndex(out["local_day"].values)
    keep = (days.year == year) & (days.month == month)
    return out.isel(local_day=np.flatnonzero(keep))


def to_dataframe(daily: xr.Dataset, stations: list[Station]) -> pd.DataFrame:
    """(local_day, station) Dataset -> tidy DataFrame with id + name only.

    Drops stations whose nearest cell is entirely NaN (outside the clip footprint)
    via ``dropna(how="all")`` on the value columns.
    """
    import pandas as pd

    df = daily[VALUE_COLS].to_dataframe().reset_index()
    df = df.rename(columns={"local_day": "date", "station": "station_id"})
    df = df.dropna(how="all", subset=VALUE_COLS)

    name_by_id = {s.id: s.name for s in stations}
    df["station_name"] = df["station_id"].map(name_by_id)

    df = df[["station_id", "station_name", "date", *VALUE_COLS]]
    df = df.sort_values(["station_id", "date"]).reset_index(drop=True)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    for col in VALUE_COLS:
        df[col] = df[col].round(3)
    return df


def hourly_to_dataframe(pts: xr.Dataset, stations: list[Station]) -> pd.DataFrame:
    """(time, station) point Dataset -> tidy hourly DataFrame, native UTC + units.

    One row per (station, UTC hour). Values are the raw store values, unconverted
    and unrounded: ``t2m``/``d2m`` in Kelvin and ``tp`` the raw accumulation in
    metres. The ``time_utc`` column is the store's naive-UTC hourly stamp as-is.

    Drops stations whose nearest cell is entirely NaN (outside the clip footprint)
    via ``dropna(how="all")`` on the value columns. The explicit final column
    selection also drops the ``latitude``/``longitude`` coords that
    :func:`extract_points` attaches per station.
    """

    df = pts[HOURLY_VALUE_COLS].to_dataframe().reset_index()
    df = df.rename(columns={"time": "time_utc", "station": "station_id"})
    df = df.dropna(how="all", subset=HOURLY_VALUE_COLS)

    name_by_id = {s.id: s.name for s in stations}
    df["station_name"] = df["station_id"].map(name_by_id)

    df = df[["station_id", "station_name", "time_utc", *HOURLY_VALUE_COLS]]
    df = df.sort_values(["station_id", "time_utc"]).reset_index(drop=True)
    return df
