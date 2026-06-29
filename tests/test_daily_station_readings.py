"""Unit tests for the per-station daily aggregation (Central-time day).

These exercise the pure logic in :mod:`dagster_pri.era5.stations` against a small
synthetic in-memory dataset (short-name vars t2m/tp/d2m, like the real store) --
no Icechunk or S3. The two things worth pinning down are (1) local-day bucketing
vs UTC and (2) precipitation de-accumulation.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import xarray as xr

from dagster_pri.era5.stations import (
    Station,
    daily_summary_local,
    extract_points,
    to_dataframe,
)

# One station sitting exactly on a grid point so "nearest" is itself.
STATION = Station(id="42", name="Test Gauge", lat=40.25, lon=-89.75)
LATS = [40.0, 40.25, 40.5]
LONS = [-90.0, -89.75, -89.5]


def _synthetic_store() -> xr.Dataset:
    """Hourly UTC dataset over 2024-06-14..06-17 with known, checkable values.

    * t2m: 10 degC everywhere, except a 40 degC spike at 2024-06-16 02:00 UTC,
      which is 2024-06-15 21:00 CDT -> Central day 2024-06-15 (NOT 06-16).
    * d2m: constant 5 degC.
    * tp: ramps 0.001 m per UTC hour and resets at 00 UTC (value at 00:00 is the
      prior day's 0.024 m total), i.e. 1 mm every hour -> 24 mm per full day.
    """
    times = pd.date_range("2024-06-14T00:00", "2024-06-17T23:00", freq="1h")
    shape = (len(times), len(LATS), len(LONS))

    t2m = np.full(shape, 283.15)  # 10 degC
    spike = np.flatnonzero(times == pd.Timestamp("2024-06-16T02:00"))[0]
    t2m[spike, :, :] = 313.15  # 40 degC

    d2m = np.full(shape, 278.15)  # 5 degC

    hours = times.hour.to_numpy()
    tp_hourly = np.where(hours == 0, 24, hours) * 0.001  # m; 00:00 = prior-day total
    tp = np.broadcast_to(tp_hourly[:, None, None], shape).copy()

    return xr.Dataset(
        {
            "t2m": (("time", "latitude", "longitude"), t2m),
            "d2m": (("time", "latitude", "longitude"), d2m),
            "tp": (("time", "latitude", "longitude"), tp),
        },
        coords={"time": times, "latitude": LATS, "longitude": LONS},
    )


def _run() -> pd.DataFrame:
    ds = _synthetic_store()
    pts = extract_points(ds, [STATION])
    daily = daily_summary_local(pts, 2024, 6, tz="America/Chicago").compute()
    return to_dataframe(daily, [STATION])


def test_only_requested_month_and_station_columns():
    df = _run()
    assert list(df.columns) == [
        "station_id",
        "station_name",
        "date",
        "t2m_max_c",
        "t2m_min_c",
        "t2m_mean_c",
        "d2m_mean_c",
        "precip_total_mm",
    ]
    assert set(df["station_id"]) == {"42"}
    assert set(df["station_name"]) == {"Test Gauge"}
    # Trim keeps only June days (the synthetic window spans 06-13..06-17 in local
    # time; the real asset pads +/- 1 UTC day so every in-month day is complete).
    dates = pd.to_datetime(df["date"])
    assert (dates.dt.month == 6).all() and (dates.dt.year == 2024).all()


def test_spike_buckets_into_central_day_not_utc():
    df = _run().set_index("date")
    # 06-16 02:00 UTC == 06-15 21:00 CDT -> the 40 degC max lands on 06-15.
    assert df.loc["2024-06-15", "t2m_max_c"] == 40.0
    # The following Central day sees no spike.
    assert df.loc["2024-06-16", "t2m_max_c"] == 10.0
    # Dewpoint mean is the constant 5 degC.
    assert df.loc["2024-06-15", "d2m_mean_c"] == 5.0


def test_precip_full_central_day_totals_24mm():
    df = _run().set_index("date")
    # 1 mm/hour * 24 hours = 24 mm for each fully covered interior Central day.
    assert df.loc["2024-06-14", "precip_total_mm"] == 24.0
    assert df.loc["2024-06-15", "precip_total_mm"] == 24.0
    assert df.loc["2024-06-16", "precip_total_mm"] == 24.0
