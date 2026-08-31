"""Unit tests for the per-station daily aggregation (Central-time day).

These exercise the pure logic in :mod:`dagster_pri.era5.stations` against a small
synthetic in-memory dataset (short-name vars t2m/tp/tp_hourly/d2m, like the real
store) -- no Icechunk or S3. The two things worth pinning down are (1) local-day
bucketing vs UTC and (2) that precipitation is the store's precomputed
``tp_hourly`` summed by local day, with the raw ``tp`` accumulation never read.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from dagster_pri.era5.stations import (
    DAILY_SOURCE_VARS,
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
    * tp_hourly: a flat 0.001 m every hour -> 1 mm/hour -> 24 mm per full day.
    * tp: deliberate garbage (a constant 9 m). The daily path must never read the
      raw accumulation, so a wrong `tp` is what proves it doesn't.
    """
    times = pd.date_range("2024-06-14T00:00", "2024-06-17T23:00", freq="1h")
    shape = (len(times), len(LATS), len(LONS))

    t2m = np.full(shape, 283.15)  # 10 degC
    spike = np.flatnonzero(times == pd.Timestamp("2024-06-16T02:00"))[0]
    t2m[spike, :, :] = 313.15  # 40 degC

    d2m = np.full(shape, 278.15)  # 5 degC

    return xr.Dataset(
        {
            "t2m": (("time", "latitude", "longitude"), t2m),
            "d2m": (("time", "latitude", "longitude"), d2m),
            "tp": (("time", "latitude", "longitude"), np.full(shape, 9.0)),
            "tp_hourly": (("time", "latitude", "longitude"), np.full(shape, 0.001)),
        },
        coords={"time": times, "latitude": LATS, "longitude": LONS},
    )


def _run(ds: xr.Dataset | None = None) -> pd.DataFrame:
    ds = _synthetic_store() if ds is None else ds
    pts = extract_points(ds, [STATION], DAILY_SOURCE_VARS)
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


def test_precip_sums_stored_increments_and_ignores_raw_tp():
    df = _run().set_index("date")
    # 1 mm/hour * 24 hours = 24 mm for each fully covered interior Central day.
    # The fixture's raw `tp` is a nonsense 9 m; these totals prove it is not read.
    assert df.loc["2024-06-14", "precip_total_mm"] == 24.0
    assert df.loc["2024-06-15", "precip_total_mm"] == 24.0
    assert df.loc["2024-06-16", "precip_total_mm"] == 24.0


def test_a_nan_increment_hour_makes_the_whole_local_day_nan():
    """The month-edge case: tp_hourly is NaN at 00:00 UTC on the 1st of a month.

    Central day D spans 05:00 UTC on D through 04:00 UTC on D+1, so it swallows
    D+1's midnight step. When that step is NaN, skipna=False makes the day NaN
    rather than a total quietly short one hour of rain.
    """
    ds = _synthetic_store()
    ds["tp_hourly"].loc[{"time": pd.Timestamp("2024-06-17T00:00")}] = np.nan

    df = _run(ds).set_index("date")
    assert np.isnan(df.loc["2024-06-16", "precip_total_mm"])
    # The day before is untouched.
    assert df.loc["2024-06-15", "precip_total_mm"] == 24.0


def test_missing_tp_hourly_is_an_actionable_error():
    ds = _synthetic_store().drop_vars("tp_hourly")
    with pytest.raises(ValueError, match="tp_hourly"):
        extract_points(ds, [STATION], DAILY_SOURCE_VARS)
