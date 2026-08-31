"""Unit tests for the per-station hourly extraction (native UTC, native units).

These exercise the pure logic in :mod:`dagster_pri.era5.stations` against a small
synthetic in-memory dataset (short-name vars t2m/tp/d2m, like the real store) --
no Icechunk or S3. The things worth pinning down are (1) exact UTC-month slicing
with no padding, (2) that values stay raw (Kelvin / metre accumulation, no
conversion, no de-accumulation), and (3) UTC timestamps pass through unchanged.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import xarray as xr

from dagster_pri.era5.stations import (
    HOURLY_VALUE_COLS,
    Station,
    extract_points,
    hourly_to_dataframe,
    utc_month_slice,
)

# One station sitting exactly on a grid point so "nearest" is itself.
STATION = Station(id="42", name="Test Gauge", lat=40.25, lon=-89.75)
LATS = [40.0, 40.25, 40.5]
LONS = [-90.0, -89.75, -89.5]


def _synthetic_store() -> xr.Dataset:
    """Hourly UTC dataset over 2024-05-31..07-01 with known, checkable values.

    Spans a one-day pad on each side of June so we can prove the unpadded
    :func:`utc_month_slice` keeps *only* June UTC hours.

    * t2m: 283.15 K (10 degC) everywhere, except a 313.15 K (40 degC) spike at
      2024-06-16 02:00 UTC.
    * d2m: constant 278.15 K (5 degC).
    * tp: ramps 0.001 m per UTC hour and resets at 00 UTC (value at 00:00 is the
      prior day's 0.024 m total) -- the raw ERA5 accumulation, kept as-is.
    """
    times = pd.date_range("2024-05-31T00:00", "2024-07-01T23:00", freq="1h")
    shape = (len(times), len(LATS), len(LONS))

    t2m = np.full(shape, 283.15)
    spike = np.flatnonzero(times == pd.Timestamp("2024-06-16T02:00"))[0]
    t2m[spike, :, :] = 313.15

    d2m = np.full(shape, 278.15)

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
    ds = utc_month_slice(ds, 2024, 6)
    pts = extract_points(ds, [STATION], HOURLY_VALUE_COLS)
    return hourly_to_dataframe(pts, [STATION])


def test_columns_and_station_identity():
    df = _run()
    assert list(df.columns) == [
        "station_id",
        "station_name",
        "time_utc",
        "t2m",
        "tp",
        "d2m",
    ]
    assert set(df["station_id"]) == {"42"}
    assert set(df["station_name"]) == {"Test Gauge"}


def test_only_requested_utc_month_and_all_hours():
    df = _run()
    ts = pd.to_datetime(df["time_utc"])
    # Exactly June 2024, no padding days leaking in.
    assert (ts.dt.year == 2024).all()
    assert (ts.dt.month == 6).all()
    # 30 days * 24 hours, one station.
    assert len(df) == 30 * 24
    assert ts.min() == pd.Timestamp("2024-06-01T00:00")
    assert ts.max() == pd.Timestamp("2024-06-30T23:00")


def test_values_are_native_units_unconverted():
    df = _run().set_index("time_utc")
    # Kelvin, not degC: baseline 283.15, the spike row is 313.15.
    assert df.loc["2024-06-16T02:00", "t2m"] == 313.15
    assert df.loc["2024-06-16T01:00", "t2m"] == 283.15
    # Dewpoint stays the constant 278.15 K.
    assert df.loc["2024-06-16T02:00", "d2m"] == 278.15


def test_precip_is_raw_accumulation_in_metres():
    df = _run().set_index("time_utc")
    # Raw accumulation-since-00Z in metres, NOT de-accumulated: hour H holds
    # H * 0.001 m, and 00:00 holds the prior day's 0.024 m full-day total.
    assert df.loc["2024-06-15T05:00", "tp"] == 0.005
    assert df.loc["2024-06-15T23:00", "tp"] == 0.023
    assert df.loc["2024-06-16T00:00", "tp"] == 0.024
    assert df.loc["2024-06-16T01:00", "tp"] == 0.001
