"""Unit tests for de-accumulating ERA5-Land running totals.

Pure logic over small synthetic in-memory datasets -- no Icechunk, S3 or CDS. The
things worth pinning down are the two special hours (01:00 takes the raw value,
every other hour differences), the block-boundary NaN, the negative clamp, and the
CDS-long-name -> stored-short-name resolution.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from dagster_pri.era5.accumulation import (
    ACCUMULATED_SHORT_NAMES,
    DEFAULT_ACCUMULATED_VARIABLES,
    HOURLY_SUFFIX,
    accumulated_subset,
    add_hourly_increments,
    hourly_increment,
    resolve_stored_name,
)
from dagster_pri.era5.axis import DEFAULT_VARIABLES

LATS = [40.0, 41.0]
LONS = [-90.0, -89.0]


def _tp_dataset(start: str, end: str, name: str = "tp") -> xr.Dataset:
    """Hourly dataset whose accumulation ramps 0.001 m per hour, resetting at 00Z.

    Hour H holds ``H * 0.001`` m, except 00:00 which holds the prior day's full
    0.024 m total -- the raw ERA5 shape. So every real hourly increment is exactly
    0.001 m.
    """
    times = pd.date_range(start, end, freq="1h")
    shape = (len(times), len(LATS), len(LONS))
    hours = times.hour.to_numpy()
    ramp = np.where(hours == 0, 24, hours) * 0.001
    tp = np.broadcast_to(ramp[:, None, None], shape).copy()

    ds = xr.Dataset(
        {name: (("time", "latitude", "longitude"), tp)},
        coords={"time": times, "latitude": LATS, "longitude": LONS},
    )
    ds[name].attrs = {"units": "m", "long_name": "Total precipitation"}
    return ds


def _series(da: xr.DataArray) -> pd.Series:
    """Collapse the (constant) spatial dims so hours can be indexed by timestamp."""
    return da.isel(latitude=0, longitude=0).to_series()


def test_hour_01_takes_the_raw_value():
    # The 00Z sample is the prior day's total, so a raw diff at 01:00 would be
    # -0.023; the reset rule must yield the raw 0.001 instead.
    inc = _series(hourly_increment(_tp_dataset("2024-06-01T00:00", "2024-06-30T23:00")["tp"]))
    assert inc["2024-06-02T01:00"] == pytest.approx(0.001)
    assert inc["2024-06-16T01:00"] == pytest.approx(0.001)


def test_every_other_hour_is_the_difference():
    inc = _series(hourly_increment(_tp_dataset("2024-06-01T00:00", "2024-06-30T23:00")["tp"]))
    # Mid-day, end-of-day, and the 23:00 -> 00:00 step across the daily reset.
    assert inc["2024-06-15T05:00"] == pytest.approx(0.001)
    assert inc["2024-06-15T23:00"] == pytest.approx(0.001)
    assert inc["2024-06-16T00:00"] == pytest.approx(0.001)
    # Every defined step is the same 0.001 m: only the block's first is NaN.
    defined = inc.dropna()
    assert len(defined) == len(inc) - 1
    assert np.allclose(defined.to_numpy(), 0.001)


def test_first_step_of_a_month_block_is_nan():
    # A CDS month starts at 00:00 on day 1, whose predecessor is in the previous
    # month's file. NaN beats a wrong number.
    inc = _series(hourly_increment(_tp_dataset("2024-06-01T00:00", "2024-06-30T23:00")["tp"]))
    assert np.isnan(inc["2024-06-01T00:00"])


def test_block_starting_at_01_needs_no_predecessor():
    inc = _series(hourly_increment(_tp_dataset("2024-06-01T01:00", "2024-06-02T23:00")["tp"]))
    assert inc["2024-06-01T01:00"] == pytest.approx(0.001)
    assert not inc.isna().any()


def test_negative_noise_is_clamped_to_zero():
    ds = _tp_dataset("2024-06-01T00:00", "2024-06-02T23:00")
    # Make 05:00 smaller than 04:00 so the raw difference goes negative.
    ds["tp"].loc[{"time": "2024-06-01T05:00"}] = 0.0039
    inc = _series(hourly_increment(ds["tp"]))
    assert inc["2024-06-01T05:00"] == 0.0
    assert (inc.dropna() >= 0).all()


def test_add_hourly_increments_resolves_the_short_name():
    ds = _tp_dataset("2024-06-01T00:00", "2024-06-02T23:00")
    out = add_hourly_increments(ds, ["total_precipitation"])
    assert f"tp{HOURLY_SUFFIX}" in out.data_vars
    # The raw accumulation is retained untouched.
    xr.testing.assert_identical(out["tp"], ds["tp"])


def test_add_hourly_increments_falls_back_to_the_long_name():
    ds = _tp_dataset("2024-06-01T00:00", "2024-06-02T23:00", name="total_precipitation")
    out = add_hourly_increments(ds, ["total_precipitation"])
    assert f"total_precipitation{HOURLY_SUFFIX}" in out.data_vars


def test_derived_variable_keeps_units_and_annotates_long_name():
    out = add_hourly_increments(
        _tp_dataset("2024-06-01T00:00", "2024-06-02T23:00"), ["total_precipitation"]
    )
    attrs = out[f"tp{HOURLY_SUFFIX}"].attrs
    assert attrs["units"] == "m"
    assert attrs["long_name"] == "Total precipitation (hourly increment)"
    assert attrs["cell_methods"] == "time: sum (1 hour)"


def test_empty_list_is_a_no_op():
    ds = _tp_dataset("2024-06-01T00:00", "2024-06-02T23:00")
    xr.testing.assert_identical(add_hourly_increments(ds, []), ds)


def test_resolve_stored_name_prefers_the_short_name():
    ds = _tp_dataset("2024-06-01T00:00", "2024-06-01T23:00")
    assert resolve_stored_name(ds, "total_precipitation") == "tp"

    long_named = _tp_dataset("2024-06-01T00:00", "2024-06-01T23:00", name="total_precipitation")
    assert resolve_stored_name(long_named, "total_precipitation") == "total_precipitation"


def test_accumulated_subset_keeps_only_accumulations_in_order():
    picked = accumulated_subset(
        ["2m_temperature", "total_precipitation", "soil_temperature_level_1", "snowfall"]
    )
    assert picked == ["total_precipitation", "snowfall"]


def test_instantaneous_variables_are_never_accumulated():
    # A wrong entry here would silently difference an instantaneous field.
    instantaneous = [
        "2m_temperature",
        "2m_dewpoint_temperature",
        "10m_u_component_of_wind",
        "surface_pressure",
        "skin_temperature",
        "snow_depth_water_equivalent",
        "volumetric_soil_water_layer_1",
        "leaf_area_index_low_vegetation",
        "forecast_albedo",
    ]
    assert accumulated_subset(instantaneous) == []


def test_every_default_accumulation_has_a_short_name():
    # The evaporation components in DEFAULT_VARIABLES are accumulations too, and
    # were once missing from the table -- so they were silently stored raw.
    for name in (
        "evaporation_from_vegetation_transpiration",
        "evaporation_from_bare_soil",
        "evaporation_from_open_water_surfaces_excluding_oceans",
    ):
        assert name in DEFAULT_VARIABLES
        assert name in ACCUMULATED_SHORT_NAMES


def test_the_default_covers_every_accumulated_default_variable():
    # The point of the default: nothing downloaded by default is left accumulating
    # without an `_hourly` sibling.
    assert DEFAULT_ACCUMULATED_VARIABLES == accumulated_subset(DEFAULT_VARIABLES)
    assert len(DEFAULT_ACCUMULATED_VARIABLES) == 17
    assert "total_precipitation" in DEFAULT_ACCUMULATED_VARIABLES
    assert "2m_temperature" not in DEFAULT_ACCUMULATED_VARIABLES


def test_the_default_only_names_variables_that_get_downloaded():
    # A name here that is not in DEFAULT_VARIABLES would fail every default init.
    assert set(DEFAULT_ACCUMULATED_VARIABLES) <= set(DEFAULT_VARIABLES)


def test_missing_accumulated_variable_raises_with_the_available_vars():
    ds = _tp_dataset("2024-06-01T00:00", "2024-06-01T23:00")
    with pytest.raises(ValueError, match="snowfall") as excinfo:
        add_hourly_increments(ds, ["snowfall"])
    message = str(excinfo.value)
    assert "'sf'" in message  # the short name it looked for
    assert "'tp'" in message  # what the dataset actually has
    assert "accumulated_variables" in message
