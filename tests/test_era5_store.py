"""Store-logic tests against a real (in-memory) Icechunk repo. No S3, no CDS."""

import numpy as np
import pytest
from era5_helpers import make_clipped_ds, month_index

from dagster_pri.era5.axis import global_axis
from dagster_pri.era5.store import (
    init_store,
    open_store_dataset,
    validate_variables_against_store,
    write_month,
)

VARS = ["2m_temperature", "total_precipitation"]


def _init_jan_through(repo, axis_end: str, *, ref_ndays=2):
    """Init the axis [1950-01-01, axis_end] from a Jan reference month."""
    times = global_axis(axis_end)
    ref = make_clipped_ds(month_index(1950, 1, ndays=ref_ndays), VARS, fill=1.0)
    wrote = init_store(repo, times, ref, time_chunk=24)
    return times, wrote


def test_init_store_lays_axis_and_is_idempotent(in_memory_repo):
    times, wrote = _init_jan_through(in_memory_repo, "1950-02-28T23:00")
    assert wrote is True

    ds = open_store_dataset(in_memory_repo)
    assert ds.sizes["time"] == len(times)
    assert set(VARS) == set(ds.data_vars)
    assert ds.chunks["time"][0] == 24
    assert bool(np.isnan(ds["2m_temperature"].isel(time=0)).all())  # all-NaN template

    # Re-running init is a no-op.
    assert (
        init_store(
            in_memory_repo,
            times,
            make_clipped_ds(month_index(1950, 1, ndays=2), VARS),
            time_chunk=24,
        )
        is False
    )


def test_init_store_refuses_to_retemplate_a_different_variable_set(in_memory_repo):
    """Adding a variable is not an init: mode="w" would drop the ingested months."""
    times, _ = _init_jan_through(in_memory_repo, "1950-01-31T23:00")
    jan = make_clipped_ds(month_index(1950, 1, ndays=2), VARS, fill=1.0)
    write_month(in_memory_repo, jan, 1950, 1)

    grown = make_clipped_ds(month_index(1950, 1, ndays=2), [*VARS, "total_precipitation_hourly"])
    with pytest.raises(ValueError, match="already holds variables"):
        init_store(in_memory_repo, times, grown, time_chunk=24)

    # The refusal left the store intact.
    ds = open_store_dataset(in_memory_repo)
    assert set(ds.data_vars) == set(VARS)
    assert np.all(ds["2m_temperature"].sel(time="1950-01-01T00:00").values == 1.0)


def test_write_month_region_out_of_order(in_memory_repo):
    _init_jan_through(in_memory_repo, "1950-02-28T23:00")

    feb = make_clipped_ds(month_index(1950, 2, ndays=2), VARS, fill=2.0)
    jan = make_clipped_ds(month_index(1950, 1, ndays=2), VARS, fill=1.0)
    assert write_month(in_memory_repo, feb, 1950, 2) == "region"
    assert write_month(in_memory_repo, jan, 1950, 1) == "region"

    ds = open_store_dataset(in_memory_repo)
    v = ds["2m_temperature"]
    assert np.all(v.sel(time="1950-01-01T00:00").values == 1.0)
    assert np.all(v.sel(time="1950-02-01T00:00").values == 2.0)
    # An hour in neither written slot stays NaN.
    assert np.isnan(v.sel(time="1950-01-15T00:00").values).all()


def test_write_month_append_extends_axis(in_memory_repo):
    _init_jan_through(in_memory_repo, "1950-01-31T23:00")
    before = open_store_dataset(in_memory_repo).sizes["time"]

    feb = make_clipped_ds(month_index(1950, 2, ndays=2), VARS, fill=2.0)
    assert write_month(in_memory_repo, feb, 1950, 2) == "append"

    ds = open_store_dataset(in_memory_repo)
    assert ds.sizes["time"] == before + 48  # 2 days x 24h appended
    assert np.all(ds["2m_temperature"].sel(time="1950-02-01T00:00").values == 2.0)


def test_write_month_gap_beyond_axis_raises(in_memory_repo):
    _init_jan_through(in_memory_repo, "1950-01-31T23:00")
    march = make_clipped_ds(month_index(1950, 3, ndays=2), VARS, fill=3.0)
    with pytest.raises(ValueError, match="beyond the allocated axis"):
        write_month(in_memory_repo, march, 1950, 3)


def test_validate_variables_against_store_mismatch(in_memory_repo):
    _init_jan_through(in_memory_repo, "1950-01-31T23:00")
    wrong = make_clipped_ds(month_index(1950, 1, ndays=2), ["surface_pressure"], fill=1.0)
    with pytest.raises(ValueError, match="does not match"):
        validate_variables_against_store(in_memory_repo, wrong)


def test_validate_variables_against_store_match(in_memory_repo):
    _init_jan_through(in_memory_repo, "1950-01-31T23:00")
    ok = make_clipped_ds(month_index(1950, 1, ndays=2), VARS, fill=1.0)
    validate_variables_against_store(in_memory_repo, ok)  # no raise
