"""Unit tests for the axis math and alignment guards."""

import numpy as np
import pandas as pd
import pytest
from era5_helpers import month_index

from dagster_pri.era5.axis import (
    AXIS_START,
    assert_subset_of_axis,
    axis_initialized,
    check_time_chunk,
    default_axis_end,
    global_axis,
)


@pytest.mark.parametrize("good", [1, 2, 3, 4, 6, 8, 12, 24])
def test_check_time_chunk_accepts_divisors_of_24(good):
    check_time_chunk(good)  # no raise


@pytest.mark.parametrize("bad", [0, -1, 5, 7, 16, 48])
def test_check_time_chunk_rejects_non_divisors(bad):
    with pytest.raises(ValueError):
        check_time_chunk(bad)


def test_global_axis_is_hourly_and_inclusive():
    axis = global_axis("1950-01-02T23:00")
    assert axis[0] == pd.Timestamp(AXIS_START)
    assert axis[-1] == pd.Timestamp("1950-01-02T23:00")
    assert len(axis) == 48  # 2 days x 24h
    assert (axis.to_series().diff().dropna() == pd.Timedelta(hours=1)).all()


def test_default_axis_end_is_last_complete_year():
    from datetime import datetime, timezone

    expected_year = datetime.now(timezone.utc).year - 1
    assert default_axis_end() == f"{expected_year}-12-31T23:00"


def test_assert_subset_of_axis_accepts_contiguous_month():
    axis = global_axis("1950-02-28T23:00").values
    jan = month_index(1950, 1).values
    assert_subset_of_axis(jan, axis)  # no raise


def test_assert_subset_of_axis_rejects_off_hour_offset():
    axis = global_axis("1950-02-28T23:00").values
    shifted = (month_index(1950, 1) + pd.Timedelta(minutes=30)).values
    with pytest.raises(ValueError, match="not all present"):
        assert_subset_of_axis(shifted, axis)


def test_assert_subset_of_axis_rejects_non_contiguous():
    axis = global_axis("1950-03-31T23:00").values
    gapped = np.concatenate([month_index(1950, 1).values, month_index(1950, 3).values])
    with pytest.raises(ValueError, match="not contiguous"):
        assert_subset_of_axis(gapped, axis)


def test_axis_initialized_predicate():
    assert axis_initialized(None, 10, ["a"]) is False
