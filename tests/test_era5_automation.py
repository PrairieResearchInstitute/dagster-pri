"""Tests for the monthly ERA5-Land backfill automation.

The pure decision helpers are unit-tested directly. The sensor is driven against a
**real local parquet dataset** (written under ``tmp_path``) through a
local-filesystem ``IcechunkStorageResource`` subclass, so ``landed_months``
exercises the actual pyarrow hive-partition discovery path -- no S3.
"""

from __future__ import annotations

from pathlib import Path

import dagster as dg
import pandas as pd
import pytest

from dagster_pri.defs.era5_automation import (
    decide_target_month,
    era5_monthly_sensor,
    landed_months,
    next_month,
    parse_ym,
)
from dagster_pri.defs.resources import IcechunkStorageResource


# --------------------------------------------------------------------------- #
# Pure helpers
# --------------------------------------------------------------------------- #
def test_parse_ym():
    assert parse_ym("2024-01") == (2024, 1)
    assert parse_ym(" 2024-12 ") == (2024, 12)
    for bad in ("2024-13", "2024-00", "2024/01", "24-01", "2024-1", "nope"):
        with pytest.raises(ValueError):
            parse_ym(bad)


def test_next_month_rolls_over():
    assert next_month(2024, 1) == (2024, 2)
    assert next_month(2024, 11) == (2024, 12)
    assert next_month(2024, 12) == (2025, 1)


def test_decide_target_empty_is_start():
    assert decide_target_month(set(), (2024, 1), None) == (2024, 1)
    assert decide_target_month(set(), (2024, 1), (2024, 6)) == (2024, 1)


def test_decide_target_advances_past_latest():
    existing = {(2024, 1), (2024, 2)}
    assert decide_target_month(existing, (2024, 1), None) == (2024, 3)
    assert decide_target_month(existing, (2024, 1), (2024, 12)) == (2024, 3)


def test_decide_target_caught_up_to_end():
    existing = {(2024, 1), (2024, 2)}
    assert decide_target_month(existing, (2024, 1), (2024, 2)) is None


def test_decide_target_ignores_out_of_bounds():
    # Parquet below start and above end are both ignored.
    existing = {(2023, 12), (2024, 5), (2025, 1)}
    assert decide_target_month(existing, (2024, 1), (2024, 6)) == (2024, 6)
    # December -> January rollover at the top of the window.
    assert decide_target_month({(2024, 12)}, (2024, 1), None) == (2025, 1)


# --------------------------------------------------------------------------- #
# landed_months + sensor against a real local parquet dataset
# --------------------------------------------------------------------------- #
class LocalIcechunkStorageResource(IcechunkStorageResource):
    """Points ``filesystem()`` at the local disk and the buckets at tmp dirs."""

    def filesystem(self):
        import fsspec

        return fsspec.filesystem("local")


def _write_parquet(bucket: Path, state: str, year: int, month: int) -> None:
    out = bucket / f"era5-land/parquet/STATE={state}/YEAR={year}/MONTH={month:02d}/data.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"station_id": ["1"], "value": [1.0]}).to_parquet(out, index=False)


@pytest.fixture
def resource(tmp_path):
    # Two distinct roots: the sensor must read the private one (where
    # daily_station_readings writes), so a regression to the public bucket
    # leaves it seeing nothing landed.
    return LocalIcechunkStorageResource(
        bucket=str(tmp_path / "bucket"),
        private_bucket=str(tmp_path / "private"),
        endpoint_url="http://test",
        access_key_id="x",
        secret_access_key="y",
    )


def test_landed_months_reads_partitions(resource):
    bucket = Path(resource.private_bucket)
    _write_parquet(bucket, "IL", 2024, 1)
    _write_parquet(bucket, "IL", 2024, 2)
    _write_parquet(bucket, "IN", 2024, 5)  # different state, must not leak

    fs = resource.filesystem()
    assert landed_months(fs, resource.private_bucket, "IL") == {(2024, 1), (2024, 2)}
    # Nothing lands in the public bucket any more.
    assert landed_months(fs, resource.bucket, "IL") == set()


def test_landed_months_empty_when_no_prefix(resource):
    assert landed_months(resource.filesystem(), resource.private_bucket, "IL") == set()


def _eval(resource, monkeypatch, **env):
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    ctx = dg.build_sensor_context(resources={"icechunk": resource})
    return era5_monthly_sensor(ctx)


def test_sensor_first_run_is_start(resource, monkeypatch):
    result = _eval(resource, monkeypatch, ERA5_START_YM="2024-03", ERA5_STATE="IL")
    assert isinstance(result, dg.RunRequest)
    assert result.run_key == "IL-2024-03"
    cfg = result.run_config["ops"]
    assert cfg["era5_iceberg"]["config"] == {"state": "IL", "year": 2024, "month": 3}
    assert cfg["daily_station_readings"]["config"] == {"state": "IL", "year": 2024, "month": 3}


def test_sensor_advances_past_landed(resource, monkeypatch):
    bucket = Path(resource.private_bucket)
    _write_parquet(bucket, "IL", 2024, 1)
    _write_parquet(bucket, "IL", 2024, 2)
    result = _eval(resource, monkeypatch, ERA5_START_YM="2024-01", ERA5_STATE="IL")
    assert isinstance(result, dg.RunRequest)
    assert result.run_key == "IL-2024-03"


def test_sensor_skips_when_caught_up(resource, monkeypatch):
    bucket = Path(resource.private_bucket)
    _write_parquet(bucket, "IL", 2024, 1)
    _write_parquet(bucket, "IL", 2024, 2)
    result = _eval(
        resource, monkeypatch, ERA5_START_YM="2024-01", ERA5_END_YM="2024-02", ERA5_STATE="IL"
    )
    assert isinstance(result, dg.SkipReason)


def test_sensor_skips_without_start(resource, monkeypatch):
    monkeypatch.delenv("ERA5_START_YM", raising=False)
    ctx = dg.build_sensor_context(resources={"icechunk": resource})
    result = era5_monthly_sensor(ctx)
    assert isinstance(result, dg.SkipReason)
