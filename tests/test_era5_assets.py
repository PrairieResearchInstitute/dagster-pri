"""End-to-end tests for the era5_init job and era5_iceberg asset.

No real S3 or CDS: Icechunk uses local-filesystem storage under tmp_path, and a
synthetic CDS client writes a fake NetCDF for the requested month.
"""

from pathlib import Path

import dagster as dg
import numpy as np
import pytest
import xarray as xr
from era5_helpers import month_index

from dagster_pri.defs.era5_ingest import Era5IngestConfig, era5_iceberg
from dagster_pri.defs.era5_init import Era5InitConfig, era5_init
from dagster_pri.defs.resources import CDSClientResource, IcechunkStorageResource
from dagster_pri.era5.geometry import repo_prefix
from dagster_pri.era5.store import open_store_dataset

VARS = ["2m_temperature", "total_precipitation"]
# Narrowing `variables` means narrowing `accumulated_variables` to match: the
# default names every accumulation in the *default* download, none of which these
# tests ask CDS for.
ACCUM = ["total_precipitation"]
# The synthetic CDS client names data vars after the request's CDS long names, so
# the derived accumulation array is spelled with the long name here (a real
# download carries short names, giving "tp_hourly").
DERIVED_VAR = "total_precipitation_hourly"


class _SynthCDSClient:
    """Stand-in for cdsapi.Client that synthesizes a raw NetCDF per request."""

    def retrieve(self, dataset: str, request: dict, target: str) -> None:
        year, month = int(request["year"]), int(request["month"])
        ndays = len(request["day"])
        north, west, south, east = request["area"]
        lats = np.arange(south, north + 1e-9, 0.25)
        lons = np.arange(west, east + 1e-9, 0.25)
        times = month_index(year, month, ndays)
        shape = (len(times), len(lats), len(lons))
        data = {
            v: (("valid_time", "lat", "lon"), np.full(shape, float(month), dtype="float64"))
            for v in request["variable"]
        }
        ds = xr.Dataset(data, coords={"valid_time": times, "lat": lats, "lon": lons})
        ds.to_netcdf(target)
        ds.close()


class FakeCDSClientResource(CDSClientResource):
    def get_client(self):
        return _SynthCDSClient()


class LocalIcechunkStorageResource(IcechunkStorageResource):
    base_dir: str

    def storage(self, prefix: str):
        import icechunk

        path = Path(self.base_dir) / prefix
        path.mkdir(parents=True, exist_ok=True)
        return icechunk.local_filesystem_storage(str(path))

    def filesystem(self):
        """Local disk in place of s3fs, so ``bucket`` reads resolve under tmp_path."""
        import fsspec

        return fsspec.filesystem("local")


@pytest.fixture
def resources(tmp_path, bucket_root):
    # Dummy connection fields: the fakes override storage()/get_client(), but
    # Dagster still resolves the (otherwise EnvVar) config, so give it values.
    return {
        "icechunk": LocalIcechunkStorageResource(
            base_dir=str(tmp_path / "store"),
            bucket=str(bucket_root),
            endpoint_url="http://test",
            access_key_id="x",
            secret_access_key="y",
        ),
        "cds": FakeCDSClientResource(url="http://test", key="x"),
    }


def _init_config(work_dir: Path) -> Era5InitConfig:
    return Era5InitConfig(
        state="IL",
        ref_year=1950,
        ref_month=1,
        axis_end="1950-02-28T23:00",
        variables=VARS,
        accumulated_variables=ACCUM,
        time_chunk=24,
        ndays=2,
        work_dir=str(work_dir),
    )


def _ingest_config(year: int, month: int, work_dir: Path) -> Era5IngestConfig:
    return Era5IngestConfig(
        state="IL",
        year=year,
        month=month,
        variables=VARS,
        accumulated_variables=ACCUM,
        ndays=2,
        work_dir=str(work_dir),
    )


def test_init_then_ingest_out_of_order(resources, tmp_path, local_clip_mask):
    work = tmp_path / "work"

    init_cfg = _init_config(work)
    init_result = era5_init.execute_in_process(
        run_config=dg.RunConfig(ops={"init_state_store": init_cfg}), resources=resources
    )
    assert init_result.success

    # Ingest February, then January -- out of order region writes.
    for month in (2, 1):
        cfg = _ingest_config(1950, month, work)
        result = dg.materialize(
            [era5_iceberg],
            run_config=dg.RunConfig(ops={"era5_iceberg": cfg}),
            resources=resources,
        )
        assert result.success
        mats = result.asset_materializations_for_node("era5_iceberg")
        meta = mats[0].metadata
        assert meta["mode"].value == "region"
        assert meta["timesteps"].value == 48

    # Both months landed in the store with their distinguishing fill values.
    repo = resources["icechunk"].open_repo(repo_prefix("IL"))
    ds = open_store_dataset(repo)
    assert np.all(ds["2m_temperature"].sel(time="1950-01-01T00:00").values == 1.0)
    assert np.all(ds["2m_temperature"].sel(time="1950-02-01T00:00").values == 2.0)


def test_accumulated_variable_gets_a_derived_hourly_array(resources, tmp_path, local_clip_mask):
    work = tmp_path / "work"
    assert era5_init.execute_in_process(
        run_config=dg.RunConfig(ops={"init_state_store": _init_config(work)}), resources=resources
    ).success

    result = dg.materialize(
        [era5_iceberg],
        run_config=dg.RunConfig(ops={"era5_iceberg": _ingest_config(1950, 1, work)}),
        resources=resources,
    )
    assert result.success
    meta = result.asset_materializations_for_node("era5_iceberg")[0].metadata
    assert meta["accumulated_variables"].value == ["total_precipitation"]

    ds = open_store_dataset(resources["icechunk"].open_repo(repo_prefix("IL")))
    # The derived array joined the schema; the raw accumulation is still there.
    assert {"total_precipitation", DERIVED_VAR} <= set(ds.data_vars)

    derived = ds[DERIVED_VAR]
    # The synthetic month is a constant 1.0 accumulation, so: NaN at the block's
    # first step, the raw value at 01:00 (post-reset), and 0.0 differences after.
    assert np.isnan(derived.sel(time="1950-01-01T00:00").values).all()
    assert np.all(derived.sel(time="1950-01-01T01:00").values == 1.0)
    assert np.all(derived.sel(time="1950-01-01T02:00").values == 0.0)
    # An unwritten hour still reads as the NaN fill, not 0.
    assert np.isnan(derived.sel(time="1950-02-15T03:00").values).all()


def test_ingest_splits_variables_across_cds_requests(resources, tmp_path, local_clip_mask):
    """One variable per request: every batch lands and merges back into one month."""
    work = tmp_path / "work"
    init_cfg = _init_config(work)
    init_cfg = init_cfg.model_copy(update={"variables_per_request": 1})
    assert era5_init.execute_in_process(
        run_config=dg.RunConfig(ops={"init_state_store": init_cfg}), resources=resources
    ).success

    cfg = _ingest_config(1950, 2, work).model_copy(update={"variables_per_request": 1})
    result = dg.materialize(
        [era5_iceberg],
        run_config=dg.RunConfig(ops={"era5_iceberg": cfg}),
        resources=resources,
    )
    assert result.success
    meta = result.asset_materializations_for_node("era5_iceberg")[0].metadata
    assert meta["cds_requests"].value == len(VARS)

    # One staged NetCDF per variable batch, for both the init and ingest months.
    assert sorted(p.name for p in work.glob("*.nc")) == [
        "era5land_il_195001_b00.nc",
        "era5land_il_195001_b01.nc",
        "era5land_il_195002_b00.nc",
        "era5land_il_195002_b01.nc",
    ]

    ds = open_store_dataset(resources["icechunk"].open_repo(repo_prefix("IL")))
    for var in (*VARS, DERIVED_VAR):
        assert var in ds.data_vars
    assert np.all(ds["2m_temperature"].sel(time="1950-02-01T00:00").values == 2.0)
    assert np.all(ds["total_precipitation"].sel(time="1950-02-01T00:00").values == 2.0)


def test_ingest_without_init_fails_clearly(resources, tmp_path, local_clip_mask):
    work = tmp_path / "work"
    cfg = _ingest_config(1950, 1, work)
    result = dg.materialize(
        [era5_iceberg],
        run_config=dg.RunConfig(ops={"era5_iceberg": cfg}),
        resources=resources,
        raise_on_error=False,
    )
    assert not result.success
    message = result.failure_data_for_node("era5_iceberg").error.message
    assert "era5_init" in message
