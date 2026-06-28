"""Unit tests for open_and_clip: coord normalization, ERA5T cleanup, clipping."""

import zipfile

import numpy as np
import pytest
from era5_helpers import write_raw_era5_nc

from dagster_pri.era5.transform import open_and_clip

VARS = ["2m_temperature", "total_precipitation"]


def _gdf(lon=(-90.5, -88.5), lat=(39.5, 41.5)):
    import geopandas as gpd
    from shapely.geometry import box

    return gpd.GeoDataFrame(geometry=[box(lon[0], lat[0], lon[1], lat[1])], crs="EPSG:4326")


@pytest.mark.parametrize("expver_shape", [None, "scalar", "time", "dim"])
@pytest.mark.parametrize("add_number", [False, True])
def test_open_and_clip_normalizes_and_drops_housekeeping(tmp_path, expver_shape, add_number):
    nc = write_raw_era5_nc(
        tmp_path / "raw.nc",
        2024,
        1,
        VARS,
        ndays=1,
        expver_shape=expver_shape,
        add_number=add_number,
    )
    clipped = open_and_clip(nc, _gdf())

    # valid_time -> time; lat/lon -> latitude/longitude
    assert "time" in clipped.dims and "valid_time" not in clipped.variables
    assert "latitude" in clipped.coords and "longitude" in clipped.coords
    # ERA5T housekeeping never enters the schema
    assert "expver" not in clipped.variables
    assert "number" not in clipped.variables
    assert set(VARS) == set(clipped.data_vars)
    # Cells inside the polygon keep their value (month == 1).
    assert float(np.nanmax(clipped["2m_temperature"].values)) == pytest.approx(1.0)


def test_open_and_clip_wraps_longitude_to_180(tmp_path):
    nc = write_raw_era5_nc(tmp_path / "raw.nc", 2024, 1, VARS, ndays=1, lon_0_360=True)
    clipped = open_and_clip(nc, _gdf())
    lons = clipped["longitude"].values
    assert lons.max() <= 180.0
    assert np.all(np.diff(lons) > 0)  # sorted ascending after the wrap


def test_open_and_clip_drops_cells_outside_polygon(tmp_path):
    # A grid row at lat 45 lies outside the clip polygon and must be dropped.
    nc = write_raw_era5_nc(
        tmp_path / "raw.nc",
        2024,
        1,
        VARS,
        ndays=1,
        lats=[40.0, 41.0, 45.0],
    )
    clipped = open_and_clip(nc, _gdf(lat=(39.5, 41.5)))
    assert 45.0 not in clipped["latitude"].values


def test_open_and_clip_handles_zipped_payload(tmp_path):
    inner = write_raw_era5_nc(tmp_path / "inner.nc", 2024, 1, VARS, ndays=1)
    zipped = tmp_path / "payload.nc"  # named .nc but actually a zip, like the CDS quirk
    with zipfile.ZipFile(zipped, "w") as zf:
        zf.write(inner, arcname="data.nc")

    clipped = open_and_clip(zipped, _gdf())
    assert "time" in clipped.dims and set(VARS) == set(clipped.data_vars)
