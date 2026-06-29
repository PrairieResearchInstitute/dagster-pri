"""Unit tests for geometry helpers (no Census download)."""

import pytest
from era5_helpers import square_boundary_geojson

from dagster_pri.era5.geometry import (
    bbox_from_geometry,
    get_state_geometry,
    normalize_stusps,
    repo_prefix,
)


@pytest.mark.parametrize(
    "raw,expected",
    [("il", "IL"), ("IL", "IL"), (" in ", "IN"), ("Wi", "WI")],
)
def test_normalize_stusps_ok(raw, expected):
    assert normalize_stusps(raw) == expected


@pytest.mark.parametrize("bad", ["I", "ILL", "I1", "12", ""])
def test_normalize_stusps_rejects_non_codes(bad):
    with pytest.raises(ValueError):
        normalize_stusps(bad)


def test_repo_prefix():
    assert repo_prefix("il") == "era5-land/icechunk/IL"
    assert repo_prefix("IN") == "era5-land/icechunk/IN"


def test_bbox_from_geometry_order_pad_round(tmp_path):
    square_boundary_geojson(tmp_path / "b.geojson", lon=(-90.0, -88.0), lat=(40.0, 42.0))
    gdf = get_state_geometry("IL", str(tmp_path / "b.geojson"))
    # [North, West, South, East], padded 0.25, rounded to 3 dp.
    assert bbox_from_geometry(gdf, pad_deg=0.25) == [42.25, -90.25, 39.75, -87.75]


def test_get_state_geometry_isolates_requested_state(tmp_path):
    import geopandas as gpd
    from shapely.geometry import box

    multi = gpd.GeoDataFrame(
        {"STUSPS": ["IL", "IN"]},
        geometry=[box(-91, 37, -87, 42), box(-88, 37, -84, 41)],
        crs="EPSG:4326",
    )
    path = tmp_path / "states.geojson"
    multi.to_file(path, driver="GeoJSON")

    gdf = get_state_geometry("IN", str(path))
    assert len(gdf) == 1  # dissolved to a single row
    assert str(gdf.crs).upper().endswith("4326")
    # Indiana's western edge is -88, so the bbox west must be ~ -88 (± pad), not -91.
    assert gdf.total_bounds[0] == pytest.approx(-88.0)


def test_get_state_geometry_missing_state_raises(tmp_path):
    p = square_boundary_geojson(tmp_path / "b.geojson", stusps="IL")
    with pytest.raises(ValueError, match="WY"):
        get_state_geometry("WY", str(p))
