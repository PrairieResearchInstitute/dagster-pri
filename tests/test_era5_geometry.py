"""Unit tests for geometry helpers (no network: masks are read from tmp_path)."""

import pytest
from era5_helpers import square_clip_mask_parquet

from dagster_pri.era5.geometry import (
    bbox_from_geometry,
    clip_mask_path,
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


def test_clip_mask_path_keys_on_the_postal_code():
    assert clip_mask_path("public", "in") == (
        "public/shapefiles/state-watershed/IN/in_huc8_clip_mask.parquet"
    )


def test_bbox_from_geometry_order_pad_round(local_fs, bucket_root):
    square_clip_mask_parquet(bucket_root, "IL", lon=(-90.0, -88.0), lat=(40.0, 42.0))
    gdf = get_state_geometry(local_fs, str(bucket_root), "IL")
    # [North, West, South, East], padded 0.25, rounded to 3 dp.
    assert bbox_from_geometry(gdf, pad_deg=0.25) == [42.25, -90.25, 39.75, -87.75]


def test_get_state_geometry_reads_the_requested_state(local_fs, bucket_root):
    square_clip_mask_parquet(bucket_root, "IL", lon=(-91.0, -87.0), lat=(37.0, 42.0))
    square_clip_mask_parquet(bucket_root, "IN", lon=(-88.0, -84.0), lat=(37.0, 41.0))

    gdf = get_state_geometry(local_fs, str(bucket_root), "IN")
    assert len(gdf) == 1  # dissolved to a single row
    assert gdf.crs.to_epsg() == 4326
    # Indiana's mask starts at -88, so the west bound must be -88, not Illinois' -91.
    assert gdf.total_bounds[0] == pytest.approx(-88.0)


def test_get_state_geometry_missing_mask_raises(local_fs, bucket_root):
    square_clip_mask_parquet(bucket_root, "IL")
    with pytest.raises(ValueError, match="WY"):
        get_state_geometry(local_fs, str(bucket_root), "WY")
