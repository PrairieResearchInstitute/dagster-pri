"""State clip-mask resolution, bounding box, and per-state Icechunk prefix.

The clip mask for a state is the dissolved union of every HUC8 watershed that
drains through it (see ``scripts/extract-huc8-clip-mask.py``) -- i.e. the state
plus its watersheds, not the bare political boundary. Masks are published as one
GeoParquet object per state, keyed by USPS code, in the same bucket as the
Icechunk stores, and are read through the same filesystem/credentials
(:class:`dagster_pri.defs.resources.IcechunkStorageResource`).
"""

from __future__ import annotations

import logging

log = logging.getLogger("era5land")

# Object layout of the published per-state HUC8 clip masks within the bucket.
CLIP_MASK_PREFIX = "shapefiles/state-watershed"


def normalize_stusps(stusps: str) -> str:
    """Validate and upper-case a 2-letter USPS state code."""
    code = stusps.strip().upper()
    if len(code) != 2 or not code.isalpha():
        raise ValueError(f"Expected a 2-letter USPS state code (e.g. 'IL', 'IN'); got {stusps!r}.")
    return code


def repo_prefix(stusps: str) -> str:
    """Bucket prefix for a state's Icechunk repo.

    Each state gets a plain per-state folder under ``era5-land/icechunk`` so
    sibling states live side by side (era5-land/icechunk/IL,
    era5-land/icechunk/IN, ...).
    """
    return f"era5-land/icechunk/{normalize_stusps(stusps)}"


def clip_mask_path(bucket: str, stusps: str) -> str:
    """Bucket-prefixed object path of a state's clip-mask GeoParquet.

    Bucket-prefixed rather than ``s3://``-schemed, matching how the rest of the
    codebase addresses objects through an fsspec filesystem.
    """
    code = normalize_stusps(stusps)
    return f"{bucket}/{CLIP_MASK_PREFIX}/{code}/{code.lower()}_huc8_clip_mask.parquet"


def get_state_geometry(fs, bucket: str, stusps: str):
    """Return a single-row GeoDataFrame (EPSG:4326) with the state's clip mask.

    Reads the published GeoParquet for ``stusps`` through ``fs``/``bucket``,
    normally ``IcechunkStorageResource.filesystem()`` and its ``bucket`` (tests
    pass a local filesystem and a directory standing in for the bucket).
    """
    import geopandas as gpd

    code = normalize_stusps(stusps)
    path = clip_mask_path(bucket, code)

    log.info("Reading %s clip mask from %s", code, path)
    try:
        with fs.open(path, "rb") as f:
            gdf = gpd.read_parquet(f)
    except FileNotFoundError as e:
        raise ValueError(
            f"No clip mask for {code} at {path!r}. Build one with "
            f"scripts/extract-huc8-clip-mask.py --state {code} and upload it there."
        ) from e

    if gdf.empty:
        raise ValueError(f"The {code} clip mask at {path!r} holds no geometry.")

    gdf = gdf.to_crs("EPSG:4326")
    # Dissolve in case of multipart rows.
    gdf = gdf.dissolve().reset_index(drop=True)
    return gdf[["geometry"]]


def bbox_from_geometry(gdf, pad_deg: float = 0.25) -> list[float]:
    """CDS 'area' bbox [North, West, South, East], padded so clip keeps edge cells."""
    minx, miny, maxx, maxy = gdf.total_bounds
    north = round(maxy + pad_deg, 3)
    west = round(minx - pad_deg, 3)
    south = round(miny - pad_deg, 3)
    east = round(maxx + pad_deg, 3)
    return [north, west, south, east]
