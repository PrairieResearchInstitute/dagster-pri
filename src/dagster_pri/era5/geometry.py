"""State boundary resolution, bounding box, and per-state Icechunk prefix.

Generalized from the Illinois-only original so any US state (by USPS code) can
be ingested into its own sibling Icechunk repo.
"""

from __future__ import annotations

import logging

log = logging.getLogger("era5land")

# US Census 2022 cartographic boundary, state level (1:500k). Carries a STUSPS
# column for every state/territory, so filtering by code generalizes for free.
CENSUS_STATES_ZIP = "https://www2.census.gov/geo/tiger/GENZ2022/shp/cb_2022_us_state_500k.zip"


def normalize_stusps(stusps: str) -> str:
    """Validate and upper-case a 2-letter USPS state code."""
    code = stusps.strip().upper()
    if len(code) != 2 or not code.isalpha():
        raise ValueError(f"Expected a 2-letter USPS state code (e.g. 'IL', 'IN'); got {stusps!r}.")
    return code


def repo_prefix(stusps: str) -> str:
    """Bucket prefix for a state's Icechunk repo.

    ``STATE=XX`` is a deliberate Hive-style segment so sibling states live side
    by side (era5/STATE=IL, era5/STATE=IN, ...).
    """
    return f"era5/STATE={normalize_stusps(stusps)}"


def get_state_geometry(stusps: str, boundary_path: str | None = None):
    """Return a single-row GeoDataFrame (EPSG:4326) for the given state.

    With no ``boundary_path`` the US Census cartographic boundary is downloaded
    and filtered by STUSPS. With a ``boundary_path`` (shapefile/GeoJSON), the
    requested state is isolated by matching the code against any code-bearing
    column.
    """
    import geopandas as gpd

    code = normalize_stusps(stusps)

    if boundary_path:
        log.info("Reading boundary from %s", boundary_path)
        gdf = gpd.read_file(boundary_path)
        # If a multi-state file was supplied, isolate the requested state by the
        # first recognized code/name column. A recognized column with no match
        # yields an empty frame (and the empty check below raises). A file with
        # none of these columns is assumed to already be a single state.
        wanted = {code, _full_name(gdf, code)}
        for col in ("STUSPS", "STATE_ABBR", "NAME", "state"):
            if col in gdf.columns:
                gdf = gdf[gdf[col].astype(str).str.upper().isin(wanted)]
                break
    else:
        log.info("Downloading %s boundary from US Census...", code)
        gdf = gpd.read_file(CENSUS_STATES_ZIP)
        gdf = gdf[gdf["STUSPS"] == code]

    if gdf.empty:
        raise ValueError(f"Could not locate the {code} polygon in the boundary source.")

    gdf = gdf.to_crs("EPSG:4326")
    # Dissolve in case of multipart rows.
    gdf = gdf.dissolve().reset_index(drop=True)
    return gdf[["geometry"]]


def _full_name(gdf, code: str) -> str:
    """Best-effort full state name for ``code`` from a NAME/STUSPS-bearing file.

    Returns the code itself if no mapping can be found, so the membership test in
    :func:`get_state_geometry` stays a no-op rather than matching nothing.
    """
    if "STUSPS" in gdf.columns and "NAME" in gdf.columns:
        match = gdf[gdf["STUSPS"].astype(str).str.upper() == code]
        if len(match):
            return str(match["NAME"].iloc[0]).upper()
    return code


def bbox_from_geometry(gdf, pad_deg: float = 0.25) -> list[float]:
    """CDS 'area' bbox [North, West, South, East], padded so clip keeps edge cells."""
    minx, miny, maxx, maxy = gdf.total_bounds
    north = round(maxy + pad_deg, 3)
    west = round(minx - pad_deg, 3)
    south = round(miny - pad_deg, 3)
    east = round(maxx + pad_deg, 3)
    return [north, west, south, east]
