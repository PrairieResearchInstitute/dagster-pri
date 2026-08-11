"""Shared helpers for ERA5-Land ingest tests.

No real S3 or CDS access: Icechunk runs against in-memory / local-filesystem
storage, and synthetic xarray datasets stand in for CDS downloads.
"""

from __future__ import annotations

import calendar
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

# A tiny spatial grid reused across tests (degrees, EPSG:4326).
LATS = [40.0, 41.0]
LONS = [-90.0, -89.0]


def month_index(year: int, month: int, ndays: int | None = None) -> pd.DatetimeIndex:
    """Hourly DatetimeIndex for (the first ``ndays`` of) a calendar month."""
    days = calendar.monthrange(year, month)[1] if ndays is None else ndays
    start = pd.Timestamp(year, month, 1)
    return pd.date_range(start, periods=days * 24, freq="1h")


def make_clipped_ds(
    times: pd.DatetimeIndex,
    variables: list[str],
    fill: float = 1.0,
    lats: list[float] = LATS,
    lons: list[float] = LONS,
    with_crs: bool = True,
) -> xr.Dataset:
    """A post-clip month: dims (time, latitude, longitude), constant ``fill``.

    Includes a scalar ``spatial_ref`` coord (like rioxarray) so the region-write
    coord-stripping path is exercised.
    """
    shape = (len(times), len(lats), len(lons))
    data = {
        v: (("time", "latitude", "longitude"), np.full(shape, fill, dtype="float64"))
        for v in variables
    }
    ds = xr.Dataset(data, coords={"time": times, "latitude": lats, "longitude": lons})
    if with_crs:
        ds = ds.assign_coords(spatial_ref=0)
    return ds


def write_raw_era5_nc(
    target: Path,
    year: int,
    month: int,
    variables: list[str],
    *,
    ndays: int | None = None,
    lon_0_360: bool = False,
    expver_shape: str | None = None,
    add_number: bool = False,
    lats: list[float] | None = None,
    lons: list[float] | None = None,
) -> Path:
    """Write a synthetic *raw* (pre-normalization) ERA5-Land NetCDF.

    Mimics the new CDS backend: ``valid_time`` time axis, optional [0, 360)
    longitudes, and optional ERA5T ``expver`` / ``number`` housekeeping in one of
    several shapes (``"scalar"``, ``"dim"``, ``"time"``).
    """
    times = month_index(year, month, ndays)
    lats = [40.0, 41.0] if lats is None else lats
    raw_lons = [270.0, 271.0] if lon_0_360 else [-90.0, -89.0]
    lons = raw_lons if lons is None else lons

    shape = (len(times), len(lats), len(lons))
    data = {
        v: (("valid_time", "lat", "lon"), np.full(shape, float(month), dtype="float64"))
        for v in variables
    }
    ds = xr.Dataset(data, coords={"valid_time": times, "lat": lats, "lon": lons})

    if expver_shape == "scalar":
        ds = ds.assign_coords(expver="0001")
    elif expver_shape == "time":
        ds["expver"] = ("valid_time", np.array(["0001"] * len(times)))
    elif expver_shape == "dim":
        ds = ds.expand_dims(expver=["0001", "0005"])
    if add_number:
        ds = ds.assign_coords(number=0)

    ds.to_netcdf(target)
    ds.close()
    return target


def square_clip_mask_parquet(
    root: Path, stusps: str = "IL", *, lon=(-90.0, -88.0), lat=(40.0, 42.0)
) -> Path:
    """Write a square clip mask where :func:`get_state_geometry` looks for it.

    Mirrors the published layout under ``root`` (which stands in for the bucket
    root): ``<root>/<PREFIX>/<CODE>/<code>_huc8_clip_mask.parquet``.
    """
    import geopandas as gpd
    from shapely.geometry import box

    from dagster_pri.era5.geometry import CLIP_MASK_PREFIX, normalize_stusps

    code = normalize_stusps(stusps)
    path = root / CLIP_MASK_PREFIX / code / f"{code.lower()}_huc8_clip_mask.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)

    gdf = gpd.GeoDataFrame(
        {"state": [code]},
        geometry=[box(lon[0], lat[0], lon[1], lat[1])],
        crs="EPSG:4326",
    )
    gdf.to_parquet(path)
    return path
