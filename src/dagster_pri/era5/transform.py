"""Open a monthly ERA5-Land NetCDF, normalize coords, and clip to a polygon."""

from __future__ import annotations

from pathlib import Path


def open_and_clip(nc_path: Path, gdf):
    """Open a monthly NetCDF, normalize coords, and clip to the state polygon."""
    import rioxarray  # noqa: F401  (registers the .rio accessor)
    import xarray as xr

    # The new CDS backend may hand back a zip even with download_format=unarchived
    # in some edge cases; guard against it.
    if _looks_like_zip(nc_path):
        nc_path = _extract_single_nc(nc_path)

    ds = xr.open_dataset(nc_path)

    # New CDS NetCDF often uses 'valid_time' for the time axis.
    if "valid_time" in ds.variables and "time" not in ds.dims:
        ds = ds.rename({"valid_time": "time"})

    # Drop ERA5T housekeeping (`number`, `expver`) entirely. These appear in
    # several shapes depending on data recency: a scalar coord, a real dimension
    # (ERA5T overlap), or a coord that VARIES ALONG TIME. They must NOT enter the
    # archive schema -- a time-varying `expver` in particular breaks region writes
    # ("non-pre-existing variables ['expver']") because the all-NaN template,
    # built from a single timestep, never carries it.
    for extra in ("number", "expver"):
        if extra in ds.dims:
            # Collapse the overlap dimension by taking the most recent expver.
            ds = ds.ffill(extra).isel({extra: -1}, drop=True)
        if extra in ds.variables:
            ds = ds.drop_vars(extra)

    # Standardize spatial coord names.
    rename = {}
    if "lat" in ds.coords:
        rename["lat"] = "latitude"
    if "lon" in ds.coords:
        rename["lon"] = "longitude"
    if rename:
        ds = ds.rename(rename)

    # Normalize longitude to [-180, 180] so it matches the state polygon.
    if float(ds.longitude.max()) > 180.0:
        ds = ds.assign_coords(longitude=(((ds.longitude + 180) % 360) - 180))
        ds = ds.sortby("longitude")

    # rioxarray needs ascending y handled internally; just declare CRS + dims.
    ds = ds.rio.write_crs("EPSG:4326")
    ds = ds.rio.set_spatial_dims(x_dim="longitude", y_dim="latitude")

    # The actual state filter: clip the bbox down to the state polygon.
    clipped = ds.rio.clip(gdf.geometry.values, gdf.crs, drop=True, all_touched=True)
    return clipped


def _looks_like_zip(path: Path) -> bool:
    with open(path, "rb") as fh:
        return fh.read(2) == b"PK"


def _extract_single_nc(zip_path: Path) -> Path:
    import zipfile

    with zipfile.ZipFile(zip_path) as zf:
        ncs = [n for n in zf.namelist() if n.endswith(".nc")]
        if not ncs:
            raise RuntimeError(f"No .nc inside {zip_path}")
        target = zip_path.with_suffix(".extracted.nc")
        with zf.open(ncs[0]) as src, open(target, "wb") as dst:
            dst.write(src.read())
    return target
