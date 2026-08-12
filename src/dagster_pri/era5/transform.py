"""Open a monthly ERA5-Land NetCDF, normalize coords, and clip to a polygon."""

from __future__ import annotations

import logging
from pathlib import Path

log = logging.getLogger("era5land")

# Decimals kept on latitude/longitude. ERA5-Land is a 0.1-degree grid, so this
# only strips float noise (see _snap_grid).
GRID_DECIMALS = 6


def open_and_clip_batches(nc_paths: list[Path], gdf):
    """Open the variable batches of one month, clip each, and merge them.

    The batches are the same month over the same bbox, so their grids and time
    axes are identical by construction; ``join="exact"`` makes any drift fail
    loudly here rather than silently outer-joining NaN padding into the store.
    """
    import xarray as xr

    if not nc_paths:
        raise ValueError("no staged NetCDF paths to open.")

    parts = [open_and_clip(p, gdf) for p in nc_paths]
    if len(parts) == 1:
        return parts[0]

    # compat="equals": disjoint variable sets are expected, so a name appearing
    # in two batches with different values is a bug, not something to blend.
    merged = xr.merge(parts, join="exact", compat="equals", combine_attrs="override")

    def close_parts() -> None:
        for part in parts:
            part.close()

    # Merging drops the parts' file handles; close them with the merged dataset.
    merged.set_close(close_parts)
    return merged


def check_variable_count(ds, variables: list[str]) -> None:
    """Fail if the downloaded month has fewer arrays than the request asked for.

    CDS answers with the CF SHORT names, so the requested long names cannot be
    matched one-to-one -- but the counts must agree. Worth checking because a
    partially-read payload is otherwise silent: `init` would bake the short
    variable set into the store's schema for good (see the zip-of-many-NetCDFs
    case in :func:`open_and_clip`).
    """
    got = sorted(ds.data_vars)
    if len(got) == len(variables):
        return
    raise ValueError(
        f"the downloaded month carries {len(got)} variables but "
        f"{len(variables)} were requested: got {got}, requested {sorted(variables)}. "
        f"The CDS payload is incomplete -- do not initialize a store from it."
    )


def open_and_clip(nc_path: Path, gdf):
    """Open one staged CDS payload, normalize coords, and clip to the state polygon.

    A payload is usually a single NetCDF, but the new CDS backend hands back a zip
    of SEVERAL NetCDFs even with ``download_format=unarchived`` -- it splits one
    request's variables across ``data_0.nc``, ``data_1.nc``, ... by how they sit in
    the archive (e.g. ``stl2, stl3`` / ``v10, ssr, e, tp, pev`` / ``evavt``). All
    members are the same month over the same bbox, so they are opened, normalized
    and merged into one dataset; reading only the first would silently drop most of
    the requested variables.
    """
    import rioxarray  # noqa: F401  (registers the .rio accessor)
    import xarray as xr

    parts = [_open_normalized(p) for p in _payload_members(nc_path)]
    if len(parts) == 1:
        ds = parts[0]
    else:
        # Same request, same grid: exact join + compat="equals" so any drift or
        # duplicated variable fails loudly instead of being blended or padded.
        ds = xr.merge(parts, join="exact", compat="equals", combine_attrs="override")

    ds = _add_spatial_metadata(ds)

    # The actual state filter: clip the bbox down to the state polygon.
    clipped = ds.rio.clip(gdf.geometry.values, gdf.crs, drop=True, all_touched=True)

    def close_parts() -> None:
        for part in parts:
            part.close()

    # Clipping (and merging) drops the sources' file handles; close them with the
    # dataset the caller actually holds.
    clipped.set_close(close_parts)
    return clipped


def _payload_members(nc_path: Path) -> list[Path]:
    """The NetCDF file(s) inside a staged payload: itself, or a zip's members."""
    if not _looks_like_zip(nc_path):
        return [nc_path]
    members = _extract_ncs(nc_path)
    log.info("  %s is a zip of %d NetCDF(s); merging them", nc_path.name, len(members))
    return members


def _open_normalized(nc_path: Path):
    """Open one NetCDF and normalize its coords/dims to the archive's conventions."""
    import xarray as xr

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

    return _snap_grid(ds)


def _snap_grid(ds):
    """Round lat/lon to ``GRID_DECIMALS`` so every payload lands on the same grid.

    ERA5-Land is an exact 0.1-degree grid, so this is a no-op on well-formed
    coords -- but it is load-bearing for two paths that MUST produce
    bit-identical coordinates: members of one CDS zip can disagree on the
    longitude convention (some come back 0..360), and the modular wrap above
    leaves float noise (-92.9 becomes -92.89999999999998), which is enough to
    break the exact-join merge and, later, a region write against the store's
    grid.
    """
    coords = {
        name: ds[name].round(GRID_DECIMALS)
        for name in ("latitude", "longitude")
        if name in ds.coords
    }
    return ds.assign_coords(coords) if coords else ds


def _add_spatial_metadata(ds):
    """Declare the CRS + spatial dims rioxarray needs to clip."""
    # rioxarray needs ascending y handled internally; just declare CRS + dims.
    ds = ds.rio.write_crs("EPSG:4326")
    return ds.rio.set_spatial_dims(x_dim="longitude", y_dim="latitude")


def _looks_like_zip(path: Path) -> bool:
    with open(path, "rb") as fh:
        return fh.read(2) == b"PK"


def _extract_ncs(zip_path: Path) -> list[Path]:
    """Extract EVERY .nc member alongside the zip, in archive order."""
    import zipfile

    targets = []
    with zipfile.ZipFile(zip_path) as zf:
        ncs = [n for n in zf.namelist() if n.endswith(".nc")]
        if not ncs:
            raise RuntimeError(f"No .nc inside {zip_path}")
        for i, name in enumerate(ncs):
            target = zip_path.with_suffix(f".extracted_{i:02d}.nc")
            with zf.open(name) as src, open(target, "wb") as dst:
                dst.write(src.read())
            targets.append(target)
    return targets
