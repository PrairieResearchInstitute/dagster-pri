#!/usr/bin/env python3
"""
era5_land_illinois_to_zarr.py

Build a cloud-optimized, analysis-ready (ARCO) replica of Copernicus ERA5-Land
for the state of Illinois, one calendar month per invocation.

Pipeline
--------
1. Resolve the Illinois boundary polygon and derive a tight bounding box.
2. Retrieve the ERA5-Land NetCDF for the requested month over that bbox via the
   CDS API (the API only subsets to a rectangle, so this is the coarse spatial
   filter).
3. Open the month, normalize coordinates, and CLIP to the actual Illinois
   polygon (the fine spatial filter -> cells outside the state become NaN/dropped).
4. Create or append to a chunked, compressed, consolidated Zarr store in an
   S3-compatible object store. Whether this run creates the store or appends to
   it is decided automatically by probing for an existing store.

Why month-by-month
------------------
A full year of hourly ERA5-Land is 8,760 timesteps *per variable*. Requesting it
in one shot strains CDS per-request limits and queues badly. Each invocation
handles a single month, which keeps memory bounded and lets an orchestrator
drive the months independently (in parallel or as separate retries), appending
to the same Zarr store incrementally.

Setup
-----
  pip install "cdsapi>=0.7.4" "xarray>=2024.6" "rioxarray>=0.15" \
              "geopandas>=0.14" "pyogrio" "zarr>=3" "s3fs>=2024.6" \
              "numcodecs" "netcdf4" "dask" "python-dotenv"

  # CDS credentials (~/.cdsapirc):
  #   url: https://cds.climate.copernicus.eu/api
  #   key: <YOUR-PERSONAL-ACCESS-TOKEN>
  # You must also accept the ERA5-Land licence once, on the dataset's web page.

  # Object-store connection comes from a .env file at the project root, loaded
  # via python-dotenv. The store is a Ceph-based (OSN) S3 endpoint, not AWS:
  #   S3_ENDPOINT_URL=https://<ceph-endpoint>
  #   AWS_ACCESS_KEY_ID=...
  #   AWS_SECRET_ACCESS_KEY=...
  #   BUCKET_NAME=<bucket>
  # The Zarr store is written to s3://${BUCKET_NAME}/era5/STATE=IL.

Example
-------
  # Fetch one month. Re-run with the next --month to append to the same store.
  # S3 connection is read from .env; nothing about it is passed on the CLI.
  python era5_land_illinois_to_zarr.py \
      --year 2023 --month 1 \
      --variables 2m_temperature total_precipitation 2m_dewpoint_temperature

Notes
-----
* GCS / Azure: change the URI scheme to gs:// or az:// and adjust
  build_storage_options(); fsspec handles the rest. Nothing else changes.
* This pulls the Illinois polygon from the US Census cartographic boundary file
  by default. Use --boundary to supply your own shapefile/GeoJSON instead.
* Zarr v2 *format* is written deliberately (zarr_format=2): it is the de-facto
  interchange format for the ARCO ecosystem. We use the zarr-python 3.x library
  to emit it — the library version and the on-disk format version are
  independent, and zarr-python 3 reads/writes v2-format stores natively.
"""

from __future__ import annotations

import argparse
import calendar
import logging
import os
import sys
import tempfile
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("era5land-il")

DATASET = "reanalysis-era5-land"

# Where the Zarr store lives inside the bucket. A single, appendable store: each
# month appends along the time axis. STATE=IL is a deliberate Hive-style segment
# so sibling states can be added later (era5/STATE=IN, ...).
DEFAULT_ZARR_PREFIX = "era5/STATE=IL"

# Sensible default surface variables. Override with --variables.
DEFAULT_VARIABLES = [
    "2m_temperature",
    "total_precipitation",
    "2m_dewpoint_temperature",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "surface_pressure",
]

# US Census 2022 cartographic boundary, state level (1:500k).
CENSUS_STATES_ZIP = (
    "https://www2.census.gov/geo/tiger/GENZ2022/shp/cb_2022_us_state_500k.zip"
)


# --------------------------------------------------------------------------- #
# Boundary / bounding box
# --------------------------------------------------------------------------- #
def get_illinois_geometry(boundary_path: str | None):
    """Return a single-row GeoDataFrame (EPSG:4326) for Illinois."""
    import geopandas as gpd

    if boundary_path:
        log.info("Reading boundary from %s", boundary_path)
        gdf = gpd.read_file(boundary_path)
        # If a multi-state file was supplied, try to isolate Illinois.
        for col in ("STUSPS", "STATE_ABBR", "NAME", "state"):
            if col in gdf.columns:
                hit = gdf[gdf[col].astype(str).str.upper().isin({"IL", "ILLINOIS"})]
                if len(hit):
                    gdf = hit
                    break
    else:
        log.info("Downloading Illinois boundary from US Census...")
        gdf = gpd.read_file(CENSUS_STATES_ZIP)
        gdf = gdf[gdf["STUSPS"] == "IL"]

    if gdf.empty:
        raise ValueError("Could not locate the Illinois polygon in the boundary source.")

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


# --------------------------------------------------------------------------- #
# CDS retrieval
# --------------------------------------------------------------------------- #
def download_month(client, year: int, month: int, variables: list[str],
                   area: list[float], out_path: Path,
                   ndays: int | None = None) -> Path:
    """Retrieve one month of hourly ERA5-Land as NetCDF over the bbox."""
    if out_path.exists() and out_path.stat().st_size > 0:
        log.info("  cached: %s", out_path.name)
        return out_path

    days_in_month = calendar.monthrange(year, month)[1]
    ndays = days_in_month if ndays is None else min(ndays, days_in_month)
    request = {
        "variable": variables,
        "year": str(year),
        "month": f"{month:02d}",
        "day": [f"{d:02d}" for d in range(1, ndays + 1)],
        "time": [f"{h:02d}:00" for h in range(24)],
        "data_format": "netcdf",
        "download_format": "unarchived",  # plain .nc, not zipped
        "area": area,  # [N, W, S, E]
    }
    log.info("  requesting %04d-%02d (%d days x 24h, %d vars)...",
             year, month, ndays, len(variables))
    client.retrieve(DATASET, request, str(out_path))
    return out_path


# --------------------------------------------------------------------------- #
# Open, normalize, clip
# --------------------------------------------------------------------------- #
def open_and_clip(nc_path: Path, gdf):
    """Open a monthly NetCDF, normalize coords, and clip to the Illinois polygon."""
    import xarray as xr

    # The new CDS backend may hand back a zip even with download_format=unarchived
    # in some edge cases; guard against it.
    if _looks_like_zip(nc_path):
        nc_path = _extract_single_nc(nc_path)

    ds = xr.open_dataset(nc_path)

    # New CDS NetCDF often uses 'valid_time' for the time axis.
    if "valid_time" in ds.variables and "time" not in ds.dims:
        ds = ds.rename({"valid_time": "time"})

    # Drop scalar housekeeping coords/dims if present.
    for extra in ("number", "expver"):
        if extra in ds.coords and ds[extra].ndim == 0:
            ds = ds.drop_vars(extra)
        elif extra in ds.dims:
            # ERA5T overlap can add an expver dimension for very recent data;
            # collapse it by taking the first non-null value across expver.
            ds = ds.ffill(extra).isel({extra: -1}, drop=True)

    # Standardize spatial coord names.
    rename = {}
    if "lat" in ds.coords:
        rename["lat"] = "latitude"
    if "lon" in ds.coords:
        rename["lon"] = "longitude"
    if rename:
        ds = ds.rename(rename)

    # Normalize longitude to [-180, 180] so it matches the Illinois polygon.
    if float(ds.longitude.max()) > 180.0:
        ds = ds.assign_coords(longitude=(((ds.longitude + 180) % 360) - 180))
        ds = ds.sortby("longitude")

    # rioxarray needs ascending y handled internally; just declare CRS + dims.
    ds = ds.rio.write_crs("EPSG:4326")
    ds = ds.rio.set_spatial_dims(x_dim="longitude", y_dim="latitude")

    # The actual Illinois filter: clip the bbox down to the state polygon.
    clipped = ds.rio.clip(
        gdf.geometry.values, gdf.crs, drop=True, all_touched=True
    )
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


# --------------------------------------------------------------------------- #
# Zarr output
# --------------------------------------------------------------------------- #
def build_storage_options() -> dict:
    """S3/Ceph storage options sourced from .env (loaded via python-dotenv)."""
    return {
        "key": os.environ["AWS_ACCESS_KEY_ID"],
        "secret": os.environ["AWS_SECRET_ACCESS_KEY"],
        "client_kwargs": {"endpoint_url": os.environ["S3_ENDPOINT_URL"]},
        # Ceph RGW: address buckets as a URL path segment, not a DNS subdomain.
        "config_kwargs": {"s3": {"addressing_style": "path"}},
    }


def zarr_store_exists(zarr_uri: str, storage_options: dict) -> bool:
    """True if a Zarr store already lives at the URI (so this run appends)."""
    import fsspec

    fs, path = fsspec.core.url_to_fs(zarr_uri, **storage_options)
    return fs.exists(path)


def build_encoding(ds, time_chunk: int) -> dict:
    """Per-variable chunking + Zstd compression (Zarr v2 style)."""
    import numcodecs

    compressor = numcodecs.Blosc(cname="zstd", clevel=5,
                                 shuffle=numcodecs.Blosc.SHUFFLE)
    nlat = ds.sizes["latitude"]
    nlon = ds.sizes["longitude"]
    enc = {}
    for name, var in ds.data_vars.items():
        # Spatial extent of an Illinois subset is tiny, so keep lat/lon whole
        # in a single chunk and chunk only along time. This favors long
        # time-series reads at a point while staying cheap for full-state maps.
        chunks = []
        for dim in var.dims:
            if dim == "time":
                chunks.append(min(time_chunk, ds.sizes["time"]))
            elif dim == "latitude":
                chunks.append(nlat)
            elif dim == "longitude":
                chunks.append(nlon)
            else:
                chunks.append(ds.sizes[dim])
        enc[name] = {"compressor": compressor, "chunks": tuple(chunks)}
    return enc


def write_zarr(ds, zarr_uri: str, storage_options: dict,
               time_chunk: int, first_write: bool) -> None:
    """Write (mode=w) or append (mode=a, append_dim=time) one month to Zarr."""
    ds = ds.chunk({"time": time_chunk, "latitude": -1, "longitude": -1})

    common = dict(consolidated=True, zarr_format=2)
    if first_write:
        ds.to_zarr(
            zarr_uri,
            mode="w",
            encoding=build_encoding(ds, time_chunk),
            storage_options=storage_options,
            **common,
        )
    else:
        # Encoding must NOT be re-specified on append.
        ds.to_zarr(
            zarr_uri,
            mode="a",
            append_dim="time",
            storage_options=storage_options,
            **common,
        )


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="Replicate ERA5-Land for Illinois into an ARCO Zarr store.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--year", type=int, required=True, help="Calendar year to fetch.")
    p.add_argument("--month", type=int, required=True, choices=range(1, 13),
                   metavar="{1..12}", help="Calendar month to fetch (1-12).")
    p.add_argument("--variables", nargs="+", default=DEFAULT_VARIABLES,
                   help="ERA5-Land variable names.")
    p.add_argument("--boundary", default=None,
                   help="Optional path to an Illinois shapefile/GeoJSON. "
                        "If omitted, the US Census boundary is downloaded.")
    p.add_argument("--time-chunk", type=int, default=24,
                   help="Hours per time chunk. Must divide 24 so that whole "
                        "months (which are always a multiple of 24h) tile the "
                        "chunk grid exactly and stay append-safe (default: 24 "
                        "= one day).")
    p.add_argument("--bbox-pad", type=float, default=0.25,
                   help="Degrees of padding around the state bbox before clipping.")
    p.add_argument("--work-dir", default=None,
                   help="Where to stage downloaded NetCDF (default: a temp dir).")
    p.add_argument("--ndays", type=int, default=None,
                   help="Only fetch the first N days of the month "
                        "(for testing). Default: the whole month.")
    args = p.parse_args(argv)

    # The time chunk is baked into the store's encoding at creation and every
    # later append must conform to it. A safe append requires the existing time
    # length to always be an exact multiple of the chunk size. Months are always
    # a whole number of 24h days (672/696/720/744h), so only a chunk that DIVIDES
    # 24 keeps every cumulative length chunk-aligned. A weekly chunk (168), say,
    # leaves a partial tail chunk after the first month and the next append fails
    # with "would overlap multiple Dask chunks". Reject it up front.
    if args.time_chunk < 1 or 24 % args.time_chunk != 0:
        p.error(
            f"--time-chunk must be a positive divisor of 24 "
            f"(1, 2, 3, 4, 6, 8, 12, 24); got {args.time_chunk}. "
            f"This keeps whole-month appends aligned to the Zarr chunk grid."
        )

    # S3/Ceph connection details live in .env at the project root.
    from dotenv import load_dotenv
    load_dotenv()

    # OSN (Ceph) rejects the default checksums newer botocore adds; only send/validate
    # them when the request actually requires it.
    os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"] = "when_required"
    os.environ["AWS_RESPONSE_CHECKSUM_VALIDATION"] = "when_required"

    try:
        import cdsapi  # noqa: F401
        import rioxarray  # noqa: F401  (registers the .rio accessor)
    except ImportError as e:
        log.error("Missing dependency: %s. See the setup block at the top.", e)
        return 1

    zarr_uri = f"s3://{os.environ['BUCKET_NAME']}/{DEFAULT_ZARR_PREFIX}"

    gdf = get_illinois_geometry(args.boundary)
    area = bbox_from_geometry(gdf, pad_deg=args.bbox_pad)
    log.info("Illinois bbox [N, W, S, E] = %s", area)

    work = Path(args.work_dir) if args.work_dir else Path(tempfile.mkdtemp(prefix="era5land_"))
    work.mkdir(parents=True, exist_ok=True)
    log.info("Staging NetCDF in %s", work)

    import cdsapi
    client = cdsapi.Client()
    storage_options = build_storage_options()

    # One month at a time: create the store if it's not there yet, else append.
    first_write = not zarr_store_exists(zarr_uri, storage_options)

    nc_path = work / f"era5land_il_{args.year}{args.month:02d}.nc"
    download_month(client, args.year, args.month, args.variables, area, nc_path,
                   ndays=args.ndays)

    log.info("  clipping %04d-%02d to Illinois...", args.year, args.month)
    ds = open_and_clip(nc_path, gdf)

    log.info("  writing %04d-%02d to %s (%s)",
             args.year, args.month, zarr_uri,
             "create" if first_write else "append")
    write_zarr(ds, zarr_uri, storage_options, args.time_chunk, first_write)
    ds.close()

    # Final metadata consolidation (cheap, and harmless if already consolidated).
    try:
        import fsspec
        import zarr
        mapper = fsspec.get_mapper(zarr_uri, **storage_options)
        zarr.consolidate_metadata(mapper)
        log.info("Consolidated Zarr metadata.")
    except Exception as e:  # noqa: BLE001
        log.warning("Skipping explicit re-consolidation (%s). "
                    "Per-write consolidation already ran.", e)

    log.info("Done. ARCO ERA5-Land for Illinois %04d-%02d -> %s",
             args.year, args.month, zarr_uri)
    return 0


if __name__ == "__main__":
    sys.exit(main())
