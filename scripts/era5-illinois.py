#!/usr/bin/env python3
"""
era5-illinois.py

Build a cloud-optimized, analysis-ready (ARCO) replica of Copernicus ERA5-Land
for the state of Illinois in an **Icechunk** repository, one calendar month per
`ingest` invocation.

Why Icechunk (vs. the previous plain-Zarr append store)
-------------------------------------------------------
The store is an Icechunk repository written via **init-once + per-month region
writes**, which gives us:

* **Any-order ingest.** A fixed global hourly axis (1950 -> a configurable end)
  is pre-allocated once; each month is region-written into its slot. Start with
  recent months and backfill earlier years freely -- the time axis stays
  monotonic regardless of write order.
* **Atomic, retryable commits.** Every month write is one commit. An interrupted
  run leaves the store untouched rather than corrupted.
* **Idempotent re-ingest.** Re-running a month overwrites its region in place --
  no duplicate timesteps, no dedup logic.
* **Versioning.** Roll back a bad month, tag releases, time-travel reads.

Pipeline (per `ingest`)
-----------------------
1. Resolve the Illinois boundary polygon and derive a tight bounding box.
2. Retrieve the ERA5-Land NetCDF for the requested month over that bbox via the
   CDS API (the API only subsets to a rectangle -- the coarse spatial filter).
3. Open the month, normalize coordinates, and CLIP to the actual Illinois
   polygon (the fine spatial filter -> cells outside the state become NaN).
4. Region-write that month into its slot in the Icechunk repo (or, for a month
   just past the current axis end, append along time) and commit.

Subcommands
-----------
The previous script auto-decided create-vs-append by probing the store, which
races under parallel orchestration. That is now split into two explicit steps:

  # Create the repo, fix the spatial grid + variable set from a reference month,
  # pre-allocate the full hourly axis, and region-write the reference month.
  python era5-illinois.py init \
      --variables 2m_temperature total_precipitation 2m_dewpoint_temperature \
      --ref-year 2024 --ref-month 1 --axis-end 2024-12-31T23:00

  # Then fan out months in ANY order (parallel-safe; each is its own commit):
  python era5-illinois.py ingest --year 2023 --month 1
  python era5-illinois.py ingest --year 2020 --month 6

`--variables` must be identical for `init` and every `ingest` (the variable set
is fixed at array-creation time; `ingest` validates this against the store).

Reading the store
-----------------
The store is Zarr **v3** managed by Icechunk -- it is NOT plain-Zarr readable by
arbitrary tools; consumers need the `icechunk` package. Open it with:

    import icechunk, xarray as xr
    repo = icechunk.Repository.open(make_icechunk_storage("era5/STATE=IL"))
    ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)

Setup
-----
  uv sync   # installs icechunk, xarray, rioxarray, geopandas, cdsapi, ...

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
  #   S3_REGION=us-east-1            # optional; Ceph ignores it but the client wants one
  # The Icechunk repo lives at s3://${BUCKET_NAME}/era5/STATE=IL.

Notes
-----
* Icechunk uses its own Rust S3 client, NOT botocore -- so the
  AWS_*_CHECKSUM_* env-var workaround we needed for s3fs does NOT apply here.
  Ceph compatibility comes from s3_storage(force_path_style=True, endpoint_url=...).
* This pulls the Illinois polygon from the US Census cartographic boundary file
  by default. Use --boundary to supply your own shapefile/GeoJSON instead.
"""

from __future__ import annotations

import argparse
import calendar
import logging
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("era5land-il")

DATASET = "reanalysis-era5-land"

# Where the Icechunk repo lives inside the bucket. STATE=IL is a deliberate
# Hive-style segment so sibling states can be added later (era5/STATE=IN, ...).
DEFAULT_ZARR_PREFIX = "era5/STATE=IL"

# ERA5-Land's start. The global hourly axis begins here so that ANY historical
# month already has a pre-allocated slot to region-write into (see init_store).
AXIS_START = "1950-01-01T00:00"

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
# Icechunk storage / repo
# --------------------------------------------------------------------------- #
def make_icechunk_storage(prefix: str) -> "icechunk.Storage":
    """Icechunk S3 storage pointed at the OSN/Ceph endpoint from .env.

    Ceph compatibility is handled entirely here: force_path_style mirrors s3fs's
    addressing_style="path", and endpoint_url points at OSN instead of AWS.
    Icechunk's Rust S3 client ignores the botocore AWS_*_CHECKSUM_* env vars, so
    none are set on this path.
    """
    import icechunk

    endpoint = os.environ["S3_ENDPOINT_URL"]
    return icechunk.s3_storage(
        bucket=os.environ["BUCKET_NAME"],
        prefix=prefix,
        endpoint_url=endpoint,
        region=os.environ.get("S3_REGION", "us-east-1"),
        access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        force_path_style=True,                       # Ceph RGW path addressing
        allow_http=endpoint.lower().startswith("http://"),
        # checksum_algorithm=...  # escape hatch only if OSN rejects default checksums
    )


def open_or_create_repo(storage):
    """Open the repo if it exists, else create it (init's create path)."""
    import icechunk

    try:
        return icechunk.Repository.open(storage)
    except Exception:  # noqa: BLE001 -- "doesn't exist yet" surfaces differently across versions
        return icechunk.Repository.create(storage)


def open_repo(storage):
    """Open an existing repo; raise loudly if it isn't there (ingest's path)."""
    import icechunk

    return icechunk.Repository.open(storage)


def open_store_dataset(repo):
    """Open the repo's main branch read-only as an xarray Dataset."""
    import xarray as xr

    return xr.open_zarr(repo.readonly_session("main").store, consolidated=False)


# --------------------------------------------------------------------------- #
# Global axis + template
# --------------------------------------------------------------------------- #
def default_axis_end() -> str:
    """End of the most recent COMPLETE year, as the last hour of that year.

    A small buffer to year-end is fine; do NOT pad far into the future -- the
    `time` coordinate is materialized eagerly, so a padded tail bloats the
    coordinate and silently drags empty timesteps into naive `.mean("time")` /
    `.sel(...)` reads. Grow the axis when real data arrives instead (append).
    """
    last_complete_year = datetime.now(timezone.utc).year - 1
    return f"{last_complete_year}-12-31T23:00"


def global_axis(axis_end: str) -> "pd.DatetimeIndex":
    """The fixed, pre-allocated hourly axis [AXIS_START, axis_end]."""
    import pandas as pd

    return pd.date_range(AXIS_START, axis_end, freq="1h")


def build_template(times, ref_clipped, time_chunk):
    """All-NaN, dask-backed dataset spanning the full axis on the reference grid.

    Built from the reference month's clipped grid + variable set so the template
    is byte-identical to every later month's grid. Written metadata-only
    (compute=False) so only the 1-D time coordinate + array metadata hit disk --
    no data chunks. The reference month's non-time coords (lat/lon and the
    rioxarray `spatial_ref` CRS coord) are preserved.
    """
    import numpy as np
    import xarray as xr

    skeleton = ref_clipped.isel(time=0, drop=True)          # spatial grid only
    nan_like = xr.full_like(skeleton, np.nan)               # same vars/grid/attrs
    template = nan_like.expand_dims(time=times)             # add the full axis
    return template.chunk({"time": time_chunk, "latitude": -1, "longitude": -1})


def axis_initialized(repo, n_expected: int, var_names: list[str]) -> bool:
    """True once the full axis + variable set is laid down.

    Uses `>= n_expected` (not `==`) so a later forward-append that GREW the axis
    still counts as initialized; only a missing/short axis triggers (re)init.
    """
    try:
        ds = open_store_dataset(repo)
    except Exception:  # noqa: BLE001 -- empty/new repo
        return False
    if "time" not in ds.dims or ds.sizes["time"] < n_expected:
        return False
    return all(v in ds.data_vars for v in var_names)


def assert_subset_of_axis(month_time, store_times) -> None:
    """Guard region='auto': the month must be a CONTIGUOUS subset of the axis.

    region='auto' locates the slice by matching the month's `time` values against
    the store axis, so any drift (ERA5T offsets, valid_time quirks, non-on-the-
    hour stamps, tz-tagged inputs) must fail loudly here rather than mis-place
    data.
    """
    import numpy as np

    mt = np.asarray(month_time.values if hasattr(month_time, "values") else month_time)
    store = np.asarray(store_times)
    if not np.isin(mt, store).all():
        raise ValueError(
            "month timestamps are not all present in the store's time axis; "
            "the month grid is not aligned to the pre-allocated hourly axis "
            "(check for ERA5T offsets / non-on-the-hour timestamps)."
        )
    idx = np.searchsorted(store, mt)
    if len(idx) > 1 and not np.array_equal(np.diff(idx), np.ones(len(idx) - 1, dtype=idx.dtype)):
        raise ValueError("month timestamps are not contiguous within the store axis.")


def _strip_nonregion_coords(ds):
    """Drop scalar coords (e.g. rioxarray's `spatial_ref`) before a region/append
    write. region='auto' rejects variables with no dimension in common with the
    region dims; the CRS coord is written once into the template at init and does
    not need rewriting per month."""
    drop = [n for n, c in ds.coords.items() if c.ndim == 0]
    return ds.drop_vars(drop) if drop else ds


# --------------------------------------------------------------------------- #
# Commit / init / write
# --------------------------------------------------------------------------- #
def commit_with_retry(session, message: str, attempts: int = 5):
    """Commit, rebasing past conflicts from concurrent month writes.

    Daily chunks + month-aligned regions mean parallel month commits touch
    DISJOINT chunks, so Icechunk's optimistic concurrency rebases cleanly.
    """
    import icechunk

    last = None
    for _ in range(attempts):
        try:
            return session.commit(message)
        except icechunk.ConflictError as e:  # branch tip moved under us
            last = e
            session.rebase(icechunk.ConflictDetector())
    raise RuntimeError(
        f"commit failed after {attempts} rebase attempts: {message} ({last})"
    )


def init_store(repo, times, ref_clipped, time_chunk: int) -> bool:
    """Lay down the full-axis template (compute=False) as one commit.

    Idempotent: a no-op if the axis already exists. Returns True if it wrote the
    template, False if it was already initialized.
    """
    from zarr.codecs import BloscCodec, BloscShuffle

    var_names = list(ref_clipped.data_vars)
    if axis_initialized(repo, len(times), var_names):
        log.info("Axis already initialized (time >= %d, vars=%s); skipping template.",
                 len(times), var_names)
        return False

    log.info("Initializing full hourly axis %s .. %s (%d steps, chunk=%d)...",
             times[0], times[-1], len(times), time_chunk)
    session = repo.writable_session("main")
    template = build_template(times, ref_clipped, time_chunk)
    # zstd compression + NaN fill, fixed once at array-creation; region/append
    # writes inherit it and must not (and do not) re-specify encoding.
    compressors = [BloscCodec(cname="zstd", clevel=5, shuffle=BloscShuffle.shuffle)]
    encoding = {
        v: {"fill_value": float("nan"), "compressors": compressors}
        for v in var_names
    }
    # icechunk's to_icechunk has no `compute` arg; write the schema directly
    # through the session's Zarr store with compute=False so only metadata +
    # the time coordinate are materialized (no data chunks).
    template.to_zarr(
        session.store, mode="w", compute=False, consolidated=False,
        encoding=encoding,
    )
    snap = commit_with_retry(
        session,
        f"init full hourly axis {times[0]}..{times[-1]} ({len(times)} steps)",
    )
    log.info("  committed init -> snapshot %s", snap)
    return True


def write_month(repo, clipped, year: int, month: int) -> str:
    """Region-write (backfill) or append (forward-extend) one month; one commit.

    Decision per month:
      * starts within [AXIS_START, current_end] -> region='auto' (any order)
      * starts exactly at current_end + 1h      -> append_dim='time'
      * starts in an unallocated future gap      -> error (extend the axis first)
    Returns "region" or "append".
    """
    import numpy as np
    from icechunk.xarray import to_icechunk

    clipped = _strip_nonregion_coords(clipped)
    session = repo.writable_session("main")
    store_times = open_store_dataset(repo).time.values
    first = clipped.time.values[0]

    if first <= store_times[-1]:
        assert_subset_of_axis(clipped.time, store_times)
        to_icechunk(clipped, session, region="auto")
        mode = "region"
    elif first == store_times[-1] + np.timedelta64(1, "h"):
        to_icechunk(clipped, session, append_dim="time")
        mode = "append"
    else:
        raise ValueError(
            f"{year}-{month:02d} starts at {first} -- beyond the allocated axis "
            f"end {store_times[-1]} and not contiguous with it. Extend the axis "
            f"to this year first (out of scope; see plan follow-ups)."
        )

    snap = commit_with_retry(session, f"ingest {year}-{month:02d} via {mode}")
    log.info("  committed %04d-%02d (%s) -> snapshot %s", year, month, mode, snap)
    return mode


def validate_variables_against_store(repo, clipped) -> None:
    """Fail loudly if this month's variable set differs from the store's.

    The variable set is fixed at array-creation; a mismatch means `--variables`
    drifted from what `init` used (a region write would otherwise error opaquely).
    """
    store_vars = set(open_store_dataset(repo).data_vars)
    month_vars = set(clipped.data_vars)
    if month_vars != store_vars:
        raise ValueError(
            f"variable set {sorted(month_vars)} does not match the store's "
            f"{sorted(store_vars)}. `--variables` must match what `init` used "
            f"(adding a variable means creating a new array, not a region write)."
        )


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _add_common_args(sp: argparse.ArgumentParser) -> None:
    sp.add_argument("--variables", nargs="+", default=DEFAULT_VARIABLES,
                    help="ERA5-Land variable names. MUST match between init and "
                         "every ingest.")
    sp.add_argument("--prefix", default=DEFAULT_ZARR_PREFIX,
                    help="Bucket prefix for the Icechunk repo (use a scratch "
                         "prefix for testing).")
    sp.add_argument("--boundary", default=None,
                    help="Optional path to an Illinois shapefile/GeoJSON. "
                         "If omitted, the US Census boundary is downloaded.")
    sp.add_argument("--time-chunk", type=int, default=24,
                    help="Hours per time chunk. Must divide 24 so whole months "
                         "(always a multiple of 24h) tile the chunk grid exactly "
                         "and keep region writes / appends chunk-aligned "
                         "(default: 24 = one day).")
    sp.add_argument("--bbox-pad", type=float, default=0.25,
                    help="Degrees of padding around the state bbox before clipping.")
    sp.add_argument("--work-dir", default=None,
                    help="Where to stage downloaded NetCDF (default: a temp dir).")
    sp.add_argument("--ndays", type=int, default=None,
                    help="Only fetch the first N days of the month (for testing). "
                         "Default: the whole month.")


def _check_time_chunk(parser, time_chunk: int) -> None:
    # The time chunk is baked into the store's encoding at init and every later
    # write must conform. Months are always a whole number of 24h days, so only a
    # chunk that DIVIDES 24 keeps every month region-aligned and every cumulative
    # length chunk-aligned for appends.
    if time_chunk < 1 or 24 % time_chunk != 0:
        parser.error(
            f"--time-chunk must be a positive divisor of 24 "
            f"(1, 2, 3, 4, 6, 8, 12, 24); got {time_chunk}."
        )


def _load_env_and_deps(parser) -> None:
    # S3/Ceph connection details live in .env at the project root.
    from dotenv import load_dotenv
    load_dotenv()

    try:
        import cdsapi  # noqa: F401
        import icechunk  # noqa: F401
        import rioxarray  # noqa: F401  (registers the .rio accessor)
    except ImportError as e:
        parser.error(f"Missing dependency: {e}. Run `uv sync`.")


def cmd_init(args, parser) -> int:
    _check_time_chunk(parser, args.time_chunk)
    _load_env_and_deps(parser)

    import cdsapi

    axis_end = args.axis_end or default_axis_end()
    times = global_axis(axis_end)
    log.info("Global axis: %s .. %s (%d hourly steps)", times[0], times[-1], len(times))

    gdf = get_illinois_geometry(args.boundary)
    area = bbox_from_geometry(gdf, pad_deg=args.bbox_pad)
    log.info("Illinois bbox [N, W, S, E] = %s", area)

    work = Path(args.work_dir) if args.work_dir else Path(tempfile.mkdtemp(prefix="era5land_"))
    work.mkdir(parents=True, exist_ok=True)

    # Download + clip the reference month: it defines the spatial grid and the
    # variable set for the whole store, then becomes the first month of data.
    client = cdsapi.Client()
    nc_path = work / f"era5land_il_{args.ref_year}{args.ref_month:02d}.nc"
    download_month(client, args.ref_year, args.ref_month, args.variables, area,
                   nc_path, ndays=args.ndays)
    log.info("  clipping reference month %04d-%02d to Illinois...",
             args.ref_year, args.ref_month)
    ref_clipped = open_and_clip(nc_path, gdf)

    storage = make_icechunk_storage(args.prefix)
    repo = open_or_create_repo(storage)
    log.info("Repo: s3://%s/%s", os.environ["BUCKET_NAME"], args.prefix)

    init_store(repo, times, ref_clipped, args.time_chunk)

    # Region-write the reference month as the store's first real data (idempotent).
    log.info("Region-writing reference month %04d-%02d...",
             args.ref_year, args.ref_month)
    write_month(repo, ref_clipped, args.ref_year, args.ref_month)
    ref_clipped.close()

    log.info("Init complete. Now `ingest --year Y --month M` in any order.")
    return 0


def cmd_ingest(args, parser) -> int:
    _check_time_chunk(parser, args.time_chunk)
    _load_env_and_deps(parser)

    import cdsapi

    gdf = get_illinois_geometry(args.boundary)
    area = bbox_from_geometry(gdf, pad_deg=args.bbox_pad)
    log.info("Illinois bbox [N, W, S, E] = %s", area)

    storage = make_icechunk_storage(args.prefix)
    try:
        repo = open_repo(storage)
    except Exception as e:  # noqa: BLE001
        parser.error(
            f"Could not open the Icechunk repo at s3://{os.environ['BUCKET_NAME']}/"
            f"{args.prefix} ({e}). Run `init` first."
        )

    work = Path(args.work_dir) if args.work_dir else Path(tempfile.mkdtemp(prefix="era5land_"))
    work.mkdir(parents=True, exist_ok=True)

    client = cdsapi.Client()
    nc_path = work / f"era5land_il_{args.year}{args.month:02d}.nc"
    download_month(client, args.year, args.month, args.variables, area, nc_path,
                   ndays=args.ndays)
    log.info("  clipping %04d-%02d to Illinois...", args.year, args.month)
    clipped = open_and_clip(nc_path, gdf)

    validate_variables_against_store(repo, clipped)

    log.info("  writing %04d-%02d into the store...", args.year, args.month)
    write_month(repo, clipped, args.year, args.month)
    clipped.close()

    log.info("Done. ERA5-Land for Illinois %04d-%02d -> s3://%s/%s",
             args.year, args.month, os.environ["BUCKET_NAME"], args.prefix)
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="Replicate ERA5-Land for Illinois into an Icechunk repository.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    sub = p.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser(
        "init", help="Create the repo, fix the grid + variable set, pre-allocate "
                     "the full hourly axis, and write a reference month.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    _add_common_args(p_init)
    p_init.add_argument("--axis-end", default=None,
                        help="Last hour of the pre-allocated axis, e.g. "
                             "2024-12-31T23:00. Default: end of the most recent "
                             "complete year. Months past this are appended.")
    p_init.add_argument("--ref-year", type=int, required=True,
                        help="Year of the reference month (defines the grid).")
    p_init.add_argument("--ref-month", type=int, required=True, choices=range(1, 13),
                        metavar="{1..12}", help="Month of the reference month.")
    p_init.set_defaults(func=cmd_init)

    p_ing = sub.add_parser(
        "ingest", help="Region-write (or append) one month into an initialized repo.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    _add_common_args(p_ing)
    p_ing.add_argument("--year", type=int, required=True, help="Calendar year to fetch.")
    p_ing.add_argument("--month", type=int, required=True, choices=range(1, 13),
                       metavar="{1..12}", help="Calendar month to fetch (1-12).")
    p_ing.set_defaults(func=cmd_ingest)

    args = p.parse_args(argv)
    return args.func(args, p)


if __name__ == "__main__":
    sys.exit(main())