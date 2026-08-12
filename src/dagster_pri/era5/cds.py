"""Copernicus CDS retrieval of one month of hourly ERA5-Land NetCDF.

CDS prices a retrieval by its number of FIELDS -- the cross product of the
list-valued request keys, i.e. ``variables x days x hours``. The `area` bbox is a
subsetting key and does NOT shrink that count, so a whole month of the default
variable set (40 vars x 31 days x 24 h = 29,760 fields) trips the per-request
cost limit with a 403 "Your request is too large". The fix is to split the
variable list across several requests and merge the pieces after clipping (see
:func:`download_month_batched` and
:func:`dagster_pri.era5.transform.open_and_clip_batches`).
"""

from __future__ import annotations

import calendar
import logging
from pathlib import Path

log = logging.getLogger("era5land")

DATASET = "reanalysis-era5-land"

# Variables per CDS request. At a full 31-day month this is 10 x 31 x 24 = 7,440
# fields per request, comfortably under the reanalysis-era5-land cost limit.
DEFAULT_VARIABLES_PER_REQUEST = 8


def staged_nc_path(
    work_dir: Path, stusps: str, year: int, month: int, batch: int | None = None
) -> Path:
    """Per-(state, month, variable batch) staging path so nothing collides.

    Parallel states get distinct files via ``stusps``; the variable batches of a
    single month get distinct files via ``batch``.
    """
    suffix = "" if batch is None else f"_b{batch:02d}"
    return work_dir / f"era5land_{stusps.lower()}_{year}{month:02d}{suffix}.nc"


def batch_variables(variables: list[str], batch_size: int) -> list[list[str]]:
    """Split ``variables`` into consecutive chunks of at most ``batch_size``."""
    if batch_size < 1:
        raise ValueError(f"variables_per_request must be >= 1; got {batch_size}.")
    return [variables[i : i + batch_size] for i in range(0, len(variables), batch_size)]


def download_month(
    client,
    year: int,
    month: int,
    variables: list[str],
    area: list[float],
    out_path: Path,
    ndays: int | None = None,
) -> Path:
    """Retrieve one month of hourly ERA5-Land as NetCDF over the bbox.

    ``client`` is a ``cdsapi.Client`` (or a test fake exposing ``retrieve``).
    Short-circuits if a non-empty file is already staged at ``out_path``.
    """
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
        # Asks for a plain .nc, but CDS still zips SEVERAL .nc files together when
        # a request's variables span parts of the archive; the reader merges the
        # members (see dagster_pri.era5.transform.open_and_clip).
        "download_format": "unarchived",
        "area": area,  # [N, W, S, E]
    }
    log.info(
        "  requesting %04d-%02d (%d days x 24h, %d vars)...",
        year,
        month,
        ndays,
        len(variables),
    )
    client.retrieve(DATASET, request, str(out_path))
    return out_path


def download_month_batched(
    client,
    year: int,
    month: int,
    variables: list[str],
    area: list[float],
    work_dir: Path,
    stusps: str,
    ndays: int | None = None,
    variables_per_request: int = DEFAULT_VARIABLES_PER_REQUEST,
) -> list[Path]:
    """Retrieve one month as several NetCDFs, ``variables_per_request`` vars each.

    Returns the staged paths in variable order. Each batch is staged (and cached)
    independently, so a run that dies partway only re-requests the batches it
    never got.
    """
    batches = batch_variables(variables, variables_per_request)
    log.info(
        "  %04d-%02d: %d vars in %d request(s) of <=%d",
        year,
        month,
        len(variables),
        len(batches),
        variables_per_request,
    )
    return [
        download_month(
            client,
            year,
            month,
            batch,
            area,
            staged_nc_path(work_dir, stusps, year, month, batch=i),
            ndays=ndays,
        )
        for i, batch in enumerate(batches)
    ]
