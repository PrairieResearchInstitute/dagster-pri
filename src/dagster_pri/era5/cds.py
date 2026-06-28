"""Copernicus CDS retrieval of one month of hourly ERA5-Land NetCDF."""

from __future__ import annotations

import calendar
import logging
from pathlib import Path

log = logging.getLogger("era5land")

DATASET = "reanalysis-era5-land"


def staged_nc_path(work_dir: Path, stusps: str, year: int, month: int) -> Path:
    """Per-(state, month) staging path so parallel states don't collide."""
    return work_dir / f"era5land_{stusps.lower()}_{year}{month:02d}.nc"


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
        "download_format": "unarchived",  # plain .nc, not zipped
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
