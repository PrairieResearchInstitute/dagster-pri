"""The ``era5_init`` job: one-time per-state Icechunk store setup.

Mirrors the ``init`` subcommand of ``scripts/era5-illinois.py``: create the repo,
fix the spatial grid + variable set from a reference month, pre-allocate the
global hourly axis, and region-write the reference month. Run once per state
before ingesting that state's months with the ``era5_iceberg`` asset.

This is modeled as a job (not an asset) because it is a one-time, per-state
schema bootstrap with a different lifecycle than the repeated per-month ingest.
"""

import tempfile
from pathlib import Path

import dagster as dg

from dagster_pri.defs.resources import CDSClientResource, IcechunkStorageResource
from dagster_pri.era5.axis import (
    DEFAULT_VARIABLES,
    check_time_chunk,
    default_axis_end,
    global_axis,
)
from dagster_pri.era5.cds import download_month, staged_nc_path
from dagster_pri.era5.geometry import (
    bbox_from_geometry,
    get_state_geometry,
    normalize_stusps,
    repo_prefix,
)
from dagster_pri.era5.store import init_store, write_month
from dagster_pri.era5.transform import open_and_clip


class Era5InitConfig(dg.Config):
    """Run config for initializing a state's Icechunk store."""

    state: str = "IL"  # USPS code, e.g. "IL"
    ref_year: int = 2024
    ref_month: int = 1  # 1-12; defines the grid + variable set
    axis_end: str | None = None  # last hour of the axis; default: last complete year
    variables: list[str] = DEFAULT_VARIABLES
    time_chunk: int = 24  # hours per chunk; must divide 24
    bbox_pad: float = 0.25
    work_dir: str | None = None
    ndays: int | None = None  # only fetch the first N days (for testing)


@dg.op
def init_state_store(
    context: dg.OpExecutionContext,
    config: Era5InitConfig,
    icechunk: IcechunkStorageResource,
    cds: CDSClientResource,
) -> None:
    check_time_chunk(config.time_chunk)
    state = normalize_stusps(config.state)
    prefix = repo_prefix(state)

    axis_end = config.axis_end or default_axis_end()
    times = global_axis(axis_end)
    context.log.info("Global axis: %s .. %s (%d hourly steps)", times[0], times[-1], len(times))

    gdf = get_state_geometry(icechunk.filesystem(), icechunk.bucket, state)
    area = bbox_from_geometry(gdf, pad_deg=config.bbox_pad)
    context.log.info("%s bbox [N, W, S, E] = %s", state, area)

    work = Path(config.work_dir) if config.work_dir else Path(tempfile.mkdtemp(prefix="era5land_"))
    work.mkdir(parents=True, exist_ok=True)
    nc_path = staged_nc_path(work, state, config.ref_year, config.ref_month)
    download_month(
        cds.get_client(),
        config.ref_year,
        config.ref_month,
        config.variables,
        area,
        nc_path,
        ndays=config.ndays,
    )
    context.log.info(
        "clipping reference month %04d-%02d to %s...",
        config.ref_year,
        config.ref_month,
        state,
    )
    ref_clipped = open_and_clip(nc_path, gdf)

    repo = icechunk.open_or_create_repo(prefix)
    init_store(repo, times, ref_clipped, config.time_chunk)

    context.log.info(
        "region-writing reference month %04d-%02d...", config.ref_year, config.ref_month
    )
    write_month(repo, ref_clipped, config.ref_year, config.ref_month)
    ref_clipped.close()
    context.log.info(
        "Init complete for %s. Now materialize era5_iceberg in any month order.", state
    )


@dg.job(description="One-time per-state Icechunk store init for ERA5-Land ingest.")
def era5_init() -> None:
    init_state_store()
