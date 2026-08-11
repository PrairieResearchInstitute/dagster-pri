"""The ``era5_iceberg`` asset: ingest one month of ERA5-Land for a state.

Config-driven (state / year / month / variables), mirroring the ``ingest``
subcommand of ``scripts/era5-illinois.py``. The per-state store must already be
initialized by the ``era5_init`` job (see :mod:`dagster_pri.defs.era5_init`);
this asset opens the store loudly and never creates it.
"""

import tempfile
from pathlib import Path

import dagster as dg

from dagster_pri.defs.resources import CDSClientResource, IcechunkStorageResource
from dagster_pri.era5.axis import DEFAULT_VARIABLES
from dagster_pri.era5.cds import download_month, staged_nc_path
from dagster_pri.era5.geometry import (
    bbox_from_geometry,
    get_state_geometry,
    normalize_stusps,
    repo_prefix,
)
from dagster_pri.era5.store import validate_variables_against_store, write_month
from dagster_pri.era5.transform import open_and_clip


class Era5IngestConfig(dg.Config):
    """Run config for a single (state, month) ingest."""

    state: str = "IL"  # USPS code, e.g. "IL"
    year: int = 2024
    month: int = 1  # 1-12
    variables: list[str] = DEFAULT_VARIABLES
    bbox_pad: float = 0.25
    work_dir: str | None = None
    ndays: int | None = None  # only fetch the first N days (for testing)


@dg.asset(
    description="One month of ERA5-Land for a US state, region-written into its "
    "Icechunk store. Run the era5_init job for the state first.",
    kinds={"icechunk"},
)
def era5_iceberg(
    context: dg.AssetExecutionContext,
    config: Era5IngestConfig,
    icechunk: IcechunkStorageResource,
    cds: CDSClientResource,
) -> dg.MaterializeResult:
    state = normalize_stusps(config.state)
    prefix = repo_prefix(state)

    gdf = get_state_geometry(icechunk.filesystem(), icechunk.bucket, state)
    area = bbox_from_geometry(gdf, pad_deg=config.bbox_pad)
    context.log.info("%s bbox [N, W, S, E] = %s", state, area)

    try:
        repo = icechunk.open_repo(prefix)
    except Exception as e:  # noqa: BLE001 -- translate to an actionable message
        raise dg.Failure(
            description=(
                f"Could not open the Icechunk repo for {state} at prefix "
                f"{prefix!r} ({e}). Run the `era5_init` job for {state} first."
            )
        ) from e

    work = Path(config.work_dir) if config.work_dir else Path(tempfile.mkdtemp(prefix="era5land_"))
    work.mkdir(parents=True, exist_ok=True)
    nc_path = staged_nc_path(work, state, config.year, config.month)
    download_month(
        cds.get_client(),
        config.year,
        config.month,
        config.variables,
        area,
        nc_path,
        ndays=config.ndays,
    )

    context.log.info("clipping %04d-%02d to %s...", config.year, config.month, state)
    clipped = open_and_clip(nc_path, gdf)
    validate_variables_against_store(repo, clipped)

    context.log.info("writing %04d-%02d into the store...", config.year, config.month)
    mode = write_month(repo, clipped, config.year, config.month)
    n_steps = int(clipped.sizes["time"])
    clipped.close()

    return dg.MaterializeResult(
        metadata={
            "state": state,
            "year": config.year,
            "month": config.month,
            "mode": mode,
            "timesteps": n_steps,
            "variables": list(config.variables),
            "prefix": prefix,
        }
    )
