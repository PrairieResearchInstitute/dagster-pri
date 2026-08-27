"""The ``era5_iceberg`` asset: ingest one month of ERA5-Land for a state.

Config-driven (state / year / month), mirroring the ``ingest`` subcommand of
``scripts/era5-illinois.py``. The per-state store must already be initialized by
the ``era5_init`` job (see :mod:`dagster_pri.defs.era5_init`); this asset opens the
store loudly and never creates it.

The store's variable set is fixed when its arrays are created, so *the store*
decides what this month must contain -- neither list is run config. The CDS request
list is read back from the attribute ``era5_init`` recorded it in, and the variables
to de-accumulate are read off the store's ``_hourly`` arrays (see
:func:`dagster_pri.era5.store.read_store_variables`). Those accumulated variables
are running totals that reset at 00:00 UTC; the derived array holds the per-hour
increment (see :mod:`dagster_pri.era5.accumulation`).

The month is retrieved in batches of ``variables_per_request`` variables and the
batches are merged after clipping, because CDS rejects a whole-month request for
the full variable set on cost (see :mod:`dagster_pri.era5.cds`).
"""

import tempfile
from pathlib import Path

import dagster as dg

from dagster_pri.defs.resources import CDSClientResource, IcechunkStorageResource
from dagster_pri.era5.accumulation import add_hourly_increments
from dagster_pri.era5.cds import DEFAULT_VARIABLES_PER_REQUEST, download_month_batched
from dagster_pri.era5.geometry import (
    bbox_from_geometry,
    get_state_geometry,
    normalize_stusps,
    repo_prefix,
)
from dagster_pri.era5.store import (
    read_store_variables,
    validate_variables_against_store,
    write_month,
)
from dagster_pri.era5.transform import open_and_clip_batches


class Era5IngestConfig(dg.Config):
    """Run config for a single (state, month) ingest."""

    state: str = "IL"  # USPS code, e.g. "IL"
    year: int = 2024
    month: int = 1  # 1-12
    # CDS variables to request. Leave unset: the store records the list `era5_init`
    # created its arrays for, and any other list is rejected downstream anyway.
    # Only needed as an override for a store initialized before that list was
    # recorded (see dagster_pri.era5.store.CDS_VARIABLES_ATTR).
    variables: list[str] | None = None
    # Variables per CDS request; the month is fetched as ceil(len(variables) /
    # this) downloads and merged after clipping. Lower it if CDS answers 403
    # "cost limits exceeded" (see dagster_pri.era5.cds).
    variables_per_request: int = DEFAULT_VARIABLES_PER_REQUEST
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

    store_vars = read_store_variables(repo)
    variables = config.variables or store_vars.cds
    if not variables:
        raise dg.Failure(
            description=(
                f"the store for {state} at prefix {prefix!r} does not record the CDS "
                f"variable list it was initialized with (it predates that metadata). "
                f"Set `variables` in the run config to the list `era5_init` used for "
                f"{state}."
            )
        )
    context.log.info(
        "store variables: %d requested from CDS, %d de-accumulated (%s)",
        len(variables),
        len(store_vars.accumulated),
        ", ".join(store_vars.accumulated) or "none",
    )

    work = Path(config.work_dir) if config.work_dir else Path(tempfile.mkdtemp(prefix="era5land_"))
    work.mkdir(parents=True, exist_ok=True)
    nc_paths = download_month_batched(
        cds.get_client(),
        config.year,
        config.month,
        variables,
        area,
        work,
        state,
        ndays=config.ndays,
        variables_per_request=config.variables_per_request,
    )

    context.log.info(
        "clipping %04d-%02d (%d file(s)) to %s...",
        config.year,
        config.month,
        len(nc_paths),
        state,
    )
    clipped = open_and_clip_batches(nc_paths, gdf)
    clipped = add_hourly_increments(clipped, store_vars.accumulated)
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
            "variables": list(variables),
            "accumulated_variables": store_vars.accumulated,
            "cds_requests": len(nc_paths),
            "prefix": prefix,
        }
    )
