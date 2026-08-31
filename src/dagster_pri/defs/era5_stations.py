"""The ``daily_station_readings`` asset: per-station daily ERA5-Land summaries.

Reads the per-state ERA5-Land Icechunk store (written by ``era5_iceberg``) for one
month, looks up each station's nearest grid cell, aggregates to **local
Central-time days**, and writes a single parquet file to the **private** bucket at
``era5-land/parquet/STATE=<state>/YEAR=<year>/MONTH=<month>/data.parquet``.

Both ends of this asset are private: the stations CSV it reads and the parquet it
writes live in ``PRIVATE_BUCKET_NAME``, not the public bucket that holds the
Icechunk stores and the clip masks. ``era5_monthly_sensor`` reads the same private
parquet to decide the next month.

No CDS download: this is a read-only summary over already-ingested data. The store
must already hold the requested month (run ``era5_init`` + ``era5_iceberg`` first).

Two ways to run it. ``era5_monthly_job`` (see
:mod:`dagster_pri.defs.era5_automation`) does ingest then summaries for a new
month; ``daily_station_readings_job``, defined at the bottom of this module, runs
just this asset against a month that is already in the store -- for recomputing
daily station data without re-downloading the month. That job takes its
parameters as run config, and every field of ``DailyStationReadingsConfig`` is
defaulted, so a run with no config silently builds ``IL`` 2024-01: set the month
you actually want.

.. code-block:: yaml

    ops:
      daily_station_readings:
        config:
          state: IL
          year: 2024
          month: 3
"""

import dagster as dg

from dagster_pri.defs.resources import IcechunkStorageResource
from dagster_pri.era5.geometry import normalize_stusps, repo_prefix
from dagster_pri.era5.stations import (
    DAILY_SOURCE_VARS,
    daily_summary_local,
    extract_points,
    load_stations,
    padded_utc_slice,
    to_dataframe,
)


class DailyStationReadingsConfig(dg.Config):
    """Run config for a single (state, month) station-summary build."""

    state: str = "IL"  # USPS code, e.g. "IL"
    year: int = 2024
    month: int = 1  # 1-12
    stations_key: str = "pri_data/stations.csv"  # key under the private bucket
    tz: str = "America/Chicago"  # local day definition
    workers: int = 32  # dask threads for parallel, I/O-bound S3 reads


@dg.asset(
    deps=["era5_iceberg"],  # reads what the ingest wrote; orders ingest-then-stations in a job
    description="Per-station daily ERA5-Land summaries (local Central-time day) for "
    "one state/month, written as a parquet file to the private bucket.",
    kinds={"parquet"},
)
def daily_station_readings(
    context: dg.AssetExecutionContext,
    config: DailyStationReadingsConfig,
    icechunk: IcechunkStorageResource,
) -> dg.MaterializeResult:
    import xarray as xr

    state = normalize_stusps(config.state)
    prefix = repo_prefix(state)

    try:
        repo = icechunk.open_repo(prefix)
    except Exception as e:  # noqa: BLE001 -- translate to an actionable message
        raise dg.Failure(
            description=(
                f"Could not open the Icechunk repo for {state} at prefix {prefix!r} "
                f"({e}). Run the `era5_init` + `era5_iceberg` pipeline for {state} first."
            )
        ) from e

    fs = icechunk.filesystem()
    stations = load_stations(fs, icechunk.private_bucket, config.stations_key)
    context.log.info("Loaded %d stations from %s", len(stations), config.stations_key)

    session = repo.readonly_session("main")
    ds = xr.open_zarr(session.store, consolidated=False, decode_timedelta=True)
    ds = padded_utc_slice(ds, config.year, config.month)
    if ds.sizes.get("time", 0) == 0:
        raise dg.Failure(
            description=(
                f"No timesteps for {config.year}-{config.month:02d} in the {state} "
                f"store (prefix {prefix!r}). Ingest that month first."
            )
        )

    pts = extract_points(ds, stations, DAILY_SOURCE_VARS)
    daily = daily_summary_local(pts, config.year, config.month, tz=config.tz)

    context.log.info("Reading + aggregating with dask (%d threads)...", config.workers)
    daily = daily.compute(scheduler="threads", num_workers=config.workers)

    df = to_dataframe(daily, stations)

    out_key = (
        f"{icechunk.private_bucket}/era5-land/parquet/"
        f"STATE={state}/YEAR={config.year}/MONTH={config.month:02d}/data.parquet"
    )
    with fs.open(out_key, "wb") as f:
        df.to_parquet(f, index=False)
    context.log.info("Wrote %d rows to %s", len(df), out_key)

    return dg.MaterializeResult(
        metadata={
            "state": state,
            "year": config.year,
            "month": config.month,
            "n_stations": len(stations),
            "n_rows": len(df),
            "output_path": out_key,
            "tz": config.tz,
        }
    )


daily_station_readings_job = dg.define_asset_job(
    "daily_station_readings_job",
    selection=["daily_station_readings"],
    description="Rebuild the daily station parquet for one state/month from the "
    "already-ingested Icechunk store. Ingest is not re-run; the month must already "
    "be present (use era5_monthly_job to ingest a new month).",
)
