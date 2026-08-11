"""The ``hourly_station_readings`` asset: per-station hourly ERA5-Land readings.

Reads the per-state ERA5-Land Icechunk store (written by ``era5_iceberg``) for one
month, looks up each station's nearest grid cell, and writes the raw **hourly**
values to a single parquet file at
``era5-land/hourly/STATE=<state>/YEAR=<year>/MONTH=<month>/data.parquet``.

Unlike ``daily_station_readings``, nothing is aggregated or converted: readings stay
in ERA5's native UTC timezone (one row per station per UTC hour) and in native units
(``t2m``/``d2m`` in Kelvin, ``tp`` the raw accumulation-since-00Z in metres).

No CDS download: this is a read-only pull over already-ingested data. The store must
already hold the requested month (run ``era5_init`` + ``era5_iceberg`` first).
"""

import dagster as dg

from dagster_pri.defs.resources import IcechunkStorageResource
from dagster_pri.era5.geometry import normalize_stusps, repo_prefix
from dagster_pri.era5.stations import (
    extract_points,
    hourly_to_dataframe,
    load_stations,
    utc_month_slice,
)


class HourlyStationReadingsConfig(dg.Config):
    """Run config for a single (state, month) hourly-readings build."""

    state: str = "IL"  # USPS code, e.g. "IL"
    year: int = 2024
    month: int = 1  # 1-12
    stations_key: str = "pri_data/stations.csv"  # object-store key, under the bucket
    workers: int = 32  # dask threads for parallel, I/O-bound S3 reads


@dg.asset(
    deps=["era5_iceberg"],  # reads what the ingest wrote; orders ingest-then-readings in a job
    description="Per-station hourly ERA5-Land readings (native UTC, native units) for "
    "one state/month, written as a parquet file to the object store.",
    kinds={"parquet"},
)
def hourly_station_readings(
    context: dg.AssetExecutionContext,
    config: HourlyStationReadingsConfig,
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
    stations = load_stations(fs, icechunk.bucket, config.stations_key)
    context.log.info("Loaded %d stations from %s", len(stations), config.stations_key)

    session = repo.readonly_session("main")
    ds = xr.open_zarr(session.store, consolidated=False, decode_timedelta=True)
    ds = utc_month_slice(ds, config.year, config.month)
    if ds.sizes.get("time", 0) == 0:
        raise dg.Failure(
            description=(
                f"No timesteps for {config.year}-{config.month:02d} in the {state} "
                f"store (prefix {prefix!r}). Ingest that month first."
            )
        )

    pts = extract_points(ds, stations)

    context.log.info("Reading with dask (%d threads)...", config.workers)
    pts = pts.compute(scheduler="threads", num_workers=config.workers)

    df = hourly_to_dataframe(pts, stations)

    out_key = (
        f"{icechunk.bucket}/era5-land/hourly/"
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
            "tz": "UTC",
        }
    )
