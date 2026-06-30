"""Monthly ERA5-Land backfill automation.

A single job (``era5_monthly_job``) materializes ``era5_iceberg`` followed by
``daily_station_readings`` for one (state, year, month). A sensor
(``era5_monthly_sensor``) walks a state forward one month at a time: each tick it
queries the **landed parquet output** in the object store to find the latest month
already produced, then requests a run for the next month -- bounded by a configured
start month and an optional end month.

Detecting "the last successful month" reads the parquet output, not Icechunk:
``daily_station_readings`` runs strictly after (and depends on) ``era5_iceberg``
and writes its parquet only on success, so the presence of a month's parquet
partition implies the whole chain succeeded for that month. The parquet dataset is
inspected with the parquet / s3fs / pyarrow stack; Icechunk holds only the raw
ERA5-Land data.

Configuration (environment variables, loaded from ``.env`` by ``dg dev`` / ``dg
launch``):

* ``ERA5_START_YM`` (required) -- first month to process, ``"YYYY-MM"`` (e.g. ``2024-01``).
* ``ERA5_END_YM``   (optional) -- last month, inclusive, ``"YYYY-MM"``.
* ``ERA5_STATE``    (optional) -- USPS state code; defaults to ``IL``.

Sequencing: the ``run_key`` is ``"{state}-{year}-{month:02d}"``, so each month is
launched at most once. The next month's parquet does not exist until its run
finishes, so the sensor keeps proposing the same (deduplicated) ``run_key`` until
the run completes, then advances -- one month at a time, in order. If a month's run
*fails*, its parquet is never written and its ``run_key`` is already used, so the
sensor will not auto-relaunch it; progress halts on that month until an operator
re-runs it (by design -- don't hammer a broken month).
"""

from __future__ import annotations

import os
import re

import dagster as dg

from dagster_pri.defs.resources import IcechunkStorageResource
from dagster_pri.era5.geometry import normalize_stusps

# (year, month) pairs sort/compare correctly as plain tuples.
YearMonth = tuple[int, int]

_YM_RE = re.compile(r"^(\d{4})-(\d{2})$")
_PART_RE = re.compile(r"YEAR=(\d{4})/MONTH=(\d{2})")


era5_monthly_job = dg.define_asset_job(
    "era5_monthly_job",
    selection=["era5_iceberg", "daily_station_readings"],
    description="Ingest one ERA5-Land month for a state, then build its station parquet.",
)


# --------------------------------------------------------------------------- #
# Pure helpers (no I/O)
# --------------------------------------------------------------------------- #
def parse_ym(s: str) -> YearMonth:
    """Parse a ``"YYYY-MM"`` string into a ``(year, month)`` tuple."""
    m = _YM_RE.match(s.strip())
    if not m:
        raise ValueError(f"Expected a 'YYYY-MM' month (e.g. '2024-01'); got {s!r}.")
    year, month = int(m.group(1)), int(m.group(2))
    if not 1 <= month <= 12:
        raise ValueError(f"Month must be 1-12; got {month} from {s!r}.")
    return year, month


def next_month(year: int, month: int) -> YearMonth:
    """The month after ``(year, month)``, rolling December over to January."""
    return (year, month + 1) if month < 12 else (year + 1, 1)


def decide_target_month(
    existing: set[YearMonth], start: YearMonth, end: YearMonth | None
) -> YearMonth | None:
    """The next month to process, or ``None`` if caught up to ``end``.

    Only landed months within ``[start, end]`` count: if none exist yet the target
    is ``start``; otherwise it is the month after the latest landed one. A target
    past ``end`` (when set) means there is nothing left to do.
    """
    within = [ym for ym in existing if ym >= start and (end is None or ym <= end)]
    target = start if not within else next_month(*max(within))
    if end is not None and target > end:
        return None
    return target


# --------------------------------------------------------------------------- #
# I/O helper (parquet / s3fs / pyarrow)
# --------------------------------------------------------------------------- #
def landed_months(fs, bucket: str, state: str) -> set[YearMonth]:
    """The ``(year, month)`` parquet partitions already landed for ``state``.

    Queries the hive-partitioned parquet dataset under
    ``{bucket}/era5-land/parquet/STATE={state}`` with pyarrow over the given s3fs
    filesystem. pyarrow only discovers partitions that actually contain parquet
    files, so partial/empty directories are excluded. Returns an empty set if the
    state's prefix does not exist yet.
    """
    import pyarrow.dataset as pads
    from pyarrow.fs import FSSpecHandler, PyFileSystem

    root = f"{bucket}/era5-land/parquet/STATE={normalize_stusps(state)}"
    pa_fs = PyFileSystem(FSSpecHandler(fs))
    try:
        dataset = pads.dataset(root, filesystem=pa_fs, format="parquet", partitioning="hive")
        files = dataset.files
    except (FileNotFoundError, OSError):
        # No STATE= prefix yet (nothing landed for this state).
        return set()

    months: set[YearMonth] = set()
    for path in files:
        m = _PART_RE.search(path)
        if m:
            months.add((int(m.group(1)), int(m.group(2))))
    return months


# --------------------------------------------------------------------------- #
# Sensor
# --------------------------------------------------------------------------- #
@dg.sensor(
    job=era5_monthly_job,
    minimum_interval_seconds=60 * 10,
    default_status=dg.DefaultSensorStatus.STOPPED,
    description="Advance one ERA5-Land month at a time from the latest landed parquet output.",
)
def era5_monthly_sensor(
    context: dg.SensorEvaluationContext,
    icechunk: IcechunkStorageResource,
):
    start_env = os.environ.get("ERA5_START_YM")
    if not start_env:
        return dg.SkipReason("ERA5_START_YM is not set; nothing to schedule.")
    start = parse_ym(start_env)

    end_env = os.environ.get("ERA5_END_YM")
    end = parse_ym(end_env) if end_env else None

    state = normalize_stusps(os.environ.get("ERA5_STATE", "IL"))

    fs = icechunk.filesystem()  # generic s3fs transport (not Icechunk access)
    existing = landed_months(fs, icechunk.bucket, state)
    context.log.info("%s: %d landed parquet month(s) found.", state, len(existing))

    target = decide_target_month(existing, start, end)
    if target is None:
        return dg.SkipReason(f"{state} caught up to end {end}; nothing to do.")

    year, month = target
    run_config = {
        "ops": {
            "era5_iceberg": {"config": {"state": state, "year": year, "month": month}},
            "daily_station_readings": {"config": {"state": state, "year": year, "month": month}},
        }
    }
    return dg.RunRequest(
        run_key=f"{state}-{year}-{month:02d}",
        run_config=run_config,
        tags={"era5/state": state, "era5/year": str(year), "era5/month": f"{month:02d}"},
    )
