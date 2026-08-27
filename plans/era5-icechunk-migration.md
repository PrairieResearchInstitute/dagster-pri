# Migrating `era5-illinois.py` to Icechunk

## Goal

Replace the plain-Zarr "probe-then-create-or-append" write path in
`scripts/era5-illinois.py` with an Icechunk repository written via
**init-once + per-month region writes**, so that:

- Months can be ingested **in any order** (start recent, backfill earlier years)
  with a correct, monotonic time axis.
- Every month write is an **atomic, retryable commit** — an interrupted run
  leaves the store untouched instead of corrupted.
- Re-running a month is **idempotent** (overwrites its region in place; no
  duplicate timesteps, no dedup logic).
- The archive gains **versioning** (roll back a bad month, tag releases,
  time-travel reads).

This was validated end-to-end against the real OSN/Ceph endpoint by
`scripts/icechunk_spike.py` (Icechunk 2.0.6): out-of-order region writes read
back time-correct, and Ceph needed only `force_path_style=True` + `endpoint_url`
(no botocore checksum workaround).

## What stays the same

- CDS retrieval (`download_month`) — unchanged.
- Boundary / bbox logic (`get_illinois_geometry`, `bbox_from_geometry`) — unchanged.
- Open / normalize / clip (`open_and_clip`) — unchanged; still produces a
  clipped monthly `xarray.Dataset`.
- The `--time-chunk` guard (must divide 24) — **keep it.** Daily chunks are what
  make every month region-aligned, so concurrent month writes never touch the
  same chunk.

## What changes (function-by-function)

| Current | Fate | Replacement |
|---|---|---|
| `build_storage_options()` (s3fs dict) | replace | `make_icechunk_storage()` → `icechunk.s3_storage(...)` |
| `zarr_store_exists()` (fsspec probe) | replace | `repo_exists()` + `axis_initialized()` |
| `build_encoding()` | adapt | keep chunk logic; add `fill_value=NaN`; Zarr **v3** |
| `write_zarr()` (mode=w / append) | replace | `init_store()` + `write_month()` (region or append) |
| `consolidate_metadata` block in `main()` | delete | Icechunk manages its own metadata |
| `first_write = not zarr_store_exists(...)` | replace | explicit `init` subcommand (see below) |
| botocore `AWS_*_CHECKSUM_*` env vars | keep only for cleanup/s3fs paths | Icechunk's Rust client ignores them |

## Key design decisions

### 1. The axis grows in two directions: region-write to backfill, append to extend
Region writes need a slot to already exist, so we pre-allocate a **fixed global
hourly axis** for the historical range we intend to backfill:

- `AXIS_START = "1950-01-01T00:00"` (ERA5-Land's start).
- `AXIS_END` = configurable, default **end of the most recent complete year**
  (a small buffer to year-end is fine; do **not** pad far into the future).
- `freq = "1h"`, built with `pd.date_range`.

Pre-allocation is cheap on disk (with `compute=False` only metadata + the 1-D
time coordinate are written — no data chunks), **but the `time` coordinate is
materialized eagerly.** Padding to the far future is therefore a poor trade: it
bloats the coordinate array and, worse, every consumer sees a `time` axis
running to the pad date that is almost entirely NaN — naive `.mean("time")` /
`.sel(time=slice(...))` / "iterate all timesteps" silently drag in the empty
tail. Avoid it.

Instead, **grow the axis when real data arrives**, which is cheap and supported:

- **Backfill within `[START, END]` → `region="auto"`** (order-independent). This
  is the whole reason `START` is 1950: any historical month already has a slot.
- **Extend beyond the current end → `append_dim="time"`.** When a month past the
  current axis end arrives, `to_icechunk(month_ds, session, append_dim="time")`
  grows the array at the tail. It's a single append, not a rewrite. Appends only
  add to the end and must be chronological at the tail (which forward-in-time
  data naturally is).

**The region-vs-append rule** (decided per ingested month):

| Month falls … | Mechanism |
|---|---|
| within `[START, current_end]` | `region="auto"` (any order) |
| exactly at `current_end + 1h`, contiguous | `append_dim="time"` |
| in a future year not yet allocated | extend to that year-end first (append a NaN span, or re-run a small `extend` step), then region-write |

Chunk alignment holds for appends too: `END` is always a day/year boundary, so
the existing length is a whole multiple of 24 and an append starts exactly on a
daily-chunk boundary (the `--time-chunk 24` decision keeps paying off).

### 2. Init must establish the exact spatial grid AND the full variable set
The lat/lon grid is whatever `open_and_clip` produces for Illinois, and the set
of data variables is fixed at array-creation time. Therefore **init downloads
one reference month, clips it**, and uses that clipped dataset's grid + var list
as the schema template (then region-writes that same month as its first data).
This guarantees the template grid is byte-identical to every later month's grid.

Consequence: `--variables` must be identical for init and all subsequent month
ingests. Adding a variable later means creating a new array, not a region write.

`init_store` now refuses this case outright rather than silently re-running
`to_zarr(mode="w")` over a populated store, which would have dropped every
ingested month. Changing the variable set means deleting the store prefix,
re-initing, and re-ingesting.

### 2b. Accumulated variables get a derived per-hour array
ERA5-Land's `total_precipitation` (and its siblings: snowfall, evaporation,
radiation, runoff) are **running accumulations since 00:00 UTC that reset daily**,
not per-hour amounts. Every consumer would otherwise have to redo the reset math.

Which variables accumulate is documented by ERA5-Land, not inferable from the
data, so `dagster_pri.era5.accumulation.ACCUMULATED_SHORT_NAMES` carries that list
(and doubles as the CDS-long-name → CF-short-name map). Every accumulated variable
in the default download therefore gets a second array, `<short>_hourly` — `tp`
keeps the raw accumulation and `tp_hourly` holds the per-hour increment. The kernel lives in
`dagster_pri.era5.accumulation` and is shared with the daily station aggregation:

    hourly(t) = raw(t) - raw(t-1)   for t = 02:00 .. 23:00 and 00:00
    hourly(t) = raw(t)              for t = 01:00  (first step after the reset)
    hourly(t) = NaN                 when t-1 is outside the ingested block

Since a CDS month spans 00:00 day 1 .. 23:00 last day, only the month's very first
step is NaN; it is filled by no one (the predecessor lives in the previous month's
file). Negative increments are clamped to 0 — accumulations are monotonic within a
UTC day, so any negative is differencing noise.

`accumulated_variables` defaults to exactly that set and can be narrowed (`[]`
disables de-accumulation entirely) to trade derived arrays for store size. Because
the derived arrays are part of the variable set, it is fixed at init for the life
of the store — so it is config on `init` only, and the ingest reads it back off
the store's `_hourly` arrays instead of being configured to match. `variables` is
fixed the same way, but cannot be recovered from the store's (CF short) array
names, so init records the CDS request list in the root attribute
`era5_cds_variables` for the ingest to reuse.

### 3. Explicit `init` subcommand instead of auto-probe
The current script auto-decides create-vs-append by probing. Under parallel
orchestration that races. Split into subcommands:

- `init` — create repo (if absent), download+clip a reference month, write the
  full-axis template (`compute=False`), region-write the reference month, commit.
  Idempotent: if the axis already exists, it's a no-op (optionally still writes
  the reference month if missing).
- `ingest --year --month` — open repo, region-write that month, commit. Fails
  loudly if the store isn't initialized yet.

This makes the orchestrator contract explicit: run `init` once, then fan out
`ingest` over months in any order.

### 4. region="auto" requires exact coordinate alignment
`to_icechunk(ds, session, region="auto")` locates the slice by matching the
month's `time` coordinate values against the store's axis. The clipped month's
time coordinate must therefore match the pre-allocated axis **exactly** (same
hourly grid, same `datetime64[ns]`, on-the-hour). Add a normalization/assert in
the ingest path: the month's timestamps must be a contiguous subset of the axis,
else raise a clear error (guards against ERA5T offsets, `valid_time` quirks,
DST-free but timezone-tagged inputs, etc.).

### 5. Concurrent commits need a rebase-retry loop
If the orchestrator writes months in parallel, two commits race on the branch
tip. Because daily chunks + month-aligned regions mean **disjoint chunks**,
Icechunk's optimistic concurrency rebases cleanly. Wrap `session.commit` in a
retry that calls `session.rebase(icechunk.ConflictDetector())` on conflict and
re-commits, with a bounded number of attempts.

## New / changed code (sketch)

```python
import icechunk
from icechunk.xarray import to_icechunk

AXIS_START = "1950-01-01T00:00"

def make_icechunk_storage(prefix: str) -> "icechunk.Storage":
    endpoint = os.environ["S3_ENDPOINT_URL"]
    return icechunk.s3_storage(
        bucket=os.environ["BUCKET_NAME"],
        prefix=prefix,
        endpoint_url=endpoint,
        region=os.environ.get("S3_REGION", "us-east-1"),
        access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        force_path_style=True,                      # Ceph RGW path addressing
        allow_http=endpoint.lower().startswith("http://"),
        # checksum_algorithm=...  # only if OSN later rejects default checksums
    )

def global_axis(axis_end: str) -> "pd.DatetimeIndex":
    return pd.date_range(AXIS_START, axis_end, freq="1h")

def open_or_create_repo(storage):
    try:
        return icechunk.Repository.open(storage)
    except Exception:
        return icechunk.Repository.create(storage)

def axis_initialized(repo, n_expected: int) -> bool:
    try:
        ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
        return "time" in ds.dims and ds.sizes["time"] == n_expected
    except Exception:
        return False

def build_template(times, ref_clipped, variables, time_chunk):
    # all-NaN dask arrays on the reference grid, chunked (time_chunk, nlat, nlon)
    ...

def commit_with_retry(session, message, attempts=5):
    for _ in range(attempts):
        try:
            return session.commit(message)
        except icechunk.ConflictError:
            session.rebase(icechunk.ConflictDetector())
    raise RuntimeError(f"commit failed after {attempts} rebase attempts: {message}")

def init_store(repo, times, ref_clipped, variables, time_chunk):
    if axis_initialized(repo, len(times)):
        return
    session = repo.writable_session("main")
    build_template(times, ref_clipped, variables, time_chunk).to_zarr(
        session.store, mode="w", compute=False, consolidated=False,
        encoding={v: {"fill_value": float("nan")} for v in variables},
    )
    commit_with_retry(session, "init full hourly axis")

def write_month(repo, clipped, year, month):
    # decision #1: region-write if the month's slot already exists, else append.
    session = repo.writable_session("main")
    store_times = xr.open_zarr(session.store, consolidated=False).time.values
    if clipped.time.values[0] <= store_times[-1]:
        assert_subset_of_axis(clipped.time, store_times)   # decision #4
        to_icechunk(clipped, session, region="auto")
    elif clipped.time.values[0] == store_times[-1] + np.timedelta64(1, "h"):
        to_icechunk(clipped, session, append_dim="time")
    else:
        raise ValueError(
            f"{year}-{month:02d} starts beyond the allocated axis end and is not "
            f"contiguous with it; extend the axis to this year first."
        )
    commit_with_retry(session, f"ingest {year}-{month:02d}")
```

## CLI changes

```
era5-illinois.py init   [--variables ...] [--axis-end 2024-12-31T23:00]
                        [--time-chunk 24] [--bbox-pad 0.25]
                        [--ref-year 2024 --ref-month 1]   # reference month for grid
era5-illinois.py ingest --year Y --month M [--variables ...]
```

Keep `--year/--month` only on `ingest`. Keep the `--time-chunk` divisor-of-24
guard on both. `--variables` must match what `init` used.

## Step-by-step execution

1. **Deps**: add `icechunk>=2.0` (and `pandas` if not already transitive) to
   `pyproject.toml`; `uv lock`.
2. **Refactor storage layer**: add `make_icechunk_storage`, `open_or_create_repo`,
   delete `build_storage_options`/`zarr_store_exists` (keep an s3fs helper only if
   a `cleanup`/delete path is wanted).
3. **Add axis + template helpers**: `global_axis`, `build_template`,
   `axis_initialized`, `assert_subset_of_axis`.
4. **Replace `write_zarr`** with `init_store` + `write_month` (region-or-append
   branch) + `commit_with_retry`.
5. **Restructure `main()`** into `init` / `ingest` subcommands; delete the
   `consolidate_metadata` block.
6. **Update the module docstring** (it currently advertises Zarr v2 as the ARCO
   interchange format — Icechunk is Zarr v3 and requires the `icechunk` reader;
   document the read path: `xr.open_zarr(repo.readonly_session("main").store,
   consolidated=False)`).

## Testing / acceptance

- Reuse the proven flow from `scripts/icechunk_spike.py` as the integration
  smoke test (synthetic data, out-of-order writes, NaN coverage check).
- Real-data check on a scratch prefix:
  1. `init --ref-year 2024 --ref-month 1 --axis-end 2024-12-31T23:00`
  2. `ingest --year 2023 --month 1` then `ingest --year 2020 --month 6`
     (out of order).
  3. Open the store; assert monotonic time, both months populated with sane
     ERA5 values, an un-ingested month all-NaN, and the IL clip mask intact
     (corners NaN, interior finite).
- Idempotency: re-run an `ingest` month; assert the time axis length is
  unchanged and values are identical.
- Forward extension: with `--axis-end 2024-12-31T23:00`, ingest a month past the
  end (e.g. `--year 2025 --month 1`); assert it took the `append_dim` branch, the
  axis grew by exactly that month's length, time stayed monotonic, and a prior
  region-written month is unaffected.
- (If parallel ingest is planned) fan out 3–4 months concurrently; assert all
  commits land and `repo.ancestry` shows them all.

## Risks & mitigations

- **Interchange break**: store is no longer plain Zarr-v2 readable by arbitrary
  tools — consumers need `icechunk`. *Mitigation*: document the read path;
  confirm downstream consumers (Dagster assets, notebooks) can take the dep.
- **Coordinate misalignment** breaks `region="auto"`. *Mitigation*: the
  `assert_subset_of_axis` guard with a clear error message.
- **Axis end too short** → can't ingest newer months later. *Mitigation*: set a
  generous `--axis-end`; document the resize procedure as a follow-up.
- **Variable-set drift** between init and ingest → region write error.
  *Mitigation*: validate `--variables` against the store schema in `ingest`.
- **OSN checksum rejection** under heavier writes (not seen in the spike).
  *Mitigation*: `s3_storage(checksum_algorithm=...)` is the escape hatch.

## Out of scope (follow-ups)

- A dedicated `extend` step for crossing into a not-yet-allocated future year
  (appending a NaN span to year-end before region-writing). The contiguous
  forward-append case is in scope (decision #1); only the "jump to a far future
  year" gap-fill is deferred.
- Adding new variables to an existing store.
- A `cleanup`/delete subcommand.
- Wiring `init`/`ingest` into a Dagster asset graph (partitioned by month).
```
