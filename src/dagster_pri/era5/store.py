"""Icechunk repo lifecycle: open/create, init the axis, region/append month writes.

Storage construction lives in the Dagster resource
(:class:`dagster_pri.defs.resources.IcechunkStorageResource`); these helpers take
an already-built ``icechunk.Storage`` (or an open ``Repository``) so they stay
testable against in-memory / local-filesystem storage.
"""

from __future__ import annotations

import logging

from dagster_pri.era5.axis import (
    assert_subset_of_axis,
    axis_initialized,
    build_template,
    strip_nonregion_coords,
)

log = logging.getLogger("era5land")


# --------------------------------------------------------------------------- #
# Repo open / create
# --------------------------------------------------------------------------- #
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


def _open_store_dataset_or_none(repo):
    try:
        return open_store_dataset(repo)
    except Exception:  # noqa: BLE001 -- empty/new repo
        return None


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
    raise RuntimeError(f"commit failed after {attempts} rebase attempts: {message} ({last})")


def init_store(repo, times, ref_clipped, time_chunk: int) -> bool:
    """Lay down the full-axis template (compute=False) as one commit.

    Idempotent: a no-op if the axis already exists. Returns True if it wrote the
    template, False if it was already initialized.

    Refuses to re-template a store that already holds a DIFFERENT variable set:
    the write below is ``mode="w"``, so proceeding would drop the existing arrays
    and every month already ingested into them.
    """
    from zarr.codecs import BloscCodec, BloscShuffle

    var_names = list(ref_clipped.data_vars)
    existing = _open_store_dataset_or_none(repo)
    if axis_initialized(existing, len(times), var_names):
        log.info(
            "Axis already initialized (time >= %d, vars=%s); skipping template.",
            len(times),
            var_names,
        )
        return False

    # An axis already on disk means real data may be there too; only a repo that
    # never got a template is safe to write with mode="w".
    if existing is not None and "time" in existing.dims:
        store_vars = set(existing.data_vars)
        if store_vars != set(var_names):
            raise ValueError(
                f"the store already holds variables {sorted(store_vars)} but this "
                f"init would create {sorted(var_names)}. The variable set is fixed "
                f"when the arrays are created, so adding or removing one is not an "
                f"init -- re-initializing here would destroy the ingested data. "
                f"Delete the store prefix and re-init, then re-ingest its months."
            )

    log.info(
        "Initializing full hourly axis %s .. %s (%d steps, chunk=%d)...",
        times[0],
        times[-1],
        len(times),
        time_chunk,
    )
    session = repo.writable_session("main")
    template = build_template(times, ref_clipped)
    # zstd compression + NaN fill, fixed once at array-creation; region/append
    # writes inherit it and must not (and do not) re-specify encoding.
    compressors = [BloscCodec(cname="zstd", clevel=5, shuffle=BloscShuffle.shuffle)]
    # The template is a single dask chunk per variable (see build_template), so
    # the store's real chunk shape is declared here: `time_chunk` hours by the
    # whole spatial grid.
    encoding = {
        v: {
            "fill_value": float("nan"),
            "compressors": compressors,
            "chunks": (time_chunk, *template[v].shape[1:]),
        }
        for v in var_names
    }
    # icechunk's to_icechunk has no `compute` arg; write the schema directly
    # through the session's Zarr store with compute=False so only metadata +
    # the time coordinate are materialized (no data chunks).
    template.to_zarr(session.store, mode="w", compute=False, consolidated=False, encoding=encoding)
    snap = commit_with_retry(
        session, f"init full hourly axis {times[0]}..{times[-1]} ({len(times)} steps)"
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

    clipped = strip_nonregion_coords(clipped)
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

    The variable set is fixed at array-creation; a mismatch means the configured
    variables drifted from what `init` used (a region write would otherwise error
    opaquely).
    """
    store_vars = set(open_store_dataset(repo).data_vars)
    month_vars = set(clipped.data_vars)
    if month_vars != store_vars:
        raise ValueError(
            f"variable set {sorted(month_vars)} does not match the store's "
            f"{sorted(store_vars)}. The configured variables must match what "
            f"`init` used (adding a variable means creating a new array, not a "
            f"region write)."
        )
