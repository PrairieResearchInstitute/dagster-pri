"""Global hourly time axis, the all-NaN template, and alignment guards.

The Icechunk store is written init-once + per-month region writes against a
fixed, pre-allocated hourly axis (see :mod:`dagster_pri.era5.store`). This
module owns the axis math and the constants (variable set, axis start) that
must stay identical across ``init`` and every ``ingest``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

# ERA5-Land's start. The global hourly axis begins here so that ANY historical
# month already has a pre-allocated slot to region-write into (see init_store).
AXIS_START = "1950-01-01T00:00"

# Sensible default surface variables. Override via config.
DEFAULT_VARIABLES = [
    "2m_temperature",
    "total_precipitation",
    "2m_dewpoint_temperature",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "surface_pressure",
]


def check_time_chunk(time_chunk: int) -> None:
    """Validate that ``time_chunk`` is a positive divisor of 24.

    The time chunk is baked into the store's encoding at init and every later
    write must conform. Months are always a whole number of 24h days, so only a
    chunk that DIVIDES 24 keeps every month region-aligned and every cumulative
    length chunk-aligned for appends.
    """
    if time_chunk < 1 or 24 % time_chunk != 0:
        raise ValueError(
            f"time_chunk must be a positive divisor of 24 "
            f"(1, 2, 3, 4, 6, 8, 12, 24); got {time_chunk}."
        )


def default_axis_end() -> str:
    """End of the most recent COMPLETE year, as the last hour of that year.

    A small buffer to year-end is fine; do NOT pad far into the future -- the
    `time` coordinate is materialized eagerly, so a padded tail bloats the
    coordinate and silently drags empty timesteps into naive `.mean("time")` /
    `.sel(...)` reads. Grow the axis when real data arrives instead (append).
    """
    last_complete_year = datetime.now(timezone.utc).year - 1
    return f"{last_complete_year}-12-31T23:00"


def global_axis(axis_end: str) -> pd.DatetimeIndex:
    """The fixed, pre-allocated hourly axis [AXIS_START, axis_end]."""
    import pandas as pd

    return pd.date_range(AXIS_START, axis_end, freq="1h")


def build_template(times, ref_clipped, time_chunk: int):
    """All-NaN, dask-backed dataset spanning the full axis on the reference grid.

    Built from the reference month's clipped grid + variable set so the template
    is byte-identical to every later month's grid. Written metadata-only
    (compute=False) so only the 1-D time coordinate + array metadata hit disk --
    no data chunks. The reference month's non-time coords (lat/lon and the
    rioxarray `spatial_ref` CRS coord) are preserved.
    """
    import numpy as np
    import xarray as xr

    skeleton = ref_clipped.isel(time=0, drop=True)  # spatial grid only
    nan_like = xr.full_like(skeleton, np.nan)  # same vars/grid/attrs
    template = nan_like.expand_dims(time=times)  # add the full axis
    return template.chunk({"time": time_chunk, "latitude": -1, "longitude": -1})


def axis_initialized(store_dataset, n_expected: int, var_names: list[str]) -> bool:
    """True once the full axis + variable set is laid down.

    Uses `>= n_expected` (not `==`) so a later forward-append that GREW the axis
    still counts as initialized; only a missing/short axis triggers (re)init.

    ``store_dataset`` is the opened store Dataset, or ``None`` for an empty/new
    repo that could not be opened.
    """
    if store_dataset is None:
        return False
    if "time" not in store_dataset.dims or store_dataset.sizes["time"] < n_expected:
        return False
    return all(v in store_dataset.data_vars for v in var_names)


def assert_subset_of_axis(month_time, store_times) -> None:
    """Guard region='auto': the month must be a CONTIGUOUS subset of the axis.

    region='auto' locates the slice by matching the month's `time` values against
    the store axis, so any drift (ERA5T offsets, valid_time quirks, non-on-the-
    hour stamps, tz-tagged inputs) must fail loudly here rather than mis-place
    data.
    """
    import numpy as np

    mt = np.asarray(month_time.values if hasattr(month_time, "values") else month_time)
    store = np.asarray(store_times)
    if not np.isin(mt, store).all():
        raise ValueError(
            "month timestamps are not all present in the store's time axis; "
            "the month grid is not aligned to the pre-allocated hourly axis "
            "(check for ERA5T offsets / non-on-the-hour timestamps)."
        )
    idx = np.searchsorted(store, mt)
    if len(idx) > 1 and not np.array_equal(np.diff(idx), np.ones(len(idx) - 1, dtype=idx.dtype)):
        raise ValueError("month timestamps are not contiguous within the store axis.")


def strip_nonregion_coords(ds):
    """Drop scalar coords (e.g. rioxarray's `spatial_ref`) before a region/append
    write. region='auto' rejects variables with no dimension in common with the
    region dims; the CRS coord is written once into the template at init and does
    not need rewriting per month."""
    drop = [n for n, c in ds.coords.items() if c.ndim == 0]
    return ds.drop_vars(drop) if drop else ds
