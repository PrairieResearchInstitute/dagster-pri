"""De-accumulate ERA5-Land running totals into per-hour increments.

ERA5-Land mixes two kinds of variable. Most (``t2m``, ``d2m``, ``u10``, ...) are
instantaneous values at the timestamp. Others -- ``tp`` and friends -- are a
**running accumulation since 00:00 UTC that resets at 00:00 each day**: the value
at 14:00 is everything that fell since midnight, and the value at 00:00 is the
whole previous day's total. Researchers almost always want the per-hour amount,
and getting the reset right is easy to botch, so the ingest derives it once and
stores it alongside the raw accumulation::

    hourly(t) = raw(t) - raw(t-1)   for t = 02:00 .. 23:00 and 00:00
    hourly(t) = raw(t)              for t = 01:00  (first step after the reset)
    hourly(t) = NaN                 when t-1 is missing and t is not 01:00

A month downloaded from CDS spans 00:00 on day 1 through 23:00 on the last day
(see :func:`dagster_pri.era5.cds.download_month`), so only the block's very first
step lacks a predecessor; every later step -- including each subsequent day's
00:00 -- de-accumulates from within the block.

Which variables accumulate is a documented property of ERA5-Land, not something
the data reveals, so it lives here as :data:`ACCUMULATED_SHORT_NAMES`. By default
every accumulated variable in the default download gets a derived array
(:data:`DEFAULT_ACCUMULATED_VARIABLES`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dagster_pri.era5.axis import DEFAULT_VARIABLES

if TYPE_CHECKING:
    import xarray as xr

# Appended to the *stored* variable name to name the derived array (tp -> tp_hourly).
HOURLY_SUFFIX = "_hourly"

# CDS long name -> the CF short name the CDS NetCDF actually carries. Membership in
# this table is what makes a variable "accumulated": it is the full set of
# ERA5-Land's accumulate-since-00-UTC variables, taken from the ERA5-Land
# documentation, and nothing derives it from the data. Instantaneous variables
# (t2m, soil moisture/temperature, winds, snow state, ...) are deliberately absent
# and must never be de-accumulated. (The ingest path does not rename data vars, so
# the store holds these short names -- see dagster_pri.era5.stations.)
ACCUMULATED_SHORT_NAMES = {
    "total_precipitation": "tp",
    "snowfall": "sf",
    "snowmelt": "smlt",
    "runoff": "ro",
    "surface_runoff": "sro",
    "sub_surface_runoff": "ssro",
    "total_evaporation": "e",
    "potential_evaporation": "pev",
    "evaporation_from_bare_soil": "evabs",
    "evaporation_from_open_water_surfaces_excluding_oceans": "evaow",
    "evaporation_from_the_top_of_canopy": "evatc",
    "evaporation_from_vegetation_transpiration": "evavt",
    "snow_evaporation": "es",
    "surface_solar_radiation_downwards": "ssrd",
    "surface_thermal_radiation_downwards": "strd",
    "surface_net_solar_radiation": "ssr",
    "surface_net_thermal_radiation": "str",
    "surface_latent_heat_flux": "slhf",
    "surface_sensible_heat_flux": "sshf",
}


def accumulated_subset(variables: list[str]) -> list[str]:
    """The accumulated members of ``variables``, in the order given.

    Anything not in :data:`ACCUMULATED_SHORT_NAMES` is instantaneous and passes
    through untouched.
    """
    return [v for v in variables if v in ACCUMULATED_SHORT_NAMES]


# Every accumulated variable in the default download, so a default store holds an
# `_hourly` array for each. Narrow it (or set it to []) to trade the derived arrays
# for store size; a variable listed here must also be in `variables`.
DEFAULT_ACCUMULATED_VARIABLES = accumulated_subset(DEFAULT_VARIABLES)


def hourly_increment(da: xr.DataArray) -> xr.DataArray:
    """Per-hour increment of an accumulation-since-00Z DataArray.

    Hour 01 is taken as-is (the previous sample is the prior day's full total, so
    a raw diff would be wildly negative); every other hour is a plain difference.
    ``shift`` yields NaN at the first step, which is the right answer when that
    step's predecessor is outside the block.

    Increments are clamped at 0: accumulations are monotonic within a UTC day, so
    any negative is float noise from the differencing.
    """
    import xarray as xr

    inc = da - da.shift(time=1)
    inc = xr.where(da["time"].dt.hour == 1, da, inc, keep_attrs=True)
    return inc.clip(min=0)


def resolve_stored_name(ds: xr.Dataset, cds_name: str) -> str:
    """Map a configured CDS variable name to the data var actually in ``ds``.

    Prefers the CF short name (what real CDS downloads carry); falls back to the
    long name itself for datasets that were built with long names.
    """
    short = ACCUMULATED_SHORT_NAMES.get(cds_name)
    if short is not None and short in ds.data_vars:
        return short
    if cds_name in ds.data_vars:
        return cds_name
    raise ValueError(
        f"accumulated variable {cds_name!r} "
        f"{f'(short name {short!r}) ' if short else ''}"
        f"is not among the dataset's variables {sorted(ds.data_vars)}. "
        f"Either add it to `variables`, or drop it from `accumulated_variables` "
        f"(set `accumulated_variables: []` to disable de-accumulation entirely)."
    )


def add_hourly_increments(ds: xr.Dataset, cds_names: list[str]) -> xr.Dataset:
    """Return ``ds`` with a ``<stored>_hourly`` array per accumulated variable.

    The raw accumulation is left untouched -- the derived array is additive, so
    nothing already in the store changes meaning.
    """
    if not cds_names:
        return ds

    out = ds
    for cds_name in cds_names:
        stored = resolve_stored_name(ds, cds_name)
        source = ds[stored]
        inc = hourly_increment(source)
        inc.attrs = {
            **source.attrs,
            "long_name": f"{source.attrs.get('long_name', stored)} (hourly increment)",
            "cell_methods": "time: sum (1 hour)",
            "comment": (
                f"Per-hour increment derived from {stored}, which accumulates from "
                f"00:00 UTC and resets daily. Hour 01 is the raw value; other hours "
                f"are the difference from the previous hour, clamped at 0. NaN where "
                f"the previous hour is outside the ingested block."
            ),
        }
        out = out.assign({f"{stored}{HOURLY_SUFFIX}": inc})
    return out
