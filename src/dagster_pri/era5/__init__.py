"""Pure ERA5-Land ingest logic, ported from ``scripts/era5-illinois.py``.

This package holds the Dagster-free domain logic (geometry, CDS retrieval,
NetCDF transform, Icechunk axis/store management). The Dagster wrappers
(asset, job, resources) live under ``dagster_pri.defs`` and call into here so
this code stays unit-testable without a Dagster context or live S3/CDS access.
"""
