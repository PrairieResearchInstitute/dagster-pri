"""Bind the ERA5-Land resources to the code location.

The ``era5_iceberg`` asset and the ``era5_init`` job are auto-discovered from
their own modules; this module supplies the resources they require. All
``Definitions`` across the defs folder are merged by ``load_from_defs_folder``.
"""

from __future__ import annotations

import dagster as dg

from dagster_pri.defs.resources import CDSClientResource, IcechunkStorageResource


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "icechunk": IcechunkStorageResource(),
            "cds": CDSClientResource(),
        }
    )
