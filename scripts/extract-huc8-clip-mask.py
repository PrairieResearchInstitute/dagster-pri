#!/usr/bin/env python3
"""
extract-huc8-clip-mask.py

Extract the HUC8 (8-digit Hydrologic Unit) watershed polygons touching a state
from the USGS NHD File Geodatabase and dissolve them into a single clip-mask
polygon, written to ``data/`` as GeoParquet.

Every HUC8 in the WBD is delineated regardless of state lines, so a watershed
that only partly overlaps the state is still stored whole. Dissolving every
HUC8 whose `states` attribute mentions the target state therefore yields the
union of the state's territory with all watersheds that drain through it --
exactly the geometry the ERA5 ingest clips to ("state plus its watersheds"
instead of the bare political boundary).

Upload the output to the public clip-mask bucket at
``s3://public/shapefiles/state-watershed/<CODE>/<code>_huc8_clip_mask.parquet``;
that is where ``era5.geometry.get_state_geometry`` reads it from.

The Lake Michigan HUC8 (04190000) is excluded by default: it is open water
shared with IN/MI/WI, not a terrestrial drainage basin, and including it
balloons the mask hundreds of km north into Lake Michigan for no benefit to
an ERA5-Land clip.

Run
---
  uv run scripts/extract-huc8-clip-mask.py
  uv run scripts/extract-huc8-clip-mask.py --state IN --out data/watersheds_IN.parquet
  uv run scripts/extract-huc8-clip-mask.py --layer WBDHU10
  uv run scripts/extract-huc8-clip-mask.py --exclude-names ""  # keep every HUC8
"""

from __future__ import annotations

import argparse
from pathlib import Path

DEFAULT_GDB = "data/USGS_NHD/NHD_H_Illinois_State_GDB.gdb"
DEFAULT_LAYER = "WBDHU8"
DEFAULT_EXCLUDE_NAMES = ("Lake Michigan",)


def extract_clip_mask(gdb_path: Path, layer: str, state: str, exclude_names: tuple[str, ...] = ()):
    """Return a single-row GeoDataFrame (EPSG:4326): the dissolved union of every
    ``layer`` polygon whose `states` field mentions ``state``, minus any polygon
    whose `name` matches (case-insensitively) one of ``exclude_names``.
    """
    import geopandas as gpd

    gdf = gpd.read_file(gdb_path, layer=layer)
    matched = gdf[gdf["states"].str.contains(state, na=False)]
    if matched.empty:
        raise ValueError(f"No {layer} features mention state {state!r} in {gdb_path}")

    if exclude_names:
        excluded = {n.strip().lower() for n in exclude_names if n.strip()}
        dropped = matched[matched["name"].str.lower().isin(excluded)]
        for _, row in dropped.iterrows():
            print(f"  excluding {row.get('huc8', row.get('name'))} ({row['name']})")
        matched = matched[~matched["name"].str.lower().isin(excluded)]
        if matched.empty:
            raise ValueError(f"Excluding {exclude_names} left no {layer} features for {state!r}")

    huc_col = next((c for c in ("huc8", "huc10", "huc12") if c in matched.columns), None)
    huc_codes = sorted(matched[huc_col]) if huc_col else []

    dissolved = matched.dissolve().reset_index(drop=True)
    dissolved = dissolved.to_crs("EPSG:4326")
    dissolved["state"] = state
    dissolved["huc_count"] = len(matched)
    dissolved["huc_codes"] = ",".join(huc_codes)
    dissolved["source_layer"] = layer
    return dissolved[["state", "huc_count", "huc_codes", "source_layer", "geometry"]]


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--gdb", default=DEFAULT_GDB, help="Path to the NHD File Geodatabase.")
    p.add_argument(
        "--layer", default=DEFAULT_LAYER, help="WBD layer to dissolve (e.g. WBDHU8, WBDHU10)."
    )
    p.add_argument("--state", default="IL", help="2-letter USPS state code.")
    p.add_argument(
        "--exclude-names",
        default=",".join(DEFAULT_EXCLUDE_NAMES),
        help="Comma-separated `name` values to drop before dissolving (default: "
        f"{DEFAULT_EXCLUDE_NAMES[0]!r}). Pass an empty string to keep every HUC8.",
    )
    p.add_argument(
        "--out",
        default=None,
        help="Output GeoParquet path (default: data/<state>_huc8_clip_mask.parquet).",
    )
    args = p.parse_args()

    from dagster_pri.era5.geometry import normalize_stusps

    state = normalize_stusps(args.state)
    gdb_path = Path(args.gdb)
    if not gdb_path.exists():
        p.error(f"Geodatabase not found: {gdb_path}")

    out_path = Path(args.out) if args.out else Path(f"data/{state.lower()}_huc8_clip_mask.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    exclude_names = tuple(n for n in args.exclude_names.split(",") if n.strip())

    print(f"Reading layer {args.layer!r} from {gdb_path} ...")
    mask = extract_clip_mask(gdb_path, args.layer, state, exclude_names)

    huc_count = int(mask["huc_count"].iloc[0])
    minx, miny, maxx, maxy = mask.total_bounds
    print(f"Dissolved {huc_count} {args.layer} feature(s) touching {state}")
    print(f"Extent (EPSG:4326): ({minx:.3f}, {miny:.3f}) - ({maxx:.3f}, {maxy:.3f})")

    mask.to_parquet(out_path)
    print(f"Wrote clip mask to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
