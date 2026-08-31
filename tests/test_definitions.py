"""Smoke tests that the Dagster code location loads cleanly.

These don't exercise any real assets yet — they guard against import errors,
duplicate keys, and broken component YAML so the code location always boots.
"""

from dagster import Definitions

from dagster_pri.definitions import defs


def _load() -> Definitions:
    """Resolve the lazy, decorated ``@definitions`` into a Definitions object."""
    return defs.load_fn()


def test_definitions_resolve():
    """The top-level Definitions object builds without errors."""
    assert isinstance(_load(), Definitions)


def test_repository_builds():
    """Resolving the repository validates the full graph (jobs, schedules, etc.).

    This raises if there are duplicate asset keys or other invalid definitions.
    """
    repo = _load().get_repository_def()
    assert repo is not None


def test_no_duplicate_asset_keys():
    """Loading resolves the asset graph; duplicate keys would surface here."""
    asset_keys = _load().resolve_all_asset_keys()
    assert len(asset_keys) == len(set(asset_keys))


def test_daily_station_readings_job_registered():
    """The stand-alone recompute job is discovered and scoped to its one asset."""
    job = _load().resolve_job_def("daily_station_readings_job")
    assert {node.name for node in job.nodes} == {"daily_station_readings"}
