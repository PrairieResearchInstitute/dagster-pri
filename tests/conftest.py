"""Pytest fixtures for the ERA5-Land ingest tests."""

import pytest


@pytest.fixture
def in_memory_repo():
    """A fresh in-memory Icechunk repository."""
    import icechunk

    return icechunk.Repository.create(icechunk.in_memory_storage())
