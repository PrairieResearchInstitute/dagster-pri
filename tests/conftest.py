"""Pytest fixtures for the ERA5-Land ingest tests."""

import pytest


@pytest.fixture
def in_memory_repo():
    """A fresh in-memory Icechunk repository."""
    import icechunk

    return icechunk.Repository.create(icechunk.in_memory_storage())


@pytest.fixture
def bucket_root(tmp_path):
    """A local directory standing in for the S3 bucket.

    The bucket is just a path prefix in every path we build, so a local
    directory + a local fsspec filesystem stands in for it without any network.
    """
    root = tmp_path / "bucket"
    root.mkdir(parents=True, exist_ok=True)
    return root


@pytest.fixture
def local_fs():
    """A local fsspec filesystem, standing in for ``resource.filesystem()``."""
    import fsspec

    return fsspec.filesystem("local")


@pytest.fixture
def local_clip_mask(bucket_root):
    """A small square IL clip mask in the local bucket root."""
    from era5_helpers import square_clip_mask_parquet

    square_clip_mask_parquet(bucket_root, "IL")
    return bucket_root
