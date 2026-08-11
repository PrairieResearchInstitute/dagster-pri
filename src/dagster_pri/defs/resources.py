"""Dagster resources for the ERA5-Land ingest: S3/Icechunk storage and the CDS client.

Both read their connection details from environment variables (loaded from
``.env`` automatically by ``dg dev`` / ``dg launch``). They wrap the pure helpers
in :mod:`dagster_pri.era5.store` so assets/jobs stay thin and tests can inject
fakes (e.g. local-filesystem Icechunk storage, a stub CDS client).
"""

from __future__ import annotations

import dagster as dg

from dagster_pri.era5.store import open_or_create_repo, open_repo


class IcechunkStorageResource(dg.ConfigurableResource):
    """Builds Icechunk S3 storage pointed at the S3 endpoint from ``.env``.

    Ceph compatibility is handled here: ``force_path_style`` mirrors s3fs's
    ``addressing_style="path"`` and ``endpoint_url`` points at that endpoint
    instead of AWS. Icechunk's Rust S3 client ignores the botocore
    AWS_*_CHECKSUM_* env vars, so none are set on this path.
    """

    bucket: str = dg.EnvVar("BUCKET_NAME")
    endpoint_url: str = dg.EnvVar("AWS_ENDPOINT_URL")
    access_key_id: str = dg.EnvVar("AWS_ACCESS_KEY_ID")
    secret_access_key: str = dg.EnvVar("AWS_SECRET_ACCESS_KEY")
    region: str = "us-east-1"  # Ceph ignores it but the client wants one

    def storage(self, prefix: str):
        """Return an ``icechunk.Storage`` for the given bucket prefix."""
        import icechunk

        return icechunk.s3_storage(
            bucket=self.bucket,
            prefix=prefix,
            endpoint_url=self.endpoint_url,
            region=self.region,
            access_key_id=self.access_key_id,
            secret_access_key=self.secret_access_key,
            force_path_style=True,  # Ceph RGW path addressing
            allow_http=self.endpoint_url.lower().startswith("http://"),
        )

    def filesystem(self):
        """An ``s3fs`` filesystem on the same bucket/endpoint as the Icechunk store.

        For plain object reads/writes (e.g. the stations CSV, parquet outputs) that
        don't go through Icechunk. ``addressing_style="path"`` mirrors the
        ``force_path_style=True`` used for Icechunk so Ceph RGW is addressed the same
        way; ``use_ssl`` follows the endpoint scheme.

        ``request_checksum_calculation="when_required"`` disables botocore's default
        (>=1.36) ``aws-chunked`` upload checksum, which streams ``PutObject`` without
        a ``Content-Length`` header -- Ceph RGW rejects that with
        ``MissingContentLength``. With it off, s3fs sends a plain ``PutObject`` with
        ``Content-Length``, the same way Icechunk's Rust S3 client already does.
        """
        import s3fs

        return s3fs.S3FileSystem(
            key=self.access_key_id,
            secret=self.secret_access_key,
            use_ssl=self.endpoint_url.lower().startswith("https://"),
            client_kwargs={"endpoint_url": self.endpoint_url, "region_name": self.region},
            config_kwargs={
                "s3": {"addressing_style": "path"},  # Ceph RGW path addressing
                "request_checksum_calculation": "when_required",
                "response_checksum_validation": "when_required",
            },
        )

    def open_repo(self, prefix: str):
        """Open an existing repo at ``prefix``; raises if it isn't there."""
        return open_repo(self.storage(prefix))

    def open_or_create_repo(self, prefix: str):
        """Open the repo at ``prefix``, creating it if absent (init path)."""
        return open_or_create_repo(self.storage(prefix))


class CDSClientResource(dg.ConfigurableResource):
    """Builds a Copernicus CDS API client from explicit credentials.

    Passing ``url``/``key`` explicitly means this works in CI/containers without
    a ``~/.cdsapirc`` file.
    """

    url: str = dg.EnvVar("CDSAPI_URL")
    key: str = dg.EnvVar("CDSAPI_KEY")

    def get_client(self):
        import cdsapi

        return cdsapi.Client(url=self.url, key=self.key)
