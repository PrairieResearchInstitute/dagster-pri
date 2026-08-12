"""Unit tests for CDS request batching: splitting, staging paths, and caching."""

from __future__ import annotations

import pytest

from dagster_pri.era5.cds import (
    batch_variables,
    download_month,
    download_month_batched,
    staged_nc_path,
)


class _RecordingClient:
    """Stand-in for cdsapi.Client that records requests and writes a stub file."""

    def __init__(self) -> None:
        self.requests: list[dict] = []

    def retrieve(self, dataset: str, request: dict, target: str) -> None:
        self.requests.append(request)
        with open(target, "wb") as fh:
            fh.write(b"nc")


def test_batch_variables_splits_into_chunks():
    assert batch_variables(["a", "b", "c", "d", "e"], 2) == [["a", "b"], ["c", "d"], ["e"]]
    assert batch_variables(["a", "b"], 5) == [["a", "b"]]
    assert batch_variables([], 3) == []


def test_batch_variables_rejects_nonpositive_size():
    with pytest.raises(ValueError, match="variables_per_request"):
        batch_variables(["a"], 0)


def test_download_month_batched_splits_the_variable_list(tmp_path):
    client = _RecordingClient()
    variables = [f"v{i}" for i in range(5)]

    paths = download_month_batched(
        client,
        2024,
        1,
        variables,
        [42.0, -91.0, 37.0, -87.0],
        tmp_path,
        "IL",
        ndays=2,
        variables_per_request=2,
    )

    assert [r["variable"] for r in client.requests] == [["v0", "v1"], ["v2", "v3"], ["v4"]]
    # Every batch covers the same month/days/hours -- only the variables differ.
    for request in client.requests:
        assert request["year"] == "2024"
        assert request["month"] == "01"
        assert len(request["day"]) == 2
        assert len(request["time"]) == 24

    assert paths == [staged_nc_path(tmp_path, "IL", 2024, 1, batch=i) for i in range(3)]
    assert [p.name for p in paths] == [
        "era5land_il_202401_b00.nc",
        "era5land_il_202401_b01.nc",
        "era5land_il_202401_b02.nc",
    ]
    assert all(p.exists() for p in paths)


def test_download_month_batched_reuses_already_staged_batches(tmp_path):
    client = _RecordingClient()
    args = (2024, 1, ["a", "b", "c", "d"], [42.0, -91.0, 37.0, -87.0], tmp_path, "IL")

    # Pre-stage the second batch as if a previous run had gotten that far.
    staged_nc_path(tmp_path, "IL", 2024, 1, batch=1).write_bytes(b"nc")

    download_month_batched(client, *args, ndays=1, variables_per_request=2)

    assert [r["variable"] for r in client.requests] == [["a", "b"]]


def test_download_month_skips_a_nonempty_staged_file(tmp_path):
    client = _RecordingClient()
    out = tmp_path / "staged.nc"
    out.write_bytes(b"nc")

    download_month(client, 2024, 1, ["a"], [42.0, -91.0, 37.0, -87.0], out)

    assert client.requests == []
