#!/usr/bin/env python
"""Fail-fast startup checks for the dagster-pri production image.

Run by ``docker/entrypoint.sh`` before the webserver, daemon, or code server
starts. Every problem checked here otherwise surfaces late and quietly:

* An instance config naming a class this image cannot import (the common case is
  ``dagster_docker.DockerRunLauncher``) leaves the **webserver starting up
  perfectly healthy** -- the run launcher is rehydrated lazily, so the failure
  appears only when someone launches a run.
* A bind mount owned by root is invisible until Dagster tries to write a compute
  log, at which point runs look fine and the UI shows no logs at all.
* The S3 credentials this image reads are ``AWS_*``; a deployment that supplies
  ``S3_ENDPOINT`` / ``S3_ACCESS_KEY`` / ``S3_SECRET_KEY`` instead comes up clean
  and fails inside the first run's resource init.

Deliberately does **no** database I/O: it rehydrates the configurable classes
from ``$DAGSTER_HOME/dagster.yaml``, which needs no connection. Container start
therefore does not depend on Postgres being reachable yet.

Set ``DAGSTER_PRI_SKIP_PREFLIGHT=1`` to bypass everything below.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

# The variables the code actually reads. Keep in step with
# src/dagster_pri/defs/resources.py and defs/era5_automation.py, and with the
# table in docs/deploy.md.
S3_VARS = ("AWS_ENDPOINT_URL", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "BUCKET_NAME")
CDS_VARS = ("CDSAPI_URL", "CDSAPI_KEY")

# Names a deployer plausibly supplies instead, none of which anything here reads.
# Mapped to the name that is actually consulted.
FOREIGN_VARS = {
    "S3_ENDPOINT": "AWS_ENDPOINT_URL",
    "S3_ACCESS_KEY": "AWS_ACCESS_KEY_ID",
    "S3_SECRET_KEY": "AWS_SECRET_ACCESS_KEY",
    "S3_BUCKET": "BUCKET_NAME",
    "S3_BUCKET_NAME": "BUCKET_NAME",
}

errors: list[str] = []
notes: list[str] = []


def _uid_hint(path: str) -> str:
    return f"chown -R {os.getuid()}:{os.getgid()} <host path bound at {path}>"


def check_writable(path: str, why: str) -> None:
    """Confirm this uid can actually create files under ``path``."""
    p = Path(path)
    try:
        p.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        errors.append(f"{path} ({why}) cannot be created: {e}\n    Fix: {_uid_hint(path)}")
        return
    try:
        with tempfile.NamedTemporaryFile(dir=path, prefix=".preflight-"):
            pass
    except OSError as e:
        errors.append(
            f"{path} ({why}) is not writable by uid {os.getuid()}: {e}\n    Fix: {_uid_hint(path)}"
        )


def loads_code(role: str) -> bool:
    """Whether this process imports dagster_pri, and so needs the application env.

    ``grpc`` and ``all`` always do. A ``webserver``/``daemon`` does only when its
    workspace loads the code in-process; pointed at a ``grpc_server`` entry it
    never imports the module -- sensor ticks and run workers both execute in the
    code server's container instead.
    """
    if role in ("grpc", "all"):
        return True
    workspace = Path(os.environ.get("DAGSTER_WORKSPACE", "/opt/dagster/workspace.yaml"))
    try:
        import yaml

        entries = yaml.safe_load(workspace.read_text()).get("load_from") or []
    except Exception:  # noqa: BLE001 -- unreadable workspace: assume the code is loaded here
        return True
    return not all("grpc_server" in entry for entry in entries)


def main() -> int:
    role = sys.argv[1] if len(sys.argv) > 1 else "all"
    dagster_home = os.environ.get("DAGSTER_HOME", "/opt/dagster/home")
    config = Path(dagster_home) / "dagster.yaml"

    # --- instance config ---------------------------------------------------
    # Rehydrate the configurable classes now rather than on first use.
    local_dirs: list[tuple[str, str]] = []
    if config.is_file():
        raw = config.read_text()
        try:
            from dagster._core.instance.ref import InstanceRef

            ref = InstanceRef.from_dir(dagster_home)
            for attr in ("run_launcher", "run_coordinator", "scheduler"):
                getattr(ref, attr)  # raises if the class cannot be imported/configured
            clm = ref.compute_log_manager
            las = ref.local_artifact_storage
            base = getattr(clm, "_base_dir", None)
            if base:
                local_dirs.append((str(base), "compute logs"))
            base = getattr(las, "_base_dir", None)
            if base:
                local_dirs.append((str(base), "local artifact storage"))
        except Exception as e:  # noqa: BLE001 -- any failure here is fatal and reported verbatim
            msg = str(e)
            hint = ""
            if "Couldn't import module" in msg or isinstance(e, ModuleNotFoundError):
                hint = (
                    "\n    This image ships only the dependencies in pyproject.toml. External"
                    "\n    run launchers (dagster_docker, dagster_k8s, dagster_celery, ...) are"
                    "\n    NOT supported -- see docs/deploy.md, 'Run launcher'. Use the baked"
                    "\n    DefaultRunLauncher, which executes runs inside this container."
                )
            errors.append(f"{config} could not be loaded: {msg}{hint}")
        else:
            if "DAGSTER_PG_PASSWORD" in raw and not os.environ.get("DAGSTER_PG_PASSWORD"):
                errors.append(
                    f"{config} resolves its storage password from DAGSTER_PG_PASSWORD, "
                    "which is unset.\n    Every storage access would fail; set it."
                )
    else:
        errors.append(f"{config} is missing and the entrypoint did not install it.")

    # --- writable paths ----------------------------------------------------
    check_writable(dagster_home, "DAGSTER_HOME")
    for path, why in local_dirs:
        check_writable(path, why)
    # os.environ, not tempfile.gettempdir(): gettempdir() probes its candidates and
    # SILENTLY falls back to /tmp when TMPDIR is unusable, so checking it would
    # pass while every ERA5 month staged onto the container's writable layer.
    tmpdir = os.environ.get("TMPDIR")
    if tmpdir:
        check_writable(tmpdir, "TMPDIR -- ERA5 ingest stages whole months of NetCDF here")
        if tempfile.gettempdir() != tmpdir:
            errors.append(
                f"TMPDIR={tmpdir} is set but tempfile resolved {tempfile.gettempdir()} "
                "instead.\n    Python fell back silently; NetCDF staging would not land "
                "on the intended volume."
            )

    # --- application environment -------------------------------------------
    for wrong, right in FOREIGN_VARS.items():
        if os.environ.get(wrong) and not os.environ.get(right):
            errors.append(
                f"{wrong} is set but {right} is not. This image reads {right}; "
                f"{wrong} is ignored.\n    See the variable table in docs/deploy.md."
            )

    # Only processes that actually load the code need the application variables.
    # A webserver or daemon pointed at a grpc_server workspace does not: sensor
    # evaluation and run workers both happen in the code server's container.
    if loads_code(role):
        missing_s3 = [v for v in S3_VARS if not os.environ.get(v)]
        if missing_s3:
            notes.append(
                f"object store unset: {', '.join(missing_s3)} -- every ERA5 asset and the "
                "sensor will fail in this process"
            )
        missing_cds = [v for v in CDS_VARS if not os.environ.get(v)]
        if missing_cds:
            notes.append(f"CDS unset: {', '.join(missing_cds)} -- era5_init/era5_iceberg will fail")
        if not os.environ.get("ERA5_START_YM"):
            notes.append("ERA5_START_YM unset -- era5_monthly_sensor will skip every tick")

    # --- report ------------------------------------------------------------
    for note in notes:
        print(f"preflight: warning: {note}", file=sys.stderr)
    if errors:
        print("preflight: FAILED", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        print(
            "\n  Set DAGSTER_PRI_SKIP_PREFLIGHT=1 to start anyway.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
