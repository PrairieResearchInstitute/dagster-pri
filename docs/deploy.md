# Deployment contract

Everything a deployer needs to write a correct stack for
`ghcr.io/prairieresearchinstitute/dagster-pri` without reading the source. The
image sources are in [`docker/`](../docker) and [`Dockerfile`](../Dockerfile).

The image is one Dagster code location plus the Dagster webserver and daemon. It
needs exactly two things outside itself: a **Postgres database** for instance
storage and an **S3-compatible bucket** for the data. Nothing else — no
application database, no message broker, no Docker socket.

---

## 1. Image tags

Published by [`.github/workflows/release-image.yml`](../.github/workflows/release-image.yml)
for `linux/amd64` and `linux/arm64` when a GitHub Release is published.

**The image tag is the release tag, verbatim.** `0.1.0-alpha.1` → `:0.1.0-alpha.1`.
The workflow also emits these aliases, all of which are conditional:

| Tag form   | When it is produced                                                   |
| ---------- | --------------------------------------------------------------------- |
| `<release tag>` | Always, on a published release. This is the tag to pin.          |
| `X.Y.Z`, `X.Y` | Only when the release tag parses as **stable** semver. A pre-release tag produces neither. |
| `latest`   | Only for a release **not** marked pre-release.                         |
| `sha-<sha>`| Only for a manual `workflow_dispatch` run.                             |

**As of this writing the only tag that exists in the registry is
`0.1.0-alpha.1`.** It is a pre-release, so there is no `latest` and no `0.1`
tag. Pin the full release tag; do not write `:latest` into a stack until a
stable release exists.

Verify what is actually published before pinning:

```bash
TOKEN=$(curl -s "https://ghcr.io/token?scope=repository:prairieresearchinstitute/dagster-pri:pull&service=ghcr.io" | jq -r .token)
curl -s -H "Authorization: Bearer $TOKEN" \
  https://ghcr.io/v2/prairieresearchinstitute/dagster-pri/tags/list | jq .tags
```

A bump regex over these tags must match the four-component pre-release form,
e.g. `^0\.1\.0-alpha\.[0-9]+$`. A regex anchored on `0.1-alpha.N` matches
nothing that this repo has ever published.

---

## 2. Environment variables

Three groups. Nothing is read from a `.env` file at runtime — the resources call
`dg.EnvVar(...)`, which reads the **process environment**. Supply these as real
container environment or secrets.

### 2.1 Instance storage (Postgres)

Read by the baked [`docker/dagster.yaml`](../docker/dagster.yaml). All have
image defaults except the password.

| Variable | Required | Secret | Default | What it does |
| -------- | -------- | ------ | ------- | ------------ |
| `DAGSTER_PG_PASSWORD` | **yes** | **yes** | — | Password for the instance-storage role. Every process that touches storage needs it. |
| `DAGSTER_PG_HOST` | no | no | `postgres` | Host of the Postgres server. |
| `DAGSTER_PG_PORT` | no | no | `5432` | Port. |
| `DAGSTER_PG_DB` | no | no | `dagster` | Database name. |
| `DAGSTER_PG_USERNAME` | no | no | `dagster` | Role name. |
| `DAGSTER_MAX_CONCURRENT_RUNS` | no | no | `2` | `QueuedRunCoordinator` cap. ERA5 runs stage whole months of NetCDF and are memory-hungry; raise deliberately. |

### 2.2 Object store and CDS (the application)

Derived from [`src/dagster_pri/defs/resources.py`](../src/dagster_pri/defs/resources.py).
Required in every process that **loads the code** — see
[§3 Topologies](#3-container-roles-and-topologies) for which containers those are.

| Variable | Required | Secret | Default | What it does |
| -------- | -------- | ------ | ------- | ------------ |
| `BUCKET_NAME` | **yes** | no | — | Bucket holding the Icechunk stores, the parquet outputs, the clip masks and the stations CSV. |
| `AWS_ENDPOINT_URL` | **yes** | no | — | S3 endpoint, e.g. `https://ceph-rgw.example.edu`. The scheme selects TLS: `https://` enables it, `http://` sets `allow_http`. |
| `AWS_ACCESS_KEY_ID` | **yes** | **yes** | — | S3 access key. |
| `AWS_SECRET_ACCESS_KEY` | **yes** | **yes** | — | S3 secret key. |
| `CDSAPI_URL` | **yes** | no | — | Copernicus CDS API URL, e.g. `https://cds.climate.copernicus.eu/api`. Needed by `era5_init` and `era5_iceberg`. |
| `CDSAPI_KEY` | **yes** | **yes** | — | CDS API key. No `~/.cdsapirc` file is used or needed. |

> **The names are `AWS_*`, not `S3_*`.** They are the standard AWS SDK variable
> names, so the same environment also works for `aws`/`boto3` in a debugging
> shell. `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` and `S3_BUCKET` are
> **not read by anything in this image**. Supplying those instead used to
> produce a container that started perfectly and failed inside the first run's
> resource init; the startup preflight now rejects it with a message naming the
> correct variable (see [§7](#7-startup-preflight)).
>
> Region is not configurable via the environment; the resource hard-codes
> `us-east-1`, which Ceph RGW ignores.

### 2.3 The monthly backfill sensor

Read by [`era5_monthly_sensor`](../src/dagster_pri/defs/era5_automation.py) in
the process that evaluates it.

| Variable | Required | Secret | Default | What it does |
| -------- | -------- | ------ | ------- | ------------ |
| `ERA5_START_YM` | for the sensor | no | — | First month to process, `YYYY-MM`. **Unset, the sensor skips every tick** and nothing is ever ingested. |
| `ERA5_END_YM` | no | no | — | Last month, inclusive, `YYYY-MM`. Unbounded if unset. |
| `ERA5_STATE` | no | no | `IL` | USPS state code. |

The sensor ships `STOPPED`; it must be started once in the UI (or with
`dagster sensor start era5_monthly_sensor`).

### 2.4 Image behaviour knobs

| Variable | Required | Secret | Default | What it does |
| -------- | -------- | ------ | ------- | ------------ |
| `DAGSTER_HOME` | no | no | `/opt/dagster/home` | Where the instance config lives. Changing it changes a mount path; see [§4](#4-volumes-and-ownership). |
| `DAGSTER_WORKSPACE` | no | no | `/opt/dagster/workspace.yaml` | Workspace file for the `webserver`/`daemon`/`all` roles. Point it at your own file for the split topology. |
| `DAGSTER_WEBSERVER_PORT` | no | no | `3000` | Port the webserver binds. |
| `DAGSTER_GRPC_PORT` | no | no | `4000` | Port the `grpc` role binds. |
| `TMPDIR` | no | no | `/opt/dagster/scratch` | Staging directory for NetCDF downloads. See [§4.3](#43-tmpdir-the-staging-volume). |
| `DAGSTER_PRI_SKIP_PREFLIGHT` | no | no | `0` | Set to `1` to bypass all startup checks. |

### 2.5 Variables this image does **not** read

`S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET`, `S3_BUCKET_NAME`,
`DATABASE_URL`, `TAIGA_*`, `ODSC_SCRATCH_DIR`, `OSDC_SCRATCH_DIR`,
`DAGSTER_CURRENT_IMAGE`. Setting them has no effect. The first four are
actively rejected at startup when their `AWS_*` counterpart is missing.

---

## 3. Container roles and topologies

One image, selected by the container `command`. The default `CMD` is `["all"]`.

| Command | Exact process started | Listens on |
| ------- | --------------------- | ---------- |
| `all` *(default)* | `dagster-daemon run -w $DAGSTER_WORKSPACE` **and** `dagster-webserver -h 0.0.0.0 -p $DAGSTER_WEBSERVER_PORT -w $DAGSTER_WORKSPACE` | 3000 |
| `webserver` | `dagster-webserver -h 0.0.0.0 -p $DAGSTER_WEBSERVER_PORT -w $DAGSTER_WORKSPACE` | 3000 |
| `daemon` | `dagster-daemon run -w $DAGSTER_WORKSPACE` | — |
| `grpc` | `dagster api grpc -h 0.0.0.0 -p $DAGSTER_GRPC_PORT -m dagster_pri.definitions` | 4000 |
| anything else | Executed verbatim, with no preflight — `bash`, `dagster asset materialize ...`, `python -c ...` | — |

Both topologies below are supported and both were exercised against a live
Postgres before this document was written.

### 3.1 Single container (`all`) — the blessed shape

```yaml
services:
  dagster:
    image: ghcr.io/prairieresearchinstitute/dagster-pri:0.1.0-alpha.1
    # command omitted: the default CMD is ["all"]
```

The webserver and daemon share one container; the baked workspace loads
`dagster_pri.definitions` as a Python module, and each of them spawns its own
code-server subprocess. Run workers are subprocesses of this container. This is
the simplest correct deployment and the one to use unless there is a reason not
to.

If either process dies the container exits, so the orchestrator restarts both.

### 3.2 Split (`grpc` + `webserver` + `daemon`)

Supported. It requires a workspace file of your own and one non-obvious volume
rule.

```yaml
services:
  dagster-code:
    image: ghcr.io/prairieresearchinstitute/dagster-pri:0.1.0-alpha.1
    command: ["grpc"]
    healthcheck:
      test: ["CMD", "dagster", "api", "grpc-health-check", "-p", "4000", "-h", "127.0.0.1"]
      start_period: 60s

  dagster-web:
    image: ghcr.io/prairieresearchinstitute/dagster-pri:0.1.0-alpha.1
    command: ["webserver"]
    environment:
      DAGSTER_WORKSPACE: /etc/dagster/workspace.yaml
    volumes:
      - ./workspace.yaml:/etc/dagster/workspace.yaml:ro

  dagster-daemon:
    image: ghcr.io/prairieresearchinstitute/dagster-pri:0.1.0-alpha.1
    command: ["daemon"]
    environment:
      DAGSTER_WORKSPACE: /etc/dagster/workspace.yaml
    volumes:
      - ./workspace.yaml:/etc/dagster/workspace.yaml:ro
```

with `workspace.yaml`:

```yaml
load_from:
  - grpc_server:
      host: dagster-code
      port: 4000
      location_name: dagster_pri
```

There is **no `command:`-less form of this**. The image's default `CMD` is
`["all"]`, not `dagster api grpc …`; a code-location service that omits
`command:` starts a webserver and a daemon instead.

Four rules govern the split shape, each of which was verified by running it:

1. **Run workers execute in the `grpc` container.** The baked
   `DefaultRunLauncher` hands the run to the code location over gRPC, so the
   process that imports the assets, downloads NetCDF and writes to S3 is a
   subprocess of `dagster-code` — not of the daemon that launched it. The
   application variables from [§2.2](#22-object-store-and-cds-the-application),
   the `TMPDIR` volume, and any memory limit therefore belong on that container.
2. **The sensor evaluates in the `grpc` container too**, for the same reason.
   `ERA5_*` belongs there.
3. **`/opt/dagster/local` must be the same volume on `grpc` and `webserver`.**
   The code server writes op stdout/stderr there; the UI reads it back from its
   own filesystem. Without a shared volume every run completes and the UI shows
   no logs at all, with nothing appearing broken.
4. **Exactly one daemon per instance.** Running `all` alongside a separate
   `daemon` gives two, which Dagster detects (`Another SENSOR daemon is still
   sending heartbeats`) and which fails queued runs with `Caught an
   unrecoverable error while dequeuing the run`.

Because a `webserver`/`daemon` pointed at a `grpc_server` workspace never
imports the code, it does **not** need the application variables — only
`DAGSTER_PG_PASSWORD` and the Postgres connection settings. The startup
preflight detects this and suppresses the warnings accordingly.

### 3.3 Run launcher: external launchers are not supported

The image ships only the dependencies in
[`pyproject.toml`](../pyproject.toml). `dagster_docker`, `dagster_k8s` and
`dagster_celery` are **not installed and there is no extra that installs them**.
A mounted `dagster.yaml` naming `dagster_docker.DockerRunLauncher` cannot work.

This is deliberate, not an oversight. `DockerRunLauncher` requires binding
`/var/run/docker.sock` into the webserver and the daemon, which is
root-equivalent access to the host; the integrated image exists specifically so
that a deployment needs Postgres and a bucket and nothing else. Run isolation
here is `max_concurrent_runs` plus the container's own memory limit.

Until recently this failed in the worst possible way: Dagster rehydrates the run
launcher lazily, so **the webserver started and reported healthy**, and the
failure appeared only when someone launched a run. The startup preflight now
rejects it at container start:

```
preflight: FAILED
  - /opt/dagster/home/dagster.yaml could not be loaded: Failure condition: Couldn't
    import module dagster_docker when attempting to load the configurable class
    dagster_docker.DockerRunLauncher
    This image ships only the dependencies in pyproject.toml. External
    run launchers (dagster_docker, dagster_k8s, dagster_celery, ...) are
    NOT supported -- see docs/deploy.md, 'Run launcher'. Use the baked
    DefaultRunLauncher, which executes runs inside this container.
```

---

## 4. Volumes and ownership

### 4.1 Ownership

The container runs as **uid 1000, gid 1000** (user `dagster`). Published images
always use those values.

Every host path bound into the container must be owned by that uid:

```bash
install -d -o 1000 -g 1000 /data/dagster/local /data/dagster/scratch
```

Host directories under `/data/...` are root-owned by default, and a root-owned
bind mount fails in two distinct ways — the entrypoint cannot install
`dagster.yaml` under `$DAGSTER_HOME`, or Dagster silently cannot write compute
logs. Both are now caught at startup with the exact `chown` to run.

*Named* Docker volumes need no `chown`: Docker seeds an empty volume from the
image directory, ownership included. Only host bind mounts need this.

If chowning the host is not an option, rebuild with a matching uid:

```bash
docker build --build-arg DAGSTER_UID=1500 --build-arg DAGSTER_GID=1500 .
```

### 4.2 Paths

| Path | Must be a volume? | Contents | Notes |
| ---- | ----------------- | -------- | ----- |
| `/opt/dagster/local` | **yes** — persist | Compute (step) logs under `compute_logs/`, `io_manager` intermediates under `storage/` | Lost on restart otherwise; the UI shows no logs for past runs. In the split topology this must be the **same** volume on the `grpc` and `webserver` containers. |
| `/opt/dagster/scratch` | **yes** — sized, need not persist | NetCDF staged by `era5_init`/`era5_iceberg` | This is `TMPDIR`. A whole month of ERA5-Land at a time. Size it for the largest month; a restart-scoped volume is fine. |
| `/opt/dagster/home` | no | `dagster.yaml` only | The entrypoint writes the baked config here at start if no file is present. Nothing here needs to survive a restart **with the baked config** — see the warning in [§5](#5-instance-configuration). If you mount a volume here it must be writable by uid 1000, or you must mount your own `dagster.yaml` into it. |

Nothing else needs a volume. The instance's runs, event log, schedules and
dynamic partitions are all in Postgres.

### 4.3 `TMPDIR`, the staging volume

`era5_init` and `era5_iceberg` stage downloads through `tempfile.mkdtemp()`
unless the op is given an explicit `work_dir` config value. `tempfile` honours
`TMPDIR`, so **`TMPDIR` is the knob** — there is no separate scratch-directory
variable, and `ODSC_SCRATCH_DIR`/`OSDC_SCRATCH_DIR` are read by nothing here.

The image sets `TMPDIR=/opt/dagster/scratch` rather than leaving it at `/tmp`,
so there is one obvious, non-shared mount point for the volume that has to
absorb a month of NetCDF. Host `/tmp` is frequently small or a tmpfs.

One sharp edge, which the preflight now catches: if `TMPDIR` is not writable,
Python does **not** fail — `tempfile.gettempdir()` silently falls back to
`/tmp`, and staging lands on the container's writable layer instead of the
volume, filling the host disk with nothing to show for it.

---

## 5. Instance configuration

The image bakes [`docker/dagster.yaml`](../docker/dagster.yaml) at
`/opt/dagster/dagster.yaml` and the entrypoint copies it to
`$DAGSTER_HOME/dagster.yaml` **only if no file is already there**. Mounting your
own file at `$DAGSTER_HOME/dagster.yaml` overrides it completely.

The baked config sets:

- Postgres storage from the `DAGSTER_PG_*` variables
- `QueuedRunCoordinator` with `max_concurrent_runs` from `DAGSTER_MAX_CONCURRENT_RUNS`
- `DefaultRunLauncher`
- `local_artifact_storage` at `/opt/dagster/local`
- `LocalComputeLogManager` at `/opt/dagster/local/compute_logs`
- telemetry off

> **If you mount your own `dagster.yaml`, keep the `local_artifact_storage` and
> `compute_logs` blocks or move the volume.** Dropping them does not disable
> local storage — it relocates it. Dagster's defaults put both under
> `$DAGSTER_HOME/storage`, so a config that omits them turns
> `/opt/dagster/home` into the path that must be a persisted volume and leaves
> `/opt/dagster/local` unused. Verified:
>
> | Config | `local_artifact_storage` | compute logs |
> | ------ | ------------------------ | ------------ |
> | baked | `/opt/dagster/local` | `/opt/dagster/local/compute_logs` |
> | mounted, blocks omitted | `$DAGSTER_HOME/storage` | `$DAGSTER_HOME/storage` |

Check what an instance actually resolved:

```bash
docker run --rm --env-file .env -e DAGSTER_PG_PASSWORD=... \
  ghcr.io/prairieresearchinstitute/dagster-pri:0.1.0-alpha.1 dagster instance info
```

---

## 6. Postgres and bucket assumptions

### 6.1 Postgres

- A **server the image does not manage**; nothing in this repo starts one.
  Tested against Postgres 18.
- A database and a role that owns it. Defaults `dagster`/`dagster`; the role
  needs ordinary DDL rights on that database — Dagster runs its own schema
  migrations at startup.
- Reachable over TCP at `DAGSTER_PG_HOST:DAGSTER_PG_PORT`. Unix-socket
  connections are not configurable through the baked `dagster.yaml`.
- No extensions required.
- **One instance, one daemon.** Every container sharing these connection
  settings shares one Dagster instance; exactly one of them may run a daemon.

The container does **not** require Postgres to be up at start: the preflight
does no database I/O deliberately, so a container can come up before the
cluster and let Dagster's own retry handle it.

### 6.2 Bucket

`BUCKET_NAME` must exist. The layout is fixed by the code, not configurable:

| Prefix | Written by | Purpose |
| ------ | ---------- | ------- |
| `shapefiles/state-watershed/<ST>/<st>_huc8_clip_mask.parquet` | **you, before first run** | HUC8 clip mask per state. |
| `pri_data/stations.csv` | **you, before first run** | Station list read by the station assets. Overridable per-op via the `stations_key` config. |
| `era5-land/icechunk/<ST>/` | `era5_init`, `era5_iceberg` | Icechunk store of raw ERA5-Land. |
| `era5-land/parquet/STATE=…/YEAR=…/MONTH=…/` | `daily_station_readings` | Daily station parquet. Also what the sensor reads to decide the next month. |
| `era5-land/hourly/STATE=…/YEAR=…/MONTH=…/` | `hourly_station_readings` | Hourly station parquet. |

The first two are **deployment prerequisites**. Without the clip mask every run
fails with:

```
ValueError: No clip mask for IL at '<bucket>/shapefiles/state-watershed/IL/il_huc8_clip_mask.parquet'.
Build one with scripts/extract-huc8-clip-mask.py --state IL and upload it there.
```

Ceph RGW compatibility is handled in the resource — path-style addressing on
both the Icechunk and `s3fs` paths, and `request_checksum_calculation` set to
`when_required` so RGW does not reject uploads with `MissingContentLength`. No
deployer configuration is involved.

### 6.3 First run

1. Upload the clip mask and `stations.csv`.
2. Start the stack; confirm the code location loads.
3. Run the `era5_init` job once per state.
4. Start `era5_monthly_sensor` (it ships `STOPPED`).

---

## 7. Startup preflight

The four named roles run [`docker/preflight.py`](../docker/preflight.py) before
starting. The verbatim command form does not, so debugging shells and one-shot
commands always work.

It exits non-zero, before the server starts, on:

- an instance config naming a class this image cannot import (external run launchers)
- `DAGSTER_PG_PASSWORD` unset while the effective `dagster.yaml` resolves it
- `$DAGSTER_HOME`, the artifact/compute-log dirs, or `TMPDIR` not writable by
  this uid — reported with the exact `chown` to run
- `TMPDIR` set but silently falling back to `/tmp`
- `S3_ENDPOINT`/`S3_ACCESS_KEY`/`S3_SECRET_KEY`/`S3_BUCKET` set while the
  `AWS_*` name the code reads is not

and warns, without blocking, when a process that loads the code is missing the
object-store, CDS, or `ERA5_START_YM` variables.

It performs **no database or network I/O**. `DAGSTER_PRI_SKIP_PREFLIGHT=1`
bypasses all of it.

---

## 8. Ports

| Port | Role | Notes |
| ---- | ---- | ----- |
| 3000 | `all`, `webserver` | The UI. `EXPOSE`d. Override with `DAGSTER_WEBSERVER_PORT`. |
| 4000 | `grpc` | Code location gRPC. Not `EXPOSE`d; reachable on the container network. Override with `DAGSTER_GRPC_PORT`. |

**Dagster OSS ships no authentication.** The UI can launch and terminate runs,
which is code execution. Never publish port 3000 without an authenticating
proxy in front of it.

Health checks — the image has no `curl` (a `python:slim` base):

```yaml
# webserver
test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:3000/server_info')"]
# grpc
test: ["CMD", "dagster", "api", "grpc-health-check", "-p", "4000", "-h", "127.0.0.1"]
```

---

## 9. Worked example

Single-container, behind a proxy on an external `edge` network, against a shared
Postgres on `db`. No host port.

```yaml
services:
  dagster:
    image: ghcr.io/prairieresearchinstitute/dagster-pri:0.1.0-alpha.1
    container_name: dagster
    restart: unless-stopped
    # command omitted: default CMD is ["all"] -- webserver + daemon.

    environment:
      DAGSTER_PG_HOST: postgres
      DAGSTER_PG_DB: dagster
      DAGSTER_PG_USERNAME: dagster
      DAGSTER_PG_PASSWORD: ${DAGSTER_DB_PASSWORD:?missing}
      DAGSTER_MAX_CONCURRENT_RUNS: "2"

      BUCKET_NAME: ${BUCKET_NAME:?missing}
      AWS_ENDPOINT_URL: ${AWS_ENDPOINT_URL:?missing}
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID:?missing}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY:?missing}
      CDSAPI_URL: ${CDSAPI_URL:?missing}
      CDSAPI_KEY: ${CDSAPI_KEY:?missing}

      ERA5_START_YM: "2024-01"
      ERA5_STATE: IL

    volumes:
      # Both must be owned by uid 1000:
      #   install -d -o 1000 -g 1000 /data/dagster/local /data/dagster/scratch
      - /data/dagster/local:/opt/dagster/local
      - /data/dagster/scratch:/opt/dagster/scratch   # TMPDIR

    networks: [edge, db]

    healthcheck:
      test: ["CMD", "python", "-c",
             "import urllib.request; urllib.request.urlopen('http://127.0.0.1:3000/server_info')"]
      interval: 30s
      timeout: 5s
      retries: 5
      start_period: 90s

    logging:
      driver: json-file
      options: {max-size: "10m", max-file: "3"}

networks:
  edge: {external: true}
  db:   {external: true}
```

No `/var/run/docker.sock`, no `init: true` (the image's `ENTRYPOINT` is already
`tini`), and no `dagster.yaml` or `workspace.yaml` mount — the image ships both.
