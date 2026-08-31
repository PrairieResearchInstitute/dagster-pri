# dagster_pri

A [Dagster](https://docs.dagster.io/) code location for the Prairie Research
Institute. This is currently a bare scaffold: the code location loads cleanly
but defines no real assets yet. New definitions go under
`src/dagster_pri/defs/`.

## Project layout

```
src/dagster_pri/
  definitions.py     # top-level @definitions entrypoint; loads everything in defs/
  defs/              # assets, schedules, sensors, jobs live here (one module each)
  components/        # custom reusable component types (created on demand)
tests/               # pytest suite
pyproject.toml       # dependencies, ruff + pytest config
.pre-commit-config.yaml
```

`definitions.py` uses `load_from_defs_folder`, so anything you add under
`defs/` is picked up automatically — you rarely need to edit `definitions.py`
itself.

## Getting started

This project uses [`uv`](https://docs.astral.sh/uv/) for dependency
management. Install it via the
[official docs](https://docs.astral.sh/uv/getting-started/installation/), then
create the virtual environment and install dependencies (including dev tools):

```bash
uv sync
```

Prefix commands with `uv run` to execute them inside the project environment
(e.g. `uv run dg dev`), or activate the venv with `source .venv/bin/activate`.

## Development workflow

### Run the Dagster UI

```bash
uv run dg dev
```

Open http://localhost:3000 to explore assets, launch runs, and view logs. The
server hot-reloads when you edit code under `src/`.

### Add new definitions

Scaffold definitions with the `dg` CLI rather than writing files by hand:

```bash
# path is relative to defs/ — this creates src/dagster_pri/defs/assets/my_asset.py
uv run dg scaffold defs dagster.asset assets/my_asset.py
uv run dg list defs   # see what's registered
```

### Validate the code location

`dg check` loads the code location and validates all definitions and component
YAML without starting the UI. Run it after any change:

```bash
uv run dg check defs
```

### Tests

Smoke tests in `tests/` verify the code location loads, the repository builds,
and there are no duplicate asset keys:

```bash
uv run pytest
```

Add a test alongside every new asset or resource so the suite catches load
errors and broken graphs early.

### Linting and formatting

[`ruff`](https://docs.astral.sh/ruff/) handles both linting and formatting
(configured in `pyproject.toml`):

```bash
uv run ruff check --fix .   # lint and auto-fix
uv run ruff format .        # format
```

### Pre-commit hooks

[`pre-commit`](https://pre-commit.com/) runs ruff, basic file hygiene checks,
and `dg check defs` on every commit. Install the git hook once after cloning:

```bash
uv run pre-commit install
```

Run all hooks manually against the whole repo at any time:

```bash
uv run pre-commit run --all-files
```

## Debugging

- **Code location won't load** — run `uv run dg check defs`. It prints the full
  traceback for import errors, duplicate asset keys, and invalid component YAML,
  which is faster than waiting for the UI to surface the error.
- **Inspect what's registered** — `uv run dg list defs` shows every asset, job,
  schedule, and sensor the code location exposes.
- **Run a single asset/job locally** — `uv run dg launch --assets <asset_key>`
  executes without the UI so you can read logs straight in the terminal.
- **Step through code** — drop a `breakpoint()` in your asset and run the asset
  via `uv run pytest` or `uv run dg launch`; both run in-process so the debugger
  attaches. In the UI, runs execute in subprocesses, so prefer the CLI/tests
  for interactive debugging.
- **Verbose logs** — pass `--verbose` to most `dg` commands for extra detail.
- **Stale/odd state** — `dg dev` writes ephemeral run history to a temporary
  `DAGSTER_HOME` (the gitignored `.tmp_dagster_home_*` dirs). Deleting them
  resets local run/schedule state.

## Configuration

Local credentials and settings are read from `.env` (gitignored). See the
project's `.env` for the S3-compatible storage endpoint and bucket used by the
data-loading assets.

### Monthly ERA5-Land backfill (`era5_monthly_sensor`)

`era5_monthly_sensor` walks one state forward a month at a time: each tick it
queries the landed parquet output in the object store, finds the latest month
already produced, and launches `era5_monthly_job` (ingest → station summaries)
for the next month — bounded by a configured start and optional end. It is
`STOPPED` by default; enable it in the UI once the state's Icechunk store is
initialized via the `era5_init` job. Configure it with these `.env` vars:

| Variable        | Required | Meaning                                              |
| --------------- | -------- | ---------------------------------------------------- |
| `ERA5_START_YM` | yes      | First month to process, `YYYY-MM` (e.g. `2024-01`).  |
| `ERA5_END_YM`   | no       | Last month, inclusive, `YYYY-MM`. Unbounded if unset.|
| `ERA5_STATE`    | no       | USPS state code; defaults to `IL`.                   |

### Recomputing daily station data (`daily_station_readings_job`)

`daily_station_readings_job` runs the `daily_station_readings` asset on its own,
against a month that is already in the state's Icechunk store. Use it to rebuild
the daily station parquet — after a stations-CSV or aggregation change, say —
without re-downloading the month from CDS. (`era5_monthly_job` is the full
ingest → summaries chain for a *new* month.)

Every field of `DailyStationReadingsConfig` is defaulted, so a run with no config
builds `IL` 2024-01. Set the month you want in the Launchpad:

```yaml
ops:
  daily_station_readings:
    config:
      state: IL
      year: 2024
      month: 3
```

or from the CLI:

```bash
uv run dg launch --job daily_station_readings_job \
  --config-json '{"ops":{"daily_station_readings":{"config":{"state":"IL","year":2024,"month":3}}}}'
```

The month must already have been ingested; if it has not, the run fails with a
message telling you to run `era5_init` + `era5_iceberg` first.

## Production deployment

The repo builds a single, self-contained image that runs the Dagster webserver
**and** the daemon (the daemon is required — without it `era5_monthly_sensor`
never ticks and queued runs never launch):

```
ghcr.io/prairieresearchinstitute/dagster-pri:<release tag>
```

It is published for `linux/amd64` and `linux/arm64` by
`.github/workflows/release-image.yml` whenever a GitHub Release is published.
The image sources live in `docker/`.

**[`docs/deploy.md`](docs/deploy.md) is the deployment contract** — the complete
environment variable set split required/optional and secret/non-secret, the
container roles and the exact command for each, every path that must be a volume
and its ownership requirement, the ports, what the image assumes about Postgres
and the bucket, and the tag format. Write a stack from that document; the
summary below is orientation only.

### The short version

- **Command:** the default `CMD` is `["all"]` — webserver + daemon in one
  container. `webserver`, `daemon` and `grpc` split them; anything else runs
  verbatim. The split layout is supported, with caveats in `docs/deploy.md` §3.2.
- **Needs:** a Postgres server and an S3-compatible bucket. Nothing else — no
  application database, no Docker socket. External run launchers
  (`dagster_docker` and friends) are **not supported** and are rejected at
  startup.
- **Required secrets:** `DAGSTER_PG_PASSWORD`, `AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY`, `CDSAPI_KEY`. Note `AWS_*`, not `S3_*`.
- **Required non-secrets:** `BUCKET_NAME`, `AWS_ENDPOINT_URL`, `CDSAPI_URL`,
  plus `ERA5_START_YM` for the sensor.
- **Volumes:** `/opt/dagster/local` (persist — compute logs and artifacts) and
  `/opt/dagster/scratch` (`TMPDIR`, where ERA5 ingest stages whole months of
  NetCDF). Both must be owned by **uid 1000** if they are host bind mounts.
- **Port:** 3000. Dagster OSS has no authentication; never publish it without an
  authenticating proxy.
- **Tag:** the release tag verbatim. Only `0.1.0-alpha.1` is published today —
  there is no `latest` tag yet, because that release is a pre-release.

Before the first run the bucket must already hold the state's HUC8 clip mask and
the stations CSV; then run the `era5_init` job for the state and start
`era5_monthly_sensor` in the UI (it ships `STOPPED`).

## Learn more

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
