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

## Learn more

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
