# syntax=docker/dockerfile:1
#
# Integrated production image for the dagster_pri code location: webserver +
# daemon + the code itself, backed by an external Postgres and an S3-compatible
# bucket. The full deployment contract -- variables, roles, volumes, ownership,
# ports, tags -- is in docs/deploy.md.
#
#   docker run -p 3000:3000 --env-file .env \
#       -e DAGSTER_PG_HOST=... -e DAGSTER_PG_PASSWORD=... \
#       ghcr.io/prairieresearchinstitute/dagster-pri:0.1.0-alpha.1
#
# glibc base is required: the rasterio/pyogrio wheels are manylinux_2_28, so musl
# (Alpine) will not work. python:3.12-slim matches the cp312 wheels in uv.lock.
FROM python:3.12-slim-bookworm

# The uid/gid the container runs as. Overridable so a deployment whose host
# directories are owned by something other than 1000 can match them at build time
# rather than chowning the host:
#   docker build --build-arg DAGSTER_UID=1500 --build-arg DAGSTER_GID=1500 .
# Published images always use the defaults; see docs/deploy.md, "Ownership".
ARG DAGSTER_UID=1000
ARG DAGSTER_GID=1000

COPY --from=ghcr.io/astral-sh/uv:0.11.23 /uv /uvx /bin/

# libexpat1: pyogrio's bundled GDAL links against system expat.
# tini: PID 1 that reaps the run-worker subprocesses DefaultRunLauncher spawns.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libexpat1 \
        tini \
    && rm -rf /var/lib/apt/lists/*

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH" \
    DAGSTER_HOME=/opt/dagster/home

WORKDIR /app

# Install dependencies first for better layer caching.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

# uv installs the project editable against /app/src, so the source tree stays.
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

COPY docker/dagster.yaml /opt/dagster/dagster.yaml
COPY docker/workspace.yaml /opt/dagster/workspace.yaml
COPY docker/preflight.py /opt/dagster/preflight.py
COPY docker/entrypoint.sh /usr/local/bin/dagster-entrypoint
RUN chmod +x /usr/local/bin/dagster-entrypoint

# Defaults for everything the baked dagster.yaml reads except the password.
ENV DAGSTER_PG_HOST=postgres \
    DAGSTER_PG_PORT=5432 \
    DAGSTER_PG_DB=dagster \
    DAGSTER_PG_USERNAME=dagster \
    DAGSTER_MAX_CONCURRENT_RUNS=2

# ERA5 ingest stages whole months of NetCDF through tempfile.mkdtemp(), which
# honours TMPDIR. Pointed at a directory of its own rather than /tmp so there is
# an obvious, non-shared mount point for the volume that has to absorb it -- /tmp
# on the host is frequently small or a tmpfs. Set after the uv sync steps so the
# build does not stage into it. Overridable: TMPDIR is the only knob.
ENV TMPDIR=/opt/dagster/scratch

RUN groupadd --gid "$DAGSTER_GID" dagster \
    && useradd --create-home --uid "$DAGSTER_UID" --gid "$DAGSTER_GID" --shell /bin/bash dagster \
    && mkdir -p "$DAGSTER_HOME" "$TMPDIR" /opt/dagster/local/compute_logs \
    && chown -R "$DAGSTER_UID:$DAGSTER_GID" /opt/dagster /app

USER $DAGSTER_UID:$DAGSTER_GID

EXPOSE 3000

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/dagster-entrypoint"]
CMD ["all"]
