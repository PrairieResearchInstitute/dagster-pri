#!/usr/bin/env bash
# Entrypoint for the integrated dagster-pri production image.
#
# Usage (as the container command):
#   all         start dagster-daemon + dagster-webserver in this container (default)
#   webserver   start only the webserver
#   daemon      start only the daemon
#   grpc        serve this code location over gRPC for an external Dagster deployment
#   <anything>  executed verbatim (e.g. `dagster ...`, `python -c ...`, `bash`)
set -euo pipefail

: "${DAGSTER_HOME:=/opt/dagster/home}"
export DAGSTER_HOME

WORKSPACE="${DAGSTER_WORKSPACE:-/opt/dagster/workspace.yaml}"
PORT="${DAGSTER_WEBSERVER_PORT:-3000}"

# Install the baked instance config unless the deployment mounted its own.
if [ ! -f "$DAGSTER_HOME/dagster.yaml" ]; then
    mkdir -p "$DAGSTER_HOME"
    cp /opt/dagster/dagster.yaml "$DAGSTER_HOME/dagster.yaml"
fi

case "${1:-all}" in
    webserver)
        exec dagster-webserver -h 0.0.0.0 -p "$PORT" -w "$WORKSPACE"
        ;;
    daemon)
        exec dagster-daemon run -w "$WORKSPACE"
        ;;
    grpc)
        exec dagster api grpc -h 0.0.0.0 -p "${DAGSTER_GRPC_PORT:-4000}" \
            -m dagster_pri.definitions
        ;;
    all)
        dagster-daemon run -w "$WORKSPACE" &
        daemon_pid=$!
        dagster-webserver -h 0.0.0.0 -p "$PORT" -w "$WORKSPACE" &
        web_pid=$!

        trap 'kill -TERM "$daemon_pid" "$web_pid" 2>/dev/null || true' TERM INT

        # Exit as soon as either process dies rather than lingering half-dead, so
        # the orchestrator restarts the container.
        set +e
        wait -n
        status=$?
        kill -TERM "$daemon_pid" "$web_pid" 2>/dev/null
        wait
        exit "$status"
        ;;
    *)
        exec "$@"
        ;;
esac
