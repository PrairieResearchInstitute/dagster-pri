#!/usr/bin/env bash
# Entrypoint for the integrated dagster-pri production image.
#
# Usage (as the container command):
#   all         start dagster-daemon + dagster-webserver in this container (default)
#   webserver   start only the webserver
#   daemon      start only the daemon
#   grpc        serve this code location over gRPC for an external webserver/daemon
#   <anything>  executed verbatim (e.g. `dagster ...`, `python -c ...`, `bash`)
#
# The four named roles run docker/preflight.py first; the verbatim form does not,
# so one-shot commands and debugging shells always work. See docs/deploy.md.
set -euo pipefail

: "${DAGSTER_HOME:=/opt/dagster/home}"
export DAGSTER_HOME

ROLE="${1:-all}"
WORKSPACE="${DAGSTER_WORKSPACE:-/opt/dagster/workspace.yaml}"
PORT="${DAGSTER_WEBSERVER_PORT:-3000}"

# Install the baked instance config unless the deployment mounted its own.
install_config() {
    if [ ! -f "$DAGSTER_HOME/dagster.yaml" ]; then
        if ! mkdir -p "$DAGSTER_HOME" 2>/dev/null || ! cp /opt/dagster/dagster.yaml "$DAGSTER_HOME/dagster.yaml" 2>/dev/null; then
            echo "entrypoint: cannot write $DAGSTER_HOME/dagster.yaml as uid $(id -u)." >&2
            echo "entrypoint: a volume bound at $DAGSTER_HOME must be owned by uid $(id -u)," >&2
            echo "entrypoint: or mount your own dagster.yaml there. See docs/deploy.md." >&2
            exit 1
        fi
    fi
}

preflight() {
    [ "${DAGSTER_PRI_SKIP_PREFLIGHT:-0}" = "1" ] && return 0
    python /opt/dagster/preflight.py "$ROLE"
}

case "$ROLE" in
    webserver)
        install_config; preflight
        exec dagster-webserver -h 0.0.0.0 -p "$PORT" -w "$WORKSPACE"
        ;;
    daemon)
        install_config; preflight
        exec dagster-daemon run -w "$WORKSPACE"
        ;;
    grpc)
        # -m, not -w: this role serves exactly this image's code location. The
        # external webserver/daemon reach it with a grpc_server workspace entry.
        install_config; preflight
        exec dagster api grpc -h 0.0.0.0 -p "${DAGSTER_GRPC_PORT:-4000}" \
            -m dagster_pri.definitions
        ;;
    all)
        install_config; preflight
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
