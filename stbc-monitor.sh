#!/usr/bin/env bash
# =============================================================================
# stbc-monitor.sh — LOCAL wrapper for the Stoomboot monitor
# =============================================================================
# Run this from your LAPTOP. You never need to open a shell on the cluster.
#
# The cluster runs only the exporter + Prometheus. Grafana runs locally on
# your Mac via Homebrew and connects to Prometheus over an SSH tunnel.
#
#   ./stbc-monitor.sh            Start: ensure cluster services up, start local
#                                Grafana, open SSH tunnel, open browser.
#                                (Ctrl-C closes the tunnel; Grafana and cluster
#                                 services keep running.)
#
#   ./stbc-monitor.sh --setup    One-time: deploy cluster side (exporter +
#                                Prometheus) and install Grafana via Homebrew.
#
#   ./stbc-monitor.sh --stop     Stop cluster services + local Grafana.
#   ./stbc-monitor.sh --status   Show what's running (cluster + local).
#   ./stbc-monitor.sh --restart  Restart cluster services + local Grafana.
#   ./stbc-monitor.sh --logs     Tail the remote logs.
# =============================================================================

set -euo pipefail

# ── Defaults (override via env or ~/.stbc-monitor.conf) ───────────────────────
SSH_USER="${STBC_SSH_USER:-nsundstr}"
SSH_HOST="${STBC_SSH_HOST:-login.nikhef.nl}"
SSH_JUMP="${STBC_SSH_JUMP-}"
REMOTE_DIR="${STBC_REMOTE_DIR:-/data/alice/nsundstr/stbc-monitor}"
REPO_URL="${STBC_REPO_URL:-}"
CLUSTER_USER="${STBC_CLUSTER_USER:-}"
PORT_GRAFANA="${STBC_PORT_GRAFANA:-3000}"
PORT_PROMETHEUS="${STBC_PORT_PROMETHEUS:-9090}"

CONF="${HOME}/.stbc-monitor.conf"
# shellcheck disable=SC1090
[[ -f "$CONF" ]] && source "$CONF"

CLUSTER_USER="${CLUSTER_USER:-$SSH_USER}"
LOCAL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Local Grafana data dir (keeps dashboards/db separate from any brew defaults)
LOCAL_GRAFANA_DATA="${LOCAL_DIR}/grafana_data_local"

# ── Colour helpers ────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[local]${NC} $*"; }
warn()  { echo -e "${YELLOW}[local]${NC} $*"; }
error() { echo -e "${RED}[local]${NC} $*" >&2; }

# ── SSH plumbing ──────────────────────────────────────────────────────────────
SSH_TARGET="${SSH_USER}@${SSH_HOST}"
ssh_opts=()
[[ -n "$SSH_JUMP" ]] && ssh_opts+=(-J "${SSH_USER}@${SSH_JUMP}")

remote() {
    ssh ${ssh_opts[@]+"${ssh_opts[@]}"} "$SSH_TARGET" "$@"
}

# ── Deploy repo to cluster ────────────────────────────────────────────────────
deploy() {
    if [[ -n "$REPO_URL" ]]; then
        info "Deploying via git ($REPO_URL) → ${SSH_TARGET}:${REMOTE_DIR}"
        remote "
            set -e
            if [[ -d '${REMOTE_DIR}/.git' ]]; then
                cd '${REMOTE_DIR}' && git pull --ff-only
            else
                mkdir -p \"\$(dirname '${REMOTE_DIR}')\"
                git clone '${REPO_URL}' '${REMOTE_DIR}'
            fi
        "
    else
        info "Deploying via rsync ${LOCAL_DIR}/ → ${SSH_TARGET}:${REMOTE_DIR}/"
        local ssh_cmd="ssh"
        [[ -n "$SSH_JUMP" ]] && ssh_cmd="ssh -J ${SSH_USER}@${SSH_JUMP}"
        remote "mkdir -p '${REMOTE_DIR}'"
        rsync -az --delete \
            --exclude '.git' \
            --exclude '*.sif' \
            --exclude '.apptainer' \
            --exclude 'prom_data' \
            --exclude 'grafana_data' \
            --exclude 'grafana_data_local' \
            --exclude 'logs' \
            --exclude 'pids' \
            -e "$ssh_cmd" \
            "${LOCAL_DIR}/" "${SSH_TARGET}:${REMOTE_DIR}/"
    fi
}

# ── Local Grafana via Homebrew ────────────────────────────────────────────────
ensure_grafana_installed() {
    if ! command -v grafana-server >/dev/null 2>&1; then
        if ! command -v brew >/dev/null 2>&1; then
            error "Homebrew not found. Install it from https://brew.sh then re-run."
            exit 1
        fi
        info "Installing Grafana via Homebrew..."
        brew install grafana
    fi
}

grafana_bin() {
    # Prefer the versioned cellar path so we don't rely on PATH being set up
    local bin
    bin="$(brew --prefix grafana 2>/dev/null)/bin/grafana" 2>/dev/null || true
    if [[ -x "$bin" ]]; then echo "$bin"; return; fi
    # Fallback: whatever's on PATH
    command -v grafana-server 2>/dev/null || { error "grafana binary not found"; exit 1; }
}

start_local_grafana() {
    local pid_file="${LOCAL_GRAFANA_DATA}/grafana-local.pid"

    if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
        info "Local Grafana already running (pid $(cat "$pid_file"))"
        return
    fi

    local prov_dir="${LOCAL_GRAFANA_DATA}/provisioning"
    local dash_dir="${LOCAL_GRAFANA_DATA}/dashboards"
    mkdir -p "${LOCAL_GRAFANA_DATA}/data" "${LOCAL_GRAFANA_DATA}/logs" \
             "${LOCAL_GRAFANA_DATA}/plugins" \
             "${prov_dir}/datasources" "${prov_dir}/dashboards" \
             "${dash_dir}"

    # grafana.ini — point provisioning at our local copy
    cat > "${LOCAL_GRAFANA_DATA}/grafana.ini" <<EOF
[paths]
data     = ${LOCAL_GRAFANA_DATA}/data
logs     = ${LOCAL_GRAFANA_DATA}/logs
plugins  = ${LOCAL_GRAFANA_DATA}/plugins
provisioning = ${prov_dir}

[server]
http_port = ${PORT_GRAFANA}

[security]
admin_password = stbc_monitor

[analytics]
reporting_enabled = false
check_for_updates = false
EOF

    # datasources — copy as-is (already points at localhost:9090)
    cp "${LOCAL_DIR}/grafana/provisioning/datasources/prometheus.yml" \
        "${prov_dir}/datasources/prometheus.yml"

    # dashboards provider — rewrite path to our local dir
    cat > "${prov_dir}/dashboards/dashboards.yml" <<EOF
apiVersion: 1
providers:
  - name: stoomboot
    orgId: 1
    folder: Stoomboot
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    options:
      path: ${dash_dir}
EOF

    # copy dashboard JSONs with default-user substitution
    for dash in "${LOCAL_DIR}/grafana/provisioning/dashboards/"*.json; do
        [[ -e "$dash" ]] || continue
        sed "s/__DEFAULT_USER__/${CLUSTER_USER}/g" "$dash" \
            > "${dash_dir}/$(basename "$dash")"
    done

    local log_file="${LOCAL_GRAFANA_DATA}/grafana.log"
    info "Starting local Grafana on port ${PORT_GRAFANA}..."

    local bin
    bin="$(grafana_bin)"

    # grafana server (v10+) uses `grafana server`, older uses `grafana-server`
    if [[ "$bin" == *"/grafana" ]]; then
        nohup "$bin" server \
            --config "${LOCAL_GRAFANA_DATA}/grafana.ini" \
            --homepath "$(brew --prefix grafana)/share/grafana" \
            > "$log_file" 2>&1 &
    else
        nohup "$bin" \
            --config "${LOCAL_GRAFANA_DATA}/grafana.ini" \
            --homepath "$(brew --prefix grafana)/share/grafana" \
            > "$log_file" 2>&1 &
    fi

    echo $! > "$pid_file"
    info "Local Grafana started (pid $!, log: $log_file)"
}

stop_local_grafana() {
    local pid_file="${LOCAL_GRAFANA_DATA}/grafana-local.pid"
    if [[ -f "$pid_file" ]]; then
        local pid
        pid="$(cat "$pid_file")"
        if kill -0 "$pid" 2>/dev/null; then
            info "Stopping local Grafana (pid $pid)..."
            kill "$pid"
        fi
        rm -f "$pid_file"
    else
        info "Local Grafana is not running."
    fi
}

local_grafana_status() {
    local pid_file="${LOCAL_GRAFANA_DATA}/grafana-local.pid"
    if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
        echo -e "  ${GREEN}●${NC} grafana (local, pid $(cat "$pid_file"), port ${PORT_GRAFANA})"
    else
        echo -e "  ○ grafana (local, not running)"
    fi
}

open_browser() {
    local url="$1"
    if command -v open >/dev/null 2>&1; then
        open "$url"
    elif command -v xdg-open >/dev/null 2>&1; then
        xdg-open "$url" >/dev/null 2>&1 &
    else
        info "Open this in your browser: $url"
    fi
}

# ── Actions ───────────────────────────────────────────────────────────────────
ACTION="start"
case "${1:-}" in
    --setup)   ACTION="setup" ;;
    --stop)    ACTION="stop" ;;
    --status)  ACTION="status" ;;
    --restart) ACTION="restart" ;;
    --logs)    ACTION="logs" ;;
    "")        ACTION="start" ;;
    -h|--help)
        sed -n '2,15p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
        exit 0 ;;
    *) error "Unknown argument: $1 (try --help)"; exit 1 ;;
esac

echo ""
info "Target: ${SSH_TARGET}$( [[ -n "$SSH_JUMP" ]] && echo "  (via ${SSH_JUMP})" )"
info "Remote dir: ${REMOTE_DIR}"
echo ""

case "$ACTION" in

    setup)
        info "One-time setup — deploys cluster side (exporter + Prometheus only)"
        info "and installs Grafana locally via Homebrew."
        ensure_grafana_installed
        deploy
        remote "cd '${REMOTE_DIR}' && bash setup.sh --build-only --skip-grafana \
            --default-user '${CLUSTER_USER}'"
        echo ""
        info "Setup done. Run ./stbc-monitor-local-grafana.sh to start monitoring."
        ;;

    start)
        ensure_grafana_installed
        deploy
        info "Ensuring cluster services are running (exporter + Prometheus)..."
        remote "cd '${REMOTE_DIR}' && bash setup.sh --skip-grafana \
            --port-prometheus ${PORT_PROMETHEUS} \
            --default-user '${CLUSTER_USER}'"
        start_local_grafana
        echo ""
        info "Opening SSH tunnel:  localhost:${PORT_PROMETHEUS} → cluster Prometheus"
        info "Grafana login: admin / stbc_monitor"
        echo ""
        info "Browser will open shortly. Press Ctrl-C to close the tunnel."
        info "(Local Grafana keeps running after you disconnect.)"
        warn "To stop Grafana: ./stbc-monitor-local-grafana.sh --stop"
        echo ""
        ( sleep 4; open_browser "http://localhost:${PORT_GRAFANA}/d/stbc-overview" ) &
        # Tunnel only Prometheus — Grafana runs locally
        ssh ${ssh_opts[@]+"${ssh_opts[@]}"} -N \
            -L "${PORT_PROMETHEUS}:localhost:${PORT_PROMETHEUS}" \
            "$SSH_TARGET"
        ;;

    restart)
        deploy
        remote "cd '${REMOTE_DIR}' && bash setup.sh --restart --skip-grafana \
            --port-prometheus ${PORT_PROMETHEUS} \
            --default-user '${CLUSTER_USER}'"
        stop_local_grafana
        start_local_grafana
        info "Services restarted. Run ./stbc-monitor-local-grafana.sh to view."
        ;;

    stop)
        remote "cd '${REMOTE_DIR}' && bash setup.sh --stop"
        stop_local_grafana
        info "All services stopped."
        ;;

    status)
        remote "cd '${REMOTE_DIR}' && bash setup.sh --status"
        echo ""
        echo "══ Local Grafana"
        local_grafana_status
        ;;

    logs)
        info "Tailing remote logs (Ctrl-C to stop)..."
        remote "tail -n 40 -f '${REMOTE_DIR}'/logs/*.log"
        ;;
esac
