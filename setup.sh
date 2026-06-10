#!/usr/bin/env bash
# =============================================================================
# setup.sh — Stoomboot Monitor
# =============================================================================
# Pull this repo to the host that will run the monitor (e.g. login.nikhef.nl)
# and run:
#
#   bash setup.sh
#
# The script is idempotent — safe to re-run. It will:
#   1. Verify Python deps (prometheus_client + htcondor) are available
#   2. Pull Prometheus + Grafana Apptainer images (skips if .sif already present)
#   3. Start the exporter, Prometheus, and Grafana as background processes
#   4. Print SSH tunnel instructions for accessing from your laptop
#
# To stop everything:
#   bash setup.sh --stop
#
# Usage:
#   bash setup.sh [--port-exporter 9118] [--port-prometheus 9090]
#                 [--collector stbc-019.nikhef.nl] [--interval 60]
#                 [--stop] [--status]
# =============================================================================

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN=""   # resolved in Step 1
HTCONDOR_MODULE=""  # set to htcondor2 or htcondor in Step 1
COLLECTOR="stbc-019.nikhef.nl"
if command -v condor_config_val &>/dev/null; then
    _collector_cfg="$(condor_config_val COLLECTOR_HOST 2>/dev/null || true)"
    [[ -n "${_collector_cfg}" ]] && COLLECTOR="${_collector_cfg}"
fi
PORT_EXPORTER=9118
PORT_PROMETHEUS=9090
INTERVAL=15
DEFAULT_USER="your_username"  # pinned as the default in the Grafana user dropdown,
                               # and the user whose CPU jobs get per-job detail

PROM_SIF="${REPO_DIR}/prometheus.sif"
PROM_DATA="${REPO_DIR}/prom_data"
LOG_DIR="${REPO_DIR}/logs"
PID_DIR="${REPO_DIR}/pids"

EXPORTER_LOG="${LOG_DIR}/exporter.log"
PROMETHEUS_LOG="${LOG_DIR}/prometheus.log"

EXPORTER_PID="${PID_DIR}/exporter.pid"
PROMETHEUS_PID="${PID_DIR}/prometheus.pid"

# ── Colour helpers ────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
info()    { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }
section() { echo -e "\n${BLUE}══ $* ${NC}"; }

# ── Argument parsing ──────────────────────────────────────────────────────────
ACTION="start"
FORCE_RESTART=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --stop)             ACTION="stop"; shift ;;
        --status)           ACTION="status"; shift ;;
        --build-only)       ACTION="build"; shift ;;
        --restart)          FORCE_RESTART=true; shift ;;
        --collector)        COLLECTOR="$2"; shift 2 ;;
        --port-exporter)    PORT_EXPORTER="$2"; shift 2 ;;
        --port-prometheus)  PORT_PROMETHEUS="$2"; shift 2 ;;
        --interval)         INTERVAL="$2"; shift 2 ;;
        --python)           PYTHON_BIN="$2"; shift 2 ;;
        --default-user)     DEFAULT_USER="$2"; shift 2 ;;
        *) error "Unknown argument: $1"; exit 1 ;;
    esac
done

# ── Helpers ───────────────────────────────────────────────────────────────────
is_running() {
    local pidfile="$1"
    [[ -f "$pidfile" ]] && kill -0 "$(cat "$pidfile")" 2>/dev/null
}

resolve_htcondor_module() {
    local py_bin="$1"
    if "$py_bin" -c "import htcondor2, classad2" &>/dev/null; then
        echo "htcondor2"
        return 0
    fi
    if "$py_bin" -c "import htcondor, classad" &>/dev/null; then
        echo "htcondor"
        return 0
    fi
    return 1
}

stop_process() {
    local name="$1" pidfile="$2"
    if is_running "$pidfile"; then
        kill "$(cat "$pidfile")" 2>/dev/null && info "Stopped $name (pid $(cat "$pidfile"))"
        rm -f "$pidfile"
    else
        info "$name is not running"
    fi
}

# ── --stop ────────────────────────────────────────────────────────────────────
if [[ "$ACTION" == "stop" ]]; then
    section "Stopping all processes"
    stop_process "exporter"    "$EXPORTER_PID"
    stop_process "prometheus"  "$PROMETHEUS_PID"
    info "Done."
    exit 0
fi

# ── --status ──────────────────────────────────────────────────────────────────
if [[ "$ACTION" == "status" ]]; then
    section "Process status"
    for proc in exporter prometheus; do
        pidvar="${proc^^}_PID"  # exporter -> EXPORTER_PID
        pidfile="${PID_DIR}/${proc}.pid"
        if is_running "$pidfile"; then
            echo -e "  ${GREEN}●${NC} $proc  (pid $(cat "$pidfile"))"
        else
            echo -e "  ${RED}○${NC} $proc  (not running)"
        fi
    done
    echo ""
    echo "  Logs: ${LOG_DIR}/"
    exit 0
fi

# ═════════════════════════════════════════════════════════════════════════════
# START
# ═════════════════════════════════════════════════════════════════════════════

echo ""
echo "  ╔══════════════════════════════════════════╗"
echo "  ║   Stoomboot Monitor — setup.sh           ║"
echo "  ╚══════════════════════════════════════════╝"
echo "  Repo:      ${REPO_DIR}"
echo "  Collector: ${COLLECTOR}"
echo "  Ports:     exporter=${PORT_EXPORTER}  prometheus=${PORT_PROMETHEUS}"
echo ""

mkdir -p "$PROM_DATA" "$LOG_DIR" "$PID_DIR"

# ── Preflight: storage must be writable ───────────────────────────────────────
# Turns a confusing deep error ("/data ... is not writable") into a clear,
# actionable one that names the exact path. Group storage (/data/your_group) can be
# mounted read-only on some hosts.
check_writable() {
    local d="$1" label="$2"
    if ! mkdir -p "$d" 2>/dev/null || [[ ! -w "$d" ]]; then
        error "${label} is not writable: ${d}"
        error "This host can't write there. Check the mount is read-write:"
        error "    touch '${d}/.wtest' && rm -f '${d}/.wtest'"
        error "or point it elsewhere (e.g. --env-prefix \$HOME/envs/grafana, or set REMOTE_DIR)."
        exit 1
    fi
}
check_writable "$REPO_DIR"                  "Repo dir"


# Keep Apptainer's cache and runtime temp on writable storage. The defaults
# live under $HOME/.apptainer, and on a login node $HOME can be read-only or
# quota'd — which surfaces as "<path> is not writable" when apptainer mounts it.
export APPTAINER_CACHEDIR="${APPTAINER_CACHEDIR:-${REPO_DIR}/.apptainer/cache}"
export APPTAINER_TMPDIR="${APPTAINER_TMPDIR:-${REPO_DIR}/.apptainer/tmp}"
mkdir -p "$APPTAINER_CACHEDIR" "$APPTAINER_TMPDIR"

# ── Step 1: Python environment ────────────────────────────────────────────────
section "Step 1: Python environment"

# Search for a Python that has HTCondor bindings. Try v2 first (htcondor2),
# then v1 (htcondor). Absolute paths are checked first so an active conda env
# in the SSH session cannot shadow the system interpreter.
if [[ -n "$PYTHON_BIN" ]]; then
    if [[ ! -x "$PYTHON_BIN" ]] && ! command -v "$PYTHON_BIN" &>/dev/null; then
        error "--python '${PYTHON_BIN}' is not executable or not in PATH."
        exit 1
    fi
    [[ "$PYTHON_BIN" != /* ]] && PYTHON_BIN="$(command -v "$PYTHON_BIN")"
    HTCONDOR_MODULE="$(resolve_htcondor_module "$PYTHON_BIN" || true)"
else
    for _py in /usr/bin/python3.9 /usr/bin/python3.10 /usr/bin/python3.11 \
               /usr/bin/python3 /usr/bin/python \
               python3 python; do
        [[ -x "$_py" ]] || command -v "$_py" &>/dev/null || continue
        _resolved_py="$_py"
        [[ "$_resolved_py" != /* ]] && _resolved_py="$(command -v "$_resolved_py")"
        if _module="$(resolve_htcondor_module "$_resolved_py")"; then
            PYTHON_BIN="$_resolved_py"
            HTCONDOR_MODULE="$_module"
            break
        fi
    done
fi

if [[ -z "$PYTHON_BIN" ]]; then
    error "No Python interpreter found with htcondor available."
    error "Check: /usr/bin/python3.9 -c 'import htcondor2' (HTCondor 25+) or 'import htcondor' (older)."
    exit 1
fi
if [[ -z "$HTCONDOR_MODULE" ]]; then
    error "Python found (${PYTHON_BIN}), but no HTCondor Python module (htcondor2/htcondor) is importable."
    exit 1
fi
info "Python: ${PYTHON_BIN} (${HTCONDOR_MODULE})"

info "Installing prometheus_client..."
"${PYTHON_BIN}" -m pip install --user --quiet prometheus_client
"${PYTHON_BIN}" -c "import prometheus_client" \
    || { error "prometheus_client import failed"; exit 1; }
info "Python deps OK"

# Quick collector reachability check (non-fatal — exporter retries at runtime)
if "${PYTHON_BIN}" -c "import ${HTCONDOR_MODULE} as htcondor; htcondor.Collector('${COLLECTOR}').query(htcondor.AdTypes.Startd, constraint='GPUs >= 1', projection=['Name'])" \
    > /dev/null 2>&1; then
    info "Collector ${COLLECTOR} reachable and returning GPU slots"
else
    warn "Could not reach collector ${COLLECTOR} at setup time — exporter will retry at runtime"
    warn "If this persists: condor_status -pool ${COLLECTOR} -const 'GPUs >= 1'"
fi

# ── Step 2: Apptainer images ──────────────────────────────────────────────────
section "Step 2: Apptainer images"

if ! command -v apptainer &>/dev/null; then
    error "apptainer not found in PATH. On Stoomboot it should be available — try: module load apptainer"
    exit 1
fi
info "apptainer: $(apptainer --version)"

if [[ -f "$PROM_SIF" ]]; then
    info "prometheus.sif already present — skipping pull"
else
    info "Pulling Prometheus image (this takes a minute)..."
    apptainer --silent pull "${PROM_SIF}" docker://prom/prometheus:latest
fi

# ── --build-only stops here (one-time setup: env + images, no services) ───────
if [[ "$ACTION" == "build" ]]; then
    echo ""
    info "Build complete — Apptainer images are ready."
    info "Run without --build-only (or use the local wrapper) to start the services."
    exit 0
fi

# ── Step 3: Start / restart processes ─────────────────────────────────────────
section "Step 3: Starting services"

# Helper: ensure a process is running. By default skips if already up;
# with FORCE_RESTART=true it kills and relaunches.
# stdin is detached (< /dev/null) so this survives an ssh session closing.
launch() {
    local name="$1" pidfile="$2" logfile="$3"
    shift 3
    if is_running "$pidfile"; then
        if $FORCE_RESTART; then
            warn "$name already running (pid $(cat "$pidfile")) — restarting..."
            kill "$(cat "$pidfile")" 2>/dev/null || true
            sleep 1
        else
            info "$name already running (pid $(cat "$pidfile")) — leaving as is"
            return 0
        fi
    fi
    # "$@" is the command
    nohup "$@" >> "$logfile" 2>&1 < /dev/null &
    echo $! > "$pidfile"
    info "Started $name (pid $!), log: $logfile"
}

# 4a. Exporter
launch "exporter" "$EXPORTER_PID" "$EXPORTER_LOG" \
    "${PYTHON_BIN}" "${REPO_DIR}/stoomboot_gpu_exporter.py" \
        --collector "${COLLECTOR}" \
        --port "${PORT_EXPORTER}" \
        --interval "${INTERVAL}" \
        --detail-user "${DEFAULT_USER}"

sleep 2  # let it bind before prometheus tries to scrape

# 4b. Prometheus (update the target port in prometheus.yml if non-default)
PROM_YML="${REPO_DIR}/prometheus.yml"
# Patch target port inline so the config always matches CLI args
sed -i "s|localhost:[0-9]*|localhost:${PORT_EXPORTER}|g" "$PROM_YML"

launch "prometheus" "$PROMETHEUS_PID" "$PROMETHEUS_LOG" \
    apptainer --silent run \
        --no-home \
        --bind "${PROM_YML}:/etc/prometheus/prometheus.yml" \
        --bind "${PROM_DATA}:/prometheus" \
        "${PROM_SIF}" \
            --config.file=/etc/prometheus/prometheus.yml \
            --storage.tsdb.path=/prometheus \
            --web.listen-address=":${PORT_PROMETHEUS}"


# ── Step 4: Health check ──────────────────────────────────────────────────────
section "Step 4: Health check"

sleep 5
HEALTHY=true

if curl -sf "http://localhost:${PORT_EXPORTER}/metrics" | grep -q "stoomboot_gpu"; then
    info "Exporter OK — metrics visible on :${PORT_EXPORTER}"
else
    warn "Exporter not yet responding on :${PORT_EXPORTER} — check ${EXPORTER_LOG}"
    HEALTHY=false
fi

if curl -sf "http://localhost:${PORT_PROMETHEUS}/-/healthy" > /dev/null; then
    info "Prometheus OK — :${PORT_PROMETHEUS}"
else
    warn "Prometheus not yet responding on :${PORT_PROMETHEUS} — check ${PROMETHEUS_LOG}"
    HEALTHY=false
fi

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo "  ╔══════════════════════════════════════════════════════════════╗"
echo "  ║   Services running on the cluster                            ║"
echo "  ╚══════════════════════════════════════════════════════════════╝"
echo ""
echo "  These keep running after you disconnect (data keeps collecting)."
echo ""
echo "  The local wrapper (stbc-monitor.sh) handles the SSH tunnel and"
echo "  opens Grafana for you. If you're driving setup.sh directly on the"
echo "  cluster instead, tunnel manually:"
echo "    ssh -L ${PORT_PROMETHEUS}:localhost:${PORT_PROMETHEUS} ${USER}@$(hostname -f 2>/dev/null || hostname)"
echo ""
echo "  Stop:   bash ${BASH_SOURCE[0]} --stop"
echo "  Status: bash ${BASH_SOURCE[0]} --status"
echo "  Logs:   ${LOG_DIR}/"
echo ""

if ! $HEALTHY; then
    warn "One or more services didn't respond immediately — they may still be starting."
    warn "Check logs above if issues persist after 30 seconds."
fi
