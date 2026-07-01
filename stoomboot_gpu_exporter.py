#!/usr/bin/env python3
"""
stoomboot_gpu_exporter.py
─────────────────────────
Prometheus exporter for monitoring the detail user's HTCondor jobs on the
Stoomboot cluster (GPU + CPU). Per-job metrics are emitted for the configured
`detail_user`; for the memory + CPU fallback path, HTCondor Startd ClassAds
are queried directly (node_exporter on the workers is unreachable from the
login node).

Usage:
    python stoomboot_gpu_exporter.py [--port 9118] [--collector stbc-019.nikhef.nl]
                                     [--interval 3] [--detail-user your_username]

Requirements:
    pip install prometheus_client htcondor
"""

import argparse
import http.server
import logging
import os
import threading
import time
from collections import defaultdict

try:
    import htcondor2 as htcondor
    _binding = "htcondor2"
except ImportError:
    try:
        import htcondor
        _binding = "htcondor"
    except ImportError as exc:
        raise ImportError(
            "HTCondor Python bindings not found.\n"
            "Tried htcondor2 (HTCondor 25+) and htcondor (older).\n"
            "On Stoomboot they should be available system-wide in /usr/bin/python3.9."
        ) from exc

from prometheus_client import start_http_server, Gauge

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("stbc_exporter")

# ─── Per-user aggregates ──────────────────────────────────────────────────────
user_jobs_running = Gauge(
    "stoomboot_user_jobs_running",
    "Number of jobs currently running for the detail user",
    ["cluster", "user"],
)
user_jobs_queued = Gauge(
    "stoomboot_user_jobs_queued",
    "Number of jobs currently queued (idle) for the detail user",
    ["cluster", "user"],
)
user_units_in_use = Gauge(
    "stoomboot_user_units_in_use",
    "Compute units currently held by the detail user "
    "(GPUs on the gpu cluster, CPU cores on the cpu cluster)",
    ["cluster", "user"],
)

# ─── Per-job detail (detail user only, both clusters) ─────────────────────────
job_duration_seconds = Gauge(
    "stoomboot_job_duration_seconds",
    "Wall-clock time a running job has been executing (seconds)",
    ["cluster", "user", "job_id", "resource_type", "node"],
)
job_vram_allocated_mb = Gauge(
    "stoomboot_job_vram_allocated_mb",
    "VRAM allocated to this job (MB): RequestGPUs × GPUs_GlobalMemoryMb on the assigned node",
    ["cluster", "user", "job_id", "resource_type", "node"],
)
job_memory_requested_mb = Gauge(
    "stoomboot_job_memory_requested_mb",
    "Memory requested by the job (MB)",
    ["cluster", "user", "job_id", "resource_type", "node"],
)
# job_memory_usage_mb and job_cpu_efficiency are NOT cleared by the personal
# loop — they're owned by the startd-fallback path (which runs every 3s) when
# the schedd has no real data, and we don't want to wipe the fallback's value
# with a zero from the schedd every 15s.
job_memory_usage_mb = Gauge(
    "stoomboot_job_memory_usage_mb",
    "Actual memory used by the job (MB) — schedd RSS/MemoryUsage when fresh, otherwise startd ClassAd",
    ["cluster", "user", "job_id", "resource_type", "node"],
)
job_cpu_efficiency = Gauge(
    "stoomboot_job_cpu_efficiency",
    "CPU efficiency ratio: TotalJobRunningCpuUsage / (duration_s × requested_cpus); 1.0 = fully utilising requested cores",
    ["cluster", "user", "job_id", "resource_type", "node"],
)
job_gpu_utilization_pct = Gauge(
    "stoomboot_job_gpu_utilization_pct",
    "GPU compute utilization % (0–100) from HTCondor Startd ClassAd (DeviceGPUsAverageUsage)",
    ["cluster", "user", "job_id", "node"],
)

# ─── GPU node classification (update if cluster changes) ─────────────────────
# Source: https://kb.nikhef.nl/ct/Batch_jobs_GPU.html
GPU_NODE_MAP = {
    "wn-lot-002": "AMD_MI50",
    "wn-lot-003": "AMD_MI50",
    "wn-lot-004": "AMD_MI50",
    "wn-lot-005": "AMD_MI50",
    "wn-lot-006": "AMD_MI50",
    "wn-lot-007": "AMD_MI50",
    "wn-lot-008": "NVIDIA_V100",
    "wn-lot-009": "NVIDIA_V100",
    "wn-pijl-002": "NVIDIA_L40S",
    "wn-pijl-003": "NVIDIA_L40S",
    "wn-pijl-004": "NVIDIA_L40S",
    "wn-pijl-005": "NVIDIA_L40S",
    "wn-pijl-006": "NVIDIA_L40S",
    "wn-pijl-007": "NVIDIA_L40S",
}

# HTCondor GPU metrics cache (avoids querying collector every loop iteration)
_htcondor_gpu_cache: dict = {}   # {job_id: {util_pct, mem_used_mb, mem_total_mb, cpu_usage, memory_usage_mb}}
_htcondor_gpu_cache_time: float = 0.0
_HTCONDOR_GPU_CACHE_TTL: float = 15  # seconds
# Lock for cache read/write. Two threads may hit the collector simultaneously
# when the cache expires; without this lock they'd both fire a full ClassAd query.
_htcondor_gpu_lock = threading.Lock()

# Shared state
_collector_host: str = "stbc-019.nikhef.nl"  # set from --collector in main()
_vram_per_gpu_by_node: dict = {}              # populated by personal loop via startd
_current_node_jobs: list = []                 # jobs to poll in fallback loop
_personal_mem_labels: set = set()             # active job_memory_usage_mb labels
_personal_cpu_labels: set = set()             # active job_cpu_efficiency labels


def node_short(machine_name: str) -> str:
    """slot1@wn-pijl-002.nikhef.nl -> wn-pijl-002"""
    name = (machine_name or "").split("@")[-1]
    return name.split(".")[0] if name else "unknown"


def gpu_type_for_node(node: str) -> str:
    return GPU_NODE_MAP.get(node, "unknown")


def normalise_device_name(device_name: str, fallback: str) -> str:
    if not device_name:
        return fallback
    if "V100" in device_name:
        return "NVIDIA_V100"
    if "L40S" in device_name or "L40" in device_name:
        return "NVIDIA_L40S"
    if "MI50" in device_name or "gfx906" in device_name:
        return "AMD_MI50"
    if "1080" in device_name:
        return "NVIDIA_GTX1080"
    return fallback


def safe_get(ad, key, default=None):
    try:
        val = ad.get(key)
        return val if val is not None else default
    except Exception:
        return default


def safe_int(ad, key, default=0):
    val = safe_get(ad, key, None)
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str):
        try:
            return int(float(val))
        except ValueError:
            pass
    return default


def safe_float(ad, key, default=0.0):
    """Like safe_int but preserves fractional values. Use for CPU-seconds, etc."""
    val = safe_get(ad, key, None)
    if val is not None:
        try:
            return float(val)
        except (ValueError, TypeError):
            pass
    return default


def _compute_actual_memory_mb(job) -> float:
    """Pick the most accurate actual-memory value from a schedd/job ClassAd.

    Priority: ResidentSetSize (KB → MB) → MemoryUsage (MB) → ImageSize (KB → MB).
    Deliberately excludes MemoryProvisioned — that attribute is the cgroup LIMIT
    (= RequestMemory), not actual usage. Falling through to it makes the RAM
    panel read "max all the time" even when the job is using a fraction of it.
    Returns 0.0 when no source has data.
    """
    rss_kb = safe_int(job, "ResidentSetSize", 0)
    if rss_kb > 0:
        return rss_kb / 1024.0
    mem_usage_mb = safe_int(job, "MemoryUsage", 0)
    if mem_usage_mb > 0:
        return float(mem_usage_mb)
    img_mb = safe_int(job, "ImageSize", 0) / 1024.0
    if img_mb > 0:
        return img_mb
    return 0.0


def _build_startd_job_constraint(nodes) -> str:
    """ClassAd constraint for the Startd query: match any running job
    (CPU or GPU) on the given nodes. Matches both CPU and GPU jobs
    (anything with a JobId)."""
    node_cons = " || ".join(
        f'Machine =?= "{node}.nikhef.nl"' for node in nodes
    )
    return f"(JobId isnt undefined) && ({node_cons})"


def _invalidate_htcondor_cache() -> None:
    """Drop the cached startd-collector result so the next query re-fetches.

    Called by scrape_personal when it sees a job the previous iteration
    didn't have — otherwise the new job's memory metric is stuck on stale
    data for up to _HTCONDOR_GPU_CACHE_TTL seconds.
    """
    global _htcondor_gpu_cache, _htcondor_gpu_cache_time
    _htcondor_gpu_cache = {}
    _htcondor_gpu_cache_time = 0


# =============================================================================
# Main scrape — detail user only
# =============================================================================

_PERSONAL_GAUGES = [
    user_jobs_running, user_jobs_queued, user_units_in_use,
    job_duration_seconds, job_vram_allocated_mb, job_memory_requested_mb,
]


def scrape_personal(collector_host: str, detail_user: str) -> None:
    """Fast scrape: only the detail user's own running jobs.
    Updates per-job metrics and _current_node_jobs for the startd-fallback loop.
    """
    t0 = time.time()
    now = t0

    for g in _PERSONAL_GAUGES:
        g.clear()
    try:
        coll = htcondor.Collector(collector_host)
        schedd_ads = coll.query(
            htcondor.AdTypes.Schedd,
            projection=["Name", "MyAddress", "CondorVersion"],
        )

        node_jobs: list = []
        acc_running: dict = defaultdict(int)
        acc_queued: dict = defaultdict(int)
        acc_units: dict = defaultdict(int)
        seen_mem_labels: set = set()
        seen_cpu_labels: set = set()

        for schedd_ad in schedd_ads:
            try:
                schedd = htcondor.Schedd(schedd_ad)
                jobs = schedd.query(
                    constraint=f'Owner == "{detail_user}"',
                    projection=[
                        "ClusterId", "ProcId", "JobStatus",
                        "RequestGPUs", "RequestCpus", "RequestMemory",
                        "MemoryUsage", "ImageSize", "ResidentSetSize",
                        "TotalJobRunningCpuUsage",
                        "RemoteUserCpu", "RemoteSysCpu",
                        "JobStartDate", "RemoteHost", "LastRemoteHost",
                    ],
                )
            except Exception as exc:
                log.warning(f"Personal scrape schedd {safe_get(schedd_ad, 'Name', '?')}: {exc}")
                continue

            for job in jobs:
                status = safe_int(job, "JobStatus", 0)
                req_gpus = safe_int(job, "RequestGPUs", 0)
                req_cpus = safe_int(job, "RequestCpus", 1)
                req_mem_mb = safe_int(job, "RequestMemory", 0)
                rss_kb = safe_int(job, "ResidentSetSize", 0)
                sched_mem_usage_mb = safe_int(job, "MemoryUsage", 0)
                # Only count "real" memory (RSS or MemoryUsage), not the
                # ImageSize static fallback — the startd fallback is much more
                # accurate than ImageSize and we don't want to clobber it.
                sched_mem_mb = (rss_kb / 1024.0) if rss_kb > 0 else (
                    float(sched_mem_usage_mb) if sched_mem_usage_mb > 0 else 0.0
                )
                total_cpu_secs = safe_float(job, "TotalJobRunningCpuUsage", 0)
                user_cpu_secs = safe_float(job, "RemoteUserCpu", 0) + safe_float(job, "RemoteSysCpu", 0)
                # Only count real CPU usage — a 0 here usually means the startd
                # hasn't reported to the schedd yet, so the startd ClassAd fallback
                # has fresher data.
                sched_cpu_secs = total_cpu_secs if total_cpu_secs > 0 else (
                    user_cpu_secs if user_cpu_secs > 0 else 0.0
                )
                is_gpu = req_gpus >= 1
                cluster = "gpu" if is_gpu else "cpu"

                remote_host = safe_get(job, "RemoteHost",
                                       safe_get(job, "LastRemoteHost", ""))
                node = node_short(remote_host) if remote_host else "unknown"
                rtype = gpu_type_for_node(node) if is_gpu else "CPU"

                if status == 2:  # Running
                    job_start = safe_get(job, "JobStartDate", now) or now
                    duration = max(now - job_start, 0)
                    job_id = f"{safe_get(job, 'ClusterId', 0)}.{safe_get(job, 'ProcId', 0)}"
                    lbl = dict(cluster=cluster, user=detail_user,
                               job_id=job_id, resource_type=rtype, node=node)
                    lbl_tuple = (cluster, detail_user, job_id, rtype, node)
                    seen_mem_labels.add(lbl_tuple)
                    vram_per_gpu = _vram_per_gpu_by_node.get(node, 0)

                    acc_running[cluster] += 1
                    acc_units[cluster] += req_gpus if is_gpu else req_cpus

                    job_duration_seconds.labels(**lbl).set(duration)
                    job_memory_requested_mb.labels(**lbl).set(req_mem_mb)
                    if sched_mem_mb > 0:
                        job_memory_usage_mb.labels(**lbl).set(sched_mem_mb)
                    if sched_cpu_secs > 0:
                        cpu_eff = sched_cpu_secs / max(duration * max(req_cpus, 1), 1e-6)
                        job_cpu_efficiency.labels(**lbl).set(cpu_eff)
                        seen_cpu_labels.add(lbl_tuple)
                    job_vram_allocated_mb.labels(**lbl).set(req_gpus * vram_per_gpu)

                    # Track for startd-fallback loop
                    node_jobs.append(dict(
                        job_id=job_id, node=node,
                        req_cpus=req_cpus, cluster=cluster, user=detail_user,
                    ))

                elif status == 1:  # Queued
                    acc_queued[cluster] += 1

        # Cleanup stale job_memory_usage_mb entries for finished jobs
        global _personal_mem_labels
        for tup in list(_personal_mem_labels):
            if tup not in seen_mem_labels:
                try:
                    job_memory_usage_mb.remove(*tup)
                except (KeyError, ValueError):
                    pass
        # Invalidate the startd-fallback cache when a brand-new job appears,
        # so the next loop tick re-queries the collector immediately instead
        # of returning the previous run's stale result.
        if seen_mem_labels - _personal_mem_labels:
            _invalidate_htcondor_cache()
        _personal_mem_labels = seen_mem_labels

        # Same cleanup for job_cpu_efficiency
        global _personal_cpu_labels
        for tup in list(_personal_cpu_labels):
            if tup not in seen_cpu_labels:
                try:
                    job_cpu_efficiency.remove(*tup)
                except (KeyError, ValueError):
                    pass
        _personal_cpu_labels = seen_cpu_labels

        for cl in set(list(acc_running) + list(acc_queued)):
            user_jobs_running.labels(cluster=cl, user=detail_user).set(
                acc_running.get(cl, 0)
            )
            user_jobs_queued.labels(cluster=cl, user=detail_user).set(
                acc_queued.get(cl, 0)
            )
            user_units_in_use.labels(cluster=cl, user=detail_user).set(
                acc_units.get(cl, 0)
            )

        global _current_node_jobs
        _current_node_jobs = node_jobs

        log.debug(
            f"Personal scrape {time.time()-t0:.2f}s — "
            f"{sum(acc_running.values())} running, {sum(acc_queued.values())} queued"
        )

    except Exception as exc:
        log.error(f"Personal scrape failed: {exc}", exc_info=True)


# =============================================================================
# Startd ClassAd fallback (node_exporter on workers is unreachable from login)
# =============================================================================

def _fetch_htcondor_gpu_metrics(nodes: list) -> dict:
    """Query HTCondor collector for GPU/CPU/memory metrics via Startd ClassAds.
    Returns {job_id: {util_pct, mem_used_mb, mem_total_mb, cpu_usage, memory_usage_mb}}.
    Cached with 15s TTL to avoid hammering the collector.

    Matches both CPU and GPU jobs (anything with a JobId)."""
    global _htcondor_gpu_cache, _htcondor_gpu_cache_time

    with _htcondor_gpu_lock:
        now = time.time()
        if now - _htcondor_gpu_cache_time < _HTCONDOR_GPU_CACHE_TTL:
            return _htcondor_gpu_cache

        result = {}
        try:
            coll = htcondor.Collector(_collector_host)
            constraint = _build_startd_job_constraint(nodes)

            for ad in coll.query(htcondor.AdTypes.Startd, projection=[
                "JobId",
                "DeviceGPUsAverageUsage", "GPUsMemoryUsage",
                "GPUs_GlobalMemoryMb",
                "CPUsUsage", "ResidentSetSize", "MemoryUsage",
                "RequestCpus",
            ], constraint=constraint):
                job_id = ad.get("JobId")
                if not job_id:
                    continue
                entry = {}

                util = ad.get("DeviceGPUsAverageUsage")
                if util is not None:
                    entry["util_pct"] = float(util) * 100

                mem_used = ad.get("GPUsMemoryUsage")
                if mem_used is not None:
                    entry["mem_used_mb"] = float(mem_used)

                mem_total = ad.get("GPUs_GlobalMemoryMb")
                if mem_total is not None:
                    entry["mem_total_mb"] = float(mem_total)

                # CPU: real-time usage from the startd (hotter than schedd data)
                cpu_usage = ad.get("CPUsUsage")
                if cpu_usage is not None:
                    entry["cpu_usage"] = float(cpu_usage)

                # Memory: ResidentSetSize (KB) → MemoryUsage (MB). Startd RSS is
                # often 0/missing for CPU jobs (the lot cluster), so fall back to
                # MemoryUsage — same chain as the schedd path uses.
                mem_mb = _compute_actual_memory_mb(ad)
                if mem_mb > 0:
                    entry["memory_usage_mb"] = mem_mb

                if entry:
                    result[job_id] = entry
        except Exception as exc:
            log.warning(f"HTCondor metrics query failed: {exc}")

        _htcondor_gpu_cache = result
        _htcondor_gpu_cache_time = now
    return result


def _scrape_worker_nodes(node_jobs: list, now: float) -> None:
    """Populate memory / CPU / GPU utilisation from HTCondor Startd ClassAds.

    node_exporter on the worker nodes isn't reachable from the login node, so
    the Startd ClassAd is the only source of real-time per-job metrics for
    most of the cluster."""
    if not node_jobs:
        return
    nodes = list({j["node"] for j in node_jobs if j["node"] != "unknown"})
    if not nodes:
        return

    try:
        htcondor_metrics = _fetch_htcondor_gpu_metrics(nodes)
    except Exception as exc:
        log.warning(f"Startd metrics fallback failed: {exc}")
        return

    for j in node_jobs:
        job_id = j["job_id"]
        node = j["node"]
        m = htcondor_metrics.get(job_id, {})
        cluster, user = j["cluster"], j["user"]
        rtype = "CPU" if cluster == "cpu" else gpu_type_for_node(node)

        lbl_gpu = dict(cluster=cluster, user=user, job_id=job_id, node=node)
        lbl_job = dict(cluster=cluster, user=user, job_id=job_id,
                       node=node, resource_type=rtype)

        if rtype != "CPU":
            if m.get("util_pct") is not None:
                job_gpu_utilization_pct.labels(**lbl_gpu).set(m["util_pct"])

        if m.get("cpu_usage") is not None:
            req_cpus = j.get("req_cpus", 1)
            eff = float(m["cpu_usage"]) / max(req_cpus, 1)
            job_cpu_efficiency.labels(**lbl_job).set(eff)

        if m.get("memory_usage_mb") is not None:
            job_memory_usage_mb.labels(**lbl_job).set(m["memory_usage_mb"])


def _startd_loop(interval: int) -> None:
    """Fast loop that polls Startd ClassAd metrics independently of the HTCondor scrape."""
    while True:
        jobs = _current_node_jobs
        if jobs:
            try:
                _scrape_worker_nodes(jobs, time.time())
            except Exception as exc:
                log.error(f"Startd fast-loop error: {exc}", exc_info=True)
        time.sleep(interval)


# =============================================================================
# Log file HTTP server  (optional — only started when --log-dir is given)
# =============================================================================

_log_dir: str = ""


class _LogHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        path = self.path.split("?")[0]
        if path.startswith("/logs/"):
            job_id = path[6:]
            if job_id and job_id not in (".*", ""):
                self._serve_job(job_id)
                return
        self._serve_index()

    def _serve_job(self, job_id: str):
        candidates = [f"{job_id}.out"]
        if "." in job_id:
            candidates.append(f"{job_id.replace('.', '_')}.out")
        for name in candidates:
            p = os.path.join(_log_dir, name)
            if os.path.isfile(p):
                self._send_log(p, name)
                return
        import glob as _glob
        for pattern in {f"*{job_id}.out", f"*{job_id.replace('.', '_')}.out"}:
            globbed = _glob.glob(os.path.join(_log_dir, pattern))
            if globbed:
                p = globbed[0]
                self._send_log(p, os.path.basename(p))
                return
        msg = f"No log found for job {job_id}\n\nTried:\n" + "\n".join(
            f"  {os.path.join(_log_dir, c)}" for c in candidates
        ) + "\n  *{id}.out, *{us}.out (glob)".format(id=job_id, us=job_id.replace('.', '_'))
        self._respond(404, "text/plain", msg.encode())

    def _send_log(self, path: str, filename: str):
        import html as _html
        try:
            with open(path, encoding="utf-8", errors="replace") as fh:
                lines = fh.readlines()
        except OSError as e:
            self._respond(500, "text/plain", str(e).encode())
            return
        content = "".join(lines[-200:])
        total = len(lines)
        page = (
            "<!DOCTYPE html><html><head><meta charset='utf-8'>"
            "<meta http-equiv='refresh' content='5'>"
            "<style>body{margin:0;padding:10px;background:#0d0d0d;color:#c8c8c8;"
            "font-family:'Courier New',Courier,monospace;font-size:12px;}"
            ".hdr{color:#555;font-size:10px;margin-bottom:6px;}"
            "pre{margin:0;white-space:pre-wrap;word-break:break-all;}</style>"
            "</head><body>"
            f"<div class='hdr'>{_html.escape(filename)}"
            f" — {total} lines total (last 200 shown)</div>"
            f"<pre>{_html.escape(content)}</pre>"
            "<script>window.scrollTo(0,document.body.scrollHeight)</script>"
            "</body></html>"
        )
        self._respond(200, "text/html; charset=utf-8", page.encode("utf-8"))

    def _serve_index(self):
        import html as _html
        try:
            names = sorted(f for f in os.listdir(_log_dir) if f.endswith(".out"))
        except OSError:
            names = []
        items = "".join(f'<li><a href="/logs/{n[:-4]}">{n}</a></li>' for n in names)
        page = (
            "<!DOCTYPE html><html><head><meta charset='utf-8'>"
            "<style>body{margin:0;padding:12px;background:#0d0d0d;color:#c8c8c8;"
            "font-family:sans-serif;font-size:13px;}a{color:#6ec0e8;}p{color:#888;}</style>"
            "</head><body>"
            "<p>Select a specific job in the Grafana dropdown to view its log.</p>"
            f"<p>Log directory: <code>{_html.escape(_log_dir)}</code></p>"
            f"<ul>{items or '<li><em>No .out files found</em></li>'}</ul>"
            "</body></html>"
        )
        self._respond(200, "text/html; charset=utf-8", page.encode("utf-8"))

    def _respond(self, code: int, content_type: str, body: bytes):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass  # suppress access logs


def _start_log_server(port: int):
    srv = http.server.ThreadingHTTPServer(("", port), _LogHandler)
    threading.Thread(target=srv.serve_forever, daemon=True, name="log-server").start()
    log.info(f"Log server on :{port} — serving {_log_dir!r}")


# =============================================================================
# Entry point
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Prometheus exporter for Stoomboot GPU+CPU jobs (detail user)")
    parser.add_argument("--port", type=int, default=9118,
                        help="Port to expose metrics on (default: 9118)")
    parser.add_argument("--collector", type=str, default="stbc-019.nikhef.nl",
                        help="HTCondor collector hostname (default: stbc-019.nikhef.nl)")
    parser.add_argument("--interval", type=int, default=3,
                        help="Scrape interval in seconds (default: 3)")
    parser.add_argument("--detail-user", type=str, default="your_username",
                        help="User whose jobs are tracked (default: your_username)")
    parser.add_argument("--log-dir", type=str, default="",
                        help="Directory containing .out log files; enables log HTTP server")
    parser.add_argument("--log-port", type=int, default=9119,
                        help="Port for the log HTTP server (default: 9119)")
    args = parser.parse_args()

    log.info("Starting Stoomboot Prometheus exporter")
    log.info(f"  bindings:       {_binding}")
    log.info(f"  port:           {args.port}")
    log.info(f"  collector:      {args.collector}")
    log.info(f"  detail user:    {args.detail_user} (loop every {args.interval}s)")

    global _collector_host
    _collector_host = args.collector

    if args.log_dir:
        global _log_dir
        _log_dir = args.log_dir
        log.info(f"  log dir:        {args.log_dir}")
        log.info(f"  log port:       {args.log_port}")

    start_http_server(args.port)
    log.info(f"Metrics at http://localhost:{args.port}/metrics")

    if args.log_dir:
        try:
            _start_log_server(args.log_port)
        except OSError as exc:
            log.warning(f"Log server port {args.log_port} in use ({exc}) — skipping log server")

    # Startd fast poll (thread)
    threading.Thread(
        target=_startd_loop,
        args=(args.interval,),
        daemon=True, name="startd-loop",
    ).start()

    # Personal job fast poll (main thread)
    while True:
        scrape_personal(args.collector, args.detail_user)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
