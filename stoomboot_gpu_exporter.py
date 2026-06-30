#!/usr/bin/env python3
"""
stoomboot_gpu_exporter.py
─────────────────────────
Prometheus exporter for job monitoring on the Stoomboot HTCondor cluster.
Covers BOTH the GPU and CPU portions of the cluster — every metric carries a
`cluster="gpu"` or `cluster="cpu"` label so a single Grafana dashboard can
switch between them with one dropdown.

Exposes:
  • per-user aggregates (running/queued jobs, compute-seconds, memory efficiency)
    — for ALL users, both clusters
  • cluster-wide slot/utilisation stats — both clusters
  • per-job detail (duration, requests, memory) — GPU jobs for EVERYONE
    (small cluster, lets you see who's hogging), plus CPU jobs for the
    configured "detail user" only (keeps Prometheus cardinality sane given
    the CPU cluster can have thousands of jobs)

Usage:
    python stoomboot_gpu_exporter.py [--port 9118] [--collector stbc-019.nikhef.nl]
                                     [--interval 15] [--detail-user your_username]

Requirements:
    pip install prometheus_client htcondor

Run from an interactive node (e.g. wn-lot-001) where the htcondor Python bindings
are available. If not pip-installable, they're usually on the system already
(module load condor, or system-wide on stbc nodes).
"""

import argparse
import glob as _glob_mod
import html as _html_mod
import http.server
import logging
import os
import re
import subprocess
import threading
import time
import urllib.request
from collections import defaultdict
from typing import Optional

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

from prometheus_client import (
    start_http_server, Gauge, REGISTRY, PROCESS_COLLECTOR, PLATFORM_COLLECTOR,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("stbc_exporter")

# ─── Remove default process/platform metrics (cleaner for this use case) ──────
REGISTRY.unregister(PROCESS_COLLECTOR)
REGISTRY.unregister(PLATFORM_COLLECTOR)

# =============================================================================
# Metric definitions
# Naming: stoomboot_*  (the `cluster` label distinguishes gpu vs cpu)
# `resource_type` = GPU model (NVIDIA_L40S, …) for the gpu cluster, "CPU" for cpu.
# =============================================================================

# ── Per-user aggregates (BOTH clusters, ALL users) ───────────────────────────
user_jobs_running = Gauge(
    "stoomboot_user_jobs_running",
    "Number of jobs currently running per user",
    ["cluster", "user"],
)
user_jobs_queued = Gauge(
    "stoomboot_user_jobs_queued",
    "Number of jobs currently queued (idle) per user",
    ["cluster", "user"],
)
user_compute_seconds = Gauge(
    "stoomboot_user_compute_seconds",
    "Total compute-seconds in flight per user "
    "(GPU-seconds on the gpu cluster, core-seconds on the cpu cluster)",
    ["cluster", "user"],
)
user_units_in_use = Gauge(
    "stoomboot_user_units_in_use",
    "Compute units currently held per user "
    "(GPUs on the gpu cluster, CPU cores on the cpu cluster)",
    ["cluster", "user"],
)
user_memory_efficiency = Gauge(
    "stoomboot_user_memory_efficiency_ratio",
    "Avg actual/requested memory across the user's running jobs (<0.5 = wasteful)",
    ["cluster", "user"],
)

# ── Cluster-wide slot / utilisation (BOTH clusters) ──────────────────────────
slots_total = Gauge(
    "stoomboot_slots_total",
    "Total compute units in the cluster (GPUs or CPU cores)",
    ["cluster", "resource_type"],
)
slots_claimed_total = Gauge(
    "stoomboot_slots_claimed_total",
    "Claimed compute units in the cluster (GPUs or CPU cores)",
    ["cluster", "resource_type"],
)
slots_idle_total = Gauge(
    "stoomboot_slots_idle_total",
    "Idle/unclaimed compute units in the cluster (GPUs or CPU cores)",
    ["cluster", "resource_type"],
)
utilisation_ratio = Gauge(
    "stoomboot_utilisation_ratio",
    "Fraction of compute units in use (0–1)",
    ["cluster", "resource_type"],
)
slots_claimed_by_user = Gauge(
    "stoomboot_slots_claimed_by_user",
    "Compute units currently claimed, by user/resource_type/node",
    ["cluster", "user", "resource_type", "node"],
)

# ── Cluster-wide CPU and memory (tracked separately from GPU slot counts) ─────
cluster_cpus_total = Gauge(
    "stoomboot_cluster_cpus_total",
    "Total CPU cores on nodes of this cluster type",
    ["cluster"],
)
cluster_cpus_claimed = Gauge(
    "stoomboot_cluster_cpus_claimed",
    "Claimed CPU cores on nodes of this cluster type",
    ["cluster"],
)
cluster_memory_total_mb = Gauge(
    "stoomboot_cluster_memory_total_mb",
    "Total memory (MB) on nodes of this cluster type",
    ["cluster"],
)
cluster_memory_claimed_mb = Gauge(
    "stoomboot_cluster_memory_claimed_mb",
    "Claimed/allocated memory (MB) on nodes of this cluster type",
    ["cluster"],
)

# ── Per-job detail (GPU: everyone; CPU: detail-user only) ────────────────────
job_duration_seconds = Gauge(
    "stoomboot_job_duration_seconds",
    "Wall-clock time a running job has been executing (seconds)",
    ["cluster", "user", "job_id", "resource_type", "node"],
)
job_gpus_requested = Gauge(
    "stoomboot_job_gpus_requested",
    "GPUs requested by the job",
    ["cluster", "user", "job_id", "resource_type", "node"],
)
job_cpus_requested = Gauge(
    "stoomboot_job_cpus_requested",
    "CPUs requested by the job",
    ["cluster", "user", "job_id", "resource_type", "node"],
)
job_memory_requested_mb = Gauge(
    "stoomboot_job_memory_requested_mb",
    "Memory requested by the job (MB)",
    ["cluster", "user", "job_id", "resource_type", "node"],
)
job_memory_usage_mb = Gauge(
    "stoomboot_job_memory_usage_mb",
    "Actual memory used by the job (MB) — from MemoryUsage ClassAd (falls back to ImageSize/1024)",
    ["cluster", "user", "job_id", "resource_type", "node"],
)

# Track active job_memory_usage_mb labels per loop so we can clean up terminated
# jobs without clearing the gauge and losing last-known-good values.
_cluster_mem_labels: set[tuple[str, str, str, str, str]] = set()
_personal_mem_labels: set[tuple[str, str, str, str, str]] = set()
# Same idea for job_cpu_efficiency — the worker-node fallback (startd
# ClassAds) is the only source for some jobs, and we don't want the personal
# loop wiping its values every 15s.
_personal_cpu_labels: set[tuple[str, str, str, str, str]] = set()

job_cpu_efficiency = Gauge(
    "stoomboot_job_cpu_efficiency",
    "CPU efficiency ratio: TotalJobRunningCpuUsage / (duration_s × requested_cpus); 1.0 = fully utilising requested cores",
    ["cluster", "user", "job_id", "resource_type", "node"],
)
job_vram_allocated_mb = Gauge(
    "stoomboot_job_vram_allocated_mb",
    "VRAM allocated to this job (MB): RequestGPUs × GPUs_GlobalMemoryMb on the assigned node",
    ["cluster", "user", "job_id", "resource_type", "node"],
)

# ── Worker-node metrics (scraped from node_exporter on the worker) ────────────
job_gpu_utilization_pct = Gauge(
    "stoomboot_job_gpu_utilization_pct",
    "GPU compute utilization % (0–100) from node_exporter on the worker node",
    ["cluster", "user", "job_id", "node"],
)
job_gpu_memory_used_mb = Gauge(
    "stoomboot_job_gpu_memory_used_mb",
    "GPU memory in use (MiB) for the job's assigned GPU",
    ["cluster", "user", "job_id", "node"],
)
job_gpu_memory_total_mb = Gauge(
    "stoomboot_job_gpu_memory_total_mb",
    "Total GPU memory (MiB) for the job's assigned GPU",
    ["cluster", "user", "job_id", "node"],
)
job_cgroup_memory_rss_mb = Gauge(
    "stoomboot_job_cgroup_memory_rss_mb",
    "RSS memory used (MiB) from cgroup — more accurate than HTCondor MemoryUsage",
    ["cluster", "user", "job_id", "node"],
)
job_cgroup_cpu_pct = Gauge(
    "stoomboot_job_cgroup_cpu_pct",
    "CPU usage % derived from cgroup cpu_seconds rate (100 = 1 core fully used)",
    ["cluster", "user", "job_id", "node"],
)
job_status_gauge = Gauge(
    "stoomboot_job_status",
    "HTCondor job status (1=idle/queued, 2=running) — emitted for all tracked jobs so idle jobs appear in dropdowns",
    ["cluster", "user", "job_id"],
)

# ── Exporter health (for the 'targets online' / freshness tiles) ─────────────
exporter_up = Gauge(
    "stoomboot_exporter_up",
    "1 while the exporter process is serving metrics",
)
last_scrape_success = Gauge(
    "stoomboot_exporter_last_scrape_success",
    "1 if the last scrape succeeded, 0 otherwise",
)
last_scrape_duration_seconds = Gauge(
    "stoomboot_exporter_last_scrape_duration_seconds",
    "Duration of the last metrics scrape (seconds)",
)
last_scrape_timestamp = Gauge(
    "stoomboot_exporter_last_scrape_timestamp_seconds",
    "Unix time of the last successful scrape",
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


# ── Worker-node scraping config ───────────────────────────────────────────────
_node_exporter_port: int = 9100

# Candidate metric names in priority order (first match wins)
_GPU_UTIL_NAMES = [
    "nvidia_gpu_duty_cycle",       # nvidia-prometheus / node_exporter
    "DCGM_FI_DEV_GPU_UTIL",       # dcgm-exporter (0–100)
    "gpu_utilization_percent",
    "gpu_usage_percent",
]
_GPU_MEM_USED_NAMES = [
    "nvidia_gpu_memory_used_bytes",   # bytes
    "DCGM_FI_DEV_FB_USED",           # MiB
    "gpu_memory_used_bytes",
]
_GPU_MEM_TOTAL_NAMES = [
    "nvidia_gpu_memory_total_bytes",  # bytes
    "DCGM_FI_DEV_FB_TOTAL",          # MiB
    "gpu_memory_total_bytes",
]
_CGROUP_RSS_NAMES = [
    "cgroup_memory_rss_bytes",        # Nikhef custom exporter (bytes)
    "cgroup_memory_used_bytes",
    "container_memory_rss",           # cadvisor (bytes)
]
_CGROUP_CPU_NAMES = [
    "cgroup_cpu_usage_seconds_total",     # Nikhef custom (seconds or nanoseconds)
    "container_cpu_usage_seconds_total",  # cadvisor (nanoseconds)
]

# Rate-tracking for cgroup CPU: {(job_id, node): (timestamp, counter_value)}
_prev_cgroup_cpu: dict = {}

# Shared state: jobs to poll, updated by the personal HTCondor loop
_current_node_jobs: list = []

# VRAM cache: node → MiB per GPU, updated by the cluster loop
_vram_per_gpu_by_node: dict = {}

# HTCondor collector host (set from --collector in main())
_collector_host: str = "stbc-019.nikhef.nl"

# HTCondor GPU metrics cache (avoids querying collector every loop iteration)
_htcondor_gpu_cache: dict = {}   # {job_id: {util_pct, mem_used_mb, mem_total_mb, pid}}
_htcondor_gpu_cache_time: float = 0
_HTCONDOR_GPU_CACHE_TTL: float = 15  # seconds


def _invalidate_htcondor_cache() -> None:
    """Drop the cached startd-collector result so the next query re-fetches.

    Called by scrape_personal when it sees a job the previous iteration
    didn't have — otherwise the new job's memory metric is stuck on stale
    data for up to _HTCONDOR_GPU_CACHE_TTL seconds (the user sees the
    metric only after a 15s delay on every new run).
    """
    global _htcondor_gpu_cache, _htcondor_gpu_cache_time
    _htcondor_gpu_cache = {}
    _htcondor_gpu_cache_time = 0


# SSH /proc polling — real-time RSS that bypasses the startd's slow
# 1-5 minute ClassAd update interval. One SSH per node per scrape,
# throttled per-job to avoid hammering the worker.
_last_ssh_poll: dict = {}        # {(job_id, node): last_poll_unix_time}
_ssh_poll_interval: float = 10.0  # seconds; set by --ssh-interval
_ssh_user: str = ""              # set by --ssh-user; empty disables SSH path
_SSH_TIMEOUT: float = 3.0        # per-call hard timeout


def _should_ssh_poll(job_id: str, node: str) -> bool:
    """Return True if it's been longer than _ssh_poll_interval since we last
    SSH-polled this job. New (never-polled) jobs always return True."""
    if not _ssh_user:
        return False
    last = _last_ssh_poll.get((job_id, node))
    if last is None:
        return True
    return (time.time() - last) >= _ssh_poll_interval


def _mark_ssh_polled(job_id: str, node: str) -> None:
    _last_ssh_poll[(job_id, node)] = time.time()


def _parse_proc_status_batch(stdout: str, pids: list) -> dict:
    """Parse the output of a batched SSH that read /proc/<pid>/status for
    each pid. Output format: blocks separated by `==<pid>==` markers.
    Returns {pid: mem_mb} — PIDs whose block is missing or has no VmRSS
    are silently skipped."""
    result: dict = {}
    # Split on the delimiter; blocks[0] is whatever came before the first marker
    blocks = re.split(r"==(\d+)==", stdout)
    # blocks looks like: ['', '100', '<status of 100>', '200', '<status of 200>', ...]
    for i in range(1, len(blocks) - 1, 2):
        try:
            pid = int(blocks[i])
        except ValueError:
            continue
        body = blocks[i + 1]
        m = re.search(r"VmRSS:\s+(\d+)\s+kB", body)
        if m:
            result[pid] = float(m.group(1)) / 1024.0
    return result


def _fetch_ssh_memory_batch(node: str, pids: list) -> dict:
    """SSH to `node` and read /proc/<pid>/status for each pid in pids.
    Returns {pid: mem_mb} on success, {} on any failure (timeout,
    unreachable, auth error, missing process). One SSH session per node
    regardless of how many PIDs are requested — uses `==<pid>==` markers
    in the output to delimit per-process blocks."""
    if not pids or not _ssh_user:
        return {}
    ssh_target = f"{_ssh_user}@{node}.nikhef.nl"
    # One remote command per PID, chained with `;`; each block delimited
    # by an echo marker so we can split cleanly. `2>/dev/null` swallows
    # "No such file or directory" for PIDs that have exited.
    parts = []
    for pid in pids:
        parts.append(f"echo =={pid}==; cat /proc/{pid}/status 2>/dev/null")
    remote_cmd = " ; ".join(parts)

    try:
        completed = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", f"ConnectTimeout={int(_SSH_TIMEOUT)}",
             "-o", "StrictHostKeyChecking=accept-new",
             ssh_target, remote_cmd],
            capture_output=True, text=True, timeout=_SSH_TIMEOUT, check=False,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        log.debug(f"ssh {ssh_target} failed: {exc}")
        return {}

    if completed.returncode != 0:
        log.debug(f"ssh {ssh_target} rc={completed.returncode}: "
                  f"{(completed.stderr or '')[:200]}")
        return {}

    return _parse_proc_status_batch(completed.stdout, pids)


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
    try:
        eval_val = ad.eval(key)
        if isinstance(eval_val, (int, float)):
            return int(eval_val)
        if isinstance(eval_val, str):
            return int(float(eval_val))
    except Exception:
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
    try:
        return float(ad.eval(key))
    except Exception:
        return default


def _compute_actual_memory_mb(job) -> float:
    """Pick the most accurate actual-memory value from a schedd job ClassAd.

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
    """ClassAd constraint for the Startd-fallback query: match any running job
    (CPU or GPU) on the given nodes.

    Previously this filter required `AssignedGPUs isnt undefined`, which silently
    dropped every CPU job — those jobs then had no fallback when the schedd's
    ResidentSetSize / MemoryUsage ClassAds were missing or stale.
    """
    node_cons = " || ".join(
        f'Machine =?= "{node}.nikhef.nl"' for node in nodes
    )
    return f"(JobId isnt undefined) && ({node_cons})"


# =============================================================================
# Scrape
# =============================================================================

def _clear_all():
    for g in (
        user_jobs_running, user_jobs_queued, user_compute_seconds,
        user_units_in_use, user_memory_efficiency,
        slots_total, slots_claimed_total, slots_idle_total, utilisation_ratio,
        slots_claimed_by_user,
        cluster_cpus_total, cluster_cpus_claimed,
        cluster_memory_total_mb, cluster_memory_claimed_mb,
        job_duration_seconds, job_gpus_requested, job_cpus_requested,
        job_memory_requested_mb, job_cpu_efficiency,
        job_vram_allocated_mb,
        job_gpu_utilization_pct, job_gpu_memory_used_mb, job_gpu_memory_total_mb,
        job_cgroup_memory_rss_mb, job_cgroup_cpu_pct,
    ):
        g.clear()


def scrape(collector_host: str, detail_user: str):
    """Query HTCondor and update all Prometheus metrics for gpu + cpu clusters."""
    t0 = time.time()
    now = time.time()

    try:
        coll = htcondor.Collector(collector_host)
        _clear_all()

        # ─────────────────────────────────────────────────────────────────────
        # 1. Slots (machines).  Query ALL startd ads, bucket into gpu vs cpu.
        # ─────────────────────────────────────────────────────────────────────
        slot_ads = coll.query(
            htcondor.AdTypes.Startd,
            projection=[
                "Name", "State", "Activity", "RemoteUser",
                "Cpus", "TotalCpus",
                "Memory", "TotalMemory",
                "GPUs", "GPUs_DeviceName", "GPUs_GlobalMemoryMb", "TotalGPUs",
                "SlotType",
            ],
        )

        # gpu accounting: deduplicate totals by node so partitionable + dynamic
        # child slots don't each contribute TotalGPUs to the sum.
        # gpu_total_by_node: node -> (rtype, total_gpus) — highest seen per node.
        gpu_total_by_node = {}
        gpu_claimed = defaultdict(int)
        gpu_cpu_total_by_node: dict[str, int] = {}
        gpu_cpu_claimed = 0
        gpu_mem_total_by_node: dict[str, int] = {}
        gpu_mem_claimed = 0
        vram_per_gpu_by_node: dict[str, int] = {}
        # cpu accounting: dedupe machine totals so partitionable slots don't
        # multiply TotalCpus; claimed cores summed from Claimed slots.
        cpu_total_by_machine = {}        # node -> TotalCpus (machine cores)
        cpu_claimed = 0
        cpu_mem_total_by_machine: dict[str, int] = {}
        cpu_mem_claimed = 0

        for ad in slot_ads:
            machine = safe_get(ad, "Name", "unknown")
            node = node_short(machine)
            state = safe_get(ad, "State", "unknown")
            n_gpus = safe_int(ad, "GPUs", 0)
            total_gpus = safe_int(ad, "TotalGPUs", 0)
            n_cpus = safe_int(ad, "Cpus", 0)
            total_cpus = safe_int(ad, "TotalCpus", 0)
            n_mem_mb = safe_int(ad, "Memory", 0)
            total_mem_mb = safe_int(ad, "TotalMemory", 0)

            is_gpu = (n_gpus >= 1) or (total_gpus >= 1)

            if is_gpu:
                rtype = normalise_device_name(
                    safe_get(ad, "GPUs_DeviceName", ""), gpu_type_for_node(node)
                )
                # Record the highest total seen for this node (TotalGPUs stays
                # stable across all slot ads for the node; n_gpus may shrink as
                # dynamic slots are carved out).
                node_total = max(total_gpus, n_gpus)
                existing = gpu_total_by_node.get(node)
                if existing is None or node_total > existing[1]:
                    gpu_total_by_node[node] = (rtype, node_total)
                # Track CPU and memory capacity of GPU nodes (deduplicated by node)
                if total_cpus > 0:
                    gpu_cpu_total_by_node[node] = max(gpu_cpu_total_by_node.get(node, 0), total_cpus)
                if total_mem_mb > 0:
                    gpu_mem_total_by_node[node] = max(gpu_mem_total_by_node.get(node, 0), total_mem_mb)
                vram = safe_int(ad, "GPUs_GlobalMemoryMb", 0)
                if vram > 0:
                    vram_per_gpu_by_node[node] = max(vram_per_gpu_by_node.get(node, 0), vram)

                # Only count actually-assigned GPUs in claimed slots.
                if state == "Claimed" and n_gpus >= 1:
                    user = safe_get(ad, "RemoteUser", "unknown").split("@")[0]
                    gpu_claimed[rtype] += n_gpus
                    gpu_cpu_claimed += n_cpus
                    gpu_mem_claimed += n_mem_mb
                    slots_claimed_by_user.labels(
                        cluster="gpu", user=user, resource_type=rtype, node=node
                    ).inc(n_gpus)
            else:
                # CPU machine: record machine core total once (max seen)
                if total_cpus > 0:
                    cpu_total_by_machine[node] = max(
                        cpu_total_by_machine.get(node, 0), total_cpus
                    )
                if total_mem_mb > 0:
                    cpu_mem_total_by_machine[node] = max(
                        cpu_mem_total_by_machine.get(node, 0), total_mem_mb
                    )
                if state == "Claimed":
                    user = safe_get(ad, "RemoteUser", "unknown").split("@")[0]
                    claimed_cores = max(n_cpus, 1)
                    cpu_claimed += claimed_cores
                    cpu_mem_claimed += n_mem_mb
                    slots_claimed_by_user.labels(
                        cluster="cpu", user=user, resource_type="CPU", node=node
                    ).inc(claimed_cores)

        # Share VRAM cache with personal scrape
        global _vram_per_gpu_by_node
        _vram_per_gpu_by_node = dict(vram_per_gpu_by_node)

        # Collapse per-node totals into per-rtype totals
        gpu_total: dict[str, int] = defaultdict(int)
        for _node, (rtype, total) in gpu_total_by_node.items():
            gpu_total[rtype] += total

        # Publish GPU slot stats
        for rtype, total in gpu_total.items():
            claimed = gpu_claimed.get(rtype, 0)
            slots_total.labels(cluster="gpu", resource_type=rtype).set(total)
            slots_claimed_total.labels(cluster="gpu", resource_type=rtype).set(claimed)
            slots_idle_total.labels(cluster="gpu", resource_type=rtype).set(
                max(total - claimed, 0)
            )
            utilisation_ratio.labels(cluster="gpu", resource_type=rtype).set(
                claimed / total if total > 0 else 0.0
            )

        # Publish CPU slot stats (resource_type="CPU"); totals are approximate
        # under partitionable slots — see module docstring.
        cpu_total = sum(cpu_total_by_machine.values())
        if cpu_total > 0 or cpu_claimed > 0:
            slots_total.labels(cluster="cpu", resource_type="CPU").set(cpu_total)
            slots_claimed_total.labels(cluster="cpu", resource_type="CPU").set(cpu_claimed)
            slots_idle_total.labels(cluster="cpu", resource_type="CPU").set(
                max(cpu_total - cpu_claimed, 0)
            )
            utilisation_ratio.labels(cluster="cpu", resource_type="CPU").set(
                cpu_claimed / cpu_total if cpu_total > 0 else 0.0
            )

        # Publish cluster-wide CPU and memory (both cluster types)
        cluster_cpus_total.labels(cluster="gpu").set(sum(gpu_cpu_total_by_node.values()))
        cluster_cpus_claimed.labels(cluster="gpu").set(gpu_cpu_claimed)
        cluster_memory_total_mb.labels(cluster="gpu").set(sum(gpu_mem_total_by_node.values()))
        cluster_memory_claimed_mb.labels(cluster="gpu").set(gpu_mem_claimed)
        cluster_cpus_total.labels(cluster="cpu").set(cpu_total)
        cluster_cpus_claimed.labels(cluster="cpu").set(cpu_claimed)
        cluster_memory_total_mb.labels(cluster="cpu").set(sum(cpu_mem_total_by_machine.values()))
        cluster_memory_claimed_mb.labels(cluster="cpu").set(cpu_mem_claimed)

        # ─────────────────────────────────────────────────────────────────────
        # 2. Jobs.  Query ALL jobs from every schedd, bucket gpu vs cpu.
        # ─────────────────────────────────────────────────────────────────────
        # htcondor2.Schedd(ad) requires CondorVersion in the ad.
        schedd_ads = coll.query(
            htcondor.AdTypes.Schedd,
            projection=["Name", "MyAddress", "CondorVersion"],
        )

        # per (cluster, user) accumulators
        acc_running = defaultdict(int)
        acc_queued = defaultdict(int)
        acc_compute_secs = defaultdict(float)
        acc_units = defaultdict(int)
        acc_mem_ratios = defaultdict(list)

        n_gpu_jobs = 0
        n_cpu_jobs = 0
        _node_jobs: list = []  # jobs to scrape node_exporter for
        seen_mem_labels: set[tuple[str, str, str, str, str]] = set()

        for schedd_ad in schedd_ads:
            try:
                schedd = htcondor.Schedd(schedd_ad)
                jobs = schedd.query(
                    projection=[
                        "ClusterId", "ProcId", "Owner", "JobStatus",
                        "RequestGPUs", "RequestCpus", "RequestMemory",
                        "MemoryUsage", "ImageSize", "ResidentSetSize",
                        "MemoryProvisioned",
                        "TotalJobRunningCpuUsage",
                        "RemoteUserCpu", "RemoteSysCpu",
                        "QDate", "JobStartDate",
                        "RemoteHost", "LastRemoteHost",
                        "AssignedGPUs",
                    ],
                )
            except Exception as e:
                log.warning(f"Failed to query schedd {safe_get(schedd_ad, 'Name', '?')}: {e}")
                continue

            for job in jobs:
                owner = safe_get(job, "Owner", "unknown")
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
                sched_cpu_secs = total_cpu_secs if total_cpu_secs > 0 else (
                    user_cpu_secs if user_cpu_secs > 0 else 0.0
                )

                is_gpu = req_gpus >= 1
                cluster = "gpu" if is_gpu else "cpu"
                units = req_gpus if is_gpu else req_cpus

                if is_gpu:
                    n_gpu_jobs += 1
                else:
                    n_cpu_jobs += 1

                remote_host = safe_get(job, "RemoteHost", safe_get(job, "LastRemoteHost", ""))
                node = node_short(remote_host) if remote_host else "unknown"
                rtype = gpu_type_for_node(node) if is_gpu else "CPU"

                key = (cluster, owner)

                if status == 2:  # Running
                    job_start = safe_get(job, "JobStartDate", now) or now
                    duration = max(now - job_start, 0)

                    acc_running[key] += 1
                    acc_units[key] += units
                    acc_compute_secs[key] += duration * units
                    if req_mem_mb > 0 and sched_mem_mb > 0:
                        acc_mem_ratios[key].append(sched_mem_mb / req_mem_mb)

                    # Queue detail_user jobs for the worker-node fallback (startd ClassAds).
                    # GPU jobs first try node_exporter; CPU jobs skip that and go straight
                    # to the startd fallback since CPU workers don't run node_exporter.
                    if owner == detail_user:
                        job_id = f"{safe_get(job, 'ClusterId', 0)}.{safe_get(job, 'ProcId', 0)}"
                        raw_assigned = safe_get(job, "AssignedGPUs", "") or ""
                        gpu_uuid = raw_assigned.strip('" ').lower().removeprefix("gpu-")
                        _node_jobs.append(dict(
                            job_id=job_id, node=node, gpu_uuid=gpu_uuid,
                            req_cpus=req_cpus, cluster=cluster, user=owner,
                        ))

                    # Per-job detail: GPU = everyone; CPU = detail_user only
                    if is_gpu or owner == detail_user:
                        job_id = f"{safe_get(job, 'ClusterId', 0)}.{safe_get(job, 'ProcId', 0)}"
                        lbl = dict(cluster=cluster, user=owner, job_id=job_id,
                                   resource_type=rtype, node=node)
                        seen_mem_labels.add((cluster, owner, job_id, rtype, node))
                        job_duration_seconds.labels(**lbl).set(duration)
                        job_gpus_requested.labels(**lbl).set(req_gpus)
                        job_cpus_requested.labels(**lbl).set(req_cpus)
                        job_memory_requested_mb.labels(**lbl).set(req_mem_mb)
                        if sched_mem_mb > 0:
                            job_memory_usage_mb.labels(**lbl).set(sched_mem_mb)
                        if sched_cpu_secs > 0:
                            cpu_eff = sched_cpu_secs / max(duration * max(req_cpus, 1), 1e-6)
                            job_cpu_efficiency.labels(**lbl).set(cpu_eff)
                        vram_per_gpu = vram_per_gpu_by_node.get(node, 0)
                        job_vram_allocated_mb.labels(**lbl).set(req_gpus * vram_per_gpu)
                        job_status_gauge.labels(cluster=cluster, user=owner, job_id=job_id).set(2)

                elif status == 1:  # Idle / queued
                    acc_queued[key] += 1
                    if is_gpu or owner == detail_user:
                        job_id = f"{safe_get(job, 'ClusterId', 0)}.{safe_get(job, 'ProcId', 0)}"
                        job_status_gauge.labels(cluster=cluster, user=owner, job_id=job_id).set(1)

        # Publish per-user aggregates
        all_keys = set(acc_running) | set(acc_queued)
        for (cluster, user) in all_keys:
            k = (cluster, user)
            user_jobs_running.labels(cluster=cluster, user=user).set(acc_running.get(k, 0))
            user_jobs_queued.labels(cluster=cluster, user=user).set(acc_queued.get(k, 0))
            user_compute_seconds.labels(cluster=cluster, user=user).set(acc_compute_secs.get(k, 0.0))
            user_units_in_use.labels(cluster=cluster, user=user).set(acc_units.get(k, 0))
            ratios = acc_mem_ratios.get(k, [])
            if ratios:
                user_memory_efficiency.labels(cluster=cluster, user=user).set(
                    sum(ratios) / len(ratios)
                )

        # Cleanup stale job_memory_usage_mb entries for terminated jobs
        global _cluster_mem_labels
        for tup in list(_cluster_mem_labels):
            if tup not in seen_mem_labels:
                try:
                    job_memory_usage_mb.remove(*tup)
                except (KeyError, ValueError):
                    pass
        _cluster_mem_labels = seen_mem_labels

        # _current_node_jobs is owned by scrape_personal(); cluster scrape ignores it

        dt = time.time() - t0
        last_scrape_success.set(1)
        last_scrape_duration_seconds.set(dt)
        last_scrape_timestamp.set(now)
        log.info(
            f"Scrape OK in {dt:.2f}s — "
            f"GPU: {sum(gpu_claimed.values())} units claimed, {n_gpu_jobs} jobs; "
            f"CPU: {cpu_claimed} cores claimed, {n_cpu_jobs} jobs; "
            f"{len({u for _, u in all_keys})} distinct users"
        )

    except Exception as e:
        last_scrape_success.set(0)
        last_scrape_duration_seconds.set(time.time() - t0)
        log.error(f"Scrape failed: {e}", exc_info=True)


# =============================================================================
# Worker-node metric scraping helpers
# =============================================================================

def _parse_prom_text(text: str) -> dict:
    """Minimal Prometheus text-format parser. Returns {name: [(label_dict, value), ...]}."""
    result: dict = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        m = re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([^\s]+)", line)
        if not m:
            continue
        name, labels_raw, val_str = m.group(1), m.group(2) or "", m.group(3)
        try:
            value = float(val_str)
        except ValueError:
            continue
        labels = {lm.group(1): lm.group(2)
                  for lm in re.finditer(r'([a-zA-Z_]\w*)="([^"]*)"', labels_raw)}
        result.setdefault(name, []).append((labels, value))
    return result


def _fetch_node_metrics(host: str, port: int) -> dict:
    try:
        url = f"http://{host}:{port}/metrics"
        with urllib.request.urlopen(url, timeout=5) as resp:
            return _parse_prom_text(resp.read().decode("utf-8", errors="replace"))
    except Exception as exc:
        log.debug(f"node_exporter {host}:{port} unreachable: {exc}")
        return {}


def _pick_gpu(parsed: dict, candidates: list, gpu_uuid: str) -> Optional[float]:
    """Return the first matching metric value, preferring entries whose labels
    contain `gpu_uuid`. Falls back to the first entry if uuid is unknown."""
    norm_uuid = gpu_uuid.lower().removeprefix("gpu-")
    for name in candidates:
        if name not in parsed:
            continue
        entries = parsed[name]
        if norm_uuid:
            matched = [v for lbl, v in entries if norm_uuid in str(lbl).lower()]
            if matched:
                return matched[0]
        if entries:
            return entries[0][1]
    return None


def _pick_cgroup(parsed: dict, candidates: list, username: str) -> Optional[float]:
    """Return metric value for the given username from cgroup metrics."""
    for name in candidates:
        if name not in parsed:
            continue
        for lbl, val in parsed[name]:
            if lbl.get("username") == username:
                return val
    return None


def _to_mib(raw: float) -> float:
    """Convert bytes → MiB if value looks like bytes (> 1 million)."""
    return raw / (1024 * 1024) if raw > 1_000_000 else raw


def _fetch_htcondor_gpu_metrics(nodes: list) -> dict:
    """Query HTCondor collector for GPU/CPU/memory metrics via Startd ClassAds.
    Returns {job_id: {util_pct, mem_used_mb, mem_total_mb, cpu_usage, memory_usage_mb}}.
    Cached with 15s TTL to avoid hammering the collector.

    Matches both CPU and GPU jobs (anything with a JobId). The previous
    `AssignedGPUs isnt undefined` filter silently dropped CPU jobs, leaving them
    with no fallback when the schedd's RSS / MemoryUsage ClassAds were missing.
    """
    global _htcondor_gpu_cache, _htcondor_gpu_cache_time

    now = time.time()
    if now - _htcondor_gpu_cache_time < _HTCONDOR_GPU_CACHE_TTL:
        return _htcondor_gpu_cache

    result = {}
    try:
        coll = htcondor.Collector(_collector_host)
        constraint = _build_startd_job_constraint(nodes)

        for ad in coll.query(htcondor.AdTypes.Startd, projection=[
            "Name", "Machine", "JobId", "JobPid",
            "DeviceGPUsAverageUsage", "GPUsMemoryUsage",
            "GPUs_GlobalMemoryMb",
            "CPUsUsage", "ResidentSetSize", "MemoryUsage",
            "RequestCpus",
        ], constraint=constraint):
            job_id = ad.get("JobId")
            if not job_id:
                continue
            entry = {}

            # JobPid enables the SSH /proc fallback for fresher-than-startd
            # memory. Strip quotes (ClassAd string format) and require int.
            raw_pid = ad.get("JobPid")
            if raw_pid is not None:
                try:
                    entry["pid"] = int(str(raw_pid).strip('" '))
                except (TypeError, ValueError):
                    pass

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
        log.warning(f"HTCondor GPU metrics query failed: {exc}")

    _htcondor_gpu_cache = result
    _htcondor_gpu_cache_time = now
    return result


def _scrape_worker_nodes(node_jobs: list, now: float) -> None:
    """Fetch GPU + cgroup metrics from node_exporter for each job in node_jobs.
    Falls back to HTCondor Startd ClassAds for nodes without node_exporter
    (e.g. all CPU workers on the `lot` cluster, plus GPU workers where the
    node_exporter HTTP port is unreachable from the login node)."""
    by_node: dict = {}
    for j in node_jobs:
        by_node.setdefault(j["node"], []).append(j)

    # CPU workers don't run node_exporter — short-circuit them to the startd
    # fallback so we don't waste a 5s timeout per CPU node.
    cpu_only_nodes = {
        node: jobs for node, jobs in by_node.items()
        if all(j.get("cluster") == "cpu" for j in jobs)
    }
    for node, jobs in cpu_only_nodes.items():
        by_node.pop(node, None)

    nodes_without_exporter: dict = {}
    for node, jobs in by_node.items():
        if node == "unknown":
            continue
        parsed = _fetch_node_metrics(node, _node_exporter_port)
        if not parsed:
            nodes_without_exporter[node] = jobs
            continue

        for j in jobs:
            job_id, cluster, user = j["job_id"], j["cluster"], j["user"]
            gpu_uuid, req_cpus = j["gpu_uuid"], j["req_cpus"]
            lbl = dict(cluster=cluster, user=user, job_id=job_id, node=node)

            # GPU utilization (%)
            raw = _pick_gpu(parsed, _GPU_UTIL_NAMES, gpu_uuid)
            if raw is not None:
                pct = raw if raw > 1.0 else raw * 100.0
                job_gpu_utilization_pct.labels(**lbl).set(pct)

            # GPU memory used (MiB)
            raw = _pick_gpu(parsed, _GPU_MEM_USED_NAMES, gpu_uuid)
            if raw is not None:
                job_gpu_memory_used_mb.labels(**lbl).set(_to_mib(raw))

            # GPU memory total (MiB)
            raw = _pick_gpu(parsed, _GPU_MEM_TOTAL_NAMES, gpu_uuid)
            if raw is not None:
                job_gpu_memory_total_mb.labels(**lbl).set(_to_mib(raw))

            # Cgroup RSS memory (MiB)
            raw = _pick_cgroup(parsed, _CGROUP_RSS_NAMES, user)
            if raw is not None:
                job_cgroup_memory_rss_mb.labels(**lbl).set(_to_mib(raw))

            # Cgroup CPU % (rate of cpu_seconds counter)
            raw = _pick_cgroup(parsed, _CGROUP_CPU_NAMES, user)
            if raw is not None:
                key = (job_id, node)
                if key in _prev_cgroup_cpu:
                    prev_ts, prev_val = _prev_cgroup_cpu[key]
                    dt = now - prev_ts
                    if dt > 0:
                        delta = raw - prev_val
                        if delta / dt > 1e8:
                            delta /= 1e9
                        cpu_cores = delta / dt
                        job_cgroup_cpu_pct.labels(**lbl).set(
                            cpu_cores * 100.0 / max(req_cpus, 1)
                        )
                _prev_cgroup_cpu[key] = (now, raw)

    # Fallback: HTCondor Startd ClassAds for nodes without node_exporter, plus
    # all CPU-only nodes (which we routed here above).
    fallback_nodes = dict(nodes_without_exporter)
    fallback_nodes.update(cpu_only_nodes)
    if fallback_nodes:
        try:
            htcondor_metrics = _fetch_htcondor_gpu_metrics(list(fallback_nodes.keys()))
        except Exception as exc:
            log.warning(f"HTCondor GPU metrics fallback failed: {exc}")
            return

        # Per-node SSH /proc poll: real-time RSS for jobs that have a PID and
        # are due for a poll. One SSH per node regardless of job count. Failures
        # silently fall through to the startd value written below.
        ssh_results: dict = {}   # job_id -> mem_mb
        if _ssh_user:
            for node, jobs in fallback_nodes.items():
                pids_to_poll = []
                pid_to_job_id = {}
                for j in jobs:
                    pid = htcondor_metrics.get(j["job_id"], {}).get("pid")
                    if pid and _should_ssh_poll(j["job_id"], node):
                        pids_to_poll.append(pid)
                        pid_to_job_id[pid] = j["job_id"]
                if pids_to_poll:
                    ssh_mem = _fetch_ssh_memory_batch(node, pids_to_poll)
                    for pid, mem_mb in ssh_mem.items():
                        ssh_results[pid_to_job_id[pid]] = mem_mb
                        _mark_ssh_polled(pid_to_job_id[pid], node)

        for node, jobs in fallback_nodes.items():
            for j in jobs:
                job_id = j["job_id"]
                m = htcondor_metrics.get(job_id, {})
                rtype = "CPU" if j.get("cluster") == "cpu" else gpu_type_for_node(node)
                lbl_gpu = dict(cluster=j["cluster"], user=j["user"],
                               job_id=job_id, node=node)
                lbl_job = dict(cluster=j["cluster"], user=j["user"],
                               job_id=job_id, node=node, resource_type=rtype)

                if rtype != "CPU":
                    if m.get("util_pct") is not None:
                        job_gpu_utilization_pct.labels(**lbl_gpu).set(m["util_pct"])
                    if m.get("mem_used_mb") is not None:
                        job_gpu_memory_used_mb.labels(**lbl_gpu).set(m["mem_used_mb"])
                    if m.get("mem_total_mb") is not None:
                        job_gpu_memory_total_mb.labels(**lbl_gpu).set(m["mem_total_mb"])

                # CPU and memory from Startd ads (real usage, not schedd stale data)
                if m.get("cpu_usage") is not None:
                    req_cpus = j.get("req_cpus", 1)
                    eff = float(m["cpu_usage"]) / max(req_cpus, 1)
                    job_cpu_efficiency.labels(**lbl_job).set(eff)
                # SSH /proc RSS wins when available — it's real-time, while
                # the startd ClassAd updates every 1-5 minutes. Falls through
                # to the startd value if SSH didn't return data for this job.
                mem_to_write = ssh_results.get(job_id, m.get("memory_usage_mb"))
                if mem_to_write is not None:
                    job_memory_usage_mb.labels(**lbl_job).set(mem_to_write)


# =============================================================================
# Log file HTTP server  (optional — only started when --log-dir is given)
# Serves the last 200 lines of <log_dir>/<job_id>.out at /logs/<job_id>.
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
        # Try the canonical forms first, then a glob to catch prefixed names
        # like myjob_12345_0.out. On Stoomboot the underscore form is most common.
        candidates = [f"{job_id}.out"]
        if "." in job_id:
            candidates.append(f"{job_id.replace('.', '_')}.out")
        for name in candidates:
            p = os.path.join(_log_dir, name)
            if os.path.isfile(p):
                self._send_log(p, name)
                return
        for pattern in {f"*{job_id}.out", f"*{job_id.replace('.', '_')}.out"}:
            globbed = _glob_mod.glob(os.path.join(_log_dir, pattern))
            if globbed:
                p = globbed[0]
                self._send_log(p, os.path.basename(p))
                return
        msg = f"No log found for job {job_id}\n\nTried:\n" + "\n".join(
            f"  {os.path.join(_log_dir, c)}" for c in candidates
        ) + "\n  *{id}.out, *{us}.out (glob)".format(id=job_id, us=job_id.replace('.', '_'))
        self._respond(404, "text/plain", msg.encode())

    def _send_log(self, path: str, filename: str):
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
            f"<div class='hdr'>{_html_mod.escape(filename)}"
            f" — {total} lines total (last 200 shown)</div>"
            f"<pre>{_html_mod.escape(content)}</pre>"
            "<script>window.scrollTo(0,document.body.scrollHeight)</script>"
            "</body></html>"
        )
        self._respond(200, "text/html; charset=utf-8", page.encode("utf-8"))

    def _serve_index(self):
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
            f"<p>Log directory: <code>{_html_mod.escape(_log_dir)}</code></p>"
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


_PERSONAL_GAUGES = [
    user_jobs_running, user_jobs_queued, user_compute_seconds,
    user_units_in_use,
    job_duration_seconds, job_gpus_requested, job_cpus_requested,
    job_memory_requested_mb,
    job_vram_allocated_mb, job_status_gauge,
    # job_memory_usage_mb and job_cpu_efficiency are NOT cleared here — they're
    # owned by the worker-node fallback (which runs every 3s) when the schedd
    # has no real data, and we don't want to wipe the fallback's value with a
    # zero from the schedd every 15s.
]


def scrape_personal(collector_host: str, detail_user: str) -> None:
    """Fast scrape: only the detail user's own running jobs.
    Updates per-job metrics and _current_node_jobs for the node_exporter loop.
    Does NOT clear cluster-wide metrics — those are owned by scrape().
    """
    t0 = time.time()
    now = t0

    # Clear personal-loop gauges so old finished jobs disappear immediately
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
        seen_mem_labels: set[tuple[str, str, str, str, str]] = set()
        seen_cpu_labels: set[tuple[str, str, str, str, str]] = set()

        for schedd_ad in schedd_ads:
            try:
                schedd = htcondor.Schedd(schedd_ad)
                jobs = schedd.query(
                    constraint=f'Owner == "{detail_user}"',
                    projection=[
                        "ClusterId", "ProcId", "JobStatus",
                        "RequestGPUs", "RequestCpus", "RequestMemory",
                        "MemoryUsage", "ImageSize", "ResidentSetSize",
                        "MemoryProvisioned",
                        "TotalJobRunningCpuUsage",
                        "RemoteUserCpu", "RemoteSysCpu",
                        "JobStartDate", "RemoteHost", "LastRemoteHost", "AssignedGPUs",
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
                    acc_running[cluster] += 1
                    job_start = safe_get(job, "JobStartDate", now) or now
                    duration = max(now - job_start, 0)
                    job_id = f"{safe_get(job, 'ClusterId', 0)}.{safe_get(job, 'ProcId', 0)}"
                    lbl = dict(cluster=cluster, user=detail_user,
                               job_id=job_id, resource_type=rtype, node=node)
                    lbl_tuple = (cluster, detail_user, job_id, rtype, node)
                    seen_mem_labels.add(lbl_tuple)
                    vram_per_gpu = _vram_per_gpu_by_node.get(node, 0)

                    job_duration_seconds.labels(**lbl).set(duration)
                    job_gpus_requested.labels(**lbl).set(req_gpus)
                    job_cpus_requested.labels(**lbl).set(req_cpus)
                    job_memory_requested_mb.labels(**lbl).set(req_mem_mb)
                    if sched_mem_mb > 0:
                        job_memory_usage_mb.labels(**lbl).set(sched_mem_mb)
                    if sched_cpu_secs > 0:
                        cpu_eff = sched_cpu_secs / max(duration * max(req_cpus, 1), 1e-6)
                        job_cpu_efficiency.labels(**lbl).set(cpu_eff)
                        seen_cpu_labels.add(lbl_tuple)
                    job_vram_allocated_mb.labels(**lbl).set(req_gpus * vram_per_gpu)
                    job_status_gauge.labels(cluster=cluster, user=detail_user, job_id=job_id).set(2)

                    # Track for worker-node fallback (startd ClassAds) — works for
                    # both GPU and CPU jobs so schedd-missing data gets filled in.
                    raw = (safe_get(job, "AssignedGPUs", "") or "").strip('" ')
                    gpu_uuid = raw.lower().removeprefix("gpu-")
                    node_jobs.append(dict(
                        job_id=job_id, node=node, gpu_uuid=gpu_uuid,
                        req_cpus=req_cpus, cluster=cluster, user=detail_user,
                    ))

                elif status == 1:  # Queued
                    acc_queued[cluster] += 1
                    job_id = f"{safe_get(job, 'ClusterId', 0)}.{safe_get(job, 'ProcId', 0)}"
                    job_status_gauge.labels(cluster=cluster, user=detail_user, job_id=job_id).set(1)

        # Cleanup stale job_memory_usage_mb entries for finished jobs
        global _personal_mem_labels
        for tup in list(_personal_mem_labels):
            if tup not in seen_mem_labels:
                try:
                    job_memory_usage_mb.remove(*tup)
                except (KeyError, ValueError):
                    pass
        # Invalidate the startd-fallback cache when a brand-new job appears,
        # so the next _scrape_worker_nodes tick re-queries the collector
        # immediately instead of returning the previous run's stale result.
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

        global _current_node_jobs
        _current_node_jobs = node_jobs

        last_scrape_success.set(1)
        last_scrape_duration_seconds.set(time.time() - t0)
        last_scrape_timestamp.set(now)
        log.debug(
            f"Personal scrape {time.time()-t0:.2f}s — "
            f"{sum(acc_running.values())} running, {sum(acc_queued.values())} queued"
        )

    except Exception as exc:
        last_scrape_success.set(0)
        last_scrape_duration_seconds.set(time.time() - t0)
        log.error(f"Personal scrape failed: {exc}", exc_info=True)


def _cluster_loop(collector_host: str, detail_user: str, interval: int) -> None:
    """Slow background loop for cluster-wide HTCondor metrics."""
    while True:
        scrape(collector_host, detail_user)
        time.sleep(interval)


def _node_exporter_loop(interval: int) -> None:
    """Fast loop that polls worker-node metrics independently of the HTCondor scrape."""
    while True:
        jobs = _current_node_jobs
        if jobs:
            try:
                _scrape_worker_nodes(jobs, time.time())
            except Exception as exc:
                log.error(f"Node exporter fast-loop error: {exc}", exc_info=True)
        time.sleep(interval)


# =============================================================================
# Entry point
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Prometheus exporter for Stoomboot GPU+CPU jobs")
    parser.add_argument("--port", type=int, default=9118,
                        help="Port to expose metrics on (default: 9118)")
    parser.add_argument("--collector", type=str, default="stbc-019.nikhef.nl",
                        help="HTCondor collector hostname (default: stbc-019.nikhef.nl)")
    parser.add_argument("--interval", type=int, default=3,
                        help="Personal job poll interval in seconds (default: 3)")
    parser.add_argument("--full", action="store_true",
                        help="Also run cluster-wide monitoring (all users, slot stats)")
    parser.add_argument("--cluster-interval", type=int, default=30,
                        help="Cluster-wide scan interval when --full is active (default: 30)")
    parser.add_argument("--detail-user", type=str, default="your_username",
                        help="User whose CPU jobs get per-job detail (default: your_username). "
                             "GPU per-job detail is always emitted for everyone.")
    parser.add_argument("--node-exporter-port", type=int, default=9100,
                        help="Port of node_exporter on worker nodes (default: 9100)")
    parser.add_argument("--node-interval", type=int, default=3,
                        help="Poll interval in seconds for worker-node GPU/cgroup metrics (default: 3)")
    parser.add_argument("--log-dir", type=str, default="",
                        help="Directory containing .out log files; enables log HTTP server")
    parser.add_argument("--log-port", type=int, default=9119,
                        help="Port for the log HTTP server (default: 9119)")
    parser.add_argument("--ssh-user", type=str, default=os.environ.get("USER", ""),
                        help="SSH user for /proc polling on worker nodes. "
                             "Default: $USER. Empty disables the SSH path (fall "
                             "back to startd ClassAds only).")
    parser.add_argument("--ssh-interval", type=int, default=10,
                        help="Minimum seconds between SSH /proc polls for the "
                             "same job. Default: 10.")
    args = parser.parse_args()

    log.info("Starting Stoomboot GPU+CPU Prometheus exporter")
    log.info(f"  bindings:       {_binding}")
    log.info(f"  port:           {args.port}")
    log.info(f"  collector:      {args.collector}")
    log.info(f"  personal user:  {args.detail_user} (fast loop every {args.interval}s)")
    log.info(f"  cluster scan:   {'every ' + str(args.cluster_interval) + 's (background)' if args.full else 'disabled (use --full to enable)'}")
    log.info(f"  node-exporter:  :{args.node_exporter_port} on workers every {args.node_interval}s")

    global _node_exporter_port, _collector_host, _ssh_user, _ssh_poll_interval
    _node_exporter_port = args.node_exporter_port
    _collector_host = args.collector
    _ssh_user = args.ssh_user
    _ssh_poll_interval = float(args.ssh_interval)
    if _ssh_user:
        log.info(f"  ssh /proc poll:  user={_ssh_user}, interval={_ssh_poll_interval}s")
    else:
        log.info("  ssh /proc poll:  disabled (use --ssh-user to enable)")

    if args.log_dir:
        global _log_dir
        _log_dir = args.log_dir
        log.info(f"  log dir:        {args.log_dir}")
        log.info(f"  log port:       {args.log_port}")

    start_http_server(args.port)
    exporter_up.set(1)
    log.info(f"Metrics at http://localhost:{args.port}/metrics")

    if args.log_dir:
        try:
            _start_log_server(args.log_port)
        except OSError as exc:
            log.warning(f"Log server port {args.log_port} in use ({exc}) — skipping log server")

    # Cluster-wide background scan (only when --full)
    if args.full:
        threading.Thread(
            target=_cluster_loop,
            args=(args.collector, args.detail_user, args.cluster_interval),
            daemon=True, name="cluster-loop",
        ).start()

    # Worker-node GPU/cgroup fast poll
    threading.Thread(
        target=_node_exporter_loop,
        args=(args.node_interval,),
        daemon=True, name="node-exporter-loop",
    ).start()

    # Personal job fast poll (main thread)
    while True:
        scrape_personal(args.collector, args.detail_user)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
