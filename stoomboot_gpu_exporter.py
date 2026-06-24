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
import html as _html_mod
import http.server
import logging
import os
import re
import threading
import time
import urllib.request
from collections import defaultdict
from typing import Optional

try:
    import htcondor2 as htcondor
    import classad2 as classad  # noqa: F401  (imported for side effects / availability check)
    HTCONDOR_BINDINGS = "v2 (htcondor2/classad2)"
except ImportError:
    try:
        import htcondor
        import classad  # noqa: F401  (imported for side effects / availability check)
        HTCONDOR_BINDINGS = "v1 (htcondor/classad)"
    except ImportError as exc:
        raise ImportError(
            "HTCondor Python bindings not found.\n"
            "Tried htcondor2/classad2 (HTCondor 25+) and htcondor/classad (older).\n"
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
_htcondor_gpu_cache: dict = {}   # {job_id: {util_pct, mem_used_mb, mem_total_mb}}
_htcondor_gpu_cache_time: float = 0
_HTCONDOR_GPU_CACHE_TTL: float = 15  # seconds


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


_MEM_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([kKmMgGtT]?[bB]?)?\s*$")


def _parse_memory_mb(value):
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        m = _MEM_RE.match(value)
        if not m:
            return None
        amount = float(m.group(1))
        unit = (m.group(2) or "mb").lower()
        if unit in ("k", "kb"):
            return amount / 1024.0
        if unit in ("m", "mb", ""):
            return amount
        if unit in ("g", "gb"):
            return amount * 1024.0
        if unit in ("t", "tb"):
            return amount * 1024.0 * 1024.0
    return None


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


def safe_memory_mb(ad, key, default=0):
    val = safe_get(ad, key, None)
    parsed = _parse_memory_mb(val)
    if parsed is None:
        try:
            parsed = _parse_memory_mb(ad.eval(key))
        except Exception:
            parsed = None
    return int(parsed) if parsed is not None else default


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
        job_memory_requested_mb, job_memory_usage_mb, job_cpu_efficiency,
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
                req_mem_mb = safe_memory_mb(job, "RequestMemory", 0)
                # Memory: ResidentSetSize (KB) → MemoryUsage (eval'd MB) → MemoryProvisioned → ImageSize/1024
                rss_kb = safe_int(job, "ResidentSetSize", 0)
                mem_usage_mb = safe_int(job, "MemoryUsage", 0)
                prov_mb = float(safe_int(job, "MemoryProvisioned", 0))
                img_mb = safe_int(job, "ImageSize", 0) / 1024.0
                actual_mem_mb = (rss_kb / 1024.0) or mem_usage_mb or prov_mb or img_mb

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
                    if req_mem_mb > 0 and actual_mem_mb > 0:
                        acc_mem_ratios[key].append(actual_mem_mb / req_mem_mb)

                    # Queue this job for node_exporter scraping (detail_user GPU jobs)
                    if is_gpu and owner == detail_user:
                        raw_assigned = safe_get(job, "AssignedGPUs", "") or ""
                        gpu_uuid = raw_assigned.strip('" ').lower().removeprefix("gpu-")
                        _node_jobs.append(dict(
                            job_id=f"{safe_get(job, 'ClusterId', 0)}.{safe_get(job, 'ProcId', 0)}",
                            node=node, gpu_uuid=gpu_uuid,
                            req_cpus=req_cpus, cluster=cluster, user=owner,
                        ))

                    # Per-job detail: GPU = everyone; CPU = detail_user only
                    if is_gpu or owner == detail_user:
                        job_id = f"{safe_get(job, 'ClusterId', 0)}.{safe_get(job, 'ProcId', 0)}"
                        lbl = dict(cluster=cluster, user=owner, job_id=job_id,
                                   resource_type=rtype, node=node)
                        cpu_secs_used = safe_float(job, "TotalJobRunningCpuUsage", 0) or (
                            safe_float(job, "RemoteUserCpu", 0) + safe_float(job, "RemoteSysCpu", 0)
                        )
                        cpu_eff = cpu_secs_used / max(duration * max(req_cpus, 1), 1e-6)
                        job_duration_seconds.labels(**lbl).set(duration)
                        job_gpus_requested.labels(**lbl).set(req_gpus)
                        job_cpus_requested.labels(**lbl).set(req_cpus)
                        job_memory_requested_mb.labels(**lbl).set(req_mem_mb)
                        job_memory_usage_mb.labels(**lbl).set(actual_mem_mb)
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
    """Query HTCondor collector for GPU metrics via Startd ClassAds.
    Returns {job_id: {util_pct, mem_used_mb, mem_total_mb}}.
    Cached with 15s TTL to avoid hammering the collector."""
    global _htcondor_gpu_cache, _htcondor_gpu_cache_time

    now = time.time()
    if now - _htcondor_gpu_cache_time < _HTCONDOR_GPU_CACHE_TTL:
        return _htcondor_gpu_cache

    result = {}
    try:
        coll = htcondor.Collector(_collector_host)
        node_cons = " || ".join(
            f'Machine =?= "{node}.nikhef.nl"' for node in nodes
        )
        constraint = f"(AssignedGPUs isnt undefined) && ({node_cons})"

        for ad in coll.query(htcondor.AdTypes.Startd, projection=[
            "Name", "Machine", "JobId",
            "DeviceGPUsAverageUsage", "GPUsMemoryUsage",
            "GPUs_GlobalMemoryMb",
        ], constraint=constraint):
            job_id = ad.get("JobId")
            if not job_id:
                continue
            util = ad.get("DeviceGPUsAverageUsage")
            mem_used = ad.get("GPUsMemoryUsage")
            mem_total = ad.get("GPUs_GlobalMemoryMb")
            entry = {}
            if util is not None:
                entry["util_pct"] = float(util) * 100
            if mem_used is not None:
                entry["mem_used_mb"] = float(mem_used)
            if mem_total is not None:
                entry["mem_total_mb"] = float(mem_total)
            if entry:
                result[job_id] = entry
    except Exception as exc:
        log.warning(f"HTCondor GPU metrics query failed: {exc}")

    _htcondor_gpu_cache = result
    _htcondor_gpu_cache_time = now
    return result


def _scrape_worker_nodes(node_jobs: list, now: float) -> None:
    """Fetch GPU + cgroup metrics from node_exporter for each job in node_jobs.
    Falls back to HTCondor Startd ClassAds for nodes without node_exporter."""
    by_node: dict = {}
    for j in node_jobs:
        by_node.setdefault(j["node"], []).append(j)

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

    # Fallback: HTCondor Startd ClassAds for nodes without node_exporter
    if nodes_without_exporter:
        try:
            htcondor_metrics = _fetch_htcondor_gpu_metrics(list(nodes_without_exporter.keys()))
        except Exception as exc:
            log.warning(f"HTCondor GPU metrics fallback failed: {exc}")
            return

        for node, jobs in nodes_without_exporter.items():
            for j in jobs:
                job_id = j["job_id"]
                if job_id not in htcondor_metrics:
                    continue
                m = htcondor_metrics[job_id]
                lbl = dict(cluster=j["cluster"], user=j["user"], job_id=job_id, node=node)
                if m.get("util_pct") is not None:
                    job_gpu_utilization_pct.labels(**lbl).set(m["util_pct"])
                if m.get("mem_used_mb") is not None:
                    job_gpu_memory_used_mb.labels(**lbl).set(m["mem_used_mb"])
                if m.get("mem_total_mb") is not None:
                    job_gpu_memory_total_mb.labels(**lbl).set(m["mem_total_mb"])


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
        # Try: <job_id>.out → <cluster_id>.out → <cluster_id>_<proc_id>.out
        candidates = [f"{job_id}.out"]
        if "." in job_id:
            candidates.append(f"{job_id.split('.')[0]}.out")
            candidates.append(f"{job_id.replace('.', '_')}.out")
        for name in candidates:
            p = os.path.join(_log_dir, name)
            if os.path.isfile(p):
                self._send_log(p, name)
                return
        msg = f"No log found for job {job_id}\n\nTried:\n" + "\n".join(
            f"  {os.path.join(_log_dir, c)}" for c in candidates
        )
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
            "<style>body{margin:0;padding:10px;background:#0d0d0d;color:#c8c8c8;"
            "font-family:'Courier New',Courier,monospace;font-size:12px;}"
            ".hdr{color:#555;font-size:10px;margin-bottom:6px;}"
            "pre{margin:0;white-space:pre-wrap;word-break:break-all;}</style>"
            "</head><body>"
            f"<div class='hdr'>{_html_mod.escape(filename)}"
            f" — {total} lines total (last 200 shown)</div>"
            f"<pre>{_html_mod.escape(content)}</pre>"
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
    job_memory_requested_mb, job_memory_usage_mb, job_cpu_efficiency,
    job_vram_allocated_mb, job_status_gauge,
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
                req_mem_mb = safe_memory_mb(job, "RequestMemory", 0)
                rss_kb = safe_int(job, "ResidentSetSize", 0)
                mem_usage_mb = safe_int(job, "MemoryUsage", 0)
                prov_mb = float(safe_int(job, "MemoryProvisioned", 0))
                img_mb = safe_int(job, "ImageSize", 0) / 1024.0
                actual_mem_mb = (rss_kb / 1024.0) or mem_usage_mb or prov_mb or img_mb
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
                    cpu_secs = safe_float(job, "TotalJobRunningCpuUsage", 0) or (
                        safe_float(job, "RemoteUserCpu", 0) + safe_float(job, "RemoteSysCpu", 0)
                    )
                    cpu_eff = cpu_secs / max(duration * max(req_cpus, 1), 1e-6)
                    vram_per_gpu = _vram_per_gpu_by_node.get(node, 0)

                    job_duration_seconds.labels(**lbl).set(duration)
                    job_gpus_requested.labels(**lbl).set(req_gpus)
                    job_cpus_requested.labels(**lbl).set(req_cpus)
                    job_memory_requested_mb.labels(**lbl).set(req_mem_mb)
                    job_memory_usage_mb.labels(**lbl).set(actual_mem_mb)
                    job_cpu_efficiency.labels(**lbl).set(cpu_eff)
                    job_vram_allocated_mb.labels(**lbl).set(req_gpus * vram_per_gpu)

                    if is_gpu:
                        raw = (safe_get(job, "AssignedGPUs", "") or "").strip('" ')
                        gpu_uuid = raw.lower().removeprefix("gpu-")
                        node_jobs.append(dict(
                            job_id=job_id, node=node, gpu_uuid=gpu_uuid,
                            req_cpus=req_cpus, cluster=cluster, user=detail_user,
                        ))

                elif status == 1:  # Queued
                    acc_queued[cluster] += 1

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
    args = parser.parse_args()

    log.info("Starting Stoomboot GPU+CPU Prometheus exporter")
    log.info(f"  bindings:       {HTCONDOR_BINDINGS}")
    log.info(f"  port:           {args.port}")
    log.info(f"  collector:      {args.collector}")
    log.info(f"  personal user:  {args.detail_user} (fast loop every {args.interval}s)")
    log.info(f"  cluster scan:   {'every ' + str(args.cluster_interval) + 's (background)' if args.full else 'disabled (use --full to enable)'}")
    log.info(f"  node-exporter:  :{args.node_exporter_port} on workers every {args.node_interval}s")

    global _node_exporter_port, _collector_host
    _node_exporter_port = args.node_exporter_port
    _collector_host = args.collector

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
