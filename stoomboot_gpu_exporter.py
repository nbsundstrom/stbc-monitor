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
import logging
import re
import time
from collections import defaultdict

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
        job_memory_requested_mb, job_memory_usage_mb,
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
                "GPUs", "GPUs_DeviceName", "TotalGPUs",
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

        for schedd_ad in schedd_ads:
            try:
                schedd = htcondor.Schedd(schedd_ad)
                jobs = schedd.query(
                    projection=[
                        "ClusterId", "ProcId", "Owner", "JobStatus",
                        "RequestGPUs", "RequestCpus", "RequestMemory",
                        "MemoryUsage", "ImageSize", "QDate", "JobStartDate",
                        "RemoteHost", "LastRemoteHost",
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
                # MemoryUsage is directly in MB (HTCondor 8.8+); fall back to ImageSize (KB)/1024
                actual_mem_mb = float(safe_int(job, "MemoryUsage", 0)) or (safe_int(job, "ImageSize", 0) / 1024.0)

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

                    # Per-job detail: GPU = everyone; CPU = detail_user only
                    if is_gpu or owner == detail_user:
                        job_id = f"{safe_get(job, 'ClusterId', 0)}.{safe_get(job, 'ProcId', 0)}"
                        lbl = dict(cluster=cluster, user=owner, job_id=job_id,
                                   resource_type=rtype, node=node)
                        job_duration_seconds.labels(**lbl).set(duration)
                        job_gpus_requested.labels(**lbl).set(req_gpus)
                        job_cpus_requested.labels(**lbl).set(req_cpus)
                        job_memory_requested_mb.labels(**lbl).set(req_mem_mb)
                        job_memory_usage_mb.labels(**lbl).set(actual_mem_mb)

                elif status == 1:  # Idle / queued
                    acc_queued[key] += 1

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
# Entry point
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Prometheus exporter for Stoomboot GPU+CPU jobs")
    parser.add_argument("--port", type=int, default=9118,
                        help="Port to expose metrics on (default: 9118)")
    parser.add_argument("--collector", type=str, default="stbc-019.nikhef.nl",
                        help="HTCondor collector hostname (default: stbc-019.nikhef.nl)")
    parser.add_argument("--interval", type=int, default=15,
                        help="Scrape interval in seconds (default: 15)")
    parser.add_argument("--detail-user", type=str, default="your_username",
                        help="User whose CPU jobs get per-job detail (default: your_username). "
                             "GPU per-job detail is always emitted for everyone.")
    args = parser.parse_args()

    log.info("Starting Stoomboot GPU+CPU Prometheus exporter")
    log.info(f"  bindings:    {HTCONDOR_BINDINGS}")
    log.info(f"  port:        {args.port}")
    log.info(f"  collector:   {args.collector}")
    log.info(f"  interval:    {args.interval}s")
    log.info(f"  detail user: {args.detail_user} (CPU per-job detail)")

    start_http_server(args.port)
    exporter_up.set(1)
    log.info(f"Metrics available at http://localhost:{args.port}/metrics")

    while True:
        scrape(args.collector, args.detail_user)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
