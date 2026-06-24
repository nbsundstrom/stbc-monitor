# AGENTS.md — stbc-monitor

This file supplements `CLAUDE.md` with operational details an agent is likely to miss.

## Deploy methods

Two ways to get code to the cluster, set via `REPO_URL` in `~/.stbc-monitor.conf`:
- **Empty** (default): wrapper rsyncs local repo, excluding `.git`, `*.sif`, `.apptainer`, `prom_data`, `grafana_data*`, `logs/`, `pids/`
- **Set to a git remote**: cluster runs `git pull --ff-only` in `REMOTE_DIR`

When editing, both paths matter: the file on disk gets rsynced, and `setup.sh` is what actually runs on the cluster.

## Exporter: two-loop design

`stoomboot_gpu_exporter.py:1039` runs three concurrent loops:

| Loop | Interval | Scope | Enabled by |
|------|----------|-------|------------|
| Personal (main thread) | `--interval` (default 3s) | `detail_user` only | always |
| Cluster-wide (thread) | `--cluster-interval` (default 30s) | **all users**, both GPU+CPU | `--full` flag |
| Node-exporter (thread) | `--node-interval` (default 3s) | GPU/cgroup metrics from workers | always |

The `--full` flag (passed by `stbc-monitor.sh` as `--full`) enables cluster-wide metrics. Without it, only the detail user's own jobs appear in metrics. `setup.sh:43` has `FULL_MODE=false` by default — toggle with `--full`.

## HTCondor binding resolution

The exporter and `setup.sh` both try `htcondor2`/`classad2` (HTCondor 25+) first, falling back to `htcondor`/`classad` (older). See `setup.sh:91` and `stoomboot_gpu_exporter.py:42`.

`setup.sh` resolves the Python binary by trying system paths first (`/usr/bin/python3.{9,10,11}`) before falling back to PATH — this avoids activating a random conda environment from an SSH session.

## GPU node map

`GPU_NODE_MAP` at `stoomboot_gpu_exporter.py:246`. Must be updated when nodes change. The exporter also normalises `GPUs_DeviceName` ClassAd values as a cross-check in `normalise_device_name()` at line 314.

## Port allocation

`setup.sh:280` scans upward from the requested port if it's busy. The actual port is saved to `pids/exporter.port` / `pids/prometheus.port`. The wrapper (`stbc-monitor.sh:293`) reads back `pids/prometheus.port` to set the tunnel target, so the tunnel always matches the real port.

## SSH multiplexing

`stbc-monitor.sh:59` creates a ControlMaster socket at `${TMPDIR:-/tmp}/stbc-${SSH_USER}@${SSH_HOST}.sock` with 60s persistence. All `remote()` calls reuse one connection.

## Local Grafana setup

- Binary resolved via `brew --prefix grafana` first, then PATH fallback (`stbc-monitor.sh:116`)
- Config generated from a heredoc at `stbc-monitor.sh:141`; written to `grafana_data_local/grafana.ini`
- Admin password hardcoded: `admin` / `stbc_monitor`
- Two binary variants: `grafana server` (v10+) vs `grafana-server` (older) — detected at line 195
- Dashboard JSONs in `grafana/provisioning/dashboards/*.json` use `__DEFAULT_USER__` as placeholder — never hardcode usernames in those files
- Dashboard `stbc_personal.json` is the default browser target at `/d/stbc-personal`

## Apptainer on the cluster

`setup.sh:194` sets `APPTAINER_CACHEDIR` and `APPTAINER_TMPDIR` under `REMOTE_DIR/.apptainer/` because `$HOME` can be read-only or quota'd on login nodes. The Prometheus `.sif` is pulled to `prometheus.sif` in the repo root.

## Log file HTTP server

Optional — only started when `--log-dir` is given (`stbc-monitor.conf:44`). Serves last 200 lines of `<job_id>.out` files. Accessible via SSH tunnel on `PORT_LOG` (default 9119).

The log server tries three filename candidates when serving a job_id like `ClusterId.ProcId`:
1. `<ClusterId.ProcId>.out` (dot separator)
2. `<ClusterId>.out` (ClusterId only)
3. `<ClusterId>_<ProcId>.out` (underscore separator) — most common on Stoomboot

## updates.md

Contains user feature requests / notes, not a committed roadmap. Treat as background context, not specs to implement.

## Python 3.9 constraint

The cluster runs **Python 3.9** (`/usr/bin/python3.9`). This means **no PEP 604 union syntax** (`str | None`, `float | None`). Use `Optional[str]`, `Optional[float]` from `typing` instead. The exporter will crash at module load time with `TypeError: unsupported operand type(s) for |: 'type' and 'NoneType'` if any 3.10+ syntax is present.

## Config precedence

`~/.stbc-monitor.conf` is sourced after default variable declarations in `stbc-monitor.sh:40`. `STBC_*` env vars take highest precedence (read first, then potentially overwritten by the config source). `CLUSTER_USER` falls back to `SSH_USER` if unset.

## GPU utilization: HTCondor Startd ClassAd fallback

No node_exporter or DCGM exporter runs on worker nodes (`wn-pijl-*`). SSH from the login node to `wn-pijl-*` is also blocked (connection immediately closed). Therefore, real-time GPU metrics via HTTP scraping or `nvidia-smi` over SSH are unavailable for the main cluster.

**Solution:** The node-exporter loop (`_scrape_worker_nodes` at line 778) falls back to HTCondor Startd ClassAds when direct HTTP scraping fails. Queries the collector for slot ads where `AssignedGPUs isnt undefined`, extracting:

| Startd ClassAd | Metric | Example |
|----------------|--------|---------|
| `DeviceGPUsAverageUsage` (0-1) | `stoomboot_job_gpu_utilization_pct` | 0.599 → 59.9% |
| `GPUsMemoryUsage` (MB) | `stoomboot_job_gpu_memory_used_mb` | 33475 MB |
| `GPUs_GlobalMemoryMb` | `stoomboot_job_gpu_memory_total_mb` | 45460 MB |

Updated every ~1-5 minutes (HTCondor startd update interval). Cached with 15s TTL at `stoomboot_gpu_exporter.py:307`. Matching by `JobId` attribute on the slot ad. See `_fetch_htcondor_gpu_metrics()` at line 775.

The only worker accepting SSH from the login node is `wn-lot-001`, which also runs node_exporter on port 9100.
