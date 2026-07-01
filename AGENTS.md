# AGENTS.md — stbc-monitor

This file supplements `CLAUDE.md` with operational details an agent is likely to miss.

## Deploy methods

Two ways to get code to the cluster, set via `REPO_URL` in `~/.stbc-monitor.conf`:
- **Empty** (default): wrapper rsyncs local repo, excluding `.git`, `*.sif`, `.apptainer`, `prom_data`, `grafana_data*`, `logs/`, `pids/`
- **Set to a git remote**: cluster runs `git pull --ff-only` in `REMOTE_DIR`

When editing, both paths matter: the file on disk gets rsynced, and `setup.sh` is what actually runs on the cluster.

## Exporter: two-loop design

`stoomboot_gpu_exporter.py` runs two concurrent loops:

| Loop | Interval | Scope |
|------|----------|-------|
| Personal (main thread) | `--interval` (default 3s) | `detail_user`'s jobs — both clusters |
| Startd fallback (thread) | `--interval` (default 3s) | Real-time memory / CPU / GPU util via Startd ClassAds |

The exporter tracks the detail user's own jobs only. Cluster-wide metrics were removed — no dashboard queries them.

## HTCondor binding resolution

The exporter and `setup.sh` both try `htcondor2`/`classad2` (HTCondor 25+) first, falling back to `htcondor`/`classad` (older).

`setup.sh` resolves the Python binary by trying system paths first (`/usr/bin/python3.{9,10,11}`) before falling back to PATH — this avoids activating a random conda environment from an SSH session.

## GPU node map

`GPU_NODE_MAP` at `stoomboot_gpu_exporter.py` maps short hostnames to GPU types. Must be updated when nodes change. The exporter also normalises `GPUs_DeviceName` ClassAd values as a cross-check via `normalise_device_name()`.

## Port allocation

`setup.sh` scans upward from the requested port if it's busy. The actual port is saved to `pids/exporter.port` / `pids/prometheus.port`. The wrapper reads back `pids/prometheus.port` to set the tunnel target, so the tunnel always matches the real port.

## SSH multiplexing

`stbc-monitor.sh` creates a ControlMaster socket at `${TMPDIR:-/tmp}/stbc-${SSH_USER}@${SSH_HOST}.sock` with 60s persistence. All `remote()` calls reuse one connection.

## Local Grafana setup

- Binary resolved via `brew --prefix grafana` first, then PATH fallback
- Config generated from a heredoc, written to `grafana_data_local/grafana.ini`
- Admin password hardcoded: `admin` / `stbc_monitor`
- Two binary variants: `grafana server` (v10+) vs `grafana-server` (older) — detected by binary name
- Dashboard JSON in `grafana/provisioning/dashboards/stbc_personal.json` uses `__DEFAULT_USER__` as placeholder — never hardcode usernames in those files
- Dashboard `stbc_personal.json` is the default browser target at `/d/stbc-personal`

## Apptainer on the cluster

`setup.sh` sets `APPTAINER_CACHEDIR` and `APPTAINER_TMPDIR` under `REMOTE_DIR/.apptainer/` because `$HOME` can be read-only or quota'd on login nodes. The Prometheus `.sif` is pulled to `prometheus.sif` in the repo root.

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

`~/.stbc-monitor.conf` is sourced after default variable declarations in `stbc-monitor.sh`. `STBC_*` env vars take highest precedence (read first, then potentially overwritten by the config source). `CLUSTER_USER` falls back to `SSH_USER` if unset.

## GPU utilization: HTCondor Startd ClassAd

No node_exporter or DCGM exporter runs on worker nodes, and SSH from the login node to workers is blocked. Real-time GPU metrics therefore come from HTCondor Startd ClassAds (`_fetch_htcondor_gpu_metrics`):

| Startd ClassAd | Metric | Example |
|----------------|--------|---------|
| `DeviceGPUsAverageUsage` (0-1) | `stoomboot_job_gpu_utilization_pct` | 0.599 → 59.9% |
| `CPUsUsage` | `stoomboot_job_cpu_efficiency` | (divided by requested CPUs) |
| `ResidentSetSize` / `MemoryUsage` | `stoomboot_job_memory_usage_mb` | KB → MB |

Updated every ~1-5 minutes (HTCondor startd update interval). Cached with 15s TTL. Matching by `JobId` attribute on the slot ad.
