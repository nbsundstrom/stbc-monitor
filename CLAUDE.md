# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

A monitoring stack for the Stoomboot HTCondor cluster at Nikhef. The stack is split across two machines:

- **Your laptop**: runs Grafana (via Homebrew) and the local wrapper script
- **Cluster host** (`login.nikhef.nl`): runs the Python exporter + Prometheus (via Apptainer containers)

The wrapper (`stbc-monitor.sh`) deploys, manages, and tunnels to the cluster-side services so you never need to SSH in manually.

## Commands

All commands are run from your laptop:

```bash
./stbc-monitor.sh             # start: deploy → ensure cluster services up → start local Grafana → SSH tunnel → open browser
./stbc-monitor.sh --setup     # one-time: deploy + build env + pull images, install Grafana via Homebrew
./stbc-monitor.sh --status    # show what's running (cluster + local Grafana)
./stbc-monitor.sh --restart   # force-restart all services
./stbc-monitor.sh --stop      # stop cluster services + local Grafana
./stbc-monitor.sh --logs      # tail remote logs
```

Config lives at `~/.stbc-monitor.conf` (copy from `stbc-monitor.conf.example`). All variables can also be passed as env vars with a `STBC_` prefix.

## Architecture

```
stbc-monitor.sh  (laptop wrapper)
  └─ rsync (or git pull) → cluster REMOTE_DIR
  └─ ssh → setup.sh  (cluster engine)
              ├─ stoomboot_gpu_exporter.py  (Python, port 9118)
              └─ prometheus (Apptainer .sif, port 9090, scrapes exporter)

Grafana (Homebrew, laptop, port 3000)
  └─ SSH tunnel → cluster Prometheus :9090
```

`setup.sh` is the cluster-side engine. It's idempotent and manages PID files under `pids/` and logs under `logs/` relative to `REMOTE_DIR`. It prefers `htcondor2`/`classad2` (HTCondor 25+) and falls back to `htcondor`/`classad`.

The local Grafana instance stores its runtime state in `grafana_data_local/` (git-ignored). Dashboard JSONs are copied there with `__DEFAULT_USER__` substituted for the configured `CLUSTER_USER`.

## Key files

| File | Role |
|------|------|
| `stbc-monitor.sh` | Laptop wrapper — the main entry point |
| `setup.sh` | Cluster-side engine (deploy this runs on login.nikhef.nl) |
| `stoomboot_gpu_exporter.py` | Prometheus exporter: queries HTCondor → exposes metrics |
| `prometheus.yml` | Prometheus scrape config (15s interval, targets `localhost:9118`) |
| `grafana/provisioning/` | Auto-provisioned Grafana datasource + two dashboard JSONs |
| `stbc-monitor.conf.example` | Config template to copy to `~/.stbc-monitor.conf` |

## Metrics and dashboards

Every metric has a `cluster="gpu"` or `cluster="cpu"` label. The `resource_type` label is the GPU model (`NVIDIA_L40S`, `NVIDIA_V100`, `AMD_MI50`) for GPU slots or `"CPU"` for CPU slots.

The exporter tracks only the configured `detail_user` (`--detail-user` / `CLUSTER_USER`). The dashboard (`grafana/provisioning/dashboards/stbc_personal.json`) uses `__DEFAULT_USER__` as a placeholder that gets replaced with `CLUSTER_USER` at startup time (by both `stbc-monitor.sh` and `setup.sh`). Do not hardcode usernames in the JSON file.

## GPU node map

`GPU_NODE_MAP` in `stoomboot_gpu_exporter.py` maps short hostnames to GPU types. Update this dict if nodes are added, removed, or re-GPUed. The exporter also normalises device names from the `GPUs_DeviceName` ClassAd attribute as a cross-check.
