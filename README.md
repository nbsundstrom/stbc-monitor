# stbc-monitor

Prometheus exporter + Grafana dashboards for monitoring GPU **and** CPU job usage on the
Stoomboot HTCondor cluster at Nikhef — **driven entirely from your laptop.**

The monitoring stack runs on the cluster host (`login.nikhef.nl` by default) and
collects data continuously (even while your laptop is off). You drive the whole
thing with a local wrapper script and never need to open a shell on the cluster.

---

## Setup (once)

1. Clone this repo on your laptop:
   ```bash
   git clone <this-repo-url> stbc-monitor
   cd stbc-monitor
   ```

2. Create your config:
   ```bash
   cp stbc-monitor.conf.example ~/.stbc-monitor.conf
   $EDITOR ~/.stbc-monitor.conf      # set SSH_USER / SSH_HOST / SSH_JUMP
   ```

3. Run the one-time setup (deploys exporter + Prometheus to the cluster,
   installs Grafana locally via Homebrew):
   ```bash
   ./stbc-monitor.sh --setup
   ```

---

## Daily use

Start monitoring and open Grafana:
```bash
./stbc-monitor.sh
```

This makes sure the exporter and Prometheus are running on the cluster, starts
Grafana locally (via Homebrew), opens an SSH tunnel for Prometheus, and pops
Grafana open in your browser at **http://localhost:3000**
(login `admin` / `stbc_monitor`). Press **Ctrl-C** to close the tunnel —
local Grafana and the cluster services keep running after you disconnect.

| Command | What it does (all from your laptop) |
|---------|--------------------------------------|
| `./stbc-monitor.sh` | Ensure services up -> tunnel -> open Grafana |
| `./stbc-monitor.sh --setup` | One-time: deploy + build env + pull images |
| `./stbc-monitor.sh --status` | Show what's running on the cluster |
| `./stbc-monitor.sh --restart` | Force-restart the cluster services |
| `./stbc-monitor.sh --stop` | Stop the cluster services |
| `./stbc-monitor.sh --logs` | Tail the remote logs |

---

## How it fits together

```
  your laptop                               cluster host (login.nikhef.nl)
  +-----------------------------+           +--------------------------------+
  | stbc-monitor.sh             |-- ssh --> | setup.sh                       |
  |                             |           |   exporter --> Prometheus      |
  | Grafana (local, Homebrew)   |           |   :9118        :9090           |
  |   :3000  <-- tunnel:9090 --|           +---------------+----------------+
  | browser                     |                           | queries
  +-----------------------------+                           v
                                            HTCondor collector (stbc-019.nikhef.nl)
```

Grafana runs locally on your Mac (installed once via Homebrew). Only the
Prometheus port is tunnelled from the cluster. The engine (`setup.sh`) runs on
the cluster and manages the exporter + Prometheus processes. You normally only
ever touch the wrapper (`stbc-monitor.sh`).

---

## Config reference (`~/.stbc-monitor.conf`)

| Variable | Default | Meaning |
|----------|---------|---------|
| `SSH_USER` | `your_username` | Cluster username |
| `SSH_HOST` | `login.nikhef.nl` | Host that runs the server |
| `SSH_JUMP` | *(empty)* | Bastion to hop through — empty means connect directly (set only for an internal-only host) |
| `REMOTE_DIR` | `/data/your_group/your_username/stbc-monitor` | Repo location on the cluster |
| `REPO_URL` | *(empty)* | Git remote - if set, cluster does `git pull`; if empty, wrapper rsyncs your local copy |
| `CLUSTER_USER` | *(= SSH_USER)* | Your condor username — pinned as the Grafana user-dropdown default and used for CPU per-job detail |
| `PORT_GRAFANA` | `3000` | Local browser port |
| `PORT_PROMETHEUS` | `9090` | Local Prometheus port |

Anything here can also be passed as an env var with a `STBC_` prefix
(e.g. `STBC_SSH_HOST=stbc-i2.nikhef.nl ./stbc-monitor.sh`).

---

## Dashboards

One dashboard is provisioned automatically, refreshing every **15s**:

**Stoomboot — Personal** (the default browser landing page): a **Cluster**
dropdown (GPU / CPU / All) and a **My Jobs** dropdown pinned to the running
jobs of the configured `CLUSTER_USER`. Live stat tiles, per-job GPU/CPU/RAM
timeseries, and a per-job detail table — everything filterable from the top
bar.

## What gets monitored

Every metric carries a `cluster="gpu"` or `cluster="cpu"` label.

**Per user (both clusters, everyone):** running vs queued jobs, compute units
held (GPUs or CPU cores), compute-seconds in flight, memory efficiency
(actual / requested).

**Cluster-wide (both clusters):** utilisation per GPU type (L40S / V100 / MI50)
and for CPU cores, claimed vs idle units.

**Per job:** wall-clock duration, GPUs/CPUs requested, memory requested vs
actual. GPU jobs are tracked for **everyone** (small cluster → the leaderboard
shows who's hogging GPUs); CPU per-job detail is tracked **only for your user**
(set via `CLUSTER_USER`) to keep Prometheus cardinality sane given the CPU
cluster can have thousands of jobs.

---

## Running directly on the cluster (advanced)

You don't need this for normal use, but if you ssh in yourself, `setup.sh` is
the engine:

```bash
cd /data/your_group/your_username/stbc-monitor
bash setup.sh                 # build (if needed) + start services
bash setup.sh --build-only    # env + images only, no services
bash setup.sh --restart       # force-restart
bash setup.sh --stop
bash setup.sh --status
```

### setup.sh options

| Flag | Default | Description |
|------|---------|-------------|
| `--collector` | `stbc-019.nikhef.nl` | HTCondor collector host |
| `--port-exporter` | `9118` | Prometheus metrics endpoint |
| `--port-prometheus` | `9090` | Prometheus web UI |
| `--interval` | `15` | Exporter scrape interval (seconds) |
| `--default-user` | `your_username` | Pinned as Grafana user-dropdown default; CPU per-job detail user |
| `--build-only` | - | Build env + pull images, don't start services |
| `--restart` | - | Force-restart instead of leaving running services as-is |
| `--stop` / `--status` | - | Stop / show status |

`setup.sh` prefers the system HTCondor bindings (`htcondor2` on HTCondor 25+,
falling back to `htcondor` on older clusters) and uses the system Python
interpreter rather than relying on a Conda environment.

---

## File layout

```
stbc-monitor/
|- stbc-monitor.sh                <- LOCAL wrapper (run this on your laptop)
|- stbc-monitor.conf.example      <- copy to ~/.stbc-monitor.conf
|- setup.sh                       <- cluster-side engine (driven by the wrapper)
|- stoomboot_gpu_exporter.py      <- exporter: HTCondor GPU+CPU -> metrics
|- prometheus.yml                 <- Prometheus scrape config (15s)
\- grafana/
   \- provisioning/
      |- datasources/prometheus.yml      <- auto-wires Prometheus datasource
       \- dashboards/
          |- dashboards.yml
          \- stbc_personal.json            <- the only dashboard (cluster + my-jobs dropdowns)

grafana_data_local/  (runtime — git-ignored)
  |- grafana.ini      <- generated config pointing at local provisioning
  |- grafana.log      <- local Grafana log
  |- data/            <- Grafana SQLite DB
  |- plugins/         <- installed plugins
  \- dashboards/      <- rendered dashboard JSONs (user substitution applied)
```


---

## Troubleshooting

**Grafana won't load after starting:** local Grafana may still be initialising.
`./stbc-monitor.sh --status` to confirm the cluster services and local Grafana
are up, then retry. Check `grafana_data_local/grafana.log` for errors.

**`git pull` rejected on the cluster:** you have local commits on the cluster
copy. Either set `REPO_URL=""` to switch to rsync deploy, or reconcile the
cluster repo manually.

**Collector hostname wrong:** on the cluster, `condor_q` shows the Schedd host
(collector lives on the same host). Pass `--collector <host>` to `setup.sh` or
hard-set it there.

**No GPU jobs showing up:** check on the cluster with `condor_status -gpu` and
`condor_q -constraint 'RequestGPUs >= 1'`.

**Check logs without sshing in:** `./stbc-monitor.sh --logs`.

---

## Cluster GPU nodes (as of 2026)

| Node | GPU |
|------|-----|
| wn-lot-002 - wn-lot-007 | AMD MI50 |
| wn-lot-008, wn-lot-009 | NVIDIA V100 |
| wn-pijl-002 - wn-pijl-007 | NVIDIA L40S (45 GB) |

Update `GPU_NODE_MAP` in `stoomboot_gpu_exporter.py` if the cluster changes.

---

For cluster questions: `stbc-users@nikhef.nl` or `#stbc-users` on Mattermost.
