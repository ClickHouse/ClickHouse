# Keeper Stress/Chaos Suite

## Architecture

- **Scenario selection & validation**
  - `pytest_plugins/scenario_loader.py`: load modular YAML (`scenarios/*.yaml`), expand presets, apply sharding, parametrize pytest.
  - `framework/core/registry.py`: `@register_fault` for extensibility. Scenario validation is optional; if `framework/core/schema.py` is absent, a no-op validation is used.
- **Cluster orchestration**
  - `framework/core/cluster.py`: spins Keeper clusters via `ClickHouseCluster`; renders config (S3/MinIO, feature flags), starts nodes.
  - `framework/core/settings.py`: centralizes ports/defaults; xdist port offset.
  - `framework/core/preflight.py`: tool/env checks.
- **Workloads / Faults / Gates**
  - `workloads/` (keeper-bench runner, adapter).
  - `faults/` (net/disk/process, plus `parallel`, `background_schedule`).
  - `gates/` (invariants/SLA, Prom thresholds, ACLs, etc.).
  - Workload YAMLs support:
    - `ops`: mix of create/set/get/delete with `percent`, `path_prefix`, and `value_size` (or `value_bytes`)
    - `multi`: basic multi-op with `depth` and `write_ratio`
    - `sweep`: `sizes` and `depths` lists to generate families of set/create/get requests
- **I/O**
  - `framework/io/probes.py`: 4LW (`mntr/wchs/lgif/srvr`), Prometheus, readiness, CH system metrics.
  - `framework/io/prom_parse.py`: Prometheus text parser.
  - `framework/io/sink.py`: ClickHouse HTTP sink (chunking, gzip, retries) for a single table.
- **Metrics**
  - `framework/metrics` (`MetricsSampler`): collects Prom + numeric mntr and writes to the unified `keeper_metrics_ts` table.
- **Authoring helpers**
  - `framework/core/scenario_builder.py`, `framework/presets.py`; `tools/list_scenarios.py` for dry-run preview.

### Imports map

- Faults: `from tests.stress.keeper.faults import ...`
- Gates: `from tests.stress.keeper.gates import ...`
- Workloads: `from tests.stress.keeper.workloads import ...`
- Core engine: `from tests.stress.keeper.framework.core import (cluster, settings, util, registry, preflight, scenario_builder)`
- IO: `from tests.stress.keeper.framework.io import (probes, sink, prom_parse)`
- Metrics: `from tests.stress.keeper.framework.metrics import MetricsSampler`
- Scenario authoring: `from tests.stress.keeper.framework import (presets, fuzz, scenario_builder)`

## Execution flow

1. Pytest collects scenarios via `pytest_generate_tests` in `pytest_plugins/scenario_loader.py`.
   - Reads YAML(s) (default `core.yaml`), applies env filters (`KEEPER_INCLUDE_TAGS`, `KEEPER_TOTAL_SHARDS`/`KEEPER_SHARD_INDEX`), preset expansion, and macro injections (e.g., `RCFG-*` gets `config_converged`/`backlog_drains`).
2. `tests/test_scenarios.py::test_scenario` runs per parameterized scenario.
   - `conftest.py` parses CLI/env; `cluster_factory` builds nodes through `framework/cluster.py`.
3. `framework/preflight.ensure_environment` verifies bench tool availability (accepts `clickhouse` fallback), required fault tools, replay artifacts.
4. Optional `MetricsSampler` starts to stream metrics to ClickHouse; `ctx._metrics_cache` is updated atomically.
5. `pre` steps run via `faults.apply_step` (registry dispatch). Context (e.g., `watch_baseline`, `lgif_before`) may be recorded.
6. If a workload is present, `KeeperBench` runs (config or replay) against `servers_arg(nodes)` and returns a JSON summary.
7. Each `gate` is applied via `gates.apply_gate`, using cached metrics or Keeper client helpers where available.
8. Snapshots (`pre`/`post`/`fail`) of Prom and numeric mntr, plus CH metrics, are written to a single unified metrics table.
9. Sampler flush/stop, KeeperClient pools closed, and cluster shutdown (unless `--keep-containers-on-fail`).

Tip: orchestration faults `parallel` and `background_schedule` are registry‑first; they compose nested steps and propagate first error.

## Quickstart

Run a single scenario (all defaults):

```
pytest -q -n auto tests/stress/keeper/tests/test_scenarios.py::test_scenario \
  --keeper-backend=default \
  --commit-sha=$(git rev-parse --short HEAD)
```

Run all scenarios sharded in CI:

```
export KEEPER_TOTAL_SHARDS=4
export KEEPER_SHARD_INDEX=0
pytest -q -n auto tests/stress/keeper/tests/test_scenarios.py::test_scenario
```

## Environment variables

- KEEPER_CLIENT_PORT (default 9181)
- KEEPER_RAFT_PORT (default 9234)
- KEEPER_PROM_PORT (default 9363)
- KEEPER_CONTROL_PORT (default 0; enable `/ready` when >0)
- KEEPER_ENABLE_CONTROL (gate for injecting the `<http_control>` block; set to `1` only when the image supports `/ready`)
- KEEPER_ID_BASE (default 1)
- KEEPER_BACKEND (default|rocksdb via --keeper-backend)
- S3/MinIO:
  - KEEPER_S3_LOG_ENDPOINT, KEEPER_S3_SNAPSHOT_ENDPOINT, KEEPER_S3_REGION
  - or KEEPER_MINIO_ENDPOINT, KEEPER_MINIO_ACCESS_KEY, KEEPER_MINIO_SECRET_KEY
- Sharding:
  - KEEPER_TOTAL or KEEPER_TOTAL_SHARDS
  - KEEPER_INDEX or KEEPER_SHARD_INDEX
- Workload overrides:
  - KEEPER_WORKLOAD_CONFIG: path to YAML (e.g., `tests/stress/keeper/workloads/prod_mix.yaml`)
  - KEEPER_REPLAY_PATH: path to keeper-bench native request log inside container (for replay scenarios)
  - KEEPER_BENCH_CLIENTS: override concurrency for keeper-bench
- Strictness:
  - KEEPER_STRICT=1 enforces preflight failures (e.g., missing bench tool or replay file); otherwise warnings and best-effort skips

## Common runs

- Default backend:
```
pytest tests/stress/keeper/tests/test_scenarios.py::test_scenario --keeper-backend=default
```

- RocksKeeper backend:
```
pytest tests/stress/keeper/tests/test_scenarios.py::test_scenario --keeper-backend=rocksdb
```

- With MinIO tiering (enables DSK-*):
```
export KEEPER_MINIO_ENDPOINT=http://127.0.0.1:9000
export KEEPER_MINIO_ACCESS_KEY=minio
export KEEPER_MINIO_SECRET_KEY=minio123
pytest tests/stress/keeper/tests/test_scenarios.py::test_scenario
```

- With S3 tiering:
```
export KEEPER_S3_LOG_ENDPOINT=https://s3.amazonaws.com/bucket/logs/
export KEEPER_S3_SNAPSHOT_ENDPOINT=https://s3.amazonaws.com/bucket/snaps/
export KEEPER_S3_REGION=us-west-2
pytest tests/stress/keeper/tests/test_scenarios.py::test_scenario
```

- Enable readiness lifecycle gates:
```
export KEEPER_CONTROL_PORT=9182
pytest tests/stress/keeper/tests/test_scenarios.py::test_scenario -k CFG-02
```

- Sink metrics/results to ClickHouse (unified table + default.checks):
```
# Metrics sink (header auth; works with special chars)
export KEEPER_METRICS_CLICKHOUSE_URL=http://<host>:8123
export KEEPER_METRICS_CLICKHOUSE_USER=<user>
export KEEPER_METRICS_CLICKHOUSE_PASSWORD=<password>
export KEEPER_METRICS_DB=keeper_stress_tests

# (optional) One-time seed
curl -sSf \
  -H "X-ClickHouse-User: $KEEPER_METRICS_CLICKHOUSE_USER" \
  -H "X-ClickHouse-Key:  $KEEPER_METRICS_CLICKHOUSE_PASSWORD" \
  --data-binary @tests/stress/keeper/tools/create_tables.sql \
  "$KEEPER_METRICS_CLICKHOUSE_URL?query="

pytest tests/stress/keeper/tests/test_scenarios.py::test_scenario --sink-url=$KEEPER_METRICS_CLICKHOUSE_URL
```

## Replay & Synthetic Profiling

You can load a production-like workload in two ways:

- Replay real requests from system.zookeeper_log (converted to keeper-bench native request log)
- Generate a synthetic keeper-bench config statistically profiled from system.zookeeper_log

### 1) Replay production log

Steps:

- Convert production log to keeper-bench native request log (outside this repo), producing a file like `/path/to/keeper_replay.native`.
- Make it visible inside containers at the same path, e.g. bind-mount to `/artifacts` on the host:

```
sudo mkdir -p /artifacts
sudo mount -o bind /path/to/keeper_replay.native /artifacts/keeper_replay.native
```

- Run the built-in replay scenario (uses `--input-request-log`, distributes hosts, sets concurrency/timelimit):

```
pytest -q tests/stress/keeper/tests/test_scenarios.py -k LOAD-01 \
  --keeper-backend=default --duration=300
```

Notes:

- Scenario `LOAD-01` expects `workload.replay: /artifacts/system.zookeeper_log` by default. You can set another path via env (`KEEPER_REPLAY_PATH`) or copy/link accordingly.
- Preflight verifies `keeper-bench` availability and that the replay file exists inside the container.
- Gate `replay_repeatable` reruns the replay and checks that error-rate and p99 deltas are within thresholds for repeatability.

### 2) Build a synthetic profile from production log

If direct replay is not repeatable, synthesize a keeper-bench config from a JSONEachRow dump of `system.zookeeper_log`:

```
# Dump production log to JSONEachRow (example; adapt query/source accordingly)
# clickhouse-client -q "SELECT * FROM system.zookeeper_log FORMAT JSONEachRow" > zk_log.json

# Profile and emit a YAML keeper-bench config (percent mix + p90 value_size for writes)
python3 tests/stress/keeper/tools/profile_replay.py \
  --input zk_log.json --concurrency 64 --timelimit 300 \
  --output workloads/prod_profiled.yaml

# Run any scenario with:
#   workload: { config: workloads/prod_profiled.yaml, duration: 300 }
pytest -q tests/stress/keeper/tests/test_scenarios.py -k CHA-01 --keeper-backend=default
```

The runner will translate the YAML into keeper-bench config (requests, connections across all hosts, sessions allocation, output to JSON), run the tool inside the containers, and parse ops/errors/latency metrics for gates. If the bench tool is missing and `KEEPER_STRICT` is not set, the run will proceed without bench execution and gates should be tolerant.

- Replay production log (LOAD-01):
```
sudo mount -o bind /path/to/system.zookeeper_log /artifacts/system.zookeeper_log
pytest -k LOAD-01 tests/stress/keeper/tests/test_scenarios.py::test_scenario
```

## Scenario knobs (opts)

- `monitor_interval_s`: sampler interval seconds (default 5)
- `prom_allow_prefixes`: list of Prometheus metric prefixes to parse into `keeper_prom_parsed`

## Selecting scenarios

- By default, the loader uses curated modular scenarios via `scenarios/core.yaml,cha.yaml`.
- To load multiple files, set `KEEPER_SCENARIO_FILE` to a comma-separated list, e.g.:

```
export KEEPER_SCENARIO_FILE=core.yaml,experiments.yaml
pytest tests/stress/keeper/tests/test_scenarios.py::test_scenario
```

- To auto-discover all `*.yaml` under `scenarios/` (modular layout), set:

```
export KEEPER_SCENARIO_FILE=all
pytest tests/stress/keeper/tests/test_scenarios.py::test_scenario
```

Tip: Keep `core.yaml` fast and stable; put heavy or weekly cases in
separate files (e.g., `experiments.yaml`, `weekly.yaml`).

Note: The monolithic `keeper_e2e.yaml` is excluded from autodiscover by default; set `KEEPER_SCENARIO_FILE=keeper_e2e.yaml` to run it explicitly.

### Filtering scenarios via tags

Scenarios automatically receive a tag based on their ID prefix (e.g., `CFG-07` → tag `cfg`).
You can include/exclude by tags using env vars:

```
export KEEPER_INCLUDE_TAGS=cfg,ldr   # run only config and leader scenarios
export KEEPER_EXCLUDE_TAGS=weekly    # skip weekly cases
pytest tests/stress/keeper/tests/test_scenarios.py::test_scenario
```

## CI (GitHub Actions)

```
jobs:
  keeper-stress:
    runs-on: ubuntu-22.04
    strategy:
      matrix: { shard: [0,1,2,3], backend: [default, rocksdb] }
    steps:
      - uses: actions/checkout@v4
      - name: System deps
        run: sudo apt-get update && sudo apt-get install -y iproute2 iptables dmsetup curl
      - name: Run tests
        env:
          KEEPER_TOTAL_SHARDS: 4
          KEEPER_SHARD_INDEX: ${{ matrix.shard }}
        run: |
          pytest -q -n auto tests/stress/keeper/tests/test_scenarios.py::test_scenario \
            --keeper-backend=${{ matrix.backend }} \
            --commit-sha=${{ github.sha }}

## System dependencies (local and CI)

Install these OS packages on test hosts/runners:

- iproute2 (ip, tc)
- iptables (iptables, ip6tables)
- dmsetup (for dm faults)
- curl
- netcat-openbsd (nc)
- stress-ng (optional; enables stress_ng fault)

### Secrets & auth (CI and local)

- CI DB (default.checks): Praktika secret retrieval with env fallback in `tests/test_scenarios.py` uses:
  - Secrets: `KEEPER_CIDB_URL`, `KEEPER_CIDB_USER`, `KEEPER_CIDB_PASSWORD`
  - Fallback env vars of the same names for local smoke.
- Metrics sink: header-based auth (`X-ClickHouse-User/Key`) to avoid password quoting issues.

### Data model for dashboards

- Results: `default.checks` (authoritative for gating and pass/fail).
- Metrics: `keeper_stress_tests.keeper_metrics_ts` (unified timeseries with tags: run_id, commit_sha, backend, scenario, topology, node, stage, source, name, value, labels_json).
