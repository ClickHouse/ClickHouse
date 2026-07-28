# Keeper Stress Tests

Scenario-driven stress-testing framework for ClickHouse Keeper. Runs keeper-bench
workloads against a 3-node cluster (default Keeper, RocksDB Keeper, Apache ZooKeeper,
or RaftKeeper backends), optionally with fault injection.

## Current setup

- **Topology:** 3 Keeper nodes, single machine, 3 Docker containers.
- **Workload:** Scenario-driven via YAML configs under `workloads/`. The active workload
  for each scenario is declared in `scenarios/*.yaml`. At runtime the framework patches
  `concurrency`, `connections` (host:port per node with sessions distributed), and
  `timelimit` (from `--duration`). Request mix, setup tree, and value sizes come from
  the workload YAML.
- **Backends:** `default` (NuRaft log store), `rocks` (RocksDB log store), `zookeeper`
  (Apache ZooKeeper), `raftkeeper` (JDRaftKeeper).

**Example patched config sent to keeper-bench** (`[keeper][bench][config patched]` in logs):

```yaml
name: prod_mix
concurrency: 640
report_delay: 0.0
continue_on_error: true
connections:
  operation_timeout_ms: 30000
  connection_timeout_ms: 400000
  connection:
  - host: localhost:19181
    sessions: 214
  - host: localhost:19182
    sessions: 213
  - host: localhost:19183
    sessions: 213
setup:
  node:
    name: e2e
    node:
      name: prod
      node:
      - name: hot
        data:
          random_string:
            size: {min_value: 0, max_value: 162}
      - name: pool
        node:
        - repeat: 64
          name:
            random_string: {size: 12}
          data:
            random_string:
              size: {min_value: 0, max_value: 162}
      - name: churn
generator:
  requests:
    get:
      path:
        children_of: /e2e/prod
      weight: 38
    create:
      path:
        children_of: /e2e/prod/pool
      name_length: {min_value: 8, max_value: 16}
      data:
        random_string:
          size: {min_value: 0, max_value: 162}
      remove_factor: 0.49
      weight: 34
    set:
      path:
        children_of: /e2e/prod/pool
      data:
        random_string:
          size: {min_value: 0, max_value: 162}
      weight: 9
    multi:
      size: {min_value: 3, max_value: 7}
      create:
        path: /e2e/prod/churn
        data:
          random_string:
            size: {min_value: 0, max_value: 162}
      set:
        path: /e2e/prod/hot
        data:
          random_string:
            size: {min_value: 0, max_value: 162}
      weight: 11
    list:
      path:
        children_of: /e2e/prod
      weight: 4
timelimit: 900
output:
  file:
    path: /tmp/keeper_bench_out_*.json
    with_timestamp: false
  stdout: true
```

## Running locally

**Prerequisites:** Build ClickHouse (`keeper-bench` uses the binary). Run from the **repo root**.

```bash
PYTHONPATH=".:tests/stress:ci" CLICKHOUSE_BINARY=$(pwd)/build/programs/clickhouse \
  pytest tests/stress/keeper/tests/test_scenarios.py \
  -k '<scenario> and <backend>' --matrix-backends=<backend> -x -s
```

**Example — single no-fault scenario, default backend, 15 min:**

```bash
export CLICKHOUSE_BINARY=$(pwd)/build/programs/clickhouse
export PYTHONPATH=.:tests/stress:ci

pytest -p no:cacheprovider --durations=0 -vv -s \
  tests/stress/keeper/tests/test_scenarios.py \
  -k 'prod-mix-no-fault and default' \
  --matrix-backends=default
```

**Example — fault scenario, all backends:**

```bash
pytest -p no:cacheprovider --durations=0 -vv -s \
  tests/stress/keeper/tests/test_scenarios.py \
  -k 'prod-mix-fault' \
  --matrix-backends=default,rocks
```

**Key options:**

| Option / Env | Meaning |
|---|---|
| `CLICKHOUSE_BINARY` | Path to `clickhouse` binary used by keeper-bench. |
| `PYTHONPATH=.:tests/stress:ci` | Required so `keeper.*` and `ci.*` imports resolve. |
| `-k 'prod-mix-no-fault and default'` | Filter by scenario name and/or backend. |
| `--matrix-backends=default,rocks` | Backends to test (comma-separated). |
| `--matrix-topologies=3` | Node count (default 3). |
| `--seed N` | Reproducible fault randomization. |
| `KEEPER_DURATION` | Override scenario duration in seconds. |
| `KEEPER_FAULTS` | `true`/`false` — override fault injection toggle. |
| `KEEPER_WORKLOAD_CONFIG` | Path to a workload YAML to use instead of the scenario's default. |

Available backends: `default`, `rocks`, `zookeeper`, `raftkeeper`.

## Changing workload

**Override for a single run (no file edits)** — set `KEEPER_WORKLOAD_CONFIG`:

```bash
export KEEPER_WORKLOAD_CONFIG=tests/stress/keeper/workloads/read_requests.yaml
pytest ... -k 'prod-mix-no-fault and default'
```

Takes precedence over the scenario's declared workload. Use any YAML under
`tests/stress/keeper/workloads/`.

**Permanent change** — edit the workload YAML directly (`workloads/prod_mix.yaml` etc.)
and adjust `generator.requests` weights, `setup` tree, or value sizes. The framework
patches `concurrency`, `connections`, and `timelimit` at runtime regardless.

## Adding workload types

To add a read-heavy or write-heavy variant:

1. Copy an existing YAML: `cp workloads/prod_mix.yaml workloads/read_heavy.yaml`
2. Tune `generator.requests` weights:
   - **Read-heavy:** raise `get`/`list`, lower `create`/`set`/`multi`.
   - **Write-heavy:** raise `create`/`set`/`multi`, lower `get`/`list`.
3. Keep the same `setup` tree so results stay comparable across workloads.
4. Reference it in a scenario YAML (`workload: {config: workloads/read_heavy.yaml}`)
   or point at it via `KEEPER_WORKLOAD_CONFIG` for ad-hoc runs.

## Early comparison: Default vs RocksDB

Early no-fault results at two concurrency levels (3-node cluster, `prod-mix` workload):

| Concurrency | Backend | RPS | P99 (ms) | Container memory |
|---|---|---|---|---|
| 640 | default | 4.96K | 577 | 2.59 GiB |
| 640 | rocks | 1.31K | 2480 | 1.52 GiB |
| 32 | default | 2.1K | 44 | 1.30 GiB |
| 32 | rocks | 950 | 235 | 1.10 GiB |

**Takeaways:** Default leads on throughput and P99 at both concurrency levels. Rocks uses
less memory at 640 concurrency but pays in RPS (×3.8 lower) and tail latency (×4.3 higher).
At 32 concurrency the gap narrows but default remains ahead. See the full benchmark tables
below for 15-minute results across all 10 scenarios and 3 backends.

## Reference

- **Workload configs:** `tests/stress/keeper/workloads/` — one YAML per workload type.
- **Scenarios:** `tests/stress/keeper/scenarios/` — `core_no_faults.yaml`, `core_faults.yaml`.
- **Framework entry point:** `tests/stress/keeper/tests/test_scenarios.py`.
- **Settings / timeouts:** `tests/stress/keeper/framework/core/settings.py`.

## Benchmark Results

All results at 640 concurrency, 3-node cluster, 15-minute runs.

### No-Fault Baseline — Run 1 (Default / RocksDB / ZooKeeper)

| Scenario | Explanation | Default RPS | Rocks RPS | ZooKeeper RPS |
|----------|-------------|-------------|-----------|---------------|
| `prod-mix-no-fault` | Tree `/e2e/prod` with hot (1 node), pool (64 children, 12-char names). Mix: get 38% (hot or children_of /e2e/prod), create 34% (churn/pool/children_of pool; name 8–16 chars; remove_factor 0.49), set 9% (hot or pool children), multi 11% (3–7 ops/req: create on churn + set on hot), list 4% (pool or children_of prod). Values 0–162 B. | 5.09K | 1.41K | Not supported |
| `read-no-fault` | Mix: get 90% (hot or children_of prod), list 10% (pool or children_of prod). Read-only. Values 0–162 B. | 137K | 137K | Timed out/Error |
| `write-no-fault` | Mix: create 60% (churn/pool/children_of pool; name 8–16 chars; remove_factor 0.49), set 40% (hot or pool children). Write-only. Values 0–162 B. | 34.5K | 18.2K | 24.9K |
| `read-multi-no-fault` | Mix: get 85% (hot or children_of prod), list 15% (pool or children_of prod). Uses MultiRead batching. Values 0–162 B. | 138K | 137K | Not supported |
| `write-multi-no-fault` | Mix: create 25% (churn/pool; name 8–16 chars; remove_factor 0.49), set 25% (hot/pool children), multi 50% (5–15 ops/req: create on churn + set on hot). Values 0–162 B. | 8.41K | 2.96K | Timed out/Error |
| `list-heavy-no-fault` | Mix: list 75% (pool or children_of prod), get 25% (hot or children_of prod). Values 0–162 B. | 131K | 133K | Timed out/Error |
| `churn-no-fault` | Mix: create 80% (churn/pool/children_of pool; name 8–16 chars; remove_factor 0.7 — ~70% removes), set 20% (hot/pool children). Heavy create/remove churn. Values 0–162 B. | 32.6K | 18.4K | 27.6K |
| `large-payload-no-fault` | Same tree; values 1–4 KB. Mix same as prod-mix (get 38%, create 34%, set 9%, multi 11%, list 4%). remove_factor 0.49. Stresses serialization and network. | 3.37K | 465 | Timed out/Error |
| `single-hot-get-no-fault` | Mix: get 100% on `/e2e/prod/hot` only (no children_of). Values 0–162 B. Single hot path read. | 133K | 142K | 26.7K |
| `multi-large-no-fault` | Mix: multi 100% (10–30 ops/req). Each multi: create on churn (remove_factor 0.3) + set on hot. Values 0–162 B. Large batch size, small values. | 4.64K | 1.37K | Timed out/Error |

### No-Fault Baseline — Run 2 (Default / RocksDB / RaftKeeper)

| Scenario | Explanation | Default RPS | Rocks RPS | RaftKeeper RPS |
|----------|-------------|-------------|-----------|----------------|
| `prod-mix-no-fault` | Tree `/e2e/prod` with hot (1 node), pool (64 children, 12-char names). Mix: get 38%, create 34% (remove_factor 0.49), set 9%, multi 11% (3–7 ops/req), list 4%. Values 0–162 B. | 5.12K | 1.42K | 3.08K |
| `read-no-fault` | Mix: get 90%, list 10%. Read-only. Values 0–162 B. | 140K | 139K | 178K |
| `write-no-fault` | Mix: create 60% (remove_factor 0.49), set 40%. Write-only. Values 0–162 B. | 36K | 19K | 12K |
| `read-multi-no-fault` | Mix: get 85%, list 15%. Uses MultiRead batching. Values 0–162 B. | 137K | 140K | 179K |
| `write-multi-no-fault` | Mix: create 25% (remove_factor 0.49), set 25%, multi 50% (5–15 ops/req). Values 0–162 B. | 9K | 3K | Timed out/Error |
| `list-heavy-no-fault` | Mix: list 75%, get 25%. Values 0–162 B. | 138K | 137K | 141K |
| `churn-no-fault` | Mix: create 80% (remove_factor 0.7), set 20%. Heavy create/remove churn. Values 0–162 B. | 33K | 19K | 19K |
| `large-payload-no-fault` | Same tree; values 1–4 KB. Same mix as prod-mix. Stresses serialization and network. | 3.46K | 463 | 3.09K |
| `single-hot-get-no-fault` | Mix: get 100% on `/e2e/prod/hot`. Values 0–162 B. Single hot path read. | 142K | 140K | 180K |
| `multi-large-no-fault` | Mix: multi 100% (10–30 ops/req: create + set, remove_factor 0.3). Values 0–162 B. Large batch size, small values. | 4.67K | 1.39K | 7.54K |

### Fault Tests — Run 1 (Default / RocksDB, netem only)

**Setup:** 16 scenarios, 2 backends (Default, RocksDB), 7 network fault shapes.
`tc netem` applied for the full 20-minute run in 30-second rounds. No node restarts.

**Fault shapes:**

| Shape | Description |
|-------|-------------|
| Uniform latency | All 3 nodes: +10ms ±5ms egress |
| Multi-region | Leader +10ms ±5ms, all followers +80ms ±20ms simultaneously (`kind: parallel`) |
| WAN + loss | All 3 nodes: +25ms ±7ms, 2% packet loss |
| High jitter | All 3 nodes: +20ms ±15ms (high jitter:delay ratio) |
| Slow followers | All followers +80ms ±20ms, leader clean |
| Single slow follower | First follower only +80ms ±20ms, leader and second follower clean |
| WAN client path | Leader only +50ms ±10ms, both followers clean (inter-node Raft unaffected) |

**Uniform latency highlights:**

- Read-heavy workloads are backend-neutral under latency — RPS nearly identical across backends:
  - `read`: 55,600 / 55,300
  - `read-multi`: 55,500 / 55,300
  - `list-heavy`: 55,300 / 55,000
  - `single-hot-get`: 55,500 / 55,500
- Write & mixed workloads show a growing gap with complexity (Default / Rocks):
  - `write`: 7,100 / 6,800 (−4%)
  - `churn`: 7,100 / 6,700 (−6%)
  - `prod-mix`: 1,300 / 1,200 (−8%)
  - `write-multi`: 3,500 / 2,500 (−29%)
  - `multi-large`: 1,800 / 1,200 (−33%)
  - `large-payload`: 1,200 / 429 (−64%)
- Specialized latency shapes on `prod-mix` workload (Default / Rocks):
  - `jitter`: 862 / 834
  - `WAN`: 779 / 735
  - `single-follower`: 731 / 684
  - `WAN-client`: 634 / 599
  - `slow-followers`: 574 / 541
  - `multi-region`: 561 / 532
  - Default consistently ~5% ahead across all fault shapes.

**Full results:**

| Fault Test | Default RPS | Rocks RPS | Explanation |
|------------|-------------|-----------|-------------|
| `prod-mix-fault` | 1.3K | 1.2K | Tree: `/e2e/prod/{hot,pool×64,churn}`, values 0–162 B. Mix: get 38%, create/delete 34%, multi 3–7 ops 11%, set 9%, list 4%. Fault: all 3 nodes +10ms ±5ms egress netem for full 1200s. Pair: `prod-mix-no-fault`. |
| `read-fault` | 55.6K | 55.3K | Same tree. Mix: get 90%, list 10% (read-only). Fault: all 3 nodes +10ms ±5ms. Tests read path resilience under latency. Pair: `read-no-fault`. |
| `write-fault` | 7.1K | 6.8K | Same tree. Mix: create/delete 60% (remove_factor 0.49), set 40% (write-only). Fault: all 3 nodes +10ms ±5ms. Tests Raft consensus under latency. Pair: `write-no-fault`. |
| `read-multi-fault` | 55.5K | 55.3K | Same tree. Mix: get 85%, list 15%. Feature flag `multi_read: 1` enables batched MultiRead. Fault: all 3 nodes +10ms ±5ms. Tests MultiRead batching under latency. Pair: `read-multi-no-fault`. |
| `write-multi-fault` | 3.5K | 2.5K | Same tree. Mix: multi 5–15 ops/batch 50%, create/delete 25%, set 25%. Fault: all 3 nodes +10ms ±5ms. Tests multi-op transaction batching under latency. Pair: `write-multi-no-fault`. |
| `list-heavy-fault` | 55.3K | 55K | Same tree. Mix: list 75%, get 25%. Heavy list stresses metadata scan. Fault: all 3 nodes +10ms ±5ms. Pair: `list-heavy-no-fault`. |
| `churn-fault` | 7.1K | 6.7K | Same tree. Mix: create/delete 80% (remove_factor 0.7), set 20%. High churn stresses create/remove and journal GC. Fault: all 3 nodes +10ms ±5ms. Pair: `churn-no-fault`. |
| `large-payload-fault` | 1.2K | 429 | Same tree but values 1–4 KB. Same mix as prod-mix. Fault: all 3 nodes +10ms ±5ms. Tests serialization + log size + network throughput under latency. Pair: `large-payload-no-fault`. |
| `single-hot-get-fault` | 55.5K | 55.5K | Same tree. Mix: 100% get on `/e2e/prod/hot`. Saturates cache-hot read path. Fault: all 3 nodes +10ms ±5ms. Pair: `single-hot-get-no-fault`. |
| `multi-large-fault` | 1.8K | 1.2K | Same tree. Mix: 100% multi batches of 10–30 ops/batch (create+set, remove_factor 0.3). Saturates multi-transaction path and log batching. Fault: all 3 nodes +10ms ±5ms. Pair: `multi-large-no-fault`. |
| `latency-multi-region-fault` | 561 | 532 | `prod-mix` workload. Fault: `kind: parallel` — leader +10ms ±5ms AND all followers +80ms ±20ms simultaneously. Models geo-distributed deployment where clients and leader are co-located but followers are remote. Raft AppendEntries acks arrive 80ms late; writes must wait for quorum. |
| `latency-wan-fault` | 779 | 735 | `prod-mix` workload. Fault: all 3 nodes +25ms ±7ms + 2% packet loss. TCP retransmits under loss make tail latency unbounded. Models high-latency WAN with unreliable link. Max error rate relaxed to 5%. |
| `latency-jitter-fault` | 862 | 834 | `prod-mix` workload, 180s extended for more fault rounds. Fault: all 3 nodes +20ms ±15ms jitter (very high jitter:delay ratio). Tests Raft election timeout stability under irregular RTTs. Max error rate relaxed to 5%. |
| `latency-slow-follower-fault` | 574 | 541 | `prod-mix` workload. Fault: all followers +80ms ±20ms, leader stays fast. Raft AppendEntries acks arrive late so the leader waits for quorum before committing writes. Write p99 rises sharply; reads served by leader are unaffected. Compare with `latency-multi-region-fault` (leader also has latency). |
| `latency-single-follower-fault` | 731 | 684 | `prod-mix` workload. Fault: first follower only +80ms ±20ms; leader and second follower stay clean. Quorum (leader + one fast follower) always maintained. Models one node in a distant DC while the rest of the cluster is local. Compare with `latency-slow-follower-fault` (all followers affected). |
| `latency-wan-client-fault` | 634 | 599 | `prod-mix` workload. Fault: leader only +50ms ±10ms; both followers stay clean. Client↔leader link is the WAN path; inter-node Raft traffic is completely unaffected. Tests how client-perceived latency rises when the leader is geographically distant. Compare with `latency-slow-follower-fault` (Raft acks slow, client path fast). |
