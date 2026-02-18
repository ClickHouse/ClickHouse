# Keeper stress tests

## Current setup

- **Scope:** Single scenario (e.g. CHA-01), **no faults** — baseline workload only.
- **Topology:** 3 Keeper nodes, single ARM machine, 3 Docker containers.
- **Workload:** One config today (`workloads/prod_mix.yaml`) with a fixed request mix and concurrency.

**This is what we are generating at the moment** (patched config actually passed to keeper-bench; `[keeper][bench][config patched]` in logs):

```yaml
name: prod_mix
concurrency: 224
report_delay: 0.0
continue_on_error: true
connections:
  operation_timeout_ms: 30000
  connection_timeout_ms: 400000
  connection:
  - host: localhost:19181
    sessions: 75
  - host: localhost:19182
    sessions: 75
  - host: localhost:19183
    sessions: 74
setup:
  node:
    name: e2e
    node:
      name: prod
      node:
      - name: hot
        data:
          random_string:
            size:
              min_value: 0
              max_value: 162
      - name: pool
        node:
        - repeat: 64
          name:
            random_string:
              size: 12
          data:
            random_string:
              size:
                min_value: 0
                max_value: 162
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
      name_length:
        min_value: 8
        max_value: 16
      data:
        random_string:
          size:
            min_value: 0
            max_value: 162
      remove_factor: 0.49
      weight: 34
    set:
      path:
        children_of: /e2e/prod/pool
      data:
        random_string:
          size:
            min_value: 0
            max_value: 162
      weight: 9
    multi:
      size:
        min_value: 3
        max_value: 7
      create:
        path: /e2e/prod/churn
        data:
          random_string:
            size:
              min_value: 0
              max_value: 162
      set:
        path: /e2e/prod/hot
        data:
          random_string:
            size:
              min_value: 0
              max_value: 162
      weight: 11
    list:
      path:
        children_of: /e2e/prod
      weight: 4
timelimit: 1140
output:
  file:
    path: /tmp/keeper_bench_out_*.json
    with_timestamp: false
  stdout: true
```

Concurrency and `connection` hosts/sessions are patched at runtime from the scenario (duration → `timelimit`; topology → multiple hosts with sessions distributed). Request mix (get 38, create 34, set 9, multi 11, list 4) and setup (e2e/prod with hot, pool×64, churn) come from `prod_mix.yaml`.

***Example job*** - https://s3.amazonaws.com/clickhouse-test-reports/PRs/91474/ffeb1364e96fac99aa8900523aab64b18585f407/keeper_stress_tests_pr/job.log**

---

## Running locally

**Prerequisites:** Build ClickHouse (keeper-bench uses the binary). Run from the **repo root**.

**Example — single scenario, no faults, default backend, 6000s duration:**

```bash
export CLICKHOUSE_BINARY=/home/ubuntu/ClickHouse/build/programs/clickhouse
export PYTHONPATH=.:tests/stress

pytest -p no:cacheprovider --durations=0 -vv -s \
  tests/stress/keeper/tests/test_scenarios.py \
  -k 'CHA-01' \
  --duration 6000 \
  --matrix-backends=default \
  --faults=false
```

| Part | Meaning |
|------|--------|
| `CLICKHOUSE_BINARY` | Path to `clickhouse` binary (used by keeper-bench). |
| `PYTHONPATH=.:tests/stress` | So imports like `keeper.*` and test paths resolve. |
| `-k 'CHA-01'` | Run only scenarios matching CHA-01. |
| `--duration 6000` | Scenario duration in seconds (overrides scenario YAML). |
| `--matrix-backends=default` | Only default backend (`rocks` to add Rocks, or `default,rocks` for both). |
| `--faults=false` | No fault injection; workload only. |

**Other useful options:** `--replay <path>` for replay; `--matrix-topologies 3` (default); `--seed N` for reproducible fault runs when `--faults=true`. Env: `KEEPER_DURATION`, `KEEPER_MATRIX_BACKENDS`, `KEEPER_FAULTS`, `KEEPER_REPLAY_PATH`.

---

## Changing workload (how to alter or override)

**1. Use a different workload YAML for this run (no scenario change)**  
Set **`KEEPER_WORKLOAD_CONFIG`** to the path of the config file (relative to cwd or absolute):

```bash
export KEEPER_WORKLOAD_CONFIG=tests/stress/keeper/workloads/read_heavy.yaml
pytest ... tests/stress/keeper/tests/test_scenarios.py -k 'CHA-01' --duration 6000 --matrix-backends=default --faults=false
```

Takes precedence over the scenario’s `workload.config`. Paths starting with `tests/` are relative to repo root; e.g. `workloads/read_heavy.yaml` is relative to the keeper stress dir (or cwd).

**2. Edit the workload file**  
Change `workloads/prod_mix.yaml` (or a copy) and run as usual: adjust `concurrency`, `generator.requests` weights (get/create/set/multi/list), or `setup`. The patched config (connections, timelimit) is still applied at runtime.

**3. Replay instead of generator**  
Use a scenario that has `workload.replay` (e.g. CHA-01-REPLAY), or pass replay from the CLI:

```bash
pytest ... --replay /path/to/log.parquet
# or
export KEEPER_REPLAY_PATH=/path/to/log.parquet
```

**4. Possible future support**  
We could add a CLI option to pass a workload config path (e.g. `--workload-config`) or overrides (e.g. `--workload-override concurrency=320`) for a single run without editing files or env. Not implemented yet; today use `KEEPER_WORKLOAD_CONFIG` or edit the YAML.

---

## Changing workload type (read-heavy, write-heavy, etc.)

People often want multiple workload types (read-heavy, write-heavy, mixed). Today we only have **one** workload config: `workloads/prod_mix.yaml`. To **override** it for a run, see [Changing workload (how to alter or override)](#changing-workload-how-to-alter-or-override) above (`KEEPER_WORKLOAD_CONFIG` or edit YAML).

**How to add more workload types:**

1. **New YAML per mix** — Copy `prod_mix.yaml` to e.g. `workloads/read_heavy.yaml` / `workloads/write_heavy.yaml` under `tests/stress/keeper/workloads/`.
2. **Tune the generator weights** in `generator.requests`:
   - **Read-heavy:** Increase `get` and/or `list` `weight`, decrease `create` / `set` / `multi`.
   - **Write-heavy:** Increase `create`, `set`, `multi` weights; decrease `get` / `list`.
   - Keep the same `connections` / `setup` / paths so runs stay comparable.
3. **Use the new config** — Either set `KEEPER_WORKLOAD_CONFIG=tests/stress/keeper/workloads/read_heavy.yaml` when running, or in `scenarios/*.yaml` set `workload: { config: workloads/read_heavy.yaml }` for a scenario so CI or manual runs pick it.

**Replay (existing):** Use a scenario with `workload.replay` (e.g. CHA-01-REPLAY) or pass `--replay <path>`. Replay runs until the log is consumed (duration can differ slightly between backends).

---

## Rocks vs default — observations

Summary of runs on the current setup (3 nodes, ARM, Docker, no faults):

| Concurrency | Backend | RPS   | P99 (ms) | Total ops | Container memory | Container CPU (usage) |
|-------------|---------|-------|----------|-----------|------------------|------------------------|
| 640         | default | 4.96k | 577      | ~5.96M    | 2.59 GiB         | 995084                 |
| 640         | rocks   | 1.31k | 2480     | ~1.57M    | 1.52 GiB         | 999634                 |
| 32          | default | 2.1k  | 44       | —         | 1.30 GiB         | 622240                 |
| 32          | rocks   | 950   | 235      | —         | 1.10 GiB         | 793781                 |

**Replay (32 concurrency):** Default RPS ≈ 4.05k, Rocks ≈ 4.14k; P99 default ≈ 17.8k ms, Rocks ≈ 20.7k ms; total ops ~4.86M default, ~4.97M Rocks (Rocks ran a bit longer before bench stopped, so replay numbers are slightly skewed).

**Takeaways:**

- **Throughput:** At 640 concurrency, default clearly leads (4.96k vs 1.31k RPS) and completes many more total ops (5.96M vs 1.57M). At 32 concurrency the gap is smaller (2.1k vs 950) but default still leads.
- **Latency:** Default P99 is much lower (577 ms vs 2480 ms at 640; 44 ms vs 235 ms at 32). Rocks shows higher tail latency under load.
- **Resource use:** At 640, default uses more memory (2.59 GiB vs 1.52 GiB); container CPU usage is similar. At 32, memory is similar (1.30 vs 1.10 GiB); default uses less CPU (622240 vs 793781) while doing more RPS, so default is more CPU-efficient at that concurrency.
- **Replay:** With replay logs at 32 concurrency, RPS is close (default 4.05k, Rocks 4.14k); P99 is high for both (17.8k / 20.7k ms). Total ops are similar; small skew from Rocks running a bit longer.

**Conclusion:** For this no-fault, single-workload setup, **default backend gives better throughput and lower P99** than Rocks, especially at high concurrency (640). Rocks uses less memory at 640 but pays in RPS and latency. Replay shows the two backends closer on throughput when concurrency is moderate.

---

## Reference

- **Workload config:** `tests/stress/keeper/workloads/prod_mix.yaml` (concurrency, generator weights, paths).
- **Scenarios:** `tests/stress/keeper/scenarios/` (e.g. `core.yaml` for CHA-01, CHA-01-REPLAY).
