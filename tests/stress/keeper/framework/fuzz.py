import os
import random

from keeper.framework.core.registry import fault_registry
from keeper.framework.core.scenario_builder import ScenarioBuilder

_EXCLUDE = {
    # orchestrators / wrappers / non-chaos helpers
    "parallel",
    "background_schedule",
    "partition_symmetric_during",
    "download",
    "sql",
    "four",
    "leader_only",
    "expect_ready",
    "record_watch_baseline",
    "record_lgif_before",
    "record_lgif_after",
    "create_ephemerals",
    "stop_ephemeral_client",
    "reconfig",
    "nbank_seed",
    "nbank_transfers",
}

_FAULT_CANDIDATES = None


def _fault_candidates():
    global _FAULT_CANDIDATES
    if _FAULT_CANDIDATES is None:
        if isinstance(fault_registry, dict) and fault_registry:
            _FAULT_CANDIDATES = sorted(
                [k for k in fault_registry.keys() if k not in _EXCLUDE]
            )
        else:
            # fallback static list
            _FAULT_CANDIDATES = [
                "netem",
                "partition_symmetric",
                "partition_oneway",
                "stop_cont",
                "dm_delay",
                "dm_error",
                "cpu_hog",
                "fd_pressure",
                "mem_hog",
                "stress_ng",
            ]
    return _FAULT_CANDIDATES


def _rand_targets(rnd, names):
    k = rnd.choice(["leader", "followers", "all", "one"])
    if k == "one":
        return [rnd.choice(names)]
    return k


def _choose_weighted(rnd, weights):
    candidates = _fault_candidates()
    kinds = []
    wts = []
    for k in candidates:
        w = int((weights or {}).get(k, 1))
        if w > 0:
            kinds.append(k)
            wts.append(w)
    if not kinds:
        kinds = candidates
        wts = [1] * len(kinds)
    # Python <3.11: use manual cumulative selection
    s = sum(wts)
    x = rnd.randint(1, s)
    c = 0
    for k, w in zip(kinds, wts):
        c += w
        if x <= c:
            return k
    return kinds[-1]


def _rand_fault(rnd, node_names, weights=None, dur_min=15, dur_max=120):
    kind = _choose_weighted(rnd, weights or {})
    step = {"kind": kind}
    step["on"] = _rand_targets(rnd, node_names)
    # parameters by kind
    if kind == "netem":
        if rnd.random() < 0.6:
            step["delay_ms"] = rnd.choice([5, 10, 20, 40])
        if rnd.random() < 0.6:
            step["jitter_ms"] = rnd.choice([3, 5, 15])
        if rnd.random() < 0.3:
            step["loss_pct"] = rnd.choice([1, 2])
        step["duration_s"] = rnd.randint(max(5, dur_min), max(dur_min, dur_max))
        step["target_parallel"] = True
    elif kind in ("dm_delay",):
        step["ms"] = rnd.choice([3, 5, 10])
        step["duration_s"] = rnd.randint(max(5, dur_min), max(dur_min, dur_max))
        step["target_parallel"] = True
    elif kind in ("dm_error",):
        step["duration_s"] = rnd.randint(max(5, dur_min), max(dur_min, dur_max))
        step["target_parallel"] = True
    elif kind in ("partition_symmetric", "partition_oneway"):
        step["duration_s"] = rnd.randint(
            max(5, dur_min), min(60, max(dur_min, dur_max))
        )
    elif kind == "stop_cont":
        step["count"] = 1
        step["sleep_s"] = rnd.choice([1.0, 2.0, 3.0])
    elif kind == "cpu_hog":
        step["seconds"] = rnd.choice([30, 60])
    elif kind == "fd_pressure":
        step["fds"] = rnd.choice([2048, 4096, 8192])
        step["seconds"] = rnd.choice([30, 60])
    elif kind == "mem_hog":
        step["mb"] = rnd.choice([256, 512, 1024])
        step["seconds"] = rnd.choice([30, 60])
        step["target_parallel"] = True
    elif kind == "stress_ng":
        step["seconds"] = rnd.randint(max(5, dur_min), max(dur_min, dur_max))
        # randomize stressors lightly
        if rnd.random() < 0.7:
            step["cpu"] = rnd.choice([1, 2, 4])
        if rnd.random() < 0.4:
            step["vm"] = rnd.choice([1, 2])
        if rnd.random() < 0.3:
            step["io"] = rnd.choice([1, 2])
        if rnd.random() < 0.3:
            step["hdd"] = rnd.choice([1])
    return step


def generate_fuzz_scenario(
    seed, steps, duration_s, weights=None, dur_min=None, dur_max=None
):
    rnd = random.Random(seed)
    sb = ScenarioBuilder("FUZZ-01", f"Fuzz chaos seed={seed}", topology=3)
    sb.set_workload_config("workloads/prod_mix.yaml", duration=duration_s)
    # Optional pre baseline
    if rnd.random() < 0.5:
        sb.pre({"kind": "record_watch_baseline"})
    node_names = ["keeper1", "keeper2", "keeper3"]
    w = weights or {}
    dmin = int(
        dur_min
        if dur_min is not None
        else int(os.environ.get("KEEPER_FUZZ_DUR_MIN", "15"))
    )
    dmax = int(
        dur_max
        if dur_max is not None
        else int(os.environ.get("KEEPER_FUZZ_DUR_MAX", "120"))
    )
    for _ in range(max(1, steps)):
        sb.fault(_rand_fault(rnd, node_names, w, dmin, dmax))
    # Gates (invariants)
    sb.gate({"type": "single_leader"})
    sb.gate({"type": "config_converged", "timeout_s": 30})
    sb.gate({"type": "error_rate_le", "max_ratio": 0.1})
    sb.gate({"type": "p99_le", "max_ms": 10000})
    return sb.build()
