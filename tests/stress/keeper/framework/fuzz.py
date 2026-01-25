import random

from keeper.framework.core.registry import fault_registry
from keeper.framework.core.settings import keeper_node_names
import keeper.faults

_EXCLUDE = {
    "parallel", "background_schedule", "partition_symmetric_during", "download", "sql",
    "four", "leader_only", "expect_ready", "record_watch_baseline", "record_lgif_before",
    "record_lgif_after", "create_ephemerals", "stop_ephemeral_client", "reconfig",
    "nbank_seed", "nbank_transfers",
}


def _fault_candidates():
    if isinstance(fault_registry, dict) and fault_registry:
        return sorted(k for k in fault_registry.keys() if k not in _EXCLUDE)
    return []


def _rand_targets(rnd, names):
    k = rnd.choice(["leader", "followers", "all", "one"])
    if k == "one":
        return [rnd.choice(names)]
    return k


def _choose_fault(rnd, weights, candidates=None):
    if candidates is None:
        candidates = _fault_candidates()
    if not candidates:
        return None
    if weights:
        weighted = [(k, int(weights.get(k, 1))) for k in candidates if int(weights.get(k, 1)) > 0]
        if weighted:
            kinds, wts = zip(*weighted)
            return rnd.choices(kinds, weights=wts)[0]
    return rnd.choice(candidates)


def _rand_fault(rnd, node_names, weights=None, dur_min=15, dur_max=120, fault_kinds=None):
    kind = _choose_fault(rnd, weights, candidates=fault_kinds)
    if not kind:
        return None
    step = {"kind": kind, "on": _rand_targets(rnd, node_names)}
    # Ensure dur_min <= dur_max
    dur_min = max(5, min(dur_min, dur_max))
    dur_max = max(dur_min, dur_max)
    dur = rnd.randint(dur_min, dur_max)
    
    if kind == "netem":
        if rnd.random() < 0.6:
            step["delay_ms"] = rnd.choice([5, 10, 20, 40])
        if rnd.random() < 0.6:
            step["jitter_ms"] = rnd.choice([3, 5, 15])
        if rnd.random() < 0.3:
            step["loss_pct"] = rnd.choice([1, 2])
        step["duration_s"] = dur
        step["target_parallel"] = True
    elif kind == "dm_delay":
        step["ms"] = rnd.choice([3, 5, 10])
        step["duration_s"] = dur
        step["target_parallel"] = True
    elif kind == "dm_error":
        step["duration_s"] = dur
        step["target_parallel"] = True
    elif kind in ("partition_symmetric", "partition_oneway"):
        step["duration_s"] = rnd.randint(dur_min, min(60, dur_max))
    elif kind == "stop_cont":
        step["count"] = rnd.randint(1, 3)
        step["sleep_s"] = rnd.choice([1.0, 2.0, 3.0])
    elif kind == "cpu_hog":
        step["seconds"] = min(dur, 60)  # Cap at 60s
    elif kind == "fd_pressure":
        step["fds"] = rnd.choice([2048, 4096, 8192])
        step["seconds"] = min(dur, 60)  # Cap at 60s
    elif kind == "mem_hog":
        step["mb"] = rnd.choice([256, 512, 1024])
        step["seconds"] = min(dur, 60)  # Cap at 60s
        step["target_parallel"] = True
    elif kind == "stress_ng":
        step["seconds"] = dur
        if rnd.random() < 0.7:
            step["cpu"] = rnd.choice([1, 2, 4])
        if rnd.random() < 0.4:
            step["vm"] = rnd.choice([1, 2])
        if rnd.random() < 0.3:
            step["io"] = rnd.choice([1, 2])
        if rnd.random() < 0.3:
            step["hdd"] = 1
    return step


def generate_random_faults(steps, rnd, dur_min, dur_max, topology=3, weights=None, fault_kinds=None):
    """Generate a list of random fault injection steps.

    Args:
        steps: Number of faults to generate
        rnd: Random number generator set to seed
        dur_min: Minimum duration in seconds
        dur_max: Maximum duration in seconds
        topology: Cluster topology (for node names)
        weights: Optional dict of fault kind -> weight for weighted selection
        fault_kinds: Optional list of fault kinds to choose from (if None, uses all registered faults)
    """
    node_names = keeper_node_names(topology)
    return [
        x
        for x in (
            _rand_fault(rnd, node_names, weights, dur_min, dur_max, fault_kinds)
            for _ in range(max(1, steps))
        )
        if x is not None
    ]
