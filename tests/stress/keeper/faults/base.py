import copy
import os
import random
import shlex
import time

import yaml
from keeper.framework.core.registry import fault_registry
from keeper.framework.core.settings import (
    CLIENT_PORT,
    DEFAULT_FAULT_DURATION_S,
    RAFT_PORT,
)
from keeper.framework.core.util import (
    env_int,
    leader_or_first,
    resolve_targets,
    sh_strict,
    wait_until,
)
from keeper.framework.io.probes import count_leaders, four, mntr, ready, wchs_total
from keeper.gates.base import apply_gate


def _get_duration(step, default=None):
    """Extract duration from step config."""
    if default is None:
        default = DEFAULT_FAULT_DURATION_S
    try:
        kind = str((step or {}).get("kind") or "").strip().lower()
    except Exception:
        kind = ""
    try:
        subs = (step or {}).get("steps") or []
    except Exception:
        subs = []
    if kind == "sequence" and subs:
        ds = [_get_duration(s, 0) for s in subs]
        return int(sum([d for d in ds if d and d > 0]))
    if step.get("duration_s") is not None:
        return int(step["duration_s"])
    if step.get("seconds") is not None:
        return int(step["seconds"])
    return env_int("KEEPER_DURATION", default)


def _step_sequence(step, nodes, leader, ctx):
    """Execute sub-steps sequentially."""
    subs = step.get("steps") or []
    for sub in subs:
        apply_step(sub, nodes, leader, ctx)


def _step_background_schedule(step, nodes, leader, ctx):
    """Execute random sub-steps on a schedule until duration expires."""
    dur = _get_duration(step)
    subs = step.get("steps") or []
    if not subs:
        return

    seed = None
    try:
        seed = (ctx or {}).get("seed")
    except Exception:
        seed = None
    try:
        seed = int(seed) if seed is not None else None
    except Exception:
        seed = None
    rnd = random.Random(seed) if seed is not None else random

    deadline = time.time() + dur
    while time.time() < deadline:
        remaining = deadline - time.time()
        if remaining <= 0:
            break

        budget = int(max(1, min(float(DEFAULT_FAULT_DURATION_S), float(remaining))))
        sub = rnd.choice(subs)

        # Ignore any per-fault duration in YAML and cap each fault slot to remaining time.
        sub_eff = copy.deepcopy(sub) if isinstance(sub, (dict, list)) else sub
        if isinstance(sub_eff, dict):
            sub_eff.pop("duration_s", None)
            sub_eff.pop("seconds", None)
            sub_eff["duration_s"] = budget
            sub_eff["seconds"] = budget

        t0 = time.time()
        apply_step(sub_eff, nodes, leader, ctx)
        elapsed = time.time() - t0

        # If the fault implementation returns quickly (e.g. kill), don't busy-loop.
        # Fill the rest of the fault slot with sleep so total fault time ~= scenario duration.
        slack = float(budget) - float(elapsed)
        if slack > 0:
            time.sleep(min(slack, max(0.0, deadline - time.time())))


def _step_ensure_paths(step, nodes, leader, ctx):
    """Ensure znode paths exist via keeper-client."""
    paths = [str(p).strip() for p in (step.get("paths") or []) if str(p).strip()]
    if not paths:
        return
    targets = resolve_targets(step.get("on", "leader"), nodes, leader)

    def _mk_path(node, path):
        full = "/"
        for seg in path.split("/"):
            if not seg:
                continue
            full = full.rstrip("/") + "/" + seg
            # Retry touch twice for reliability
            ok = False
            last_out = ""
            for _ in range(2):
                r = sh_strict(
                    node,
                    f"HOME=/tmp clickhouse keeper-client --host 127.0.0.1 --port {CLIENT_PORT} -q \"touch {shlex.quote(full)}\" >/dev/null 2>&1; echo $?",
                    timeout=5,
                )
                last_out = str((r or {}).get("out", "") or "").strip()
                if last_out.endswith("0"):
                    ok = True
                    break
            if not ok:
                raise AssertionError(f"ensure_paths: touch failed for {full} (rc={last_out})")

    for t in targets:
        for p in paths:
            _mk_path(t, p)
    # Log znode counts
    for t in targets:
        try:
            m = mntr(t) or {}
            print(
                f"[keeper] ensure_paths node={t.name} zk_znode_count={m.get('zk_znode_count')}"
            )
        except Exception:
            pass




def _step_leader_kill_measure(step, nodes, leader, ctx):
    """Kill leader and measure re-election time."""
    from .process import kill as _kill

    if ctx is not None:
        ctx["_fault_recovered"] = False
    try:
        start = time.time()
        target = leader_or_first(nodes)
        if not target:
            raise AssertionError("leader_kill_measure: no target node")
        _kill(target)
        timeout = int(step.get("timeout_s") or 60)
        wait_until(
            lambda: count_leaders(nodes) == 1,
            timeout_s=timeout,
            interval=0.5,
            desc="re-election",
        )
        ctx["election_time_s"] = time.time() - start
    finally:
        if ctx is not None:
            ctx["_fault_recovered"] = True


def _step_record_watch_baseline(step, nodes, leader, ctx):
    """Record current watch counts as baseline."""
    per = {n.name: int(wchs_total(n) or 0) for n in (nodes or [])}
    ctx["watch_baseline_total"] = sum(per.values())
    ctx["watch_baseline_by_node"] = per


def _step_reconfig(step, nodes, leader, ctx):
    """Dynamic cluster reconfiguration via 4lw."""
    op = str(step.get("operation") or "").strip().lower()
    ok_expected = bool(step.get("ok") if step.get("ok") is not None else True)
    target = leader_or_first(nodes)
    if not target:
        raise AssertionError("reconfig: no target node")

    spec = str(step.get("spec") or "").strip()
    if not spec and op in ("add", "set"):
        sid, host = step.get("server_id"), step.get("host")
        port = int(step.get("port") or RAFT_PORT)
        if sid and host:
            spec = f"server.{int(sid)}={host}:{port}"
    if op == "add" and not spec:
        raise AssertionError("reconfig add: missing spec or server_id/host")
    if op == "remove" and not spec:
        sid = step.get("server_id")
        if sid is None:
            raise AssertionError("reconfig remove: missing spec or server_id")
        spec = str(int(sid))
    if op not in ("add", "remove", "set"):
        raise AssertionError(f"reconfig: unknown operation {op}")

    cmd = f"reconfig -{op} {spec}"
    try:
        out = four(target, cmd)
    except Exception:
        out = ""
    success = bool(out and "error" not in out.lower())
    if ok_expected and not success:
        raise AssertionError(f"reconfig failed: {cmd}; out={out[:200]}")
    if not ok_expected and success:
        raise AssertionError(f"reconfig unexpectedly succeeded: {cmd}")


def _step_sql(step, nodes, leader, ctx):
    """Execute SQL query on target nodes."""
    q = step.get("query", "")
    for t in resolve_targets(step.get("on", "leader"), nodes, leader):
        try:
            t.query(q)
        except Exception:
            pass


def _step_expect_ready(step, nodes, leader, ctx):
    """Wait for ready state to match expected value."""
    if not leader:
        raise AssertionError("expect_ready: no leader")
    ok = bool(step.get("ok") if step.get("ok") is not None else True)
    timeout_s = int(step.get("timeout_s") or 30)
    end = time.time() + timeout_s
    while time.time() < end:
        try:
            if bool(ready(leader)) == ok:
                return
        except Exception:
            if not ok:
                return
        time.sleep(0.5)


def _step_gate(step, nodes, leader, ctx):
    """Run a gate check inline as a step (checkpoint)."""
    try:
        g = step.get("gate")
        if isinstance(g, dict):
            gate = dict(g)
        else:
            gate = dict(step or {})
            gate.pop("kind", None)
            gate.pop("gate", None)
    except Exception:
        gate = {}
    if not gate:
        return
    summary = (ctx or {}).get("bench_summary") or {}
    return apply_gate(gate, nodes, leader, ctx, summary)


# Step kinds used by scenarios/presets:
#   sequence, background_schedule (cha, test_scenarios), ensure_paths (presets),
#   leader_kill_measure, record_watch_baseline, reconfig, sql, expect_ready.
# gate = inline gate step (no YAML use); checkpoint removed (was alias, unused).
_STEP_HANDLERS = {
    "sequence": _step_sequence,
    "background_schedule": _step_background_schedule,
    "ensure_paths": _step_ensure_paths,
    "leader_kill_measure": _step_leader_kill_measure,
    "record_watch_baseline": _step_record_watch_baseline,
    "reconfig": _step_reconfig,
    "sql": _step_sql,
    "expect_ready": _step_expect_ready,
    "gate": _step_gate,
}


def apply_step(step, nodes, leader, ctx):
    """
    Apply a fault injection or orchestration step.

    Priority:
    1. Use a registered fault handler from fault_registry if available.
    2. Otherwise, use a built-in step handler from _STEP_HANDLERS if defined.
    3. Otherwise, handle 'run_bench' step via workloads module.

    Fault recovery: fault handlers signal recovery via ctx["_fault_recovered"].
    For fault_registry handlers, we set _fault_recovered=False before the call
    and _fault_recovered=True in a finally after the call (when the handler
    returns, the fault has cleaned up). Built-in fault-like steps (e.g.
    leader_kill_measure) set it themselves. Callers that need to wait for
    "fault recovered" should use this instead of inferring from cluster state
    (e.g. count_leaders).

    Returns:
        The result of the handler function, or None if no handler applies.
    """
    kind = step.get("kind")
    if not kind:
        return

    # 1. Try registered fault handler
    fault_fn = fault_registry.get(kind)
    if callable(fault_fn):
        if ctx is not None:
            ctx["_fault_recovered"] = False
        try:
            return fault_fn(ctx, nodes, leader, step)
        finally:
            if ctx is not None:
                ctx["_fault_recovered"] = True

    # 2. Try built-in handler
    builtin_handler = _STEP_HANDLERS.get(kind)
    if callable(builtin_handler):
        return builtin_handler(step, nodes, leader, ctx)

    # No handler found for this step kind; do nothing
    return
