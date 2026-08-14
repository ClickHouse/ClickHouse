import copy
import os
import random
import shlex
import subprocess
import threading
import time

import yaml
from keeper.faults.registry import fault_registry
from keeper.framework.core.settings import (
    CLIENT_PORT,
    DEFAULT_FAULT_DURATION_S,
)
from keeper.framework.core.util import (
    env_int,
    resolve_targets,
    sh_strict,
)
from keeper.framework.io.probes import mntr

ENSURE_PATHS_TOUCH_RETRIES = 2
ENSURE_PATHS_ZK_HOST_TIMEOUT = 10


def _touch_path_zookeeper_from_host(cluster, node, path, retries=ENSURE_PATHS_TOUCH_RETRIES):
    """Create znode at path using keeper-client run from host (for ZooNodeWrapper / ZKBackedNode / RaftKeeperNode)."""
    # Use zk_name when set (ZKBackedNode: connect to ZK container; RaftKeeperNode: service name)
    zk_host = getattr(node, "zk_name", node.name)
    host = str(cluster.get_instance_ip(zk_host))
    # RaftKeeper uses per-node host ports (dynamic or 18101..18103); ZooKeeper uses cluster.zookeeper_port
    if getattr(node, "is_raftkeeper", False):
        port = getattr(node, "client_port", 2181)
    else:
        port = getattr(cluster, "zookeeper_port", None) or getattr(node, "client_port", 2181)
    for _ in range(retries):
        try:
            proc = subprocess.run(
                [
                    cluster.server_bin_path,
                    "keeper-client",
                    "--host",
                    host,
                    "--port",
                    str(port),
                    "-q",
                    f"touch {shlex.quote(path)}",
                ],
                capture_output=True,
                text=True,
                timeout=ENSURE_PATHS_ZK_HOST_TIMEOUT,
            )
            if proc.returncode == 0:
                return True
            if proc.stderr:
                print(f"[keeper][ensure_paths] keeper-client touch {path} for {node.name} (rc={proc.returncode}): {proc.stderr.strip()}")
        except Exception as e:
            print(f"[keeper][ensure_paths] keeper-client touch {path} for {node.name}: {e}")
    return False


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
        print(f"Fault step kind={step.get('kind')!r} has no sub-steps configured, skipping")
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
    """Ensure znode paths exist via keeper-client (in-container for Keeper; from host for ZooKeeper)."""
    paths = [str(p).strip() for p in (step.get("paths") or []) if str(p).strip()]
    if not paths:
        print("ensure_paths step has no paths configured, skipping znode creation")
        return
    targets = resolve_targets(step.get("on", "leader"), nodes, leader)
    cluster = (ctx or {}).get("cluster")

    def _mk_path(node, path):
        full = "/"
        for seg in path.split("/"):
            if not seg:
                continue
            full = full.rstrip("/") + "/" + seg
            if getattr(node, "is_zookeeper", False) and cluster:
                if not _touch_path_zookeeper_from_host(cluster, node, full):
                    raise AssertionError(f"ensure_paths: touch failed for {full} on ZooKeeper node {node.name}")
            else:
                ok = False
                last_out = ""
                for _ in range(ENSURE_PATHS_TOUCH_RETRIES):
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
        except Exception as e:
            print(f"Could not read znode count stats from node {getattr(t, 'name', t)}: {e}")




def _step_parallel(step, nodes, leader, ctx):
    """Execute sub-steps concurrently and wait for all to finish.

    The parent ``duration_s`` (set by FaultRunner after budget-capping) is
    propagated to every sub-step so that all faults run for the same budget
    window and are active simultaneously.  This is the correct way to model
    scenarios such as multi-region latency where leader and followers must
    have different netem rules applied at the same time.
    """
    subs = step.get("steps") or []
    if not subs:
        print(f"Parallel fault step has no sub-steps configured, skipping")
        return

    parent_dur = step.get("duration_s")
    errors = []
    lock = threading.Lock()

    def _run_one(sub):
        try:
            sub_eff = copy.deepcopy(sub)
            if parent_dur is not None:
                sub_eff["duration_s"] = int(parent_dur)
                sub_eff["seconds"] = int(parent_dur)
            apply_step(sub_eff, nodes, leader, ctx)
        except Exception as e:
            with lock:
                errors.append(e)

    # Non-daemon so that fault finally-blocks (e.g. netem teardown) always run,
    # even on abrupt interpreter exit.  We join with a deadline anyway.
    threads = [threading.Thread(target=_run_one, args=(sub,), daemon=False) for sub in subs]
    for t in threads:
        t.start()
    deadline = time.time() + (parent_dur or 300) + 30  # step budget + 30s slack
    for t in threads:
        remaining = max(1.0, deadline - time.time())
        t.join(timeout=remaining)
        if t.is_alive():
            # Python threads cannot be forcibly stopped.  The thread's fault handler
            # still holds a finally-block that will run cleanup (e.g. tc qdisc del)
            # once the in-flight blocking call returns.  Log immediately so the
            # timeout is visible in output even if the final AssertionError is caught.
            print(
                f"[keeper][parallel] WARNING: sub-step thread '{t.name}' still alive "
                f"after {parent_dur}s budget + 30s slack; cleanup will proceed in background"
            )
            errors.append(RuntimeError(f"parallel sub-step thread timed out after {parent_dur}s"))

    if errors:
        raise AssertionError(
            "parallel fault steps failed: " + "; ".join(str(e) for e in errors)
        )


# Step kinds used by scenarios:
#   sequence, background_schedule, ensure_paths, parallel (run sub-steps concurrently with shared budget).
_STEP_HANDLERS = {
    "sequence": _step_sequence,
    "parallel": _step_parallel,
    "background_schedule": _step_background_schedule,
    "ensure_paths": _step_ensure_paths,
}


def apply_step(step, nodes, leader, ctx):
    """
    Apply a fault injection or orchestration step.

    Priority:
    1. Use a registered fault handler from fault_registry if available.
    2. Otherwise, use a built-in step handler from _STEP_HANDLERS if defined.
    3. Otherwise, handle 'run_bench' step via workloads module.

    Fault recovery: fault handlers signal recovery via ctx["_fault_recovered"].
    For fault_registry handlers, we increment ctx["_fault_active"] before the
    call and decrement it in a finally. ctx["_fault_recovered"] becomes True
    only when _fault_active reaches 0, so parallel faults (kind: parallel) do
    not signal recovery until the last concurrent sub-step finishes. Built-in
    fault-like steps (e.g. leader_kill_measure) set _fault_recovered themselves.
    Callers that need to wait for "fault recovered" should poll this flag
    instead of inferring from cluster state (e.g. count_leaders).

    Returns:
        The result of the handler function, or None if no handler applies.
    """
    kind = step.get("kind")
    if not kind:
        print(f"Fault step is missing required 'kind' field, skipping: {step}")
        return

    # 1. Try registered fault handler
    fault_fn = fault_registry.get(kind)
    if callable(fault_fn):
        if ctx is not None:
            # Use a ref-count so parallel faults (kind: parallel) don't signal
            # recovery prematurely. _fault_recovered becomes True only when the
            # last concurrent fault handler finishes.
            # dict.setdefault is atomic in CPython (GIL-protected), so the lock
            # object is safely shared across threads.
            lock = ctx.setdefault("_fault_lock", threading.Lock())
            with lock:
                ctx["_fault_active"] = ctx.get("_fault_active", 0) + 1
                ctx["_fault_recovered"] = False
        try:
            return fault_fn(ctx, nodes, leader, step)
        finally:
            if ctx is not None:
                lock = ctx.get("_fault_lock")
                if lock is not None:
                    with lock:
                        active = max(0, ctx.get("_fault_active", 1) - 1)
                        ctx["_fault_active"] = active
                        if active == 0:
                            ctx["_fault_recovered"] = True
                else:
                    ctx["_fault_recovered"] = True

    # 2. Try built-in handler
    builtin_handler = _STEP_HANDLERS.get(kind)
    if callable(builtin_handler):
        return builtin_handler(step, nodes, leader, ctx)

    # No handler found for this step kind
    print(f"No handler registered for fault step kind={kind!r}, step will be skipped")
    return
