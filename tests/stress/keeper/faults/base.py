import os
import random
import sys
import tempfile
import threading
import time
import traceback

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
    if kind in ("sequence", "parallel") and subs:
        ds = [_get_duration(s, 0) for s in subs]
        if kind == "sequence":
            return int(sum([d for d in ds if d and d > 0]))
        return int(max([d for d in ds if d and d > 0] or [0]))
    if step.get("duration_s") is not None:
        return int(step["duration_s"])
    if step.get("seconds") is not None:
        return int(step["seconds"])
    return env_int("KEEPER_DURATION", default)


def _step_parallel(step, nodes, leader, ctx):
    """Execute sub-steps in parallel threads."""
    subs = step.get("steps") or []
    if not subs:
        return
    errors = []

    def _step_label(sub, idx):
        try:
            kind = str(sub.get("kind") or sub.get("type") or "").strip()
            cfg = str(sub.get("config") or "").strip()
            on = str(sub.get("on") or "").strip()
            dur = sub.get("duration_s")
            parts = [
                p
                for p in [
                    kind,
                    (f"cfg={cfg}" if cfg else ""),
                    (f"on={on}" if on else ""),
                    (f"dur={dur}" if dur is not None else ""),
                ]
                if p
            ]
            base = " ".join(parts) if parts else "step"
            return f"parallel[{idx}] {base}".strip()
        except Exception:
            return f"parallel[{idx}]"

    def _run(sub):
        try:
            apply_step(sub, nodes, leader, ctx)
        except Exception as e:
            errors.append(e)

    # Compute deadline from max child duration
    durs = []
    for s in subs:
        d = _get_duration(s, 0)
        if d > 0:
            durs.append(d)
    exp = max(durs) if durs else DEFAULT_FAULT_DURATION_S
    slack = 60
    deadline = time.time() + exp + slack
    try:
        pytest_to = float(env_int("KEEPER_PYTEST_TIMEOUT", 0) or 0)
    except Exception:
        pytest_to = 0.0
    if pytest_to > 0:
        try:
            deadline = min(deadline, time.time() + max(1.0, pytest_to - 120.0))
        except Exception:
            pass

    threads = []
    thread_to_sub = {}
    for i, s in enumerate(subs):
        t = threading.Thread(
            target=_run, args=(s,), daemon=True, name=_step_label(s, i)
        )
        threads.append(t)
        thread_to_sub[t] = s
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=max(0.0, deadline - time.time()))

    alive = [t for t in threads if t.is_alive()]
    if alive:
        frames = {}
        try:
            frames = sys._current_frames() or {}
        except Exception:
            frames = {}
        details = []
        for t in alive:
            sub = thread_to_sub.get(t) or {}
            try:
                sub_brief = {
                    k: sub.get(k)
                    for k in (
                        "kind",
                        "type",
                        "config",
                        "on",
                        "duration_s",
                        "ms",
                        "delay_ms",
                        "jitter_ms",
                    )
                    if sub.get(k) is not None
                }
            except Exception:
                sub_brief = {}
            stack_txt = ""
            try:
                fr = frames.get(t.ident)
                if fr is not None:
                    stack_txt = "".join(traceback.format_stack(fr)[-12:])
            except Exception:
                stack_txt = ""
            entry = f"thread={t.name} ident={t.ident} step={sub_brief}"
            if stack_txt:
                entry += "\n" + stack_txt
            details.append(entry)
        msg = (
            f"parallel: exceeded deadline {exp+slack}s; alive_threads={len(alive)}\n"
            + "\n---\n".join(details)
        )
        raise AssertionError(msg)
    if errors:
        raise errors[0]


def _step_sequence(step, nodes, leader, ctx):
    """Execute sub-steps sequentially."""
    subs = step.get("steps") or []
    for sub in subs:
        apply_step(sub, nodes, leader, ctx)


def _step_background_schedule(step, nodes, leader, ctx):
    """Execute random sub-steps on a schedule until duration expires."""
    dur = _get_duration(step)
    pmin = float(step.get("min_period_s", 5.0))
    pmax = float(step.get("max_period_s", max(pmin, 20.0)))
    subs = step.get("steps") or []
    if not subs:
        return

    deadline = time.time() + dur
    while time.time() < deadline:
        sub = random.choice(subs)
        apply_step(sub, nodes, leader, ctx)
        now = time.time()
        if now >= deadline:
            break
        sleep_s = pmin + random.random() * (pmax - pmin)
        time.sleep(min(sleep_s, deadline - now))


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
                    f"timeout 2s HOME=/tmp clickhouse keeper-client --host 127.0.0.1 --port {CLIENT_PORT} -q \"touch '{full}'\" >/dev/null 2>&1; echo $?",
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


def _step_run_bench(step, nodes, leader, ctx):
    """Run keeper-bench workload."""
    from ..workloads.adapter import servers_arg
    from ..workloads.keeper_bench import KeeperBench

    duration = _get_duration(step, 60)
    clients = int(step["clients"]) if step.get("clients") is not None else None
    cfg_main = step.get("config")
    cfg_list = step.get("configs") or []
    replicas = 1
    try:
        replicas = int(step.get("replicas", step.get("instances", 1)) or 1)
    except Exception:
        replicas = 1

    def _materialize_cfg_item(item):
        if isinstance(item, str) and item.strip():
            return item.strip()
        if isinstance(item, dict) and item:
            try:
                with tempfile.NamedTemporaryFile(
                    "w", suffix=".yaml", delete=False
                ) as tf:
                    yaml.safe_dump(item, tf, sort_keys=False)
                    return tf.name
            except Exception:
                return None
        return None

    cfg_paths = []
    if cfg_list:
        cfg_paths = [p for p in (_materialize_cfg_item(it) for it in cfg_list) if p]
    else:
        base = cfg_main or ""
        cfg_paths = [base for _ in range(max(1, int(replicas)))]

    results = []
    configs_dump = []
    lock = threading.Lock()
    errors = []

    def _run_one(path):
        # keeper-bench must execute on host, using CLICKHOUSE_BINARY=<path-to-clickhouse>.
        # KeeperBench enforces CLICKHOUSE_BINARY presence and will fail fast otherwise.
        kb = KeeperBench(
            nodes[0],
            servers_arg(nodes),
            cfg_path=path if path else None,
            duration_s=duration,
            replay_path=None,
            secure=False,
            clients=clients,
        )
        try:
            s = kb.run()
        except Exception as e:
            tb = traceback.format_exc()
            with lock:
                errors.append({"error": repr(e), "traceback": tb})
                try:
                    p = getattr(kb, "last_config_path", None)
                    if p:
                        (ctx.setdefault("bench_config_paths", [])).append(str(p))
                except Exception:
                    pass
                try:
                    op = getattr(kb, "last_output_path", None)
                    if op:
                        (ctx.setdefault("bench_output_paths", [])).append(str(op))
                except Exception:
                    pass
                try:
                    sp = getattr(kb, "last_stdout_path", None)
                    if sp:
                        (ctx.setdefault("bench_stdout_paths", [])).append(str(sp))
                except Exception:
                    pass
            return

        with lock:
            results.append(s or {})
            try:
                y = getattr(kb, "last_config_yaml", None)
                if y:
                    configs_dump.append(y)
            except Exception:
                pass
            try:
                p = getattr(kb, "last_config_path", None)
                if p:
                    (ctx.setdefault("bench_config_paths", [])).append(str(p))
            except Exception:
                pass
            try:
                op = getattr(kb, "last_output_path", None)
                if op:
                    (ctx.setdefault("bench_output_paths", [])).append(str(op))
            except Exception:
                pass
            try:
                sp = getattr(kb, "last_stdout_path", None)
                if sp:
                    (ctx.setdefault("bench_stdout_paths", [])).append(str(sp))
            except Exception:
                pass

    threads = [
        threading.Thread(target=_run_one, args=(p,), daemon=True) for p in cfg_paths
    ]
    for t in threads:
        t.start()

    slack = 60
    deadline = time.time() + float(duration) + float(slack)
    try:
        pytest_to = float(env_int("KEEPER_PYTEST_TIMEOUT", 0) or 0)
    except Exception:
        pytest_to = 0.0
    if pytest_to > 0:
        try:
            deadline = min(deadline, time.time() + max(1.0, pytest_to - 120.0))
        except Exception:
            pass
    for t in threads:
        t.join(timeout=max(0.0, deadline - time.time()))

    alive = [t for t in threads if t.is_alive()]
    if alive:
        frames = {}
        try:
            frames = sys._current_frames() or {}
        except Exception:
            frames = {}
        details = []
        for t in alive:
            stack_txt = ""
            try:
                fr = frames.get(t.ident)
                if fr is not None:
                    stack_txt = "".join(traceback.format_stack(fr)[-12:])
            except Exception:
                stack_txt = ""
            entry = f"thread={t.name} ident={t.ident}"
            if stack_txt:
                entry += "\n" + stack_txt
            details.append(entry)
        msg = (
            f"run_bench: exceeded deadline {int(duration) + slack}s; alive_threads={len(alive)}"
            f"; configs={cfg_paths}\n"
            + "\n---\n".join(details)
        )
        raise AssertionError(msg)

    if errors:
        msg = "\n\n".join(
            [
                "keeper-bench failed in background thread: "
                + (e.get("error") or "")
                + "\n"
                + (e.get("traceback") or "")
                for e in errors
            ]
        )
        raise AssertionError(msg)

    if not results:
        ctx["bench_summary"] = {}
    else:
        agg = {
            "ops": 0,
            "errors": 0,
            "duration_s": 0,
            "reads": 0,
            "writes": 0,
            "read_rps": 0.0,
            "read_bps": 0.0,
            "write_rps": 0.0,
            "write_bps": 0.0,
            "read_p50_ms": 0.0,
            "read_p95_ms": 0.0,
            "read_p99_ms": 0.0,
            "write_p50_ms": 0.0,
            "write_p95_ms": 0.0,
            "write_p99_ms": 0.0,
            "has_latency": False,
        }
        for r in results:
            try:
                agg["ops"] += int(r.get("ops") or 0)
                agg["errors"] += int(r.get("errors") or 0)
                agg["duration_s"] = max(
                    agg["duration_s"], int(r.get("duration_s") or 0)
                )
                agg["reads"] += int(r.get("reads") or 0)
                agg["writes"] += int(r.get("writes") or 0)
                agg["read_rps"] += float(r.get("read_rps") or 0.0)
                agg["read_bps"] += float(r.get("read_bps") or 0.0)
                agg["write_rps"] += float(r.get("write_rps") or 0.0)
                agg["write_bps"] += float(r.get("write_bps") or 0.0)
                agg["read_p50_ms"] = max(
                    agg["read_p50_ms"], float(r.get("read_p50_ms") or 0)
                )
                agg["read_p95_ms"] = max(
                    agg["read_p95_ms"], float(r.get("read_p95_ms") or 0)
                )
                agg["read_p99_ms"] = max(
                    agg["read_p99_ms"], float(r.get("read_p99_ms") or 0)
                )
                agg["write_p50_ms"] = max(
                    agg["write_p50_ms"], float(r.get("write_p50_ms") or 0)
                )
                agg["write_p95_ms"] = max(
                    agg["write_p95_ms"], float(r.get("write_p95_ms") or 0)
                )
                agg["write_p99_ms"] = max(
                    agg["write_p99_ms"], float(r.get("write_p99_ms") or 0)
                )
                agg["has_latency"] = agg["has_latency"] or bool(r.get("has_latency"))
            except Exception:
                continue
        tot = float(agg.get("reads") or 0) + float(agg.get("writes") or 0)
        if tot > 0:
            agg["read_ratio"] = float(agg.get("reads") or 0) / tot
            agg["write_ratio"] = float(agg.get("writes") or 0) / tot
        ctx["bench_summary"] = agg
    try:
        if configs_dump:
            ctx["bench_config_yaml"] = "\n\n---\n".join(configs_dump)
    except Exception:
        pass


def _step_leader_kill_measure(step, nodes, leader, ctx):
    """Kill leader and measure re-election time."""
    from .process import kill as _kill

    start = time.time()
    target = leader_or_first(nodes)
    _kill(target)
    timeout = int(step.get("timeout_s", 60))
    wait_until(
        lambda: count_leaders(nodes) == 1,
        timeout_s=timeout,
        interval=0.5,
        desc="re-election",
    )
    ctx["election_time_s"] = time.time() - start


def _step_record_watch_baseline(step, nodes, leader, ctx):
    """Record current watch counts as baseline."""
    per = {n.name: int(wchs_total(n) or 0) for n in (nodes or [])}
    ctx["watch_baseline_total"] = sum(per.values())
    ctx["watch_baseline_by_node"] = per


def _step_reconfig(step, nodes, leader, ctx):
    """Dynamic cluster reconfiguration via 4lw."""
    op = str(step.get("operation", "")).strip().lower()
    ok_expected = bool(step.get("ok", True))
    target = leader_or_first(nodes)
    if not target:
        raise AssertionError("reconfig: no target node")

    spec = str(step.get("spec", "")).strip()
    if not spec and op in ("add", "set"):
        sid, host = step.get("server_id"), step.get("host")
        port = step.get("port", RAFT_PORT)
        if sid and host:
            spec = f"server.{int(sid)}={host}:{int(port)}"
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
    ok = bool(step.get("ok", True))
    timeout_s = int(step.get("timeout_s", 30))
    end = time.time() + timeout_s
    while time.time() < end:
        try:
            if bool(ready(leader)) == ok:
                return
        except Exception:
            if not ok:
                return
        time.sleep(0.5)


# Step dispatcher mapping
_STEP_HANDLERS = {
    "parallel": _step_parallel,
    "sequence": _step_sequence,
    "background_schedule": _step_background_schedule,
    "ensure_paths": _step_ensure_paths,
    "run_bench": _step_run_bench,
    "leader_kill_measure": _step_leader_kill_measure,
    "record_watch_baseline": _step_record_watch_baseline,
    "reconfig": _step_reconfig,
    "sql": _step_sql,
    "expect_ready": _step_expect_ready,
}


def apply_step(step, nodes, leader, ctx):
    """Apply a fault injection or orchestration step."""
    kind = (step or {}).get("kind")
    if not kind:
        return
    # Try registered fault first
    fn = fault_registry.get(kind)
    if callable(fn):
        return fn(ctx, nodes, leader, step)
    # Try built-in handler
    handler = _STEP_HANDLERS.get(kind)
    if handler:
        return handler(step, nodes, leader, ctx)
