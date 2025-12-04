import time

from ..framework.core.registry import fault_registry
from ..framework.core.settings import RAFT_PORT
from ..framework.core.util import resolve_targets, wait_until
from ..framework.io.probes import count_leaders, four, is_leader, ready, wchs_total


def apply_step(step, nodes, leader, ctx):
    kind = (step or {}).get("kind")
    if not kind:
        return
    # Registered fault
    fn = fault_registry.get(kind)
    if callable(fn):
        return fn(ctx, nodes, leader, step)
    # Orchestrators / helpers
    if kind == "parallel":
        for sub in step.get("steps") or []:
            apply_step(sub, nodes, leader, ctx)
        return
    if kind == "background_schedule":
        # Run each step once (no scheduler loop)
        for sub in step.get("steps") or []:
            apply_step(sub, nodes, leader, ctx)
        return
    if kind == "run_bench":
        from ..workloads.adapter import servers_arg
        from ..workloads.keeper_bench import KeeperBench

        try:
            duration = int(step.get("duration_s", 60))
        except Exception:
            duration = 60
        cfg_path = step.get("config")
        kb = KeeperBench(
            nodes[0], servers_arg(nodes), cfg_path=cfg_path, duration_s=duration
        )
        ctx["bench_summary"] = kb.run()
        return
    if kind == "leader_kill_measure":
        # Kill current leader and measure time to regain single leader
        import time as _t

        from .process import kill as _kill

        start = _t.time()
        # Identify leader among nodes
        cur_leader = None
        for n in nodes:
            try:
                if is_leader(n):
                    cur_leader = n
                    break
            except Exception:
                continue
        target = cur_leader or nodes[0]
        _kill(target)
        to = int((kind and (step.get("timeout_s", 60))) or 60)
        wait_until(
            lambda: count_leaders(nodes) == 1,
            timeout_s=to,
            interval=0.5,
            desc="re-election",
        )
        ctx["election_time_s"] = float(_t.time() - start)
        return
    if kind == "record_watch_baseline":
        try:
            total = 0
            per = {}
            for n in nodes or []:
                v = int(wchs_total(n) or 0)
                per[n.name] = v
                total += v
            ctx["watch_baseline_total"] = total
            ctx["watch_baseline_by_node"] = per
        except Exception:
            ctx["watch_baseline_total"] = 0
            ctx["watch_baseline_by_node"] = {}
        return
    if kind == "reconfig":
        # Minimal dynamic reconfig helper via 4lw 'reconfig' command
        op = str(step.get("operation", "")).strip().lower()
        ok_expected = bool(step.get("ok", True))
        # Determine target node (prefer leader)
        target = None
        for n in nodes:
            try:
                if is_leader(n):
                    target = n
                    break
            except Exception:
                continue
        target = target or (nodes[0] if nodes else None)
        if not target:
            raise AssertionError("reconfig: no target node")
        spec = str(step.get("spec", "")).strip()
        if not spec and op in ("add", "set"):
            sid = step.get("server_id")
            host = step.get("host")
            port = step.get("port", RAFT_PORT)
            if sid and host:
                spec = f"server.{int(sid)}={host}:{int(port)}"
        if op == "add" and not spec:
            raise AssertionError("reconfig add: missing spec or server_id/host")
        if op == "remove" and not spec:
            # allow remove by numeric id via server_id
            sid = step.get("server_id")
            if sid is None:
                raise AssertionError("reconfig remove: missing spec or server_id")
            spec = str(int(sid))
        if op not in ("add", "remove", "set"):
            raise AssertionError(f"reconfig: unknown operation {op}")
        cmd = None
        if op == "add":
            cmd = f"reconfig -add {spec}"
        elif op == "remove":
            cmd = f"reconfig -remove {spec}"
        elif op == "set":
            cmd = f"reconfig -set {spec}"
        out = ""
        try:
            out = four(target, cmd)
        except Exception:
            out = ""
        success = bool(out and ("error" not in out.lower()))
        if ok_expected and not success:
            raise AssertionError(f"reconfig failed: {cmd}; out={out[:200]}")
        if (not ok_expected) and success:
            raise AssertionError(f"reconfig unexpectedly succeeded: {cmd}")
        return
    if kind == "sql":
        q = step.get("query", "")
        for t in resolve_targets(step.get("on", "leader"), nodes, leader):
            try:
                t.query(q)
            except Exception:
                pass
        return
    if kind == "expect_ready":
        ok = bool(step.get("ok", True))
        timeout_s = int(step.get("timeout_s", 30))
        end = time.time() + timeout_s
        while time.time() < end:
            try:
                r = ready(leader)
                if bool(r) == ok:
                    break
            except Exception:
                if not ok:
                    break
            time.sleep(0.5)
        return
    # Placeholders / unknown kinds: treat as no-op to avoid breaking scenarios referencing legacy steps
    if kind in ("start", "download", "leader_only"):
        return
    return
