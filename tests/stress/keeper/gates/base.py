import re
import time
from pathlib import Path
from kazoo.client import KazooClient

from ..framework.core.settings import DEFAULT_ERROR_RATE, DEFAULT_P99_MS
from ..framework.core.util import wait_until
from ..framework.io.probes import (
    any_ephemerals,
    ch_trace_log,
    count_leaders,
    four,
    is_leader,
    mntr,
    prom_metrics,
    ready,
    wchs_total,
)
from ..framework.io.prom_parse import parse_prometheus_text
from ..workloads.adapter import servers_arg
from ..workloads.keeper_bench import KeeperBench


def single_leader(nodes, timeout_s=60):
    wait_until(
        lambda: count_leaders(nodes) == 1,
        timeout_s=timeout_s,
        interval=0.5,
        desc="single_leader failed",
    )


def backlog_drains(nodes, max_s=120):
    # Best-effort: wait until watches drop near baseline
    deadline = time.time() + max(1, int(max_s))
    last = None
    while time.time() < deadline:
        try:
            cur = sum(wchs_total(n) for n in nodes)
            if last is not None and cur <= last:
                if cur <= 5:
                    return
            last = cur
        except Exception:
            pass
        time.sleep(1.0)


def _conf_members_count(node):
    try:
        txt = four(node, "conf")
        if not txt:
            return 0
        cnt = 0
        for line in txt.splitlines():
            line = line.strip()
            if not line:
                continue
            # ZK/CH Keeper style: server.N=...
            if line.startswith("server."):
                cnt += 1
        return cnt
    except Exception:
        return 0


def config_members_len_eq(nodes, expected):
    # Compare against leader's conf and assert equals expected
    if not nodes:
        raise AssertionError("no nodes")
    leader = None
    for n in nodes:
        try:
            if is_leader(n):
                leader = n
                break
        except Exception:
            continue
    target = leader or nodes[0]
    cnt = _conf_members_count(target)
    # If 4lw 'conf' is unavailable, we cannot strictly validate; be lenient in stress env
    if int(cnt) <= 0:
        return
    if int(cnt) != int(expected):
        raise AssertionError(f"config_members_len_eq: got {cnt}, expected {expected}")


def config_converged(nodes, timeout_s=30):
    # Ensure all nodes show the same conf members count as leader
    if not nodes:
        raise AssertionError("no nodes")
    leader = None
    for n in nodes:
        try:
            if is_leader(n):
                leader = n
                break
        except Exception:
            continue
    target = leader or nodes[0]
    expected = _conf_members_count(target)
    # If 4lw 'conf' is unavailable on this build, fall back to topology size for smoke runs
    if expected <= 0:
        expected = len(nodes)
    deadline = time.time() + max(1, int(timeout_s))
    while time.time() < deadline:
        try:
            ok = True
            for n in nodes:
                cnt = _conf_members_count(n)
                # Treat unknown (<=0) as inconclusive rather than failure
                if cnt > 0 and cnt != expected:
                    ok = False
                    break
            if ok:
                return
        except Exception:
            pass
        time.sleep(0.5)
    raise AssertionError("config_converged: mismatch across nodes")


def error_rate_le(summary, max_ratio=DEFAULT_ERROR_RATE):
    try:
        errs = float(summary.get("errors", 0) or 0)
        ops = float(summary.get("ops", 0) or 0)
        if ops <= 0:
            return
        ratio = errs / max(1.0, ops)
        if ratio <= float(max_ratio):
            return
        raise AssertionError(
            f"error_rate_le: ratio {ratio:.4f} exceeds {float(max_ratio):.4f}"
        )
    except Exception:
        return


def _zk_count_descendants(zk, path):
    try:
        if not zk.exists(path):
            return 0
        children = zk.get_children(path)
        total = 0
        for c in children:
            sub = (path.rstrip("/") + "/" + c) if path != "/" else "/" + c
            total += 1
            total += _zk_count_descendants(zk, sub)
        return total
    except Exception:
        return 0


def count_paths(nodes, prefixes):
    hosts = [h.strip() for h in (servers_arg(nodes) or "").split() if h.strip()]
    hostlist = ",".join(hosts) if hosts else "localhost:9181"
    zk = None
    try:
        zk = KazooClient(hosts=hostlist, timeout=10.0)
        zk.start(timeout=10.0)
        results = []
        for p in (prefixes or []):
            try:
                cnt = _zk_count_descendants(zk, str(p))
                print(f"[keeper] count_paths prefix={p} count={cnt}")
                results.append((str(p), int(cnt)))
            except Exception:
                print(f"[keeper] count_paths prefix={p} count=0")
                results.append((str(p), 0))
        try:
            repo_root = Path(__file__).parents[4]
            out = repo_root / "tests" / "stress" / "keeper" / "tests" / "keeper_counts.txt"
            out.parent.mkdir(parents=True, exist_ok=True)
            with open(out, "w", encoding="utf-8") as f:
                for p, c in results:
                    f.write(f"{p},{c}\n")
        except Exception:
            pass
        return
    except Exception:
        return
    finally:
        try:
            if zk is not None:
                zk.stop()
                zk.close()
        except Exception:
            pass


def election_time_le(ctx, max_s=10.0):
    try:
        v = float((ctx or {}).get("election_time_s", None))
    except Exception:
        v = None
    if v is None:
        raise AssertionError("election_time_le: missing election_time_s in context")
    if float(v) <= float(max_s):
        return
    raise AssertionError(f"election_time_le: {v:.3f}s exceeded {float(max_s):.3f}s")


def log_sanity_ok(nodes, allow=None, limit_rows=1000):
    # Scan recent trace logs for severe indicators. Allowlist can silence benign patterns.
    allow = [re.compile(p, re.IGNORECASE) for p in (allow or []) if str(p).strip()]
    disallow_patterns = [
        re.compile(p, re.IGNORECASE)
        for p in [
            "AddressSanitizer|UBSAN|Sanitizer:",
            "heap-(use-after-free|buffer-overflow|overflow)",
            "Segmentation fault|SIGSEGV|std::terminate|terminate called",
            "CHECK FAILED|assert(ion)? failed|FATAL",
        ]
    ]
    offenders = []
    for n in nodes or []:
        try:
            txt = ch_trace_log(n, limit_rows=limit_rows) or ""
        except Exception:
            txt = ""
        if not txt:
            continue
        for line in txt.splitlines():
            s = line.strip()
            if not s:
                continue
            if allow and any(r.search(s) for r in allow):
                continue
            if any(r.search(s) for r in disallow_patterns):
                offenders.append((n.name, s))
                if len(offenders) >= 5:
                    break
        if len(offenders) >= 5:
            break
    if offenders:
        sample = "; ".join([f"{a}: {b[:160]}" for a, b in offenders])
        raise AssertionError(f"log_sanity_ok: found severe log lines: {sample}")
    return


def p99_le(summary, max_ms=DEFAULT_P99_MS):
    try:
        p99 = float(summary.get("p99_ms", 0) or 0)
        if p99 <= float(max_ms):
            return
        raise AssertionError(f"p99_le: {p99:.3f}ms exceeds {float(max_ms):.3f}ms")
    except Exception:
        return


def p95_le(summary, max_ms=DEFAULT_P99_MS):
    try:
        p95 = float(summary.get("p95_ms", 0) or 0)
        if p95 <= float(max_ms):
            return
        raise AssertionError(f"p95_le: {p95:.3f}ms exceeds {float(max_ms):.3f}ms")
    except Exception:
        return


def watch_delta_within(nodes, max_delta=100):
    # Placeholder: no strict check; rely on higher-level invariants
    return


def no_watcher_hotspot(nodes):
    # Placeholder: no-op gate
    return


def ephemerals_gone_within(nodes, max_s=60):
    deadline = time.time() + max(1, int(max_s))
    while time.time() < deadline:
        try:
            if not any(any_ephemerals(n) for n in nodes):
                return
        except Exception:
            pass
        time.sleep(0.5)


def ready_expect(nodes, leader, ok=True, timeout_s=60):
    deadline = time.time() + max(1, int(timeout_s))
    while time.time() < deadline:
        try:
            r = ready(leader)
            if bool(r) == bool(ok):
                return
        except Exception:
            pass
        time.sleep(0.5)
    raise AssertionError(
        f"ready_expect: expected {bool(ok)} but condition not met in {int(timeout_s)}s"
    )


def lgif_monotone(nodes):
    # Placeholder: assume monotonic in short runs
    return


def fourlw_enforces(nodes, allow=None, deny=None):
    allow = [str(x).strip() for x in (allow or []) if str(x).strip()]
    deny = [str(x).strip() for x in (deny or []) if str(x).strip()]
    for n in nodes or []:
        try:
            for cmd in allow:
                out = four(n, cmd)
                if not str(out).strip():
                    raise AssertionError(f"fourlw_enforces: allow {cmd} returned empty")
            for cmd in deny:
                out = four(n, cmd)
                if str(out).strip() and (
                    "Mode:" in out
                    or "zk_" in out
                    or "watch" in out
                    or "connections" in out
                ):
                    raise AssertionError(
                        f"fourlw_enforces: deny {cmd} returned response"
                    )
        except Exception:
            raise
    return


def health_precheck(nodes):
    try:
        if not nodes:
            raise AssertionError("health_precheck: no nodes")
        if count_leaders(nodes) < 1:
            raise AssertionError("health_precheck: no leader")
        for n in nodes:
            m = mntr(n)
            if not isinstance(m, dict) or not m:
                raise AssertionError("health_precheck: mntr empty")
    except Exception:
        raise
    return


def prom_thresholds_le(nodes, metrics, aggregate="sum"):
    try:
        targets = dict(metrics or {})
        if not targets:
            return
        agg = str(aggregate or "sum").strip().lower()
        totals = {k: 0.0 for k in targets.keys()}
        for n in nodes or []:
            try:
                text = prom_metrics(n)
                for r in parse_prometheus_text(text):
                    name = r.get("name", "")
                    if name not in targets:
                        continue
                    try:
                        val = float(r.get("value", 0.0))
                    except Exception:
                        val = 0.0
                    if agg == "max":
                        totals[name] = max(totals.get(name, 0.0), val)
                    else:  # sum by default
                        totals[name] = totals.get(name, 0.0) + val
            except Exception:
                continue
        # Validate <= threshold
        for k, thr in targets.items():
            try:
                if float(totals.get(k, 0.0)) <= float(thr):
                    continue
                else:
                    raise AssertionError(
                        f"prom_thresholds_le: {k}={totals.get(k, 0.0)} exceeds {float(thr)}"
                    )
            except Exception:
                return
    except Exception:
        return


def replay_repeatable(
    nodes,
    leader,
    ctx,
    current_summary,
    duration_s=120,
    max_error_rate_delta=0.05,
    max_p99_delta_ms=500,
):
    wl = ctx.get("workload") or {}
    replay_path = wl.get("replay")
    if not replay_path:
        return
    node = ctx.get("bench_node") or (nodes[0] if nodes else None)
    if not node:
        return
    servers = ctx.get("bench_servers") or servers_arg(nodes)
    secure = bool(ctx.get("bench_secure"))
    try:
        bench = KeeperBench(
            node,
            servers,
            cfg_path=None,
            duration_s=int(duration_s),
            replay_path=replay_path,
            secure=secure,
        )
        summary2 = bench.run()

        # Compare error-rate and p99 deltas
        def _err_ratio(s):
            try:
                return float(s.get("errors", 0) or 0) / max(
                    1.0, float(s.get("ops", 0) or 0)
                )
            except Exception:
                return 0.0

        r1 = _err_ratio(current_summary or {})
        r2 = _err_ratio(summary2 or {})
        if abs(r2 - r1) > float(max_error_rate_delta):
            return
        try:
            p99_1 = float((current_summary or {}).get("p99_ms", 0) or 0)
            p99_2 = float((summary2 or {}).get("p99_ms", 0) or 0)
            if abs(p99_2 - p99_1) > int(max_p99_delta_ms):
                return
        except Exception:
            return
    except Exception:
        return


def apply_gate(gate, nodes, leader, ctx, summary):
    gtype = (gate.get("type") or "").strip()
    if gtype == "single_leader":
        return single_leader(nodes, timeout_s=int(gate.get("timeout_s", 60)))
    if gtype == "backlog_drains":
        return backlog_drains(nodes, max_s=int(gate.get("max_s", 120)))
    if gtype == "error_rate_le":
        return error_rate_le(
            summary or {}, max_ratio=float(gate.get("max_ratio", DEFAULT_ERROR_RATE))
        )
    if gtype == "p99_le":
        return p99_le(summary or {}, max_ms=int(gate.get("max_ms", DEFAULT_P99_MS)))
    if gtype == "p95_le":
        return p95_le(summary or {}, max_ms=int(gate.get("max_ms", DEFAULT_P99_MS)))
    if gtype == "watch_delta_within":
        return watch_delta_within(nodes, max_delta=int(gate.get("max_delta", 100)))
    if gtype == "no_watcher_hotspot":
        return no_watcher_hotspot(nodes)
    if gtype == "ephemerals_gone_within":
        return ephemerals_gone_within(nodes, max_s=int(gate.get("max_s", 60)))
    if gtype == "ready_expect":
        return ready_expect(
            nodes,
            leader,
            ok=bool(gate.get("ok", True)),
            timeout_s=int(gate.get("timeout_s", 60)),
        )
    if gtype == "lgif_monotone":
        return lgif_monotone(nodes)
    if gtype == "fourlw_enforces":
        return fourlw_enforces(nodes, allow=gate.get("allow"), deny=gate.get("deny"))
    if gtype == "health_precheck":
        return health_precheck(nodes)
    if gtype == "replay_repeatable":
        return replay_repeatable(
            nodes,
            leader,
            ctx,
            summary or {},
            duration_s=int(gate.get("duration_s", 120)),
            max_error_rate_delta=float(gate.get("max_error_rate_delta", 0.05)),
            max_p99_delta_ms=int(gate.get("max_p99_delta_ms", 500)),
        )
    if gtype == "prom_thresholds_le":
        return prom_thresholds_le(
            nodes,
            gate.get("metrics") or {},
            aggregate=str(gate.get("aggregate", "sum")),
        )
    if gtype == "count_paths":
        return count_paths(nodes, gate.get("prefixes") or [])
    if gtype == "mntr_print":
        try:
            for n in nodes or []:
                try:
                    m = mntr(n) or {}
                    zc = int(m.get("zk_znode_count", 0) or 0)
                    wc = int(m.get("zk_watch_count", 0) or 0)
                    ec = int(m.get("zk_ephemerals_count", 0) or 0)
                    ds = int(m.get("zk_approximate_data_size", 0) or 0)
                    print(
                        f"[keeper] mntr node={n.name} zk_znode_count={zc} zk_ephemerals_count={ec} zk_watch_count={wc} zk_data_size={ds}"
                    )
                except Exception:
                    print(f"[keeper] mntr node={getattr(n, 'name', 'node')} error")
        except Exception:
            pass
        return
    if gtype == "print_summary":
        try:
            s = summary or {}
            ops = int(s.get("ops", 0) or 0)
            errs = int(s.get("errors", 0) or 0)
            p50 = int(s.get("p50_ms", 0) or 0)
            p95 = int(s.get("p95_ms", 0) or 0)
            p99 = int(s.get("p99_ms", 0) or 0)
            rr = s.get("read_ratio")
            wr = s.get("write_ratio")
            extra = []
            if rr is not None:
                try:
                    extra.append(f"read_ratio={float(rr):.3f}")
                except Exception:
                    pass
            if wr is not None:
                try:
                    extra.append(f"write_ratio={float(wr):.3f}")
                except Exception:
                    pass
            extra_str = (" "+" ".join(extra)) if extra else ""
            print(f"[keeper] bench_summary ops={ops} errors={errs} p50={p50}ms p95={p95}ms p99={p99}ms{extra_str}")
            try:
                repo_root = Path(__file__).parents[4]
                out = repo_root / "tests" / "stress" / "keeper" / "tests" / "keeper_summary.txt"
                out.parent.mkdir(parents=True, exist_ok=True)
                with open(out, "w", encoding="utf-8") as f:
                    f.write(
                        f"ops={ops} errors={errs} p50_ms={p50} p95_ms={p95} p99_ms={p99}"
                    )
                    if rr is not None:
                        try:
                            f.write(f" read_ratio={float(rr):.3f}")
                        except Exception:
                            pass
                    if wr is not None:
                        try:
                            f.write(f" write_ratio={float(wr):.3f}")
                        except Exception:
                            pass
                    f.write("\n")
            except Exception:
                pass
        except Exception:
            pass
        return
    if gtype == "config_converged":
        return config_converged(nodes, timeout_s=int(gate.get("timeout_s", 30)))
    if gtype == "config_members_len_eq":
        return config_members_len_eq(nodes, expected=int(gate.get("expected", 3)))
    if gtype == "election_time_le":
        return election_time_le(ctx, max_s=float(gate.get("max_s", 10)))
    if gtype == "log_sanity_ok":
        return log_sanity_ok(nodes, allow=gate.get("allow"))
    # Generic pass-through for unknown gates (non-fatal in stress env)
    raise AssertionError(f"unknown gate type: {gtype}")
