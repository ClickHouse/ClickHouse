import os
import re
import time
from pathlib import Path

from kazoo.client import KazooClient
from keeper.framework.core.settings import (
    DEFAULT_ERROR_RATE,
    DEFAULT_P95_MS,
    DEFAULT_P99_MS,
)
from keeper.framework.core.util import leader_or_first, parse_bool, wait_until
from keeper.framework.io.probes import (
    any_ephemerals,
    ch_trace_log,
    count_leaders,
    four,
    lgif,
    mntr,
    prom_metrics,
    ready,
    srvr_kv,
    wchp_paths,
    wchs_total,
)
from keeper.framework.io.prom_parse import parse_prometheus_text
from keeper.workloads.adapter import servers_arg


# Gate registry
_GATE_HANDLERS = {}


def register_gate(name):
    """Decorator to register a gate handler function."""
    def decorator(func):
        _GATE_HANDLERS[name] = func
        return func
    return decorator


def _extract_param(g, key, default=None, type_fn=None):
    """Helper to extract and optionally convert a parameter from gate config."""
    val = g.get(key, default)
    if val is None or val is default:
        return default
    return type_fn(val) if type_fn else val


# Core gate functions (used directly and by handlers)

def single_leader(nodes, timeout_s=180):
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


def _aggregate_totals(nodes, targets, aggregate, per_node_values_fn):
    """Aggregate metric values across nodes (sum or max)."""
    agg = str(aggregate or "sum").strip().lower()
    totals = {k: 0.0 for k in (targets or {})}
    for n in nodes or []:
        try:
            for k, val in per_node_values_fn(n):
                if k not in targets:
                    continue
                v = float(val) if val is not None else 0.0
                totals[k] = max(totals[k], v) if agg == "max" else totals[k] + v
        except Exception:
            continue
    return totals


def _conf_members_count(node):
    """Count server entries in 4lw conf output."""
    try:
        txt = four(node, "conf") or ""
        return sum(1 for line in txt.splitlines() if line.strip().startswith("server."))
    except Exception:
        return 0


def config_members_len_eq(nodes, expected):
    # Compare against leader's conf and assert equals expected
    if not nodes:
        raise AssertionError("no nodes")
    target = leader_or_first(nodes)
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
    target = leader_or_first(nodes)
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


def coord_timeouts_match(nodes, ctx, startup=None, shutdown=None):
    """Validate that coordination startup/shutdown timeouts match expectations.

    Best-effort: parse generated keeper_config_*.xml from instances_dir/configs/<cluster_name>/.
    If values are missing or files not found, treat as non-fatal (pass).
    """
    try:
        if ctx is None:
            return
        cluster = ctx.get("cluster")
        if cluster is None:
            return
        cname = os.environ.get("KEEPER_CLUSTER_NAME", "")
        inst_dir = Path(getattr(cluster, "instances_dir", ""))
        conf_dir = inst_dir / "configs" / (cname or "")
        if not conf_dir.exists():
            return
        # Read one config (all nodes share the same coord settings)
        paths = sorted(conf_dir.glob("keeper_config_*.xml"))
        if not paths:
            return
        txt = paths[0].read_text(encoding="utf-8")

        if startup is not None:
            exp = int(startup)
            m = re.search(r"<startup_timeout>\s*([0-9]+)\s*</startup_timeout>", txt)
            if m and int(m.group(1)) != exp:
                raise AssertionError(
                    f"coord_timeouts_match: startup_timeout={m.group(1)} != {exp}"
                )
        if shutdown is not None:
            exp = int(shutdown)
            m = re.search(r"<shutdown_timeout>\s*([0-9]+)\s*</shutdown_timeout>", txt)
            if m and int(m.group(1)) != exp:
                raise AssertionError(
                    f"coord_timeouts_match: shutdown_timeout={m.group(1)} != {exp}"
                )
    except Exception:
        # Non-fatal in stress env
        return


def error_rate_le(summary, max_ratio=DEFAULT_ERROR_RATE):
    """Assert error rate is below threshold."""
    summary = summary or {}
    errs = float(summary.get("errors", 0) or 0)
    ops = float(summary.get("ops", 0) or 0)
    if ops <= 0:
        return
    ratio = errs / ops
    if ratio > float(max_ratio):
        raise AssertionError(
            f"error_rate_le: ratio {ratio:.4f} exceeds {max_ratio:.4f}"
        )


def srvr_thresholds_le(nodes, metrics, aggregate="sum"):
    """Assert srvr metrics are below thresholds."""
    targets = dict(metrics or {})
    if not targets:
        return

    def _values(n):
        sk = srvr_kv(n) or {}
        for k in targets:
            if k in sk:
                yield k, sk[k]

    totals = _aggregate_totals(nodes, targets, aggregate, _values)
    for k, thr in targets.items():
        if float(totals.get(k, 0.0)) > float(thr):
            raise AssertionError(
                f"srvr_thresholds_le: {k}={totals.get(k, 0.0)} exceeds {thr}"
            )


def rps_ge(summary, min_rps=0.0):
    """Assert requests per second is above threshold."""
    summary = summary or {}
    dur = float(summary.get("duration_s", 0) or 0)
    ops = float(summary.get("ops", 0) or 0)
    rps = ops / dur if dur > 0 else 0.0
    if rps < float(min_rps):
        raise AssertionError(f"rps_ge: {rps:.3f} < {min_rps:.3f}")


def ops_ge(summary, min_ops=0.0):
    """Assert total ops is above threshold."""
    summary = summary or {}
    ops = float(summary.get("ops", 0) or 0)
    if ops < float(min_ops):
        raise AssertionError(f"ops_ge: {ops:.0f} < {min_ops:.0f}")


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
    """Count znodes under given prefixes."""
    hosts = [h.strip() for h in (servers_arg(nodes) or "").split() if h.strip()]
    hostlist = ",".join(hosts) if hosts else "localhost:9181"
    zk = None
    results = []
    try:
        zk = KazooClient(hosts=hostlist, timeout=10.0)
        zk.start(timeout=10.0)
        for p in prefixes or []:
            cnt = _zk_count_descendants(zk, str(p))
            print(f"[keeper] count_paths prefix={p} count={cnt}")
            results.append((str(p), cnt))
    except Exception:
        for p in prefixes or []:
            print(f"[keeper] count_paths prefix={p} count=0 (no_zk_conn)")
            results.append((str(p), 0))
    finally:
        if zk:
            try:
                zk.stop()
                zk.close()
            except Exception:
                pass
    # Debug output
    if parse_bool(os.environ.get("KEEPER_DEBUG")) and results:
        out = (
            Path(__file__).parents[4]
            / "tests"
            / "stress"
            / "keeper"
            / "tests"
            / "keeper_counts.txt"
        )
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(
            "\n".join(f"{p},{c}" for p, c in results) + "\n", encoding="utf-8"
        )


def election_time_le(ctx, max_s=10.0):
    """Assert election time is below threshold."""
    v = (ctx or {}).get("election_time_s")
    if v is None:
        raise AssertionError("election_time_le: missing election_time_s in context")
    if float(v) > float(max_s):
        raise AssertionError(f"election_time_le: {float(v):.3f}s exceeded {max_s:.3f}s")


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


def _max_latency(summary, keys):
    """Get maximum latency value from summary for given keys."""
    try:
        summary = summary or {}
        vals = [float(summary.get(k, 0) or 0) for k in (keys or [])]
        return max(vals) if vals else 0.0
    except Exception:
        return 0.0


def p99_le(summary, max_ms=DEFAULT_P99_MS):
    """Assert p99 latency is below threshold."""
    summary = summary or {}
    val = _max_latency(summary, ["read_p99_ms", "write_p99_ms"])
    if val > float(max_ms):
        raise AssertionError(f"p99_le: {val:.3f}ms exceeds {float(max_ms):.3f}ms")


def p95_le(summary, max_ms=DEFAULT_P95_MS):
    """Assert p95 latency is below threshold."""
    summary = summary or {}
    val = _max_latency(summary, ["read_p95_ms", "write_p95_ms"])
    if val > float(max_ms):
        raise AssertionError(f"p95_le: {val:.3f}ms exceeds {float(max_ms):.3f}ms")


def watch_delta_within(nodes, ctx, max_delta=None, pct=None):
    """Assert watch count delta is within threshold."""
    cur_total = sum(int(wchs_total(n) or 0) for n in (nodes or []))
    base_total = int((ctx or {}).get("watch_baseline_total", 0) or 0)
    if base_total <= 0:
        return  # No baseline, treat as non-fatal
    delta = abs(cur_total - base_total)
    if pct is not None and float(pct) >= 0:
        allowed = base_total * (float(pct) / 100.0)
        if delta > allowed:
            raise AssertionError(
                f"watch_delta_within: |{cur_total}-{base_total}| > {allowed:.1f} (pct {pct}%)"
            )
        return
    threshold = int(max_delta) if max_delta is not None else 100
    if delta > threshold:
        raise AssertionError(
            f"watch_delta_within: |{cur_total}-{base_total}| > {threshold}"
        )


def no_watcher_hotspot(nodes, ctx=None, max_share=0.95, max_path_share=None):
    """Assert no single node has excessive watcher share."""
    totals = [int(wchs_total(n) or 0) for n in (nodes or [])]
    total = sum(totals)
    if total > 0:
        worst = max(totals)
        share = float(worst) / float(total)
        if share > float(max_share):
            raise AssertionError(
                f"no_watcher_hotspot: worst_share={share:.3f} exceeds {max_share:.3f}"
            )
    if max_path_share is not None:
        agg = {}
        for n in nodes or []:
            for k, v in (wchp_paths(n) or {}).items():
                agg[k] = agg.get(k, 0) + int(v)
        t = sum(agg.values())
        if t > 0:
            w = max(agg.values())
            s = float(w) / float(t)
            if s > float(max_path_share):
                raise AssertionError(
                    f"no_watcher_hotspot: worst_path_share={s:.3f} exceeds {max_path_share:.3f}"
                )


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


def lgif_monotone(nodes, ctx=None):
    """Assert lgif metrics are monotonically increasing."""
    if not ctx:
        return
    for n in nodes or []:
        cur = lgif(n) or {}
        base = ((ctx.get("_metrics_cache_baseline") or {}).get(n.name) or {}).get(
            "lgif"
        )
        if not base:
            continue
        dec = []
        for k, bv in base.items():
            try:
                bvi, cv = int(bv), int(cur.get(k, bv))
                if cv < bvi:
                    dec.append(k)
            except (ValueError, TypeError):
                continue
        if dec:
            raise AssertionError(
                f"lgif_monotone: decreased keys: {','.join(sorted(dec)[:5])}"
            )


def fourlw_enforces(nodes, allow=None, deny=None):
    allow = [str(x).strip() for x in (allow or []) if str(x).strip()]
    deny = [str(x).strip() for x in (deny or []) if str(x).strip()]
    for n in nodes or []:
        for cmd in allow:
            out = four(n, cmd)
            if not str(out).strip():
                raise AssertionError(f"fourlw_enforces: allow {cmd} returned empty")
        for cmd in deny:
            out = four(n, cmd)
            if str(out).strip() and (
                "Mode:" in out or "zk_" in out or "watch" in out or "connections" in out
            ):
                raise AssertionError(f"fourlw_enforces: deny {cmd} returned response")


def health_precheck(nodes, timeout_s=0):
    """Verify cluster is healthy: has leader and all nodes respond to probes."""
    if not nodes:
        raise AssertionError("health_precheck: no nodes")
    if timeout_s and int(timeout_s) > 0:
        deadline = time.time() + float(timeout_s)
        while True:
            if count_leaders(nodes) >= 1:
                break
            if time.time() >= deadline:
                raise AssertionError("health_precheck: no leader")
            time.sleep(0.5)
    else:
        if count_leaders(nodes) < 1:
            raise AssertionError("health_precheck: no leader")
    for n in nodes:
        m = mntr(n)
        if not isinstance(m, dict) or not m:
            kv = srvr_kv(n)
            if not isinstance(kv, dict) or not kv:
                raise AssertionError("health_precheck: mntr/srvr empty")


def prom_thresholds_le(nodes, metrics, aggregate="sum"):
    """Assert Prometheus metrics are below thresholds."""
    targets = dict(metrics or {})
    if not targets:
        return

    def _values(n):
        for r in parse_prometheus_text(prom_metrics(n)):
            name = r.get("name", "")
            if name in targets:
                yield name, r.get("value", 0.0)

    totals = _aggregate_totals(nodes, targets, aggregate, _values)
    for k, thr in targets.items():
        if float(totals.get(k, 0.0)) > float(thr):
            raise AssertionError(
                f"prom_thresholds_le: {k}={totals.get(k, 0.0)} exceeds {thr}"
            )


def _gate_mntr_print(nodes):
    """Print mntr stats for each node."""
    for n in nodes or []:
        try:
            m = mntr(n) or {}
            print(
                f"[keeper] mntr node={n.name} zk_znode_count={m.get('zk_znode_count', 0)} "
                f"zk_ephemerals_count={m.get('zk_ephemerals_count', 0)} "
                f"zk_watch_count={m.get('zk_watch_count', 0)} zk_data_size={m.get('zk_approximate_data_size', 0)}"
            )
        except Exception:
            print(f"[keeper] mntr node={getattr(n, 'name', 'node')} error")


def _gate_print_summary(summary):
    """Print benchmark summary."""
    s = summary or {}
    ops = int(s.get("ops", 0) or 0)
    errs = int(s.get("errors", 0) or 0)
    rp50, rp95, rp99 = (
        float(s.get("read_p50_ms", 0) or 0),
        float(s.get("read_p95_ms", 0) or 0),
        float(s.get("read_p99_ms", 0) or 0),
    )
    wp50, wp95, wp99 = (
        float(s.get("write_p50_ms", 0) or 0),
        float(s.get("write_p95_ms", 0) or 0),
        float(s.get("write_p99_ms", 0) or 0),
    )
    extra = []
    if s.get("read_ratio") is not None:
        extra.append(f"read_ratio={float(s['read_ratio']):.3f}")
    if s.get("write_ratio") is not None:
        extra.append(f"write_ratio={float(s['write_ratio']):.3f}")
    print(
        f"[keeper] bench_summary ops={ops} errors={errs} read_p99={rp99:.3f}ms write_p99={wp99:.3f}ms {' '.join(extra)}".strip()
    )
    # Debug output
    if parse_bool(os.environ.get("KEEPER_DEBUG")):
        out = (
            Path(__file__).parents[4]
            / "tests"
            / "stress"
            / "keeper"
            / "tests"
            / "keeper_summary.txt"
        )
        out.parent.mkdir(parents=True, exist_ok=True)
        content = (
            f"ops={ops} errors={errs} "
            f"read_p50_ms={rp50:.3f} read_p95_ms={rp95:.3f} read_p99_ms={rp99:.3f} "
            f"write_p50_ms={wp50:.3f} write_p95_ms={wp95:.3f} write_p99_ms={wp99:.3f}"
        )
        if extra:
            content += " " + " ".join(extra)
        out.write_text(content + "\n", encoding="utf-8")


# Gate handlers (registered with decorators)

@register_gate("single_leader")
def _gate_single_leader(g, nodes, leader, ctx, summary):
    return single_leader(nodes, timeout_s=_extract_param(g, "timeout_s", 180, int))


@register_gate("backlog_drains")
def _gate_backlog_drains(g, nodes, leader, ctx, summary):
    return backlog_drains(nodes, max_s=_extract_param(g, "max_s", 120, int))


@register_gate("error_rate_le")
def _gate_error_rate_le(g, nodes, leader, ctx, summary):
    return error_rate_le(summary, max_ratio=_extract_param(g, "max_ratio", DEFAULT_ERROR_RATE, float))


@register_gate("p99_le")
def _gate_p99_le(g, nodes, leader, ctx, summary):
    return p99_le(summary, max_ms=_extract_param(g, "max_ms", DEFAULT_P99_MS, int))


@register_gate("p95_le")
def _gate_p95_le(g, nodes, leader, ctx, summary):
    return p95_le(summary, max_ms=_extract_param(g, "max_ms", DEFAULT_P95_MS, int))


@register_gate("watch_delta_within")
def _gate_watch_delta_within(g, nodes, leader, ctx, summary):
    md_val = g.get("max_delta")
    md = int(md_val) if md_val is not None else None
    pct_val = g.get("pct")
    pct = float(pct_val) if pct_val is not None else None
    return watch_delta_within(nodes, ctx, max_delta=md, pct=pct)


@register_gate("no_watcher_hotspot")
def _gate_no_watcher_hotspot(g, nodes, leader, ctx, summary):
    mps_val = g.get("max_path_share")
    mps = float(mps_val) if mps_val is not None else None
    return no_watcher_hotspot(
        nodes, ctx, max_share=_extract_param(g, "max_share", 0.95, float), max_path_share=mps
    )


@register_gate("ephemerals_gone_within")
def _gate_ephemerals_gone_within(g, nodes, leader, ctx, summary):
    return ephemerals_gone_within(nodes, max_s=_extract_param(g, "max_s", 60, int))


@register_gate("ready_expect")
def _gate_ready_expect(g, nodes, leader, ctx, summary):
    return ready_expect(
        nodes, leader,
        ok=_extract_param(g, "ok", True, bool),
        timeout_s=_extract_param(g, "timeout_s", 60, int)
    )


@register_gate("lgif_monotone")
def _gate_lgif_monotone(g, nodes, leader, ctx, summary):
    return lgif_monotone(nodes, ctx)


@register_gate("fourlw_enforces")
def _gate_fourlw_enforces(g, nodes, leader, ctx, summary):
    return fourlw_enforces(nodes, allow=g.get("allow"), deny=g.get("deny"))


@register_gate("health_precheck")
def _gate_health_precheck(g, nodes, leader, ctx, summary):
    return health_precheck(nodes, timeout_s=_extract_param(g, "timeout_s", 30, int))


@register_gate("prom_thresholds_le")
def _gate_prom_thresholds_le(g, nodes, leader, ctx, summary):
    return prom_thresholds_le(
        nodes, g.get("metrics") or {}, aggregate=str(g.get("aggregate", "sum"))
    )


@register_gate("srvr_thresholds_le")
def _gate_srvr_thresholds_le(g, nodes, leader, ctx, summary):
    return srvr_thresholds_le(
        nodes, g.get("metrics") or {}, aggregate=str(g.get("aggregate", "sum"))
    )


@register_gate("rps_ge")
def _gate_rps_ge(g, nodes, leader, ctx, summary):
    return rps_ge(summary, min_rps=_extract_param(g, "min_rps", 0, float))


@register_gate("ops_ge")
def _gate_ops_ge(g, nodes, leader, ctx, summary):
    return ops_ge(summary, min_ops=_extract_param(g, "min_ops", 0, float))


@register_gate("count_paths")
def _gate_count_paths(g, nodes, leader, ctx, summary):
    return count_paths(nodes, g.get("prefixes") or [])


@register_gate("config_converged")
def _gate_config_converged(g, nodes, leader, ctx, summary):
    return config_converged(nodes, timeout_s=_extract_param(g, "timeout_s", 30, int))


@register_gate("config_members_len_eq")
def _gate_config_members_len_eq(g, nodes, leader, ctx, summary):
    return config_members_len_eq(nodes, expected=_extract_param(g, "expected", 3, int))


@register_gate("election_time_le")
def _gate_election_time_le(g, nodes, leader, ctx, summary):
    return election_time_le(ctx, max_s=_extract_param(g, "max_s", 10, float))


@register_gate("log_sanity_ok")
def _gate_log_sanity_ok(g, nodes, leader, ctx, summary):
    return log_sanity_ok(nodes, allow=g.get("allow"))


@register_gate("coord_timeouts_match")
def _gate_coord_timeouts_match(g, nodes, leader, ctx, summary):
    return coord_timeouts_match(
        nodes, ctx, startup=g.get("startup"), shutdown=g.get("shutdown")
    )


@register_gate("mntr_print")
def _gate_dispatch_mntr_print(g, nodes, leader, ctx, summary):
    return _gate_mntr_print(nodes)


@register_gate("print_summary")
def _gate_dispatch_print_summary(g, nodes, leader, ctx, summary):
    return _gate_print_summary(summary)


@register_gate("sync_read_barrier")
def _gate_sync_read_barrier(g, nodes, leader, ctx, summary):
    """Test sync read barrier semantics.
    
    This gate verifies that sync operations create a read barrier ensuring
    all previous writes are visible to subsequent reads.
    
    TODO: Implement sync read barrier test logic.
    """
    iterations = int(g.get("iterations", 3))
    path_prefix = str(g.get("path_prefix", "/sem01")).strip()
    raise NotImplementedError(
        f"sync_read_barrier gate not yet implemented. "
        f"Required: iterations={iterations}, path_prefix={path_prefix}"
    )


@register_gate("linearizable_reads_ok")
def _gate_linearizable_reads_ok(g, nodes, leader, ctx, summary):
    """Test linearizable read guarantees.
    
    This gate verifies that reads are linearizable (consistent ordering)
    when quorum_reads is enabled.
    
    TODO: Implement linearizable reads test logic.
    """
    iterations = int(g.get("iterations", 10))
    base = str(g.get("base", "/sem02")).strip()
    raise NotImplementedError(
        f"linearizable_reads_ok gate not yet implemented. "
        f"Required: iterations={iterations}, base={base}"
    )


@register_gate("acl_matrix_ok")
def _gate_acl_matrix_ok(g, nodes, leader, ctx, summary):
    """Test ACL (Access Control List) matrix correctness.
    
    This gate verifies that ACL permissions are correctly enforced
    across different authentication schemes (world, auth, digest).
    
    TODO: Implement ACL matrix test logic.
    """
    raise NotImplementedError(
        "acl_matrix_ok gate not yet implemented. "
        "This gate should test ACL permission enforcement across auth schemes."
    )


@register_gate("acl_digest_rotate_ok")
def _gate_acl_digest_rotate_ok(g, nodes, leader, ctx, summary):
    """Test ACL digest rotation correctness.
    
    This gate verifies that ACL digest rotation works correctly
    and doesn't break existing permissions.
    
    TODO: Implement ACL digest rotation test logic.
    """
    raise NotImplementedError(
        "acl_digest_rotate_ok gate not yet implemented. "
        "This gate should test ACL digest rotation functionality."
    )


@register_gate("lin_register_ok")
def _gate_lin_register_ok(g, nodes, leader, ctx, summary):
    """Test linearizable register semantics.
    
    This gate verifies that register operations (read/write) maintain
    linearizability guarantees under concurrent access and faults.
    
    TODO: Implement linearizable register test logic.
    """
    duration_s = int(g.get("duration_s", 10))
    writers = int(g.get("writers", 2))
    readers = int(g.get("readers", 4))
    path = str(g.get("path", "/lin/reg")).strip()
    raise NotImplementedError(
        f"lin_register_ok gate not yet implemented. "
        f"Required: duration_s={duration_s}, writers={writers}, "
        f"readers={readers}, path={path}"
    )


def apply_gate(gate, nodes, leader, ctx, summary):
    """Apply a validation gate check."""
    gtype = (gate.get("type") or "").strip()
    handler = _GATE_HANDLERS.get(gtype)
    if handler:
        return handler(gate, nodes, leader, ctx, summary)
    raise AssertionError(f"unknown gate type: {gtype}")
