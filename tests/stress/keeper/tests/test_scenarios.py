import copy
import json
import os
import pathlib
import shutil
import time
import uuid

import pytest
import yaml
from keeper.faults import apply_step as apply_step_dispatcher
from keeper.framework.core.preflight import ensure_environment
from keeper.framework.core.registry import fault_registry
from keeper.framework.core.settings import (
    CLIENT_PORT,
    DEFAULT_ERROR_RATE,
    DEFAULT_P99_MS,
    parse_bool,
)
from keeper.framework.core.util import sh, ts_ms
from keeper.framework.fuzz import _EXCLUDE as _FUZZ_EXCLUDE
from keeper.framework.fuzz import generate_fuzz_scenario
from keeper.framework.io.probes import (
    ch_async_metrics,
    ch_metrics,
    dirs,
    is_leader,
    lgif,
    mntr,
    prom_metrics,
    srvr_kv,
)
from keeper.framework.io.prom_parse import parse_prometheus_text
from keeper.framework.io.sink import has_ci_sink, sink_clickhouse
from keeper.framework.metrics.prom_env import compute_prom_config
from keeper.framework.metrics.sampler import MetricsSampler
from keeper.gates.base import apply_gate, single_leader
from keeper.workloads.adapter import servers_arg

WORKDIR = pathlib.Path(__file__).parents[1]


# ─────────────────────────────────────────────────────────────────────────────
# Wrapper functions for step/gate dispatch
# ─────────────────────────────────────────────────────────────────────────────
def _apply_step(step, nodes, leader, ctx):
    """Apply a fault injection step."""
    apply_step_dispatcher(step, nodes, leader, ctx)


def _apply_gate(gate, nodes, leader, ctx, summary):
    """Apply a validation gate."""
    apply_gate(gate, nodes, leader, ctx, summary)


def _best_effort(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Helper: CLI/env option extraction with fallback
# ─────────────────────────────────────────────────────────────────────────────
def _get_option(request, cli_name, env_name, default=None, type_fn=str):
    """Get option from CLI args first, then env var, then default."""
    try:
        val = request.config.getoption(cli_name)
        if val is not None:
            return type_fn(val)
    except Exception:
        pass
    env_val = os.environ.get(env_name, "").strip()
    if env_val:
        try:
            return type_fn(env_val)
        except (ValueError, TypeError):
            pass
    return default


def _get_int_option(request, cli_name, env_name, default=0):
    return _get_option(request, cli_name, env_name, default, int)


# ─────────────────────────────────────────────────────────────────────────────
# Helper: Metric row builder
# ─────────────────────────────────────────────────────────────────────────────
def _make_metric_row(
    run_id,
    run_meta,
    scenario_id,
    topo,
    node,
    stage,
    source,
    name,
    value,
    labels_json="{}",
):
    """Build a standardized metric row dict."""
    try:
        val = float(value)
    except (ValueError, TypeError):
        val = 0.0
    return {
        "ts": ts_ms(),
        "run_id": run_id,
        "commit_sha": run_meta.get("commit_sha", "local"),
        "backend": run_meta.get("backend", "default"),
        "scenario": scenario_id,
        "topology": topo,
        "node": node,
        "stage": stage,
        "source": source,
        "name": str(name),
        "value": val,
        "labels_json": labels_json,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Helper: Cleanup context resources
# ─────────────────────────────────────────────────────────────────────────────
def _cleanup_ctx_resources(ctx):
    """Clean up KeeperClient pools, watches, and ephemeral clients."""
    # Close pooled KeeperClient
    pool = ctx.get("_kc_pool")
    if pool:
        try:
            pool.close()
        except Exception:
            pass
    # Stop individual clients
    for c in (ctx.get("_kc_clients") or {}).values():
        try:
            c.stop()
        except Exception:
            pass
    # Cancel watch flood watches
    for w in ctx.get("_watch_flood_watches") or []:
        for method in ("cancel", "stop"):
            try:
                getattr(w, method, lambda: None)()
            except Exception:
                pass
        try:
            stopped = getattr(w, "_stopped", None)
            if stopped:
                stopped.set()
        except Exception:
            pass
        try:
            thr = getattr(w, "_thread", None)
            if thr:
                thr.join(timeout=0.5)
        except Exception:
            pass
    time.sleep(0.1)
    # Close ZK clients
    for clients_key in ("_watch_flood_clients", "_ephem_clients"):
        for zk in ctx.get(clients_key) or []:
            try:
                zk.stop()
                zk.close()
            except Exception:
                pass


def resolve_duration(request, env_name, yaml_default=None, hard_default=120):
    """Resolve duration from CLI > env > yaml > hard default."""
    cli_dur = _get_int_option(request, "--duration", "", None)
    if cli_dur:
        return cli_dur
    env_val = os.environ.get(env_name, "").strip()
    if env_val:
        try:
            return int(env_val)
        except ValueError:
            pass
    if yaml_default is not None:
        try:
            return int(yaml_default)
        except (ValueError, TypeError):
            pass
    return int(hard_default)


def build_bench_step(wl, eff_dur):
    """Build run_bench step dict."""
    rb = {"kind": "run_bench", "duration_s": eff_dur}
    cfg = (wl or {}).get("config")
    if cfg:
        rb["config"] = str(WORKDIR / cfg)
    return rb


def inject_parallel(fs_effective, step):
    if not fs_effective:
        return [step]
    if (
        len(fs_effective) == 1
        and isinstance(fs_effective[0], dict)
        and (fs_effective[0].get("kind") or "").lower() == "parallel"
    ):
        fs_effective[0]["steps"] = [step] + (fs_effective[0].get("steps") or [])
        return fs_effective
    return [
        {
            "kind": "parallel",
            "steps": [step, {"kind": "sequence", "steps": fs_effective}],
        }
    ]


def has_run_bench(actions):
    """Check if actions contain a run_bench step (including inside parallel)."""

    def _contains(step_list):
        for a in step_list or []:
            kind = (a.get("kind") or "").lower()
            if kind == "run_bench":
                return True
            if kind in ("parallel", "sequence"):
                if _contains(a.get("steps") or []):
                    return True
        return False

    return _contains(actions)


def ensure_default_gates(scenario):
    """Ensure scenario has error_rate_le and p99_le gates."""
    if not isinstance(scenario, dict):
        return
    gs = list(scenario.get("gates") or [])
    gate_types = {g.get("type") for g in gs}
    if "ops_ge" not in gate_types:
        gs.append({"type": "ops_ge", "min_ops": 1.0})
    if "error_rate_le" not in gate_types:
        gs.append({"type": "error_rate_le", "max_ratio": float(DEFAULT_ERROR_RATE)})
    if "p99_le" not in gate_types:
        gs.append({"type": "p99_le", "max_ms": int(DEFAULT_P99_MS)})
    scenario["gates"] = gs


def _resolve_faults_mode(request):
    try:
        return request.config.getoption("--faults")
    except Exception:
        return os.environ.get("KEEPER_FAULTS", "on")


def _resolve_random_faults(request, scenario, rnd_count, seed_val):
    try:
        cli_dur = request.config.getoption("--duration")
    except Exception:
        cli_dur = None
    env_dur = os.environ.get("KEEPER_DURATION")
    yaml_dur = (scenario.get("workload") or {}).get("duration")
    try:
        dur_default = int(cli_dur or env_dur or yaml_dur or 120)
    except (ValueError, TypeError):
        dur_default = 120
    if seed_val <= 0:
        seed_val = int.from_bytes(os.urandom(4), "big")
    try:
        inc_raw = _get_option(
            request, "--random-faults-include", "KEEPER_RANDOM_FAULTS_INCLUDE", ""
        )
        exc_raw = _get_option(
            request, "--random-faults-exclude", "KEEPER_RANDOM_FAULTS_EXCLUDE", ""
        )
        inc = {x.strip() for x in (inc_raw or "").split(",") if x.strip()}
        exc = {x.strip() for x in (exc_raw or "").split(",") if x.strip()}
        cands = [
            k
            for k in list(getattr(fault_registry, "keys", lambda: [])())
            if k not in _FUZZ_EXCLUDE
        ]
        if not cands and isinstance(fault_registry, dict):
            cands = [k for k in fault_registry.keys() if k not in _FUZZ_EXCLUDE]
        weights = {}
        if inc:
            for k in cands:
                weights[k] = 1 if k in inc else 0
        else:
            for k in cands:
                weights[k] = 0 if k in exc else 1
        fz = generate_fuzz_scenario(
            seed_val, max(1, rnd_count), dur_default, weights=weights
        )
        rnd_faults = fz.get("faults", []) or []
        return rnd_faults, seed_val
    except Exception:
        return None, seed_val


def _inject_workload_bench(request, scenario, nodes, fs_effective, ctx):
    if parse_bool(os.environ.get("KEEPER_DISABLE_WORKLOAD")):
        return fs_effective, False
    bench_injected = False
    wc_env = os.environ.get("KEEPER_WORKLOAD_CONFIG", "").strip()
    if "workload" in scenario:
        wl = scenario["workload"]
        rp = os.environ.get("KEEPER_REPLAY_PATH", "").strip()
        if rp:
            wl = dict(wl or {})
            wl["replay"] = rp
        if wc_env:
            wl = dict(wl or {})
            wl["config"] = wc_env
        eff_dur = resolve_duration(request, "KEEPER_DURATION", wl.get("duration"), 120)
        rb = build_bench_step(wl, eff_dur)
        ctx["workload"] = wl
        ctx["bench_node"] = nodes[0]
        ctx["bench_secure"] = False
        ctx["bench_servers"] = servers_arg(nodes)
        fs_effective = inject_parallel(fs_effective, rb)
        bench_injected = True
        ensure_default_gates(scenario)
    if not bench_injected:
        eff_dur_fb = resolve_duration(request, "KEEPER_DURATION", None, 120)
        cfg_name = wc_env or "workloads/prod_mix.yaml"
        wl_fb = {"config": cfg_name}
        rb = build_bench_step(wl_fb, eff_dur_fb)
        if not has_run_bench(fs_effective):
            fs_effective = inject_parallel(fs_effective, rb)
        bench_injected = True
        ensure_default_gates(scenario)
    return fs_effective, bench_injected


def _ensure_clickhouse_binary_for_bench():
    if parse_bool(os.environ.get("KEEPER_DISABLE_WORKLOAD")):
        return
    env_bin = str(os.environ.get("CLICKHOUSE_BINARY", "")).strip()
    if not env_bin:
        raise AssertionError(
            "keeper-bench must run on host: env var CLICKHOUSE_BINARY must point to clickhouse binary"
        )
    if not os.path.exists(env_bin) or not os.access(env_bin, os.X_OK):
        raise AssertionError(
            f"keeper-bench must run on host: CLICKHOUSE_BINARY is not an executable file: {env_bin}"
        )


def _emit_bench_summary(summary, run_id, run_meta_eff, scenario_id, topo, sink_url):
    if not summary:
        return False
    s = summary
    dur, ops, err = (
        _safe_float(s.get("duration_s")),
        _safe_float(s.get("ops")),
        _safe_float(s.get("errors")),
    )
    rps = ops / dur if dur > 0 else 0.0
    names_vals = {
        "ops": ops,
        "errors": err,
        "duration_s": dur,
        "rps": rps,
        "reads": _safe_float(s.get("reads")),
        "writes": _safe_float(s.get("writes")),
        "read_rps": _safe_float(s.get("read_rps")),
        "read_bps": _safe_float(s.get("read_bps")),
        "write_rps": _safe_float(s.get("write_rps")),
        "write_bps": _safe_float(s.get("write_bps")),
        "read_ratio": _safe_float(s.get("read_ratio")),
        "write_ratio": _safe_float(s.get("write_ratio")),
        "error_rate": err / ops if ops > 0 else 0.0,
        "bench_has_latency": 1.0 if s.get("has_latency") else 0.0,
        "read_p50_ms": _safe_float(s.get("read_p50_ms")),
        "read_p95_ms": _safe_float(s.get("read_p95_ms")),
        "read_p99_ms": _safe_float(s.get("read_p99_ms")),
        "write_p50_ms": _safe_float(s.get("write_p50_ms")),
        "write_p95_ms": _safe_float(s.get("write_p95_ms")),
        "write_p99_ms": _safe_float(s.get("write_p99_ms")),
    }
    rows = [
        _make_metric_row(
            run_id,
            run_meta_eff,
            scenario_id,
            topo,
            "bench",
            "summary",
            "bench",
            nm,
            val,
        )
        for nm, val in names_vals.items()
    ]
    rows.append(
        _make_metric_row(
            run_id,
            run_meta_eff,
            scenario_id,
            topo,
            "bench",
            "summary",
            "bench",
            "bench_ran",
            1.0,
        )
    )
    print("[keeper][push-metrics] begin")
    for rr in rows:
        print(json.dumps(rr, ensure_ascii=False))
    print("[keeper][push-metrics] end")
    if sink_url:
        sink_clickhouse(sink_url, "keeper_metrics_ts", rows)
    return True


def _emit_derived_metrics(
    pre_rows,
    post_rows,
    run_id,
    run_meta_eff,
    scenario_id,
    topo,
    sink_url,
    bench_ran_flag,
):
    def _pick(rows, source, name):
        return {
            r.get("node", ""): _safe_float(r.get("value"))
            for r in rows
            if r.get("source") == source and r.get("name") == name
        }

    metrics = {
        "znode": "zk_znode_count",
        "ephem": "zk_ephemerals_count",
        "watch": "zk_watch_count",
    }
    pre = {k: _pick(pre_rows, "mntr", v) for k, v in metrics.items()}
    pre["dirs_bytes"] = _pick(pre_rows, "dirs", "bytes")
    post = {k: _pick(post_rows, "mntr", v) for k, v in metrics.items()}
    post["dirs_bytes"] = _pick(post_rows, "dirs", "bytes")
    nodes_names = sorted(
        set().union(*[d.keys() for d in [*pre.values(), *post.values()]])
    )
    derived = []
    tot = {"znode": 0.0, "ephem": 0.0, "watch": 0.0, "dirs_bytes": 0.0}
    delta_names = {
        "znode": "delta_znode_count",
        "ephem": "delta_ephemerals_count",
        "watch": "delta_watch_count",
        "dirs_bytes": "delta_dirs_bytes",
    }
    for nn in nodes_names:
        deltas = {k: post[k].get(nn, 0.0) - pre[k].get(nn, 0.0) for k in tot}
        for k, dname in delta_names.items():
            derived.append(
                _make_metric_row(
                    run_id,
                    run_meta_eff,
                    scenario_id,
                    topo,
                    nn,
                    "summary",
                    "derived",
                    dname,
                    deltas[k],
                )
            )
            tot[k] += deltas[k]
    for key, val in (
        ("total_delta_znode_count", tot["znode"]),
        ("total_delta_ephemerals_count", tot["ephem"]),
        ("total_delta_watch_count", tot["watch"]),
        ("total_delta_dirs_bytes", tot["dirs_bytes"]),
    ):
        derived.append(
            _make_metric_row(
                run_id,
                run_meta_eff,
                scenario_id,
                topo,
                "all",
                "summary",
                "derived",
                key,
                val,
            )
        )
    derived.append(
        _make_metric_row(
            run_id,
            run_meta_eff,
            scenario_id,
            topo,
            "all",
            "summary",
            "derived",
            "nodes_count",
            topo,
        )
    )
    if sink_url and derived:
        sink_clickhouse(sink_url, "keeper_metrics_ts", derived)
    if bench_ran_flag:
        bench_totals = [
            _make_metric_row(
                run_id,
                run_meta_eff,
                scenario_id,
                topo,
                "bench",
                "summary",
                "bench",
                "total_znode_delta",
                tot["znode"],
            ),
            _make_metric_row(
                run_id,
                run_meta_eff,
                scenario_id,
                topo,
                "bench",
                "summary",
                "bench",
                "total_dirs_bytes_delta",
                tot["dirs_bytes"],
            ),
        ]
        if sink_url:
            sink_clickhouse(sink_url, "keeper_metrics_ts", bench_totals)
    node_lines = [
        f"{nn}: znode+{int(post['znode'].get(nn, 0) - pre['znode'].get(nn, 0))} dirs_bytes+{int(post['dirs_bytes'].get(nn, 0) - pre['dirs_bytes'].get(nn, 0))}"
        for nn in nodes_names
    ]
    print(
        f"[keeper][bench-created] nodes={len(nodes_names)} total_znode_delta={int(tot['znode'])} total_dirs_bytes_delta={int(tot['dirs_bytes'])}"
    )
    if node_lines:
        print("[keeper][bench-created-per-node] " + ", ".join(node_lines))


def _fault_event_to_metric_row(ev, run_id, run_meta_eff, scenario_id, topo):
    node = str((ev or {}).get("node") or "") or "unknown"
    labels = {}
    for k, v in (ev or {}).items():
        if k in ("node",):
            continue
        if v is None:
            continue
        labels[str(k)] = v
    return _make_metric_row(
        run_id,
        run_meta_eff,
        scenario_id,
        topo,
        node,
        "summary",
        "fault",
        "fault_phase",
        1.0,
        labels_json=json.dumps(labels, ensure_ascii=False),
    )


def _emit_fault_events_metrics(ctx, run_id, run_meta_eff, scenario_id, topo, sink_url):
    if not sink_url:
        return 0
    evs = (ctx or {}).get("fault_events") or []
    if not evs:
        return 0
    rows = []
    for ev in evs:
        try:
            rows.append(
                _fault_event_to_metric_row(ev, run_id, run_meta_eff, scenario_id, topo)
            )
        except Exception:
            continue
    if rows:
        sink_clickhouse(sink_url, "keeper_metrics_ts", rows)
    return len(rows)


def _gate_event_rows(
    run_id, run_meta_eff, scenario_id, topo, gate_type, ok, duration_ms, error=""
):
    labels = {"type": str(gate_type or "")}
    if error:
        labels["error"] = str(error)[:500]
    return [
        _make_metric_row(
            run_id,
            run_meta_eff,
            scenario_id,
            topo,
            "all",
            "summary",
            "gate",
            "gate_ok",
            1.0 if ok else 0.0,
            labels_json=json.dumps(labels, ensure_ascii=False),
        ),
        _make_metric_row(
            run_id,
            run_meta_eff,
            scenario_id,
            topo,
            "all",
            "summary",
            "gate",
            "gate_duration_ms",
            float(duration_ms or 0.0),
            labels_json=json.dumps({"type": str(gate_type or "")}, ensure_ascii=False),
        ),
    ]


def _safe_float(val, default=0.0):
    """Safely convert to float."""
    try:
        return float(val or 0)
    except (ValueError, TypeError):
        return default


def _append_row(
    rows,
    run_meta,
    scenario_id,
    topo,
    node_name,
    stage,
    source,
    name,
    value,
    labels_json="{}",
    run_id="",
):
    """Append a metric row to rows list."""
    rows.append(
        _make_metric_row(
            run_id,
            run_meta,
            scenario_id,
            topo,
            node_name,
            stage,
            source,
            name,
            value,
            labels_json,
        )
    )


def _append_kv_rows(
    rows,
    run_meta,
    scenario_id,
    topo,
    node_name,
    stage,
    source,
    mapping,
    run_id="",
):
    """Append rows for a simple key/value mapping."""
    for k, v in (mapping or {}).items():
        _append_row(
            rows,
            run_meta,
            scenario_id,
            topo,
            node_name,
            stage,
            source,
            k,
            v,
            run_id=run_id,
        )


def _parse_prom_text_local(txt, cfg):
    _nallow = cfg.get("prom_name_allowlist")
    if isinstance(_nallow, list) and len(_nallow) == 0:
        _nallow = None
    if cfg.get("prom_allow_prefixes") is None:
        return parse_prometheus_text(
            txt,
            name_allowlist=_nallow,
            exclude_label_keys=cfg.get("prom_exclude_label_keys"),
        )
    return parse_prometheus_text(
        txt,
        allow_prefixes=cfg.get("prom_allow_prefixes"),
        name_allowlist=_nallow,
        exclude_label_keys=cfg.get("prom_exclude_label_keys"),
    )


def _snapshot_and_sink(nodes, stage, scenario_id, topo, run_meta, sink_url, run_id=""):
    """Snapshot cluster metrics and optionally sink to CI database."""
    rows = []
    # For fail stage, only snapshot leader to reduce volume
    snap_nodes = nodes
    if stage == "fail":
        # Do not attempt leader detection on fail path: it can hang if docker exec is wedged.
        snap_nodes = nodes[:1]

    snap_prom = os.environ.get("KEEPER_SNAPSHOT_PROM", "1").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    prom_cfg = compute_prom_config({}) if snap_prom else None

    for n in snap_nodes:
        # mntr metrics
        try:
            _append_kv_rows(
                rows,
                run_meta,
                scenario_id,
                topo,
                n.name,
                stage,
                "mntr",
                mntr(n),
                run_id=run_id,
            )
        except Exception:
            pass
        # Prometheus metrics
        if snap_prom:
            try:
                for r in _parse_prom_text_local(prom_metrics(n), prom_cfg):
                    _append_row(
                        rows,
                        run_meta,
                        scenario_id,
                        topo,
                        n.name,
                        stage,
                        "prom",
                        r.get("name", ""),
                        r.get("value", 0.0),
                        r.get("labels_json", "{}"),
                        run_id,
                    )
            except Exception:
                pass
        # Directory stats
        try:
            d_txt = dirs(n) or ""
            _append_kv_rows(
                rows,
                run_meta,
                scenario_id,
                topo,
                n.name,
                stage,
                "dirs",
                {
                    "lines": len(d_txt.splitlines()),
                    "bytes": len(d_txt.encode("utf-8")),
                },
                run_id=run_id,
            )
        except Exception:
            pass
        # ClickHouse metrics
        try:
            _append_kv_rows(
                rows,
                run_meta,
                scenario_id,
                topo,
                n.name,
                stage,
                "ch_metrics",
                {r.get("name", ""): r.get("value", 0) for r in ch_metrics(n)},
                run_id=run_id,
            )
        except Exception:
            pass
        try:
            _append_kv_rows(
                rows,
                run_meta,
                scenario_id,
                topo,
                n.name,
                stage,
                "ch_async_metrics",
                {r.get("name", ""): r.get("value", 0) for r in ch_async_metrics(n)},
                run_id=run_id,
            )
        except Exception:
            pass

    if rows:
        print("[keeper][push-metrics] begin")
        for r in rows:
            print(json.dumps(r, ensure_ascii=False))
        print("[keeper][push-metrics] end")
        if sink_url:
            sink_clickhouse(sink_url, "keeper_metrics_ts", rows)
    return rows


def _print_local_metrics(
    nodes, run_id, scenario_id, topo, run_meta, ctx, bench_summary
):
    try:
        kp = os.environ.get("KEEPER_PRINT_LOCAL_METRICS", "")
        do_print = (
            parse_bool(kp)
            if kp != ""
            else (
                str(os.environ.get("CI", "")).lower() not in ("true", "1")
                and str(os.environ.get("GITHUB_ACTIONS", "")).lower()
                not in ("true", "1")
            )
        )
    except Exception:
        do_print = False
    if not do_print:
        return
    try:
        lines = []
        lines.append(
            f"[keeper][local] run={run_id} sha={run_meta.get('commit_sha','local')} backend={run_meta.get('backend','default')} scenario={scenario_id} topo={topo}"
        )
        cache = (ctx or {}).get("_metrics_cache") or {}
        for n in nodes:
            entry = dict(cache.get(getattr(n, "name", "")) or {})
            # Fallback to direct probes if sampler cache is absent (typical for local runs)
            try:
                srvr = entry.get("srvr") or srvr_kv(n)
            except Exception:
                srvr = entry.get("srvr") or {}
            try:
                mn = entry.get("mntr") or mntr(n)
            except Exception:
                mn = entry.get("mntr") or {}
            try:
                lg = entry.get("lgif") or lgif(n)
            except Exception:
                lg = entry.get("lgif") or {}
            try:
                d_txt = dirs(n)
                dl = entry.get("dirs_lines", None)
                db = entry.get("dirs_bytes", None)
                if dl is None:
                    dl = len(d_txt.splitlines()) if d_txt else 0
                if db is None:
                    db = len((d_txt or "").encode("utf-8"))
            except Exception:
                dl = entry.get("dirs_lines", 0)
                db = entry.get("dirs_bytes", 0)
            try:
                if "prom_rows" in entry:
                    prc = len(entry.get("prom_rows") or [])
                else:
                    txt = prom_metrics(n)
                    try:
                        if parse_bool(os.environ.get("KEEPER_DEBUG")):
                            _head = [
                                ln
                                for ln in (txt or "").splitlines()
                                if ln and not ln.startswith("#")
                            ]
                            print(
                                f"[keeper][prom-head] node={getattr(n,'name','')} head="
                                + "|".join(_head[:5])
                            )
                    except Exception:
                        pass
                    cfg = compute_prom_config({})
                    _nallow = cfg.get("prom_name_allowlist")
                    if isinstance(_nallow, list) and len(_nallow) == 0:
                        _nallow = None
                    parsed = _parse_prom_text_local(txt, cfg)
                    prc = len(parsed or [])
            except Exception:
                prc = len(entry.get("prom_rows") or [])
            srvr_keys = {
                k: v
                for k, v in srvr.items()
                if k in ("connections", "outstanding", "received", "sent")
            }
            mn_keys = {
                k: mn.get(k)
                for k in (
                    "zk_server_state",
                    "zk_znode_count",
                    "zk_watch_count",
                    "zk_ephemerals_count",
                    "zk_packets_sent",
                    "zk_packets_received",
                )
                if k in mn
            }
            lg_keys = {
                k: lg.get(k)
                for k in ("term", "leader", "last_zxid", "commit_index")
                if k in lg
            }
            lines.append(
                f"[keeper][local] node={getattr(n,'name','')} srvr={srvr_keys} mntr={mn_keys} lgif={lg_keys} dirs_lines={dl} dirs_bytes={db} prom_rows={prc}"
            )
        s = bench_summary or {}
        if s:
            dur, ops = _safe_float(s.get("duration_s")), _safe_float(s.get("ops"))
            rps = ops / dur if dur > 0 else 0.0
            lines.append(
                f"[keeper][local] bench ops={ops} errors={_safe_float(s.get('errors'))} read_p99_ms={_safe_float(s.get('read_p99_ms'))} write_p99_ms={_safe_float(s.get('write_p99_ms'))} rps={rps:.2f}"
            )
        print("\n".join(lines))
    except Exception:
        pass


def _print_keeper_configs(cluster, topo, cname):
    try:
        conf_root = (
            pathlib.Path(getattr(cluster, "instances_dir", ""))
            / "configs"
            / (cname or "")
        )
        for i in range(1, int(topo) + 1):
            p = conf_root / f"keeper_config_keeper{i}.xml"
            if p.exists():
                try:
                    print(f"==== keeper{i} config ====\n" + p.read_text())
                except Exception:
                    pass
    except Exception:
        pass


def _print_manual_znode_counts(nodes):
    for n in nodes:
        try:
            r = sh(
                n,
                f"HOME=/tmp timeout 3s clickhouse keeper-client --host 127.0.0.1 --port {CLIENT_PORT} -q 'mntr' 2>&1 || true",
                timeout=5,
            )
            out = (r or {}).get("out", "")
            val = None
            for ln in (out or "").splitlines():
                if "zk_znode_count" in ln:
                    try:
                        parts = [t for t in ln.replace("\t", " ").split() if t.strip()]
                        for t in reversed(parts):
                            try:
                                val = int(float(t))
                                break
                            except Exception:
                                continue
                    except Exception:
                        pass
                    break
            if val is not None:
                print(
                    f"[keeper][manual] node={getattr(n,'name','')} zk_znode_count={val}"
                )
            else:
                print(
                    f"[keeper][manual] node={getattr(n,'name','')} zk_znode_count=<unparsed>"
                )
        except Exception:
            pass
def test_scenario(scenario, cluster_factory, request, run_meta):
    topo = scenario.get("topology", 3)
    backend = scenario.get("backend") or request.config.getoption("--keeper-backend")
    _ensure_clickhouse_binary_for_bench()
    # Effective run_meta per test with the scenario-selected backend
    run_meta_eff = dict(run_meta or {})
    run_meta_eff["backend"] = backend
    opts = scenario.get("opts", {}) or {}
    faults_mode = _resolve_faults_mode(request)
    rnd_count = _get_int_option(
        request, "--random-faults-count", "KEEPER_RANDOM_FAULTS_COUNT", 1
    )
    seed_val = _get_int_option(request, "--seed", "", 0)
    fs_original = scenario.get("faults", []) or []
    fs_effective = fs_original
    if faults_mode == "off":
        fs_effective = []
    elif faults_mode == "random":
        resolved, seed_val = _resolve_random_faults(
            request, scenario, rnd_count, seed_val
        )
        fs_effective = resolved if resolved is not None else fs_original
    uses_dm = any(f.get("kind") in ("dm_delay", "dm_error") for f in fs_effective)
    if uses_dm:
        os.environ.setdefault("KEEPER_PRIVILEGED", "1")
    # compute run_id early for reproducible artifact paths
    run_id = f"{scenario.get('id','')}-{run_meta.get('commit_sha','local')}-{uuid.uuid4().hex[:8]}"
    # Use a unique cluster name per test to avoid instance-dir collisions
    cname = run_id.replace("/", "_").replace(" ", "_")
    os.environ["KEEPER_CLUSTER_NAME"] = cname
    cluster, nodes = cluster_factory(topo, backend, opts)

    scenario_id = scenario.get("id", "")
    sink_url = "ci" if has_ci_sink() else ""
    summary = {}
    ctx = {"cluster": cluster, "faults_mode": faults_mode, "seed": seed_val}
    sampler = None
    leader = None
    pre_rows = []
    post_rows = []
    bench_ran_flag = False
    bench_injected = False
    try:
        scenario_for_env = copy.deepcopy(scenario)
        scenario_for_env["faults"] = fs_effective
        ensure_environment(nodes, scenario_for_env)
        single_leader(nodes)
        leader = next(n for n in nodes if is_leader(n))
        pre_rows = (
            _snapshot_and_sink(
                nodes,
                "pre",
                scenario_id,
                topo,
                run_meta_eff,
                sink_url,
                run_id,
            )
            or []
        )
        if sink_url:
            cfg = compute_prom_config(opts if isinstance(opts, dict) else {})
            sampler = MetricsSampler(
                nodes,
                run_meta_eff,
                scenario_id,
                topo,
                sink_url=sink_url,
                interval_s=cfg["interval_s"],
                stage_prefix="run",
                run_id=run_id,
                prom_allow_prefixes=cfg.get("prom_allow_prefixes"),
                prom_name_allowlist=cfg.get("prom_name_allowlist"),
                prom_exclude_label_keys=cfg.get("prom_exclude_label_keys"),
                prom_every_n=cfg.get("prom_every_n", 3),
                ctx=ctx,
            )
            sampler.start()
        for step in scenario.get("pre", []) or []:
            _apply_step(step, nodes, leader, ctx)
        fs_effective, bench_injected = _inject_workload_bench(
            request, scenario, nodes, fs_effective, ctx
        )
        for action in fs_effective:
            _apply_step(action, nodes, leader, ctx)
        summary = ctx.get("bench_summary") or {}
        # Sink keeper-bench summary to CIDB as scalar metrics tagged to this run
        if summary:
            bench_ran_flag = _emit_bench_summary(
                summary,
                run_id,
                run_meta_eff,
                scenario_id,
                topo,
                sink_url,
            )
        for gate in scenario.get("gates", []) or []:
            gtype = (gate.get("type") or "") if isinstance(gate, dict) else ""
            t0 = time.time()
            try:
                _apply_gate(gate, nodes, leader, ctx, summary)
            except Exception as e:
                if sink_url:
                    try:
                        rows = _gate_event_rows(
                            run_id,
                            run_meta_eff,
                            scenario_id,
                            topo,
                            gtype,
                            False,
                            int((time.time() - t0) * 1000),
                            error=repr(e),
                        )
                        sink_clickhouse(sink_url, "keeper_metrics_ts", rows)
                    except Exception:
                        pass
                raise
            if sink_url:
                try:
                    rows = _gate_event_rows(
                        run_id,
                        run_meta_eff,
                        scenario_id,
                        topo,
                        gtype,
                        True,
                        int((time.time() - t0) * 1000),
                    )
                    sink_clickhouse(sink_url, "keeper_metrics_ts", rows)
                except Exception:
                    pass
        post_rows = (
            _snapshot_and_sink(
                nodes,
                "post",
                scenario_id,
                topo,
                run_meta_eff,
                sink_url,
                run_id,
            )
            or []
        )
        _best_effort(
            _emit_derived_metrics,
            pre_rows,
            post_rows,
            run_id,
            run_meta_eff,
            scenario_id,
            topo,
            sink_url,
            bench_ran_flag,
        )
        # Check if bench was expected to run but didn't
        if bench_injected:
            ops = _safe_float((summary or {}).get("ops"))
            if (not bench_ran_flag) or ops <= 0:
                raise AssertionError(
                    f"bench did not run (or produced no ops) for scenario={scenario_id} backend={backend}"
                )
        if sink_url and not bench_ran_flag:
            row = _make_metric_row(
                run_id,
                run_meta_eff,
                scenario_id,
                topo,
                "bench",
                "summary",
                "bench",
                "bench_ran",
                0.0,
            )
            _best_effort(sink_clickhouse, sink_url, "keeper_metrics_ts", [row])
        # per-test checks insertion is handled by praktika post-run
    except Exception:
        # Emit minimal reproducible scenario to a file for debugging
        def _write_repro():
            repro_path = f"/tmp/keeper_repro_{run_id}.yaml"
            with open(repro_path, "w", encoding="utf-8") as f:
                yaml.safe_dump(scenario, f, sort_keys=False)
            print(f"[keeper] reproducible scenario written: {repro_path}")

        _best_effort(_write_repro)
        # Attempt to snapshot fail state as well
        _best_effort(
            _snapshot_and_sink,
            nodes,
            "fail",
            scenario_id,
            topo,
            run_meta_eff,
            sink_url,
            run_id,
        )
        # per-test checks insertion is handled by praktika post-run

        _best_effort(setattr, request.node, "keeper_failed", True)
        raise
    finally:
        # Stop metrics sampler
        if sampler:
            _best_effort(sampler.stop)
            _best_effort(sampler.flush)
        # Print local metrics summary
        _best_effort(
            _print_local_metrics,
            nodes,
            run_id,
            scenario_id,
            topo,
            run_meta_eff,
            ctx,
            summary,
        )
        # Print keeper-bench config captured during run
        bcfg = (ctx or {}).get("bench_config_yaml")
        if bcfg:
            _best_effort(print, "==== keeper-bench config ====\n" + str(bcfg))
        # Print the actual config/output paths used by keeper-bench
        cps = (ctx or {}).get("bench_config_paths") or []
        if cps:
            _best_effort(
                print,
                "[keeper][bench-config-paths] " + ", ".join([str(p) for p in cps if p]),
            )
        bops = (ctx or {}).get("bench_output_paths") or []
        if bops:
            _best_effort(
                print,
                "[keeper][bench-output-paths] "
                + ", ".join([str(p) for p in bops if p]),
            )
        sps = (ctx or {}).get("bench_stdout_paths") or []
        if sps:
            _best_effort(
                print,
                "[keeper][bench-stdout-paths] " + ", ".join([str(p) for p in sps if p]),
            )
        try:
            evs = (ctx or {}).get("fault_events") or []
            if evs:
                print("[keeper][fault-events] begin")
                for ev in evs:
                    try:
                        print(json.dumps(ev, ensure_ascii=False))
                    except Exception:
                        pass
                print("[keeper][fault-events] end")
        except Exception:
            pass
        _best_effort(
            _emit_fault_events_metrics,
            ctx,
            run_id,
            run_meta_eff,
            scenario_id,
            topo,
            sink_url,
        )
        _best_effort(
            _print_keeper_configs,
            cluster,
            topo,
            os.environ.get("KEEPER_CLUSTER_NAME", ""),
        )
        _best_effort(_print_manual_znode_counts, nodes)
        # Cleanup context resources (pools, clients, watches)
        _cleanup_ctx_resources(ctx)
        # Keep containers on fail if requested
        try:
            if request.config.getoption("--keep-containers-on-fail") and getattr(
                request.node, "keeper_failed", False
            ):
                return
        except Exception:
            pass
        cluster.shutdown()
        # Optional artifact cleanup on success
        if parse_bool(os.environ.get("KEEPER_CLEAN_ARTIFACTS")) and not getattr(
            request.node, "keeper_failed", False
        ):
            try:
                inst_dir = pathlib.Path(getattr(cluster, "instances_dir", ""))
                if inst_dir.exists():
                    shutil.rmtree(inst_dir, ignore_errors=True)
                conf_dir = pathlib.Path(getattr(cluster, "base_dir", "")) / "configs"
                if conf_dir.exists():
                    shutil.rmtree(conf_dir, ignore_errors=True)
            except Exception:
                pass
