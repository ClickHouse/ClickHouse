import copy
import json
import os
import pathlib
import shutil
import time
import uuid

import pytest
import os as _os
import yaml

from ..faults import apply_step as apply_step_dispatcher
from ..framework.core.preflight import ensure_environment
from ..framework.core.registry import fault_registry
from ..framework.core.settings import parse_bool, DEFAULT_ERROR_RATE, DEFAULT_P99_MS
from ..framework.fuzz import _EXCLUDE as _FUZZ_EXCLUDE
from ..framework.fuzz import generate_fuzz_scenario
from ..framework.io.probes import (
    ch_async_metrics,
    ch_metrics,
    is_leader,
    mntr,
    lgif,
    prom_metrics,
    srvr_kv,
    dirs,
)
from ..framework.io.prom_parse import parse_prometheus_text, DEFAULT_PREFIXES
from ..framework.io.sink import has_ci_sink, sink_clickhouse
from ..framework.metrics.sampler import MetricsSampler
from ..gates.base import apply_gate, single_leader
from ..workloads.adapter import servers_arg
from ..workloads.keeper_bench import KeeperBench

WORKDIR = pathlib.Path(__file__).parents[1]


def _apply_step(step, nodes, leader, ctx):
    apply_step_dispatcher(step, nodes, leader, ctx)


def _apply_gate(gate, nodes, leader, ctx, summary):
    apply_gate(gate, nodes, leader, ctx, summary)


def _ts_ms_str():
    t = time.time()
    lt = time.gmtime(t)
    return time.strftime('%Y-%m-%d %H:%M:%S', lt) + f'.{int((t - int(t))*1000):03d}'


def _snapshot_and_sink(nodes, stage, scenario_id, topo, run_meta, sink_url, run_id=""):
    metrics_ts_rows = []
    # For fail stage, only snapshot leader to reduce volume
    snap_nodes = nodes
    if stage == "fail":
        try:
            leaders = [n for n in nodes if is_leader(n)]
            if leaders:
                snap_nodes = leaders[:1]
        except Exception:
            snap_nodes = nodes[:1]
    for n in snap_nodes:
        try:
            m = mntr(n)
            for _k, _v in (m or {}).items():
                try:
                    _val = float(_v)
                except Exception:
                    continue
                metrics_ts_rows.append(
                    {
                        "ts": _ts_ms_str(),
                        "run_id": run_id,
                        "commit_sha": run_meta.get("commit_sha", "local"),
                        "backend": run_meta.get("backend", "default"),
                        "scenario": scenario_id,
                        "topology": topo,
                        "node": n.name,
                        "stage": stage,
                        "source": "mntr",
                        "name": str(_k),
                        "value": _val,
                        "labels_json": "{}",
                    }
                )
        except Exception:
            pass
        try:
            snap_prom = True
            try:
                v = os.environ.get("KEEPER_SNAPSHOT_PROM", "1").strip().lower()
                snap_prom = v in ("1", "true", "yes", "on")
            except Exception:
                snap_prom = True
            if snap_prom:
                allow = None
                try:
                    pa = os.environ.get("KEEPER_PROM_ALLOW_PREFIXES", "").strip()
                    if pa:
                        allow = [p for p in (pa.split(",") or []) if p]
                except Exception:
                    allow = None
                name_allow = None
                try:
                    pna = os.environ.get("KEEPER_PROM_NAME_ALLOWLIST", "").strip()
                    if pna:
                        name_allow = [p for p in (pna.split(",") or []) if p]
                except Exception:
                    name_allow = None
                excl = None
                try:
                    pel = os.environ.get("KEEPER_PROM_EXCLUDE_LABEL_KEYS", "").strip()
                    if pel:
                        excl = [p for p in (pel.split(",") or []) if p]
                except Exception:
                    excl = None
                p = prom_metrics(n)
                for r in parse_prometheus_text(p, allow_prefixes=allow, name_allowlist=name_allow, exclude_label_keys=excl):
                    name = r.get("name", "")
                    val = 0.0
                    try:
                        val = float(r.get("value", 0.0))
                    except Exception:
                        val = 0.0
                    lj = r.get("labels_json", "{}")
                    metrics_ts_rows.append(
                        {
                            "ts": _ts_ms_str(),
                            "run_id": run_id,
                            "commit_sha": run_meta.get("commit_sha", "local"),
                            "backend": run_meta.get("backend", "default"),
                            "scenario": scenario_id,
                            "topology": topo,
                            "node": n.name,
                            "stage": stage,
                            "source": "prom",
                            "name": name,
                            "value": val,
                            "labels_json": lj,
                        }
                    )
        except Exception:
            pass
        try:
            for r in ch_metrics(n):
                nm = r.get("name", "")
                try:
                    val = float(r.get("value", 0))
                except Exception:
                    val = 0.0
                metrics_ts_rows.append(
                    {
                        "ts": _ts_ms_str(),
                        "run_id": run_id,
                        "commit_sha": run_meta.get("commit_sha", "local"),
                        "backend": run_meta.get("backend", "default"),
                        "scenario": scenario_id,
                        "topology": topo,
                        "node": n.name,
                        "stage": stage,
                        "source": "ch_metrics",
                        "name": nm,
                        "value": val,
                        "labels_json": "{}",
                    }
                )
        except Exception:
            pass
        try:
            for r in ch_async_metrics(n):
                nm = r.get("name", "")
                try:
                    val = float(r.get("value", 0))
                except Exception:
                    val = 0.0
                metrics_ts_rows.append(
                    {
                        "ts": _ts_ms_str(),
                        "run_id": run_id,
                        "commit_sha": run_meta.get("commit_sha", "local"),
                        "backend": run_meta.get("backend", "default"),
                        "scenario": scenario_id,
                        "topology": topo,
                        "node": n.name,
                        "stage": stage,
                        "source": "ch_async_metrics",
                        "name": nm,
                        "value": val,
                        "labels_json": "{}",
                    }
                )
        except Exception:
            pass
    if metrics_ts_rows:
        try:
            print("[keeper][push-metrics] begin")
            for r in metrics_ts_rows:
                try:
                    print(json.dumps(r, ensure_ascii=False))
                except Exception:
                    pass
            print("[keeper][push-metrics] end")
        except Exception:
            pass
        if sink_url:
            sink_clickhouse(sink_url, "keeper_metrics_ts", metrics_ts_rows)
    return metrics_ts_rows


def _print_local_metrics(nodes, run_id, scenario_id, topo, run_meta, ctx, bench_summary):
    try:
        kp = os.environ.get("KEEPER_PRINT_LOCAL_METRICS", "")
        do_print = parse_bool(kp) if kp != "" else (
            str(os.environ.get("CI", "")).lower() not in ("true", "1")
            and str(os.environ.get("GITHUB_ACTIONS", "")).lower() not in ("true", "1")
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
                    prc = len(parse_prometheus_text(txt) or [])
            except Exception:
                prc = len(entry.get("prom_rows") or [])
            srvr_keys = {k: v for k, v in srvr.items() if k in ("connections", "outstanding", "received", "sent")}
            mn_keys = {k: mn.get(k) for k in (
                "zk_server_state",
                "zk_znode_count",
                "zk_watch_count",
                "zk_ephemerals_count",
                "zk_packets_sent",
                "zk_packets_received",
            ) if k in mn}
            lg_keys = {k: lg.get(k) for k in ("term", "leader", "last_zxid", "commit_index") if k in lg}
            lines.append(
                f"[keeper][local] node={getattr(n,'name','')} srvr={srvr_keys} mntr={mn_keys} lgif={lg_keys} dirs_lines={dl} dirs_bytes={db} prom_rows={prc}"
            )
        s = bench_summary or {}
        if s:
            try:
                dur = float(s.get("duration_s") or 0)
            except Exception:
                dur = 0.0
            try:
                ops = float(s.get("ops") or 0)
            except Exception:
                ops = 0.0
            rps = (ops / dur) if (dur and dur > 0) else 0.0
            lines.append(
                f"[keeper][local] bench ops={ops} errors={float(s.get('errors') or 0)} p99_ms={float(s.get('p99_ms') or 0)} rps={rps:.2f}"
            )
        print("\n".join(lines))
    except Exception:
        pass


@pytest.mark.timeout(int(_os.environ.get("KEEPER_PYTEST_TIMEOUT", "2400") or 2400))
def test_scenario(scenario, cluster_factory, request, run_meta):
    start_ts = time.time()
    check_start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(start_ts))
    topo = scenario.get("topology", 3)
    backend = scenario.get("backend") or request.config.getoption("--keeper-backend")
    # Effective run_meta per test with the scenario-selected backend
    run_meta_eff = dict(run_meta or {})
    try:
        run_meta_eff["backend"] = backend
    except Exception:
        pass
    opts = scenario.get("opts", {})
    try:
        faults_mode = request.config.getoption("--faults")
    except Exception:
        faults_mode = os.environ.get("KEEPER_FAULTS", "on")
    try:
        rnd_count = int(request.config.getoption("--random-faults-count") or 1)
    except Exception:
        rnd_count = int(os.environ.get("KEEPER_RANDOM_FAULTS_COUNT", "1"))
    try:
        seed_val = int(request.config.getoption("--seed") or 0)
    except Exception:
        seed_val = 0
    fs_original = scenario.get("faults", []) or []
    fs_effective = fs_original
    if faults_mode == "off":
        fs_effective = []
    elif faults_mode == "random":
        # Prefer CLI/env duration override to enable ad-hoc long runs
        try:
            cli_dur = request.config.getoption("--duration")
        except Exception:
            cli_dur = None
        try:
            env_dur = os.environ.get("KEEPER_DURATION")
        except Exception:
            env_dur = None
        yaml_dur = None
        try:
            yaml_dur = (scenario.get("workload") or {}).get("duration")
        except Exception:
            yaml_dur = None
        try:
            dur_default = int(cli_dur or env_dur or yaml_dur or 120)
        except Exception:
            try:
                dur_default = int(env_dur or yaml_dur or 120)
            except Exception:
                dur_default = 120
        if seed_val <= 0:
            import os as _os

            seed_val = int.from_bytes(_os.urandom(4), "big")
        try:
            try:
                inc_raw = request.config.getoption(
                    "--random-faults-include"
                ) or os.environ.get("KEEPER_RANDOM_FAULTS_INCLUDE", "")
            except Exception:
                inc_raw = os.environ.get("KEEPER_RANDOM_FAULTS_INCLUDE", "")
            try:
                exc_raw = request.config.getoption(
                    "--random-faults-exclude"
                ) or os.environ.get("KEEPER_RANDOM_FAULTS_EXCLUDE", "")
            except Exception:
                exc_raw = os.environ.get("KEEPER_RANDOM_FAULTS_EXCLUDE", "")
            inc = set([x.strip() for x in (inc_raw or "").split(",") if x.strip()])
            exc = set([x.strip() for x in (exc_raw or "").split(",") if x.strip()])
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
            rb = {"kind": "run_bench", "duration_s": dur_default}
            fs_effective = [{"kind": "parallel", "steps": [rb] + rnd_faults}]
        except Exception:
            fs_effective = fs_original
    try:
        uses_dm = any((f.get("kind") in ("dm_delay", "dm_error")) for f in fs_effective)
        if uses_dm:
            os.environ.setdefault("KEEPER_PRIVILEGED", "1")
    except Exception:
        pass
    # compute run_id early for reproducible artifact paths
    run_id = f"{scenario.get('id','')}-{run_meta.get('commit_sha','local')}-{uuid.uuid4().hex[:8]}"
    # Use a unique cluster name per test to avoid instance-dir collisions
    try:
        cname = run_id.replace("/", "_").replace(" ", "_")
        os.environ["KEEPER_CLUSTER_NAME"] = cname
    except Exception:
        pass
    cluster, nodes = cluster_factory(topo, backend, opts)
    summary = {}
    ctx = {}
    try:
        try:
            scenario_for_env = copy.deepcopy(scenario)
            scenario_for_env["faults"] = fs_effective
        except Exception:
            scenario_for_env = scenario
        ensure_environment(nodes, scenario_for_env)
        single_leader(nodes)
        leader = [n for n in nodes if is_leader(n)][0]
        sink_url = "ci" if (has_ci_sink() or os.environ.get("KEEPER_METRICS_FILE")) else ""
        pre_rows = _snapshot_and_sink(
            nodes, "pre", scenario.get("id", ""), topo, run_meta_eff, sink_url, run_id
        ) or []
        ctx["cluster"] = cluster
        try:
            ctx["faults_mode"] = request.config.getoption("--faults")
        except Exception:
            ctx["faults_mode"] = ctx.get("faults_mode", "on")
        # removed unused log_allow plumbing
        # propagate seed if provided
        try:
            ctx["seed"] = int(request.config.getoption("--seed") or 0)
        except Exception:
            ctx["seed"] = 0
        sampler = None
        if sink_url:
            # interval: prefer env override, else scenario option, else default 5
            try:
                env_int = os.environ.get("KEEPER_MONITOR_INTERVAL_S")
                interval = int(env_int) if env_int not in (None, "") else None
            except Exception:
                interval = None
            if interval is None:
                interval = (
                    int(opts.get("monitor_interval_s", 5)) if isinstance(opts, dict) else 5
                )
            # prom allowlist: env comma-separated overrides scenario list
            prom_allow = None
            try:
                pa = os.environ.get("KEEPER_PROM_ALLOW_PREFIXES", "").strip()
                if pa:
                    prom_allow = [p for p in (pa.split(",") or []) if p]
            except Exception:
                prom_allow = None
            if prom_allow is None:
                prom_allow = (
                    opts.get("prom_allow_prefixes") if isinstance(opts, dict) else None
                )
            # prom name allowlist (exact metric names)
            prom_name_allow = None
            try:
                pna = os.environ.get("KEEPER_PROM_NAME_ALLOWLIST", "").strip()
                if pna:
                    prom_name_allow = [p for p in (pna.split(",") or []) if p]
            except Exception:
                prom_name_allow = None
            if prom_name_allow is None and isinstance(opts, dict):
                try:
                    x = opts.get("prom_name_allowlist")
                    if isinstance(x, list) and x:
                        prom_name_allow = [str(y) for y in x if str(y)]
                except Exception:
                    prom_name_allow = None
            # prom exclude label keys
            prom_excl_labels = None
            try:
                pel = os.environ.get("KEEPER_PROM_EXCLUDE_LABEL_KEYS", "").strip()
                if pel:
                    prom_excl_labels = [p for p in (pel.split(",") or []) if p]
            except Exception:
                prom_excl_labels = None
            if prom_excl_labels is None and isinstance(opts, dict):
                try:
                    x = opts.get("prom_exclude_label_keys")
                    if isinstance(x, list) and x:
                        prom_excl_labels = [str(y) for y in x if str(y)]
                except Exception:
                    prom_excl_labels = None
            # prom sampling frequency: every N snapshots
            prom_every_n = None
            try:
                pen = os.environ.get("KEEPER_PROM_EVERY_N")
                prom_every_n = int(pen) if pen not in (None, "") else None
            except Exception:
                prom_every_n = None
            if prom_every_n is None and isinstance(opts, dict):
                try:
                    prom_every_n = int(opts.get("prom_every_n"))
                except Exception:
                    prom_every_n = None
            sampler = MetricsSampler(
                nodes,
                run_meta_eff,
                scenario.get("id", ""),
                topo,
                sink_url=sink_url,
                interval_s=interval,
                stage_prefix="run",
                run_id=run_id,
                prom_allow_prefixes=prom_allow,
                prom_name_allowlist=prom_name_allow,
                prom_exclude_label_keys=prom_excl_labels,
                prom_every_n=prom_every_n if prom_every_n is not None else 3,
                ctx=ctx,
            )
            sampler.start()
        for step in scenario.get("pre", []):
            _apply_step(step, nodes, leader, ctx)
        kb = None
        bench_injected = False
        if (
            faults_mode != "random"
            and "workload" in scenario
            and not parse_bool(os.environ.get("KEEPER_DISABLE_WORKLOAD"))
        ):
            wl = scenario["workload"]
            # Environment overrides for workload paths and bench clients
            try:
                rp = os.environ.get("KEEPER_REPLAY_PATH", "").strip()
                if rp:
                    wl = dict(wl or {})
                    wl["replay"] = rp
            except Exception:
                pass
            try:
                wc = os.environ.get("KEEPER_WORKLOAD_CONFIG", "").strip()
                if wc:
                    wl = dict(wl or {})
                    wl["config"] = wc
            except Exception:
                pass
            clients_env = None
            try:
                ce = os.environ.get("KEEPER_BENCH_CLIENTS")
                clients_env = int(ce) if ce not in (None, "") else None
            except Exception:
                clients_env = None
            secure = False
            # Prefer CLI/environment duration over YAML to support ad-hoc longer runs
            try:
                cli_dur = int(request.config.getoption("--duration"))
            except Exception:
                cli_dur = None
            try:
                env_dur = (
                    int(os.environ.get("KEEPER_DURATION"))
                    if os.environ.get("KEEPER_DURATION")
                    else None
                )
            except Exception:
                env_dur = None
            eff_dur = cli_dur or env_dur or wl.get("duration") or 120
            kb = KeeperBench(
                nodes[0],
                servers_arg(nodes),
                cfg_path=str(WORKDIR / wl.get("config")) if wl.get("config") else None,
                duration_s=eff_dur,
                replay_path=wl.get("replay"),
                secure=secure,
                clients=clients_env,
            )
            ctx["workload"] = wl
            ctx["bench_node"] = nodes[0]
            ctx["bench_secure"] = secure
            ctx["bench_servers"] = servers_arg(nodes)
        forced = parse_bool(os.environ.get("KEEPER_FORCE_BENCH"))
        if forced:
            try:
                cli_dur = int(request.config.getoption("--duration"))
            except Exception:
                cli_dur = None
            try:
                env_dur = (
                    int(os.environ.get("KEEPER_DURATION"))
                    if os.environ.get("KEEPER_DURATION")
                    else None
                )
            except Exception:
                env_dur = None
            eff_dur_fb = cli_dur or env_dur or 120
            try:
                ce = os.environ.get("KEEPER_BENCH_CLIENTS")
                clients_env_fb = int(ce) if ce not in (None, "") else None
            except Exception:
                clients_env_fb = None
            try:
                wc_env = os.environ.get("KEEPER_WORKLOAD_CONFIG", "").strip()
            except Exception:
                wc_env = ""
            cfg_fb = str(WORKDIR / wc_env) if wc_env else str(WORKDIR / "workloads/prod_mix.yaml")
            rb = {
                "kind": "run_bench",
                "duration_s": eff_dur_fb,
                "config": cfg_fb,
            }
            def _has_rb(actions):
                try:
                    for a in (actions or []):
                        k = str(a.get("kind", "")).strip().lower()
                        if k == "run_bench":
                            return True
                        if k == "parallel":
                            for s in (a.get("steps") or []):
                                if str(s.get("kind", "")).strip().lower() == "run_bench":
                                    return True
                    return False
                except Exception:
                    return False
            if _has_rb(fs_effective):
                pass
            elif fs_effective:
                fs_effective = [{"kind": "parallel", "steps": [rb] + fs_effective}]
            else:
                fs_effective = [rb]
            bench_injected = True
            if kb:
                kb = None
            try:
                gs = scenario.get("gates") or []
                has_err = any((g.get("type") == "error_rate_le") for g in gs)
                has_p99 = any((g.get("type") == "p99_le") for g in gs)
                if not has_err:
                    gs = gs + [{"type": "error_rate_le", "max_ratio": float(DEFAULT_ERROR_RATE)}]
                if not has_p99:
                    gs = gs + [{"type": "p99_le", "max_ms": int(DEFAULT_P99_MS)}]
                scenario["gates"] = gs
            except Exception:
                pass
        for action in fs_effective:
            _apply_step(action, nodes, leader, ctx)
        if kb and not bench_injected:
            summary = kb.run()
        elif ctx.get("bench_summary"):
            summary = ctx.get("bench_summary") or {}
        # Sink keeper-bench summary to CIDB as scalar metrics tagged to this run
        try:
            s = summary or {}
            if s:
                # Compute rps if possible
                try:
                    dur = float(s.get("duration_s") or 0)
                except Exception:
                    dur = 0.0
                try:
                    ops = float(s.get("ops") or 0)
                except Exception:
                    ops = 0.0
                rps = (ops / dur) if (dur and dur > 0) else 0.0
                has_lat = bool(s.get("has_latency"))
                err = float(s.get("errors") or 0)
                names_vals = {
                    "ops": ops,
                    "errors": err,
                    "reads": float(s.get("reads") or 0),
                    "writes": float(s.get("writes") or 0),
                    "read_ratio": float(s.get("read_ratio") or 0),
                    "write_ratio": float(s.get("write_ratio") or 0),
                    "duration_s": dur,
                    "rps": float(rps),
                    "error_rate": (err / ops) if (ops and ops > 0) else 0.0,
                    "bench_has_latency": 1.0 if has_lat else 0.0,
                }
                if has_lat:
                    names_vals.update({
                        "p50_ms": float(s.get("p50_ms") or 0),
                        "p95_ms": float(s.get("p95_ms") or 0),
                        "p99_ms": float(s.get("p99_ms") or 0),
                    })
                rows = []
                for nm, val in names_vals.items():
                    try:
                        valf = float(val)
                    except Exception:
                        continue
                    rows.append(
                        {
                            "ts": _ts_ms_str(),
                            "run_id": run_id,
                            "commit_sha": run_meta_eff.get("commit_sha", "local"),
                            "backend": run_meta_eff.get("backend", "default"),
                            "scenario": scenario.get("id", ""),
                            "topology": topo,
                            "node": "bench",
                            "stage": "summary",
                            "source": "bench",
                            "name": nm,
                            "value": valf,
                            "labels_json": "{}",
                        }
                    )
                if rows:
                    rows.append(
                        {
                            "ts": _ts_ms_str(),
                            "run_id": run_id,
                            "commit_sha": run_meta_eff.get("commit_sha", "local"),
                            "backend": run_meta_eff.get("backend", "default"),
                            "scenario": scenario.get("id", ""),
                            "topology": topo,
                            "node": "bench",
                            "stage": "summary",
                            "source": "bench",
                            "name": "bench_ran",
                            "value": 1.0,
                            "labels_json": "{}",
                        }
                    )
                    bench_ran_flag = True
                    try:
                        print("[keeper][push-metrics] begin")
                        for rr in rows:
                            try:
                                print(json.dumps(rr, ensure_ascii=False))
                            except Exception:
                                pass
                        print("[keeper][push-metrics] end")
                    except Exception:
                        pass
                    if sink_url:
                        sink_clickhouse(sink_url, "keeper_metrics_ts", rows)
        except Exception:
            pass
        for gate in scenario.get("gates", []):
            _apply_gate(gate, nodes, leader, ctx, summary)
        post_rows = _snapshot_and_sink(
            nodes, "post", scenario.get("id", ""), topo, run_meta_eff, sink_url, run_id
        ) or []
        try:
            def _pick(rows, source, name):
                res = {}
                for r in rows:
                    try:
                        if r.get("source") == source and r.get("name") == name:
                            res[(r.get("node") or "")] = float(r.get("value") or 0)
                    except Exception:
                        continue
                return res
            pre_zn = _pick(pre_rows, "mntr", "zk_znode_count")
            pre_ep = _pick(pre_rows, "mntr", "zk_ephemerals_count")
            pre_wc = _pick(pre_rows, "mntr", "zk_watch_count")
            pre_db = _pick(pre_rows, "dirs", "bytes")
            post_zn = _pick(post_rows, "mntr", "zk_znode_count")
            post_ep = _pick(post_rows, "mntr", "zk_ephemerals_count")
            post_wc = _pick(post_rows, "mntr", "zk_watch_count")
            post_db = _pick(post_rows, "dirs", "bytes")
            nodes_names = sorted({*pre_zn.keys(), *pre_ep.keys(), *pre_wc.keys(), *pre_db.keys(), *post_zn.keys(), *post_ep.keys(), *post_wc.keys(), *post_db.keys()})
            derived = []
            tot = {"znode": 0.0, "ephem": 0.0, "watch": 0.0, "dirs_bytes": 0.0}
            for nn in nodes_names:
                dz = (post_zn.get(nn, 0.0) - pre_zn.get(nn, 0.0))
                de = (post_ep.get(nn, 0.0) - pre_ep.get(nn, 0.0))
                dw = (post_wc.get(nn, 0.0) - pre_wc.get(nn, 0.0))
                db = (post_db.get(nn, 0.0) - pre_db.get(nn, 0.0))
                for key, val in (("delta_znode_count", dz), ("delta_ephemerals_count", de), ("delta_watch_count", dw), ("delta_dirs_bytes", db)):
                    derived.append({
                        "ts": _ts_ms_str(),
                        "run_id": run_id,
                        "commit_sha": run_meta_eff.get("commit_sha", "local"),
                        "backend": run_meta_eff.get("backend", "default"),
                        "scenario": scenario.get("id", ""),
                        "topology": topo,
                        "node": nn,
                        "stage": "summary",
                        "source": "derived",
                        "name": key,
                        "value": float(val),
                        "labels_json": "{}",
                    })
                tot["znode"] += dz
                tot["ephem"] += de
                tot["watch"] += dw
                tot["dirs_bytes"] += db
            for key, val in (("total_delta_znode_count", tot["znode"]), ("total_delta_ephemerals_count", tot["ephem"]), ("total_delta_watch_count", tot["watch"]), ("total_delta_dirs_bytes", tot["dirs_bytes"])):
                derived.append({
                    "ts": _ts_ms_str(),
                    "run_id": run_id,
                    "commit_sha": run_meta_eff.get("commit_sha", "local"),
                    "backend": run_meta_eff.get("backend", "default"),
                    "scenario": scenario.get("id", ""),
                    "topology": topo,
                    "node": "all",
                    "stage": "summary",
                    "source": "derived",
                    "name": key,
                    "value": float(val),
                    "labels_json": "{}",
                })
            if sink_url and derived:
                sink_clickhouse(sink_url, "keeper_metrics_ts", derived)
        except Exception:
            pass
        try:
            bench_expected = (faults_mode != "random" and "workload" in scenario and not parse_bool(os.environ.get("KEEPER_DISABLE_WORKLOAD"))) or parse_bool(os.environ.get("KEEPER_FORCE_BENCH"))
            if bench_expected and not bench_ran_flag:
                print(f"[keeper][bench-check] bench did not run for scenario={scenario.get('id','')} backend={backend}")
        except Exception:
            pass
        if sink_url and not bench_ran_flag:
            try:
                row = {
                    "ts": _ts_ms_str(),
                    "run_id": run_id,
                    "commit_sha": run_meta_eff.get("commit_sha", "local"),
                    "backend": run_meta_eff.get("backend", "default"),
                    "scenario": scenario.get("id", ""),
                    "topology": topo,
                    "node": "bench",
                    "stage": "summary",
                    "source": "bench",
                    "name": "bench_ran",
                    "value": 0.0,
                    "labels_json": "{}",
                }
                sink_clickhouse(sink_url, "keeper_metrics_ts", [row])
            except Exception:
                pass
        # per-test checks insertion is handled by praktika post-run
    except Exception:
        # Emit minimal reproducible scenario to a file for debugging
        try:
            repro_path = f"/tmp/keeper_repro_{run_id}.yaml"
            with open(repro_path, "w", encoding="utf-8") as f:
                yaml.safe_dump(scenario, f, sort_keys=False)
            print(f"[keeper] reproducible scenario written: {repro_path}")
        except Exception:
            pass
        # Attempt to snapshot fail state as well
        try:
            sink_url = "ci" if (has_ci_sink() or os.environ.get("KEEPER_METRICS_FILE")) else ""
            _snapshot_and_sink(
                nodes,
                "fail",
                scenario.get("id", ""),
                topo,
                run_meta_eff,
                sink_url,
                run_id,
            )
        except Exception:
            pass
        # per-test checks insertion is handled by praktika post-run

        try:
            setattr(request.node, "keeper_failed", True)
        except Exception:
            pass
        raise
    finally:
        try:
            if "sampler" in locals() and sampler:
                sampler.stop()
                sampler.flush()
        except Exception:
            pass
        try:
            _print_local_metrics(nodes, run_id, scenario.get("id", ""), topo, run_meta_eff, ctx, summary)
        except Exception:
            pass
        # Close any pooled KeeperClient resources
        try:
            if ctx.get("_kc_pool"):
                ctx["_kc_pool"].close()
        except Exception:
            pass
        try:
            clients = ctx.get("_kc_clients") or {}
            for c in clients.values():
                try:
                    c.stop()
                except Exception:
                    pass
        except Exception:
            pass
        try:
            for w in (ctx.get("_watch_flood_watches") or []):
                try:
                    if hasattr(w, "cancel"):
                        w.cancel()
                except Exception:
                    pass
                try:
                    if hasattr(w, "stop"):
                        w.stop()
                except Exception:
                    pass
                try:
                    s = getattr(w, "_stopped", None)
                    if s:
                        s.set()
                except Exception:
                    pass
                try:
                    thr = getattr(w, "_thread", None)
                    if thr:
                        thr.join(timeout=0.5)
                except Exception:
                    pass
            time.sleep(0.1)
        except Exception:
            pass
        try:
            for zk in (ctx.get("_watch_flood_clients") or []):
                try:
                    zk.stop(); zk.close()
                except Exception:
                    pass
        except Exception:
            pass
        try:
            for zk in (ctx.get("_ephem_clients") or []):
                try:
                    zk.stop(); zk.close()
                except Exception:
                    pass
        except Exception:
            pass
        # Keep containers on fail (for debugging) if requested
        try:
            keep = bool(request.config.getoption("--keep-containers-on-fail"))
            failed = getattr(request.node, "keeper_failed", False)
            if keep and failed:
                return
        except Exception:
            pass
        cluster.shutdown()
        # Optional cleanup of generated artifacts if run succeeded
        try:
            failed = getattr(request.node, "keeper_failed", False)
            clean = parse_bool(os.environ.get("KEEPER_CLEAN_ARTIFACTS"))
            if clean and not failed:
                try:
                    inst_dir = pathlib.Path(getattr(cluster, "instances_dir", ""))
                    if inst_dir and inst_dir.exists():
                        shutil.rmtree(inst_dir, ignore_errors=True)
                except Exception:
                    pass
                try:
                    base_dir = pathlib.Path(getattr(cluster, "base_dir", ""))
                    conf_dir = base_dir / "configs"
                    if conf_dir.exists():
                        shutil.rmtree(conf_dir, ignore_errors=True)
                except Exception:
                    pass
        except Exception:
            pass
