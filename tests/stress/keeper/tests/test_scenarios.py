import copy
import json
import os
import pathlib
import shutil
import sys
import threading
import time
import uuid
import random
import yaml

from keeper.framework.fuzz import _fault_candidates
from keeper.faults import apply_step
from keeper.faults.runner import FaultRunner
from keeper.framework.core.preflight import ensure_environment
from keeper.workloads.keeper_bench import KeeperBench
from keeper.framework.core.settings import (
    CLIENT_PORT,
    DEFAULT_ERROR_RATE,
    DEFAULT_P99_MS,
    DEFAULT_WORKLOAD_CONFIG,
    keeper_node_names
)
from keeper.framework.core.util import sh, ts_ms, parse_bool, env_bool
from keeper.framework.fuzz import generate_random_faults
from keeper.framework.io.probes import (
    count_leaders,
    dirs,
    is_leader,
    lgif,
    mntr,
    prom_metrics,
    srvr_kv,
    _get_current_leader
)
from keeper.framework.io.prom_parse import parse_prometheus_text
from keeper.framework.io.sink import sink_clickhouse
from keeper.framework.metrics.sampler import MetricsSampler, _make_metric_row, compute_prom_config
from keeper.gates.base import apply_gate, single_leader
from keeper.workloads.adapter import servers_arg

WORKDIR = pathlib.Path(__file__).parents[1]

HARD_DEFAULT_TIMEOUT = 120


def _tail_file(path, max_lines=200):
    try:
        p = pathlib.Path(str(path))
        if not p.exists() or not p.is_file():
            return ""
        lines = p.read_text(errors="replace").splitlines()
        tail = lines[-int(max_lines) :] if max_lines and max_lines > 0 else lines
        return "\n".join(tail)
    except Exception:
        return ""


def _print_gate_failure_diagnostics(cluster, nodes, gate_type, exc):
    try:
        print(f"[keeper][gate-failed] type={gate_type} error={repr(exc)}")
    except Exception:
        pass
    try:
        print(f"[keeper][gate-failed] leaders={count_leaders(nodes)}")
    except Exception:
        pass
    for n in nodes or []:
        try:
            m = mntr(n)
        except Exception:
            m = {}
        try:
            s = srvr_kv(n)
        except Exception:
            s = {}
        try:
            print(
                f"[keeper][gate-failed] node={getattr(n,'name','')} mntr={m or {}} srvr={s or {}}"
            )
        except Exception:
            pass
    try:
        dl = getattr(cluster, "docker_logs_path", "")
        if dl:
            print(f"[keeper][gate-failed] docker_log={dl}")
            txt = _tail_file(dl, max_lines=200)
            if txt:
                print("[keeper][gate-failed] docker_log_tail begin")
                print(txt)
                print("[keeper][gate-failed] docker_log_tail end")
    except Exception:
        pass




# ─────────────────────────────────────────────────────────────────────────────
# Helper: CLI/env option extraction with fallback
# ─────────────────────────────────────────────────────────────────────────────
def _get_option(request, cli_name, env_name, default=None, type_fn=str):
    """Get option from CLI args first, then env var, then default."""
    try:
        val = request.config.getoption(cli_name)
        if val is not None:
            return type_fn(val)
    except Exception as e:
        print(f"[keeper] error getting option {cli_name} from CLI args: {repr(e)}")
    env_val = os.environ.get(env_name, "").strip()
    if env_val:
        try:
            return type_fn(env_val)
        except (ValueError, TypeError) as e:
            print(f"[keeper] error getting option {env_name} from environment: {repr(e)}")
    return default


def _get_int_option(request, cli_name, env_name, default=0):
    return _get_option(request, cli_name, env_name, default, int)


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




def ensure_default_gates(scenario):
    """Add lenient default gates only if scenario has no gates defined.
    
    Defaults are intentionally lenient (10% error rate, 10s p99) and only serve
    as a safety net for scenarios that forget to define gates. Most scenarios
    should define their own stricter thresholds.
    """
    if not isinstance(scenario, dict):
        return

    gates = list(scenario.get("gates") or [])
    # Only add defaults if scenario has no gates at all
    if not gates:
        gates = [
            {"type": "ops_ge", "min_ops": 1.0},  # Sanity: bench ran
            {"type": "error_rate_le", "max_ratio": float(DEFAULT_ERROR_RATE)},  # Lenient: 10%
            {"type": "p99_le", "max_ms": int(DEFAULT_P99_MS)},  # Lenient: 10s
        ]
        scenario["gates"] = gates


def _generate_default_fault_pool(scenario, seed_val):
    topology = scenario.get("topology")
    try:
        rnd = random.Random(seed_val)
        scenario_faults = scenario.get("faults", [])
        scenario_dur = int(scenario.get("duration") or HARD_DEFAULT_TIMEOUT)
        
        if scenario_faults:
            # Use all fault kinds from scenario (generate one of each kind)
            fault_kinds = list({f.get("kind") for f in scenario_faults if isinstance(f, dict) and f.get("kind")})
            steps = len(fault_kinds)
        else:
            # Use all registered faults, generate random number between 1 and len(all_faults)
            fault_kinds = None
            all_faults = _fault_candidates()
            steps = rnd.randint(1, max(1, len(all_faults)))

        # Calculate fault durations ensuring sum <= scenario duration
        # Distribute scenario duration across faults, with min 5s per fault
        min_per_fault = 30
        max_total = scenario_dur
        if steps * min_per_fault > max_total:
            # Not enough time for all faults, reduce steps
            steps = max(1, max_total // min_per_fault)
        # Ensure worst case (all faults at dur_max) doesn't exceed total duration
        # dur_max per fault = max_total / steps (ensures steps * dur_max <= max_total)
        avg_dur = max(min_per_fault, max_total // steps)
        dur_min = min_per_fault
        dur_max = min(avg_dur, max_total // steps)  # Worst case: all dur_max, sum = steps * dur_max <= max_total
        
        rnd_faults = generate_random_faults(
            steps,
            rnd,
            dur_min,
            dur_max,
            topology=topology,
            fault_kinds=fault_kinds,
        )
        return rnd_faults
    except Exception:
        raise Exception(f"Error generating random faults for scenario {scenario.get('id')}")


def _build_bench_step(scenario, nodes, ctx, replay_path=""):
    """Build workload config from scenario, --replay / KEEPER_REPLAY_PATH, or env.
    
    Priority: --replay > KEEPER_REPLAY_PATH > KEEPER_WORKLOAD_CONFIG / scenario.
    Scenario workload can only define one of 'config' or 'replay', not both.
    Always sets ctx["workload"] with absolute paths (uses default if nothing is defined).
    """
    wc_env = os.environ.get("KEEPER_WORKLOAD_CONFIG", "").strip()
    rp = (replay_path or "").strip()

    # Determine workload config: env / option override scenario
    wl = {}
    if wc_env:
        wl["config"] = wc_env

    if rp:
        wl["replay"] = rp
    
    if "workload" in scenario:
        scenario_wl = scenario["workload"]
        # Validate: scenario can't have both config and replay
        if "replay" in (scenario_wl or wl) and not ("config" in scenario_wl or "config" in wl):
            raise ValueError("Scenario workload cannot define 'replay' without 'config'")
        # Extract config or replay from scenario
        wl.setdefault("config", scenario_wl.get("config"))
        wl.setdefault("replay", scenario_wl.get("replay"))
    
    # If no workload defined anywhere, use default
    if not wl:
        wl = {"config": DEFAULT_WORKLOAD_CONFIG}
    
    # Convert relative paths to absolute. Paths starting with "tests/" are
    # relative to the project root (cwd); others (e.g. "workloads/...") to WORKDIR.
    for key in ("config", "replay"):
        path = wl.get(key)
        if path and not os.path.isabs(path):
            base = pathlib.Path.cwd() if path.startswith("tests/") else WORKDIR
            wl[key] = str((base / path).resolve())
    
    # Set context for bench execution
    ctx["workload"] = wl
    ctx["bench_node"] = nodes[0]
    ctx["bench_secure"] = False
    ctx["bench_servers"] = servers_arg(nodes)


def _ensure_clickhouse_binary_for_bench():
    env_bin = os.environ.get("CLICKHOUSE_BINARY", "").strip()
    if not env_bin or not os.path.isfile(env_bin) or not os.access(env_bin, os.X_OK):
        raise AssertionError(
            f"keeper-bench must run on host: CLICKHOUSE_BINARY must be an executable file (got: {env_bin or '<unset>'})"
        )


def _emit_bench_summary(summary, run_id, run_meta_eff, scenario_id, topo, sink_url):
    if not summary:
        return False
    s = summary

    # Use summary as-is; emit all scalar values (keeper-bench _parse_output_json has everything)
    names_vals = {}
    for k, v in s.items():
        if isinstance(v, (dict, list)):
            continue
        names_vals[k] = _safe_float(v)

    # Override or add only the few computed fields
    dur = names_vals.get("duration_s", 0)
    ops = names_vals.get("ops", 0)
    err = names_vals.get("errors", 0)
    names_vals["rps"] = ops / dur if dur > 0 else 0.0
    names_vals["error_rate"] = err / ops if ops > 0 else 0.0
    failed = 1.0 if (s.get("failed") or s.get("bench_failed")) else 0.0
    names_vals["bench_failed"] = failed

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
            0.0 if failed else 1.0,
        )
    )
    print("[keeper][push-bench-metrics] begin")
    for rr in rows:
        print(json.dumps(rr, ensure_ascii=False))
    print("[keeper][push-bench-metrics] end")
    if sink_url:
        sink_clickhouse(rows)
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
        sink_clickhouse(derived)
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
            sink_clickhouse(bench_totals)
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
        sink_clickhouse(rows)
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
                    parsed = parse_prometheus_text(txt)
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
        for name in keeper_node_names(int(topo)):
            p = conf_root / f"keeper_config_{name}.xml"
            if p.exists():
                try:
                    print(f"==== {name} config ====\n" + p.read_text())
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


def _set_privileged_env(scenario):
    if any(f.get("kind") in ("dm_delay", "dm_error") for f in scenario.get("faults", [])):
        os.environ.setdefault("KEEPER_PRIVILEGED", "1")

def _setup_metrics_context(faults_enabled, seed_val, cluster):
    """Setup metrics collection and context."""
    mf = (os.environ.get("KEEPER_METRICS_FILE") or "").strip()
    ctx = {"cluster": cluster, "faults_mode": faults_enabled, "seed": seed_val, "metrics_file": os.path.abspath(mf)}
    return ctx




def _run_gates(scenario, nodes, leader, ctx, cluster, sink_url, run_id, run_meta_eff, scenario_id, topo):
    """Run all validation gates."""
    for gate in scenario.get("gates", []) or []:
        gtype = (gate.get("type") or "") if isinstance(gate, dict) else ""
        t0 = time.time()
        try:
            apply_gate(gate, nodes, leader, ctx, ctx.get("bench_summary") or {})
        except Exception as e:
            _print_gate_failure_diagnostics(cluster, nodes, gtype, e)
            if sink_url:
                try:
                    rows = _gate_event_rows(
                        run_id, run_meta_eff, scenario_id, topo, gtype, False,
                        int((time.time() - t0) * 1000), error=repr(e),
                    )
                    sink_clickhouse(rows)
                except Exception:
                    pass
            raise
        if sink_url:
            try:
                rows = _gate_event_rows(
                    run_id, run_meta_eff, scenario_id, topo, gtype, True,
                    int((time.time() - t0) * 1000),
                )
                sink_clickhouse(rows)
            except Exception:
                pass


def _print_bench_debug_info(ctx):
    """Print keeper-bench debug information."""
    bcfg = (ctx or {}).get("bench_config_yaml")
    if bcfg:
        print("==== keeper-bench config ====\n" + str(bcfg))
    
    for key, prefix in [("bench_config_paths", "[keeper][bench-config-paths]"),
                        ("bench_output_paths", "[keeper][bench-output-paths]"),
                        ("bench_stdout_paths", "[keeper][bench-stdout-paths]")]:
        paths = (ctx or {}).get(key) or []
        if paths:
            print(prefix + " " + ", ".join([str(p) for p in paths if p]))
    
    sps = (ctx or {}).get("bench_stdout_paths") or []
    if sps:
        def _print_stdout_contents():
            for sp in sps:
                if not sp:
                    continue
                try:
                    p = pathlib.Path(str(sp))
                    if p.exists() and p.is_file():
                        content = p.read_text(encoding="utf-8", errors="replace")
                        print(f"[keeper][bench-stdout-content] path={sp}")
                        sys.stdout.flush()
                        print("[keeper][bench-stdout-content] begin")
                        sys.stdout.flush()
                        print(content)
                        sys.stdout.flush()
                        print("[keeper][bench-stdout-content] end")
                        sys.stdout.flush()
                    else:
                        print(f"[keeper][bench-stdout-content] path={sp} file_not_found")
                        sys.stdout.flush()
                except Exception as e:
                    print(f"[keeper][bench-stdout-content] path={sp} error={repr(e)}")
                    sys.stdout.flush()
        _print_stdout_contents()


def test_scenario(scenario, cluster_factory, request, run_meta):
    _ensure_clickhouse_binary_for_bench()

    topo = scenario.get("topology")
    backend = scenario.get("backend")
    opts = scenario.get("opts", {})
    fs_effective = scenario.get("faults", [])
    
    run_meta_eff = run_meta
    run_meta_eff["backend"] = backend

    faults_enabled = request.config.getoption("--faults")
    seed_val = request.config.getoption("--seed")
    print(f"[keeper] seed={int(seed_val)}")

    if not faults_enabled:
        fs_effective = []
    elif not fs_effective:
        fs_effective = _generate_default_fault_pool(scenario, seed_val)

    _set_privileged_env(scenario)
    
    run_id = f"{scenario.get('id','')}-{run_meta.get('commit_sha','local')}-{uuid.uuid4().hex[:8]}"
    cname = run_id.replace("/", "_").replace(" ", "_")
    os.environ["KEEPER_CLUSTER_NAME"] = cname
    cluster, nodes = cluster_factory(cname, topo, backend, opts)

    scenario_id = scenario.get("id")
    sink_url = "ci"
    ctx = _setup_metrics_context(faults_enabled, seed_val, cluster)
    
    sampler = None
    leader = None
    pre_rows = []
    post_rows = []
    fail_rows = []
    bench_ran_flag = False
    
    try:
        # Setup environment and pre-flight checks
        scenario_for_env = copy.deepcopy(scenario)
        scenario_for_env["faults"] = [
            {"kind": "background_schedule", "duration_s": scenario.get("duration"), "steps": fs_effective}
        ]
        ensure_environment(nodes, scenario_for_env)
        single_leader(nodes)
        leader = _get_current_leader(nodes)
        
        # Setup metrics collection
        cfg = compute_prom_config()
        sampler = MetricsSampler(
            nodes, run_meta_eff, scenario_id, topo, run_id,
            interval_s=cfg["interval_s"], prom_every_n=cfg.get("prom_every_n", 3), ctx=ctx,
        )
        
        # Pre-execution snapshot (captures baseline for derived metrics)
        pre_rows = sampler.snapshot_stage("pre", nodes)
        
        # Run pre-steps (setup/initialization before starting continuous sampling)
        for step in scenario.get("pre", []) or []:
            apply_step(step, nodes, leader, ctx)
        
        # Start continuous metrics sampling (time-series data during test execution)
        # 10s interval for lightweight metrics (mntr, dirs, srvr)
        # 30s interval for Prometheus metrics (prom_metrics)
        sampler.start()
        
        # Build workload config (sets ctx["workload"] for gates)
        replay_path = (request.config.getoption("--replay", "") or "").strip()
        _build_bench_step(scenario, nodes, ctx, replay_path=replay_path)

        ensure_default_gates(scenario)
        
        # Create bench and fault runners
        wl = ctx.get("workload", {})
        bench_runner = KeeperBench(
            nodes=nodes,
            ctx=ctx,
            cfg_path=wl.get("config"),
            duration_s=scenario.get("duration"),
            replay_path=wl.get("replay"),
        )
        
        fault_runner = None
        if fs_effective:
            fault_runner = FaultRunner(
                nodes=nodes,
                leader=leader,
                ctx=ctx,
                faults=fs_effective,
                duration_s=scenario.get("duration"),
                seed=seed_val,
            )
        
        # Start both runners (both target scenario duration)
        if fault_runner:
            fault_runner.start()
        bench_runner.start()
        
        # Wait for both to complete (both should finish around scenario duration)
        # Bench may take slightly longer (duration_s + 60s hard cap) due to cleanup
        # Faults complete at exactly duration_s
        bench_runner.stop()
        if fault_runner:
            fault_runner.stop()
        
        # Run post-steps
        leader = _get_current_leader(nodes)
        for step in scenario.get("post", []) or []:
            apply_step(step, nodes, leader, ctx)
        
        # Emit bench summary and run gates
        print("[keeper][emit_bench_summary] begin")
        summary = ctx.get("bench_summary") or {}
        if summary:
            bench_ran_flag = _emit_bench_summary(summary, run_id, run_meta_eff, scenario_id, topo, sink_url)
        
        leader = _get_current_leader(nodes)
        _run_gates(scenario, nodes, leader, ctx, cluster, sink_url, run_id, run_meta_eff, scenario_id, topo)
        
        # Post-execution snapshot (captures final state for derived metrics)
        sampler.stop()
        post_rows = sampler.snapshot_stage("post", nodes) or []
        _emit_derived_metrics(
            pre_rows,
            post_rows,
            run_id,
            run_meta_eff,
            scenario_id,
            topo,
            sink_url,
            bench_ran_flag,
        )
        # Validate bench ran successfully (bench always runs, even with default workload)
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
            sink_clickhouse([row])
        # per-test checks insertion is handled by praktika post-run
    except Exception:
        # Emit minimal reproducible scenario to a file for debugging
        def _write_repro():
            repro_path = f"/tmp/keeper_repro_{run_id}.yaml"
            with open(repro_path, "w", encoding="utf-8") as f:
                yaml.safe_dump(scenario, f, sort_keys=False)
            print(f"[keeper] reproducible scenario written: {repro_path}")

        _write_repro()
        # Attempt to snapshot fail state as well
        try:
            if sampler:
                sampler.stop()
                fail_nodes = nodes[:1]  # Avoid leader detection on fail path
                fail_rows = sampler.snapshot_stage("fail", fail_nodes) or []
            else:
                # Sampler not created yet (exception before line 1025), can't snapshot
                fail_rows = []
        except Exception:
            fail_rows = []

        # Best-effort derived metrics even on failure (use fail snapshot as a fallback post).
        _emit_derived_metrics(
            pre_rows,
            (fail_rows or post_rows),
            run_id,
            run_meta_eff,
            scenario_id,
            topo,
            sink_url,
            bench_ran_flag,
        )
        # per-test checks insertion is handled by praktika post-run

        setattr(request.node, "keeper_failed", True)
        raise
    finally:
        # Stop metrics sampler and flush remaining metrics
        if sampler:
            sampler.stop()
            sampler.flush()

        # If we ran keeper-bench but failed before emitting bench metrics (e.g. another fault step failed),
        # emit it best-effort from the captured context.
        if sink_url and not bench_ran_flag:
            try:
                bs = (ctx or {}).get("bench_summary") or {}
                if isinstance(bs, dict) and bs:
                    bench_ran_flag = bool(
                        _emit_bench_summary(bs, run_id, run_meta_eff, scenario_id, topo, sink_url)
                    )
            except Exception:
                pass

        # Print diagnostics and metrics
        _print_local_metrics(nodes, run_id, scenario_id, topo, run_meta_eff, ctx, (ctx or {}).get("bench_summary") or {})
        _print_bench_debug_info(ctx)
        
        # Print fault events
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
        
        _emit_fault_events_metrics(ctx, run_id, run_meta_eff, scenario_id, topo, sink_url)
        _print_keeper_configs(cluster, topo, os.environ.get("KEEPER_CLUSTER_NAME", ""))
        _print_manual_znode_counts(nodes)
        
        try:
            mf = (ctx or {}).get("metrics_file")
            if mf:
                print(f"[keeper][metrics] jsonl = {mf}")
        except Exception:
            pass
        # Cleanup context resources (pools, clients, watches)
        _cleanup_ctx_resources(ctx)
        
        # Keep containers on fail if requested
        should_keep_containers = False
        try:
            should_keep_containers = request.config.getoption("--keep-containers-on-fail") and getattr(
                request.node, "keeper_failed", False
            )
        except Exception:
            pass
        
        if not should_keep_containers:
            cluster.shutdown()
            # Optional artifact cleanup on success
            if env_bool("KEEPER_CLEAN_ARTIFACTS", True):
                try:
                    inst_dir = pathlib.Path(getattr(cluster, "instances_dir", ""))
                    if inst_dir.exists():
                        shutil.rmtree(inst_dir, ignore_errors=True)
                    conf_dir = pathlib.Path(getattr(cluster, "base_dir", "")) / "configs"
                    if conf_dir.exists():
                        shutil.rmtree(conf_dir, ignore_errors=True)
                except Exception:
                    pass
