import json
import os
import re
import shlex
from pathlib import Path

import yaml
from keeper.framework.core.settings import (
    CLIENT_PORT,
    DEFAULT_ERROR_RATE,
    DEFAULT_P99_MS,
    parse_bool,
)
from keeper.framework.core.util import (
    env_float,
    env_int,
    host_has_bin,
    host_sh,
    sh,
)
from keeper.framework.io.probes import mntr


def _parse_hosts(servers):
    return [p.strip() for p in (servers or "").split() if p.strip()]


def _to_ms(v):
    """Convert latency value to milliseconds."""
    if isinstance(v, (int, float)):
        return float(v)
    try:
        m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*(us|ms|s)?", str(v).strip().lower())
        if not m:
            return None
        val = float(m.group(1))
        unit = m.group(2) or ""
        return val / 1000.0 if unit == "us" else val * 1000.0 if unit == "s" else val
    except Exception:
        return None


def _percentile_from_list(percentiles, target):
    if not isinstance(percentiles, list):
        return None
    tgt = float(target)
    for ent in percentiles:
        if not isinstance(ent, dict) or not ent:
            continue
        for k, v in ent.items():
            try:
                fk = float(str(k))
            except Exception:
                continue
            if abs(fk - tgt) < 1e-9 or round(fk, 2) == round(tgt, 2):
                return _to_ms(v)
    return None


def _percentiles_map(percentiles):
    out = {}
    if not isinstance(percentiles, list):
        return out
    for ent in percentiles:
        if not isinstance(ent, dict) or not ent:
            continue
        for k, v in ent.items():
            try:
                fk = float(str(k))
            except Exception:
                continue
            val = _to_ms(v)
            if val is None:
                continue
            out[fk] = float(val)
    return out


def _merge_percentiles_max(dst, src):
    if not isinstance(dst, dict) or not isinstance(src, dict):
        return dst
    for k, v in src.items():
        try:
            fk = float(k)
            fv = float(v)
        except Exception:
            continue
        prev = dst.get(fk)
        if prev is None:
            dst[fk] = fv
        else:
            try:
                dst[fk] = max(float(prev), fv)
            except Exception:
                dst[fk] = fv
    return dst


def _search_latency(obj, keys):
    """Search for latency value matching any of the keys in nested dict."""
    stack = [obj]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            for k, v in cur.items():
                lk = str(k).lower().replace("_", "")
                if any(kk in lk or lk.endswith(kk) for kk in keys):
                    val = _to_ms(v)
                    if val is not None:
                        return val
            stack.extend(cur.values())
        elif isinstance(cur, list):
            stack.extend(cur)
    return None


def _canonicalize_requests(reqs):
    out = {}
    if not isinstance(reqs, dict):
        return out
    # Keep canonical if present
    for k in ("create", "set", "get", "list", "multi"):
        if k in reqs and isinstance(reqs[k], dict):
            out[k] = dict(reqs[k])
    # Merge known sweep-derived keys into canonical
    for k, v in reqs.items():
        ks = str(k)
        if ks.startswith("set_") and "set" not in out and isinstance(v, dict):
            out["set"] = {
                "path": v.get("path", "/bench"),
                "data": (v.get("data") or {"random_string": {"size": 64}}),
                "weight": int(v.get("weight", 1) or 1),
            }
        elif ks.startswith("create_d") and "create" not in out and isinstance(v, dict):
            out["create"] = {
                "path": v.get("path", "/bench"),
                "name_length": int(v.get("name_length", 10) or 10),
                "weight": int(v.get("weight", 1) or 1),
            }
        elif ks.startswith("get_d") and "get" not in out and isinstance(v, dict):
            path = v.get("path")
            if isinstance(path, dict) and "children_of" in path:
                out["get"] = {
                    "path": {"children_of": path.get("children_of")},
                    "weight": int(v.get("weight", 1) or 1),
                }
            else:
                out["get"] = {
                    "path": {"children_of": v.get("path", "/bench")},
                    "weight": int(v.get("weight", 1) or 1),
                }
    if not out:
        # Fallback minimal request
        out = {"get": {"path": "/", "weight": 1}}
    return out


def _normalize_requests(reqs):
    if isinstance(reqs, list):
        return reqs
    if not isinstance(reqs, dict) or not reqs:
        return {"get": {"path": "/", "weight": 1}}

    has_sweep_keys = any(
        str(k).startswith(("set_", "create_d", "get_d")) for k in reqs.keys()
    )
    if has_sweep_keys:
        canon = _canonicalize_requests(reqs)
        # Preserve advanced request types if present
        for k in ("list", "multi"):
            if k in reqs and k not in canon and isinstance(reqs[k], dict):
                canon[k] = dict(reqs[k])
        return canon

    return reqs


def _normalize_output(cfg):
    out = cfg.get("output")
    if not isinstance(out, dict):
        cfg["output"] = {
            "file": {"path": "/tmp/keeper_bench_out.json", "with_timestamp": False},
            "stdout": True,
        }
        return

    file_out = out.get("file")
    if isinstance(file_out, dict):
        file_out.setdefault("path", "/tmp/keeper_bench_out.json")
        file_out.setdefault("with_timestamp", False)
        return

    if isinstance(file_out, str):
        out["file"] = {"path": file_out, "with_timestamp": False}
        return

    out.setdefault(
        "file", {"path": "/tmp/keeper_bench_out.json", "with_timestamp": False}
    )


def _ensure_has_write_request(reqs):
    if not isinstance(reqs, dict) or not reqs:
        return reqs
    if "set" in reqs or "create" in reqs or "multi" in reqs:
        return reqs
    get_ent = reqs.get("get") if isinstance(reqs.get("get"), dict) else None
    base_path = "/bench"
    if get_ent is not None:
        try:
            p = get_ent.get("path")
            if isinstance(p, str) and p.startswith("/"):
                base_path = p
            elif isinstance(p, dict):
                ch = p.get("children_of")
                if isinstance(ch, str) and ch.startswith("/"):
                    base_path = ch
            elif isinstance(p, list) and p:
                for item in p:
                    if isinstance(item, str) and item.startswith("/"):
                        base_path = item
                        break
                    if isinstance(item, dict):
                        ch = item.get("children_of")
                        if isinstance(ch, str) and ch.startswith("/"):
                            base_path = ch
                            break
        except Exception:
            pass
    out = dict(reqs)
    out["set"] = {
        "path": base_path,
        "data": {"random_string": {"size": {"min_value": 32, "max_value": 256}}},
        "weight": 1,
    }
    return out


def _translate_workload(cfg_text, servers, duration_s):
    try:
        src = yaml.safe_load(cfg_text) or {}
    except yaml.YAMLError:
        src = {}

    # Optional meta-workload support: allow selecting a preset from a single YAML
    # (useful for merging prod-like and sweep/preset workloads).
    try:
        presets = src.get("presets") if isinstance(src, dict) else None
        if isinstance(presets, dict) and presets:
            preset_name = (
                (src.get("preset") if isinstance(src, dict) else None)
                or os.environ.get("KEEPER_WORKLOAD_PRESET")
                or src.get("preset_default")
            )
            if not preset_name:
                for cand in ("prod_mix", "safe_mix", "write_heavy"):
                    if cand in presets:
                        preset_name = cand
                        break
            if not preset_name:
                preset_name = next(iter(presets.keys()))
            chosen = presets.get(str(preset_name))
            if isinstance(chosen, dict) and chosen:
                merged = dict(src)
                merged.pop("presets", None)
                merged.pop("preset", None)
                merged.pop("preset_default", None)
                merged.update(chosen)
                src = merged
    except Exception:
        pass
    clients = int(src.get("concurrency", src.get("clients", 1)) or 1)
    # Pass-through: if input already resembles keeper-bench config, patch minimal fields
    if isinstance(src, dict) and (
        "generator" in src or "connections" in src or "setup" in src or "output" in src
    ):
        default_host, conn_list = _build_connections(servers, clients)
        out = dict(src)
        out["concurrency"] = int(clients)
        out["timelimit"] = int(duration_s)

        if "report_delay" not in out and "delay" in out:
            try:
                out["report_delay"] = float(out.get("delay"))
            except Exception:
                pass

        out.setdefault("report_delay", 1.0)

        if "continue_on_error" not in out and "continue_on_errors" in out:
            out["continue_on_error"] = bool(out.get("continue_on_errors"))

        out.setdefault("continue_on_error", True)

        conn = dict(out.get("connections") or {})
        conn.setdefault("operation_timeout_ms", 3000)
        conn.setdefault("connection_timeout_ms", 40000)
        conn["host"] = default_host

        existing = conn.get("connection")
        template = None
        if isinstance(existing, list) and existing and isinstance(existing[0], dict):
            template = dict(existing[0])
        elif isinstance(existing, dict):
            template = dict(existing)
        if template is None:
            template = {}
        template.pop("host", None)
        template.pop("sessions", None)
        merged_list = []
        for c in conn_list:
            ent = dict(template)
            ent.update(c)
            merged_list.append(ent)
        conn["connection"] = merged_list

        out["connections"] = conn
        _normalize_output(out)

        gen = dict(out.get("generator") or {})
        reqs = gen.get("requests") or {}
        gen["requests"] = _ensure_has_write_request(_normalize_requests(reqs))
        out["generator"] = gen
        return out
    ops = src.get("ops") or []
    requests = {}

    def _path_seq(static_paths, children_of=None):
        out = []
        if isinstance(static_paths, str):
            static_paths = [static_paths]
        if isinstance(static_paths, list):
            for p in static_paths:
                if isinstance(p, str) and p.startswith("/"):
                    out.append(p)
        if isinstance(children_of, str) and children_of.startswith("/"):
            out.append({"children_of": children_of})
        return out

    # Map our simplified ops spec into keeper-bench request generators
    for spec in ops:
        kind = str(spec.get("kind", "")).strip().lower()
        if not kind:
            continue
        weight = int(spec.get("percent", 0) or 0)
        if weight <= 0:
            weight = 1
        path_prefix = str(spec.get("path_prefix", "/bench")).strip() or "/bench"
        val_sz = int(spec.get("value_bytes", spec.get("value_size", 0)) or 0)
        if kind == "create":
            ent = {"path": path_prefix, "name_length": 10}
            paths = spec.get("paths")
            if isinstance(paths, list) and paths:
                ent["path"] = _path_seq(paths)
            if val_sz > 0:
                ent["data"] = {"random_string": {"size": val_sz}}
            ent["weight"] = weight
            requests.setdefault("create", ent)
        elif kind == "set":
            ent = {"path": path_prefix}
            paths = spec.get("paths")
            children_of = spec.get("children_of")
            if isinstance(paths, list) or isinstance(children_of, str):
                ent["path"] = _path_seq(paths, children_of or path_prefix)
            if val_sz > 0:
                ent["data"] = {"random_string": {"size": val_sz}}
            ent["weight"] = weight
            requests.setdefault("set", ent)
        elif kind == "get":
            paths = spec.get("paths")
            children_of = spec.get("children_of")
            if isinstance(paths, list) or isinstance(children_of, str):
                ent = {"path": _path_seq(paths, children_of or path_prefix)}
            else:
                ent = {"path": {"children_of": path_prefix}}
            ent["weight"] = weight
            requests.setdefault("get", ent)
        elif kind == "list":
            paths = spec.get("paths")
            children_of = spec.get("children_of")
            if isinstance(paths, list) or isinstance(children_of, str):
                ent = {"path": _path_seq(paths, children_of or path_prefix)}
            else:
                ent = {"path": {"children_of": path_prefix}}
            ent["weight"] = weight
            requests.setdefault("list", ent)
        elif kind == "delete":
            # keeper-bench uses remove_factor on create; model deletes via create with high remove_factor
            ent = {
                "path": path_prefix,
                "name_length": 10,
                "remove_factor": 0.8,
                "weight": weight,
            }
            requests.setdefault("create", ent)
        # ignore unknown kinds
    # Support simplified multi-op profile
    multi_cfg = src.get("multi") or {}
    if isinstance(multi_cfg, dict) and multi_cfg:
        size = int(multi_cfg.get("depth", 10) or 10)
        write_ratio = multi_cfg.get("write_ratio")
        try:
            write_ratio = float(write_ratio) if write_ratio is not None else None
        except Exception:
            write_ratio = None
        if write_ratio is None:
            write_ratio = 0.5

        base = str(multi_cfg.get("path_prefix", "/bench") or "/bench")
        static_paths = multi_cfg.get("paths")
        path_seq = _path_seq(static_paths, base)

        w = max(1, int(round(write_ratio * 10)))
        r = max(1, int(round((1.0 - write_ratio) * 10)))

        # Multi contains its own request mix; weights here influence intra-multi selection.
        requests["multi"] = {
            "size": size,
            "create": {
                "path": base,
                "name_length": 10,
                "remove_factor": 0.1,
                "weight": 1,
            },
            "set": {
                "path": path_seq if path_seq else {"children_of": base},
                "data": {
                    "random_string": {
                        "size": int(multi_cfg.get("value_bytes", 64) or 64)
                    }
                },
                "weight": w,
            },
            "get": {
                "path": path_seq if path_seq else {"children_of": base},
                "weight": r,
            },
            "list": {
                "path": path_seq if path_seq else {"children_of": base},
                "weight": 1,
            },
        }

    # Support sweep of sizes/depths by materializing simple request entries
    sweep_cfg = src.get("sweep") or {}
    if isinstance(sweep_cfg, dict) and sweep_cfg:
        sizes = [
            int(x)
            for x in (sweep_cfg.get("sizes") or [])
            if isinstance(x, (int, float)) and x >= 0
        ]
        depths = [
            int(x)
            for x in (sweep_cfg.get("depths") or [])
            if isinstance(x, (int, float)) and x >= 0
        ]
        # For sizes: vary value size on set operations (create distinct entries for visibility)
        for sz in sizes[:20]:  # cap to avoid overly huge configs
            key = f"set_{sz}"
            if key not in requests:
                requests[key] = {
                    "path": "/bench",
                    "data": {"random_string": {"size": int(sz)}},
                    "weight": 1,
                }
        # For depths: vary base path depth for create and read under that subtree
        for d in depths[:20]:
            sub = "/".join([f"d{i}" for i in range(1, d + 1)]) if d > 0 else ""
            base = f"/bench/{sub}" if sub else "/bench"
            ckey = f"create_d{d}"
            gkey = f"get_d{d}"
            if ckey not in requests:
                requests[ckey] = {"path": base, "name_length": 10, "weight": 1}
            if gkey not in requests:
                requests[gkey] = {"path": {"children_of": base}, "weight": 1}
        # Ensure recognized keys exist so keeper-bench actually runs operations
        has_set_sweep = any(str(k).startswith("set_") for k in requests.keys())
        has_depth_sweep = any(
            str(k).startswith("create_d") or str(k).startswith("get_d")
            for k in requests.keys()
        )
        if "set" not in requests and has_set_sweep:
            # pick a representative size (64B) for the canonical 'set' request
            requests["set"] = {
                "path": "/bench",
                "data": {"random_string": {"size": 64}},
                "weight": 1,
            }
        if "create" not in requests and has_depth_sweep:
            requests["create"] = {"path": "/bench", "name_length": 10, "weight": 1}
        if "get" not in requests and (has_depth_sweep or has_set_sweep):
            requests["get"] = {"path": {"children_of": "/bench"}, "weight": 1}
        # Keep only canonical keys recognized by keeper-bench
        _canon = {}
        for k in ("create", "set", "get", "list", "multi"):
            if k in requests:
                _canon[k] = requests[k]
        if _canon:
            requests = _canon

    requests = _ensure_has_write_request(_normalize_requests(requests))

    default_host, conn_list = _build_connections(servers, clients)
    cfg = {
        "concurrency": clients,
        "iterations": int(src.get("iterations", 0) or 0),
        "report_delay": float(src.get("report_delay", src.get("delay", 1.0)) or 1.0),
        "timelimit": int(duration_s),
        "continue_on_error": True,
        "connections": {
            "operation_timeout_ms": 3000,
            "connection_timeout_ms": 40000,
            "host": default_host,
            "connection": conn_list,
        },
        "generator": {
            "requests": requests or {"get": {"path": "/", "weight": 1}},
        },
        "output": {
            "file": {"path": "/tmp/keeper_bench_out.json", "with_timestamp": False},
            "stdout": True,
        },
    }
    return cfg


def _build_connections(servers, clients):
    hosts = _parse_hosts(servers)
    default_host = hosts[0] if hosts else "localhost:9181"
    sessions_total = max(1, int(clients))
    per_host = max(1, sessions_total // max(1, len(hosts) or 1))
    conn_list = [{"host": h, "sessions": per_host} for h in (hosts or [default_host])]
    return default_host, conn_list


def _load_cfg_text(cfg_path):
    """Load workload config text from file path."""
    if not cfg_path:
        return ""
    path_str = str(cfg_path)
    preset = None
    # Support preset selection via path suffix: file.yaml#preset_name
    if "#" in path_str:
        base, frag = path_str.split("#", 1)
        cfg_path = base
        preset = (frag or "").strip()
    # Back-compat: alias old specialized YAMLs to presets in size_depth.yaml
    try:
        alias_map = {
            "write_heavy.yaml": "write_heavy",
            "multi_op.yaml": "multi_op",
            "create_heavy.yaml": "create_heavy",
            "safe_mix.yaml": "safe_mix",
        }
        bname = Path(str(cfg_path)).name
        if not preset and bname in alias_map:
            # Point to sibling size_depth.yaml and select corresponding preset
            cfg_path = str(Path(str(cfg_path)).with_name("size_depth.yaml"))
            preset = alias_map[bname]
    except Exception:
        pass
    # Try direct path first
    txt = None
    if os.path.exists(cfg_path):
        txt = Path(cfg_path).read_text(encoding="utf-8")
    # Try relative to workloads directory
    elif "workloads/" in str(cfg_path):
        rel = str(cfg_path).split("workloads/", 1)[-1]
        alt = Path(__file__).parents[2] / "workloads" / rel.split("/", 1)[-1]
        if alt.exists():
            txt = alt.read_text(encoding="utf-8")
    if preset and txt:
        try:
            y = yaml.safe_load(txt) or {}
            if isinstance(y, dict):
                pres = y.get("presets") or {}
                if isinstance(pres, dict) and preset in pres:
                    return yaml.safe_dump(pres[preset], sort_keys=False)
        except Exception:
            pass
    return txt or ""


def _host_timeout_prefix(cap):
    """Return timeout prefix for host execution if available."""
    return f"timeout -k 20 -s SIGINT {int(cap)}" if host_has_bin("timeout") else ""


def _stage_paths(obj):
    suffix = f"{os.getpid()}_{(id(obj) & 0xffff):x}"
    out_path = f"/tmp/keeper_bench_out_stage_{suffix}.json"
    cfg_path = f"/tmp/keeper_bench_stage_{suffix}.yaml"
    return out_path, cfg_path


def _apply_output_path(cfg, out_path):
    _normalize_output(cfg)
    try:
        cfg["output"]["file"]["path"] = out_path
        cfg["output"]["file"].setdefault("with_timestamp", False)
    except Exception:
        cfg["output"] = {
            "file": {"path": out_path, "with_timestamp": False},
            "stdout": True,
        }


def _empty_stage_stats(dur):
    return {
        "ops": 0,
        "errors": 0,
        "duration_s": int(dur),
        "has_latency": False,
        "reads": 0,
        "writes": 0,
        "read_ratio": 0.0,
        "write_ratio": 0.0,
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
    }


def _precreate_paths(node, ysrc):
    """Pre-create base paths referenced by workload config."""
    bases = set()
    # Always ensure /bench exists; this path is used by default requests
    bases.add("/bench")
    # From ops spec
    for spec in ysrc.get("ops") or []:
        pfx = str(spec.get("path_prefix", "")).strip()
        if pfx.startswith("/"):
            bases.add(pfx)

    # From generator.requests
    def _add_path_val(val):
        if isinstance(val, str) and val.startswith("/"):
            bases.add(val)
        elif isinstance(val, dict):
            p = val.get("children_of")
            if isinstance(p, str) and p.startswith("/"):
                bases.add(p)
        elif isinstance(val, list):
            for x in val:
                _add_path_val(x)

    def _scan_req_ent(ent):
        if not isinstance(ent, dict):
            return
        _add_path_val(ent.get("path"))
        for k in ("create", "set", "get", "list", "multi"):
            if k in ent and isinstance(ent.get(k), dict):
                _scan_req_ent(ent.get(k))

    reqs = (ysrc.get("generator") or {}).get("requests") or {}
    if isinstance(reqs, dict):
        for ent in reqs.values():
            _scan_req_ent(ent)
    elif isinstance(reqs, list):
        for item in reqs:
            if isinstance(item, dict):
                for ent in item.values():
                    _scan_req_ent(ent)
    # Create paths
    for base in sorted(bases):
        full = "/"
        for seg in base.split("/"):
            if not seg:
                continue
            full = full.rstrip("/") + "/" + seg
            sh(
                node,
                f"HOME=/tmp timeout 2s clickhouse keeper-client --host 127.0.0.1 --port {CLIENT_PORT} -q \"touch '{full}'\" || true",
            )


class KeeperBench:
    def __init__(
        self,
        node,
        servers,
        cfg_path=None,
        duration_s=120,
        replay_path=None,
        secure=False,
        clients=None,
    ):
        self.node = node
        self.servers = servers
        self.cfg_path = cfg_path
        self.duration_s = int(duration_s)
        self.replay_path = replay_path
        self.secure = bool(secure)
        self.clients = clients
        self.last_config_yaml = None
        self.last_config_path = None
        self.last_output_path = None
        self.last_stdout_path = None

    def _extract_json_text(self, text):
        if not text:
            return ""
        try:
            lines = str(text).splitlines()
        except Exception:
            return ""
        for line in reversed(lines):
            s = str(line).strip()
            if not s:
                continue
            lb = s.find("{")
            rb = s.rfind("}")
            if lb == -1 or rb == -1 or rb <= lb:
                continue
            cand = s[lb : rb + 1]
            try:
                json.loads(cand)
                return cand
            except Exception:
                continue
        try:
            whole = str(text)
            lb = whole.find("{")
            rb = whole.rfind("}")
            if lb != -1 and rb != -1 and rb > lb:
                cand = whole[lb : rb + 1]
                json.loads(cand)
                return cand
        except Exception:
            pass
        return ""

    def _normalize_json_file(self, path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                raw = f.read()
        except Exception:
            raw = ""

        cleaned = self._extract_json_text(raw)
        if not cleaned:
            return ""

        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(cleaned)
        except Exception:
            pass
        return cleaned

    def _write_debug(self, name, content):
        """Write content to debug file if KEEPER_DEBUG is set."""
        if not parse_bool(os.environ.get("KEEPER_DEBUG")):
            return
        odir = Path(__file__).parents[4] / "tests" / "stress" / "keeper" / "tests"
        odir.mkdir(parents=True, exist_ok=True)
        (odir / name).write_text(content, encoding="utf-8")

    def _require_ch(self):
        env_bin = str(os.environ.get("CLICKHOUSE_BINARY", "")).strip()
        if not env_bin:
            raise AssertionError(
                "keeper-bench must run on host: env var CLICKHOUSE_BINARY must point to clickhouse binary"
            )
        if not os.path.exists(env_bin) or not os.access(env_bin, os.X_OK):
            raise AssertionError(
                f"keeper-bench must run on host: CLICKHOUSE_BINARY is not an executable file: {env_bin}"
            )
        return env_bin

    def _write_cfg(self, cfg_path, cfg_dump):
        try:
            with open(cfg_path, "w", encoding="utf-8") as f:
                f.write(cfg_dump)
        except Exception:
            pass

    def _run_cmd_ex(self, cmd, timeout_s, merge_stderr=True):
        full = (cmd + " 2>&1") if merge_stderr else cmd
        return host_sh(full, timeout=timeout_s)

    def _read_output(self, out_file):
        if isinstance(out_file, dict):
            out_file = out_file.get("path")

        candidates = []
        if isinstance(out_file, str) and out_file:
            candidates.append(out_file)

        candidates.extend(
            [
                "/tmp/keeper_bench_out.json",
                "/tmp/keeper_bench_out*.json",
                "keeper_bench_results.json",
                "/var/lib/clickhouse/keeper_bench_results.json",
            ]
        )

        # Important: output files can exist but be empty (e.g. crash/kill before flush).
        # Also, some candidates contain globs (e.g. /tmp/keeper_bench_out*.json),
        # so we must expand them safely.
        parts = " ".join(shlex.quote(str(p)) for p in candidates)
        cmd = (
            "shopt -s nullglob; "
            f"for p in {parts}; do "
            "  for f in $p; do "
            '    if [ -s "$f" ]; then cat "$f"; exit 0; fi; '
            "  done; "
            "done; "
            "true"
        )
        return host_sh(cmd, timeout=5)

    def _bench_base_cmd(self, cfg_path, clients, duration_s):
        bench = self._bench_cmd_host()
        if self.replay_path:
            hosts = _parse_hosts(self.servers)
            hflags = " ".join(f"-h {h}" for h in hosts)
            return f"{bench} --input-request-log {self.replay_path} {hflags} -c {int(clients)} -t {int(duration_s)} --continue_on_errors --config {cfg_path}"
        return f"{bench} --config {cfg_path} -t {int(duration_s)}"

    def _bench_cmd_host(self):
        env_bin = self._require_ch()
        return f"{env_bin} keeper-bench"

    def _parse_output_json(self, out_text):
        s = {
            "ops": 0,
            "errors": 0,
            "has_latency": False,
            "reads": 0,
            "writes": 0,
            "read_ratio": 0.0,
            "write_ratio": 0.0,
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
            "read_percentiles_ms": {},
            "write_percentiles_ms": {},
        }
        try:
            extracted = self._extract_json_text(out_text)
            data = json.loads(extracted or "{}")
            if not isinstance(data, dict):
                return s

            rr = (
                data.get("read_results")
                if isinstance(data.get("read_results"), dict)
                else {}
            )
            wr = (
                data.get("write_results")
                if isinstance(data.get("write_results"), dict)
                else {}
            )
            reads = int(rr.get("total_requests") or 0)
            writes = int(wr.get("total_requests") or 0)
            s["reads"] = reads
            s["writes"] = writes
            try:
                s["read_rps"] = float(rr.get("requests_per_second") or 0.0)
                s["read_bps"] = float(rr.get("bytes_per_second") or 0.0)
                s["write_rps"] = float(wr.get("requests_per_second") or 0.0)
                s["write_bps"] = float(wr.get("bytes_per_second") or 0.0)
            except Exception:
                pass
            total = float(reads + writes)
            if total > 0:
                s["read_ratio"] = float(reads) / total
                s["write_ratio"] = float(writes) / total

            s["ops"] = int(
                data.get("operations")
                or data.get("total_requests")
                or (reads + writes)
                or 0
            )
            s["errors"] = int(data.get("errors") or data.get("failed") or 0)

            try:
                s["read_percentiles_ms"] = _percentiles_map(rr.get("percentiles"))
                s["write_percentiles_ms"] = _percentiles_map(wr.get("percentiles"))
            except Exception:
                pass

            rp50 = _percentile_from_list(rr.get("percentiles"), 50.0)
            rp95 = _percentile_from_list(rr.get("percentiles"), 95.0)
            rp99 = _percentile_from_list(rr.get("percentiles"), 99.0)
            wp50 = _percentile_from_list(wr.get("percentiles"), 50.0)
            wp95 = _percentile_from_list(wr.get("percentiles"), 95.0)
            wp99 = _percentile_from_list(wr.get("percentiles"), 99.0)

            if rp50 is None:
                rp50 = _search_latency(rr, ["p50", "50%", "median", "50th"])
            if rp95 is None:
                rp95 = _search_latency(rr, ["p95", "95%", "95th"])
            if rp99 is None:
                rp99 = _search_latency(rr, ["p99", "99%", "99th"])

            if wp50 is None:
                wp50 = _search_latency(wr, ["p50", "50%", "median", "50th"])
            if wp95 is None:
                wp95 = _search_latency(wr, ["p95", "95%", "95th"])
            if wp99 is None:
                wp99 = _search_latency(wr, ["p99", "99%", "99th"])

            if rp50 is not None:
                s["read_p50_ms"] = float(rp50)
            if rp95 is not None:
                s["read_p95_ms"] = float(rp95)
            if rp99 is not None:
                s["read_p99_ms"] = float(rp99)
            if wp50 is not None:
                s["write_p50_ms"] = float(wp50)
            if wp95 is not None:
                s["write_p95_ms"] = float(wp95)
            if wp99 is not None:
                s["write_p99_ms"] = float(wp99)

            # Ensure percentile maps contain at least the canonical exported ones
            try:
                rpmap = s.get("read_percentiles_ms")
                if isinstance(rpmap, dict):
                    if rp50 is not None:
                        rpmap[50.0] = float(rp50)
                    if rp95 is not None:
                        rpmap[95.0] = float(rp95)
                    if rp99 is not None:
                        rpmap[99.0] = float(rp99)
                wpmap = s.get("write_percentiles_ms")
                if isinstance(wpmap, dict):
                    if wp50 is not None:
                        wpmap[50.0] = float(wp50)
                    if wp95 is not None:
                        wpmap[95.0] = float(wp95)
                    if wp99 is not None:
                        wpmap[99.0] = float(wp99)
            except Exception:
                pass

            s["has_latency"] = any(
                x is not None and float(x) > 0
                for x in (rp50, rp95, rp99, wp50, wp95, wp99)
            )
        except Exception:
            pass
        return s

    def _run_stage_host(self, base_cfg, clients, dur):
        """Run a single benchmark stage on host."""
        import copy as _copy

        st_cfg = _copy.deepcopy(base_cfg)
        st_cfg["concurrency"] = int(clients)
        out_path, host_stage_cfg = _stage_paths(self)
        _apply_output_path(st_cfg, out_path)
        try:
            with open(host_stage_cfg, "w", encoding="utf-8") as f:
                f.write(yaml.safe_dump(st_cfg, sort_keys=False))
        except Exception:
            pass
        hard_cap = max(5, int(dur) + 30)
        prefix = _host_timeout_prefix(hard_cap)
        stdout_path = out_path + ".stdout"
        stderr_path = out_path + ".stderr"
        cmd = f"{prefix} {self._bench_cmd_host()} --config {host_stage_cfg} -t {int(dur)} > {shlex.quote(stdout_path)} 2> {shlex.quote(stderr_path)}".strip()
        host_sh(cmd, timeout=hard_cap + 5)
        out = host_sh(f"cat {shlex.quote(stdout_path)} 2>/dev/null || true", timeout=5)
        if not str(out.get("out", "") or "").strip():
            out = host_sh(
                f"cat {out_path} 2>/dev/null || cat /tmp/keeper_bench_out.json 2>/dev/null || true",
                timeout=5,
            )
        st = _empty_stage_stats(dur)
        st.update(self._parse_output_json(out.get("out", "")))
        return st

    def _run_adaptive(self, bench_cfg, env_clients, summary):
        """Run adaptive benchmark that adjusts client count based on performance."""
        summary["duration_s"] = 0
        target_p99 = env_int("KEEPER_ADAPT_TARGET_P99_MS", int(DEFAULT_P99_MS))
        max_err = env_float("KEEPER_ADAPT_MAX_ERROR", float(DEFAULT_ERROR_RATE))
        stage_s = env_int("KEEPER_ADAPT_STAGE_S", 15)
        cmin = env_int("KEEPER_ADAPT_MIN_CLIENTS", 8)
        env_clients_int = int(env_clients) if env_clients else 128
        cmax = env_int("KEEPER_ADAPT_MAX_CLIENTS", env_clients_int)
        ccur = int(env_clients) if env_clients else 64
        if self.clients is not None:
            ccur = int(self.clients)
        ccur = max(cmin, min(cmax, max(1, ccur)))

        remaining = int(self.duration_s)
        adapt_log = None
        if parse_bool(os.environ.get("KEEPER_DEBUG")):
            odir = Path(__file__).parents[4] / "tests" / "stress" / "keeper" / "tests"
            odir.mkdir(parents=True, exist_ok=True)
            adapt_log = odir / "keeper_adapt_stages.jsonl"

        read_bytes_total = 0.0
        write_bytes_total = 0.0

        while remaining > 0:
            dur = min(stage_s, remaining)
            st = self._run_stage_host(bench_cfg, ccur, dur)
            summary["ops"] += int(st.get("ops") or 0)
            summary["errors"] += int(st.get("errors") or 0)
            summary["duration_s"] += int(st.get("duration_s") or 0)
            summary["reads"] += int(st.get("reads") or 0)
            summary["writes"] += int(st.get("writes") or 0)
            sd = float(st.get("duration_s") or 0)
            if sd > 0:
                read_bytes_total += float(st.get("read_bps") or 0.0) * sd
                write_bytes_total += float(st.get("write_bps") or 0.0) * sd

            summary["read_p50_ms"] = max(
                float(summary.get("read_p50_ms") or 0.0),
                float(st.get("read_p50_ms") or 0.0),
            )
            summary["read_p95_ms"] = max(
                float(summary.get("read_p95_ms") or 0.0),
                float(st.get("read_p95_ms") or 0.0),
            )
            summary["read_p99_ms"] = max(
                float(summary.get("read_p99_ms") or 0.0),
                float(st.get("read_p99_ms") or 0.0),
            )
            summary["write_p50_ms"] = max(
                float(summary.get("write_p50_ms") or 0.0),
                float(st.get("write_p50_ms") or 0.0),
            )
            summary["write_p95_ms"] = max(
                float(summary.get("write_p95_ms") or 0.0),
                float(st.get("write_p95_ms") or 0.0),
            )
            summary["write_p99_ms"] = max(
                float(summary.get("write_p99_ms") or 0.0),
                float(st.get("write_p99_ms") or 0.0),
            )
            try:
                if "read_percentiles_ms" not in summary or not isinstance(
                    summary.get("read_percentiles_ms"), dict
                ):
                    summary["read_percentiles_ms"] = {}
                if "write_percentiles_ms" not in summary or not isinstance(
                    summary.get("write_percentiles_ms"), dict
                ):
                    summary["write_percentiles_ms"] = {}
                _merge_percentiles_max(
                    summary["read_percentiles_ms"], st.get("read_percentiles_ms")
                )
                _merge_percentiles_max(
                    summary["write_percentiles_ms"], st.get("write_percentiles_ms")
                )
            except Exception:
                pass
            summary["has_latency"] = summary.get("has_latency") or bool(
                st.get("has_latency")
            )
            if adapt_log is not None:
                try:
                    with open(adapt_log, "a", encoding="utf-8") as f:
                        f.write(json.dumps({"clients": ccur, **st}) + "\n")
                except Exception:
                    pass
            remaining -= dur
            ops = float(st.get("ops") or 0)
            errs = float(st.get("errors") or 0)
            rp99 = float(st.get("read_p99_ms") or 0.0)
            wp99 = float(st.get("write_p99_ms") or 0.0)
            p99 = max(rp99, wp99)
            err_ratio = (errs / ops) if ops > 0 else 0.0
            # Adjust client count based on performance
            if ops <= 0:
                ccur = min(cmax, max(cmin, int(ccur * 1.1) + 1))
            elif err_ratio > max_err or p99 > target_p99:
                ccur = max(cmin, int(max(ccur - 1, ccur * 0.7)))
            elif p99 < 0.5 * target_p99:
                ccur = min(cmax, int(ccur * 1.3) + 1)
            else:
                ccur = min(cmax, int(ccur * 1.1) + 1)

        tot = float(summary.get("reads") or 0) + float(summary.get("writes") or 0)
        if tot > 0:
            summary["read_ratio"] = float(summary.get("reads") or 0) / tot
            summary["write_ratio"] = float(summary.get("writes") or 0) / tot

        d = float(summary.get("duration_s") or 0)
        if d > 0:
            summary["read_rps"] = float(summary.get("reads") or 0) / d
            summary["write_rps"] = float(summary.get("writes") or 0) / d
            summary["read_bps"] = read_bytes_total / d
            summary["write_bps"] = write_bytes_total / d
        return summary

    def run(self):
        cfg_text = ""
        clients = 64
        try:
            cfg_text = _load_cfg_text(self.cfg_path)
            y = yaml.safe_load(cfg_text) or {}
            clients = int(y.get("concurrency", y.get("clients", clients)) or clients)
        except Exception:
            pass
        dbg = [
            f"cfg_path={self.cfg_path or ''}\n",
            f"cfg_exists={(os.path.exists(self.cfg_path) if self.cfg_path else False)}\n",
            f"servers='{self.servers}'\n",
            f"cfg_text_preview={cfg_text[:400]}\n",
        ]
        self._write_debug("keeper_bench_meta.txt", "".join(dbg))
        # Override from explicit constructor
        if self.clients is not None:
            clients = int(self.clients)
        # Build base config with output section so we can parse JSON results
        bench_cfg = _translate_workload(cfg_text, self.servers, self.duration_s)
        # Use a unique output file per KeeperBench instance to avoid collisions
        try:
            opath = f"/tmp/keeper_bench_out_{os.getpid()}_{(id(self) & 0xffff):x}.json"
            _apply_output_path(bench_cfg, opath)
            self.last_output_path = opath
        except Exception:
            pass
        # Honor explicit clients for generator mode by overriding concurrency
        if self.clients is not None:
            bench_cfg["concurrency"] = clients
        # Best-effort: pre-create base paths referenced by workload to avoid 'No node' at bench init
        ysrc = yaml.safe_load(cfg_text) if cfg_text else {}
        _precreate_paths(self.node, ysrc or {})
        if parse_bool(os.environ.get("KEEPER_DEBUG")):
            ls_out = []
            for q in [
                "ls /",
                "ls /e2e",
                "ls /e2e/prod",
                "stat /e2e/prod",
                "stat /e2e/prod/create",
                "stat /e2e/prod/set",
            ]:
                try:
                    r = sh(
                        self.node,
                        f"HOME=/tmp timeout 2s clickhouse keeper-client --host 127.0.0.1 --port {CLIENT_PORT} -q '{q}' 2>&1 || true",
                    )
                    ls_out.append(f"$ {q}\n{(r or {}).get('out', '')}\n")
                except Exception:
                    ls_out.append(f"$ {q}\n<error>\n")
            self._write_debug("keeper_pre_ls.txt", "\n".join(ls_out))
        cfg_dump = yaml.safe_dump(bench_cfg, sort_keys=False)
        self._write_debug("keeper_bench_config.yaml", cfg_dump)
        # Save for teardown printing
        self.last_config_yaml = cfg_dump
        self._require_ch()
        # Write config file at the chosen location
        host_cfg_path = f"/tmp/keeper_bench_{os.getpid()}_{id(self) & 0xffff:x}.yaml"
        self._write_cfg(host_cfg_path, cfg_dump)
        w = env_int("KEEPER_BENCH_WARMUP_S", 0)
        if w > 0:
            wp = _host_timeout_prefix(w + 5)
            wcmd = f"{wp} {self._bench_cmd_host()} --config {host_cfg_path} -t {w}".strip()
            try:
                self._run_cmd_ex(wcmd, w + 10, merge_stderr=True)
            except Exception:
                pass
        # Prepare default summary early; used on skip
        summary = {
            "ops": 0,
            "errors": 0,
            "duration_s": self.duration_s,
            "has_latency": False,
            "reads": 0,
            "writes": 0,
            "read_ratio": 0.0,
            "write_ratio": 0.0,
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
            "read_percentiles_ms": {},
            "write_percentiles_ms": {},
        }
        adapt_env = os.environ.get("KEEPER_BENCH_ADAPTIVE")
        if str(adapt_env or "").strip().lower() in ("1", "true", "yes", "on"):
            return self._run_adaptive(bench_cfg, env_clients, summary)
        # Execute the bench tool (replay mode or generator mode)
        # Hard-cap execution: duration + 30s
        hard_cap = max(5, int(self.duration_s) + 60)
        prefix = _host_timeout_prefix(hard_cap)
        cfg_path = host_cfg_path
        try:
            self.last_config_path = cfg_path
        except Exception:
            pass
        stdout_path = (
            f"/tmp/keeper_bench_stdout_{os.getpid()}_{(id(self) & 0xffff):x}.json"
        )
        stderr_path = (
            f"/tmp/keeper_bench_stderr_{os.getpid()}_{(id(self) & 0xffff):x}.txt"
        )
        try:
            self.last_stdout_path = stdout_path
        except Exception:
            pass
        base = self._bench_base_cmd(cfg_path, clients, self.duration_s)
        base = f"{base} > {shlex.quote(stdout_path)} 2> {shlex.quote(stderr_path)}"
        cmd = f"{prefix} {base}".strip()
        run_out = self._run_cmd_ex(cmd, int(hard_cap) + 5, merge_stderr=False)
        self._write_debug("keeper_bench_cmd.txt", cmd + "\n")
        cat_out = self._run_cmd_ex(
            f"cat {shlex.quote(stdout_path)} || true", 10, merge_stderr=False
        )
        bench_stdout_raw = (cat_out or {}).get("out", "")
        if not str(bench_stdout_raw or "").strip():
            bench_stdout_raw = (run_out or {}).get("out", "")
        cleaned_json = self._normalize_json_file(stdout_path)
        bench_out_text = cleaned_json or bench_stdout_raw
        self._write_debug("keeper_bench_stdout.txt", bench_stdout_raw)
        cat_err = self._run_cmd_ex(
            f"cat {shlex.quote(stderr_path)} || true", 10, merge_stderr=False
        )
        self._write_debug("keeper_bench_stderr.txt", (cat_err or {}).get("out", ""))
        # Parse JSON output; prefer exact configured file
        out_file = None
        try:
            of = bench_cfg.get("output") or {}
            out_file = of.get("file") if isinstance(of, dict) else None
        except Exception:
            out_file = None
        try:
            out_path = None
            if isinstance(out_file, dict):
                out_path = out_file.get("path")
            elif isinstance(out_file, str):
                out_path = out_file
            if cleaned_json and isinstance(out_path, str) and out_path:
                size_cmd = f"test -s {shlex.quote(out_path)}; echo $?"
                r = (
                    host_sh(size_cmd, timeout=5)
                )
                is_non_empty = str((r or {}).get("out", "")).strip().endswith("0")
                if not is_non_empty:
                    try:
                        with open(out_path, "w", encoding="utf-8") as f:
                            f.write(cleaned_json)
                    except Exception:
                        pass
        except Exception:
            pass
        # Prefer configured output.file if it is non-empty; if empty, fallback to redirected JSON stdout file
        out = self._read_output(out_file)
        if not str(out.get("out", "") or "").strip():
            out = {"out": bench_out_text}
        self._write_debug("keeper_bench_out_raw.json", out.get("out", ""))
        if parse_bool(os.environ.get("KEEPER_DEBUG")):
            ls_cmd = "ls -l /tmp/keeper_bench_out.json /tmp/keeper_bench_out*.json /tmp/keeper_bench_stdout_*.json /tmp/keeper_bench_stderr_*.txt keeper_bench_results.json /var/lib/clickhouse/keeper_bench_results.json 2>/dev/null || true"
            files = (host_sh(ls_cmd, timeout=5)).get("out", "")
            self._write_debug("keeper_bench_files.txt", files)
        try:
            out_text = str(out.get("out", "") or "").strip()
            if not out_text:
                raise AssertionError(
                    "keeper-bench did not produce a non-empty JSON output file; "
                    "check keeper-bench stdout/stderr for crashes and ensure workload setup provides valid paths"
                )
            parsed = self._parse_output_json(out_text)
            for k, v in parsed.items():
                summary[k] = v
        except Exception:
            self._write_debug(
                "keeper_bench_parse_error.txt", (run_out or {}).get("out", "")
            )
            raise
        return summary
