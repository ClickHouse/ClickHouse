import json
import os
import re

import yaml
from pathlib import Path

from ..framework.core.util import has_bin, sh
from ..framework.core.settings import (
    CLIENT_PORT,
    DEFAULT_P99_MS,
    DEFAULT_ERROR_RATE,
    getenv_int,
    getenv_float,
    parse_bool,
)
from ..framework.io.probes import mntr


def _parse_hosts(servers):
    return [p.strip() for p in (servers or "").split() if p.strip()]


def _translate_workload(cfg_text, servers, duration_s):
    try:
        src = yaml.safe_load(cfg_text) or {}
    except Exception:
        src = {}
    clients = int(src.get("concurrency", src.get("clients", 1)) or 1)
    # Pass-through: if input already resembles keeper-bench config, patch minimal fields and return
    if isinstance(src, dict) and ("generator" in src or "connections" in src):
        default_host, conn_list = _build_connections(servers, clients)
        out = dict(src)
        try:
            out["concurrency"] = int(clients)
        except Exception:
            pass
        try:
            out["timelimit"] = int(duration_s)
        except Exception:
            pass
        conn = dict(out.get("connections") or {})
        conn.setdefault("operation_timeout_ms", 3000)
        conn.setdefault("connection_timeout_ms", 40000)
        conn["host"] = default_host
        conn["connection"] = conn_list
        out["connections"] = conn
        out.setdefault("output", {"file": "/tmp/keeper_bench_out.json", "stdout": True})
        return out
    ops = src.get("ops") or []
    requests = {}
    # Map our simplified ops spec into keeper-bench request generators
    for spec in ops:
        try:
            kind = str(spec.get("kind")).strip().lower()
        except Exception:
            continue
        weight = int(spec.get("percent", 0) or 0)
        if weight <= 0:
            weight = 1
        path_prefix = str(spec.get("path_prefix", "/bench")).strip() or "/bench"
        val_sz = int(spec.get("value_bytes", spec.get("value_size", 0)) or 0)
        if kind == "create":
            ent = {"path": path_prefix, "name_length": 10}
            if val_sz > 0:
                ent["data"] = {"random_string": {"size": val_sz}}
            ent["weight"] = weight
            requests.setdefault("create", ent)
        elif kind == "set":
            ent = {"path": path_prefix}
            if val_sz > 0:
                ent["data"] = {"random_string": {"size": val_sz}}
            ent["weight"] = weight
            requests.setdefault("set", ent)
        elif kind == "get":
            ent = {"path": {"children_of": path_prefix}}
            ent["weight"] = weight
            requests.setdefault("get", ent)
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
        # Basic mix: create+set as writes, get as reads
        requests["multi"] = {
            "size": size,
            "create": {"path": "/bench", "name_length": 10},
            "set": {"path": "/bench", "data": {"random_string": {"size": 64}}},
            "get": {"path": {"children_of": "/bench"}},
            # "weight" for multi itself can be influenced by ops list if present
        }

    # Support sweep of sizes/depths by materializing simple request entries
    sweep_cfg = src.get("sweep") or {}
    if isinstance(sweep_cfg, dict) and sweep_cfg:
        sizes = sweep_cfg.get("sizes") or []
        depths = sweep_cfg.get("depths") or []
        try:
            sizes = [int(x) for x in sizes if int(x) >= 0]
        except Exception:
            sizes = []
        try:
            depths = [int(x) for x in depths if int(x) >= 0]
        except Exception:
            depths = []
        # For sizes: vary value size on set operations
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

    default_host, conn_list = _build_connections(servers, clients)
    cfg = {
        "concurrency": clients,
        "iterations": 0,
        "report_delay": 1.0,
        "timelimit": int(duration_s),
        "continue_on_errors": True,
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
            "file": "/tmp/keeper_bench_out.json",
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
    try:
        if cfg_path and os.path.exists(cfg_path):
            with open(cfg_path, "r", encoding="utf-8") as f:
                return f.read()
        if cfg_path and "workloads/" in str(cfg_path):
            from pathlib import Path as _P

            try:
                rel = str(cfg_path).split("workloads/", 1)[1]
            except Exception:
                rel = None
            if rel:
                alt = _P(__file__).parents[2] / "workloads" / rel.split("/", 1)[-1]
                if alt.exists():
                    return alt.read_text(encoding="utf-8")
    except Exception:
        pass
    return ""


def _write_cfg_to_tmp(node, dump_text):
    sh(node, "mkdir -p /tmp || true")
    sh(node, "cat > /tmp/keeper_bench.yaml <<'YAML'\n" + dump_text + "YAML\n")


def _timeout_prefix(node, cap):
    try:
        if has_bin(node, "timeout"):
            return f"timeout {int(cap)}"
    except Exception:
        return ""
    return ""


def _precreate_paths(node, ysrc):
    try:
        bases = set()
        for spec in (ysrc.get("ops") or []):
            try:
                pfx = str(spec.get("path_prefix", "")).strip()
            except Exception:
                pfx = ""
            if pfx.startswith("/"):
                bases.add(pfx)
        # Also support bench-native YAML: generator.requests entries
        try:
            gen = ysrc.get("generator") or {}
            reqs = gen.get("requests") or {}
            if isinstance(reqs, dict):
                for _k, ent in reqs.items():
                    try:
                        val = ent.get("path")
                    except Exception:
                        val = None
                    pfx = None
                    if isinstance(val, str):
                        pfx = val
                    elif isinstance(val, dict):
                        try:
                            pfx = val.get("children_of")
                        except Exception:
                            pfx = None
                    if isinstance(pfx, str) and pfx.startswith("/"):
                        bases.add(pfx)
        except Exception:
            pass
        for base in sorted(bases):
            full = "/"
            for seg in [s for s in base.split("/") if s]:
                full = full.rstrip("/") + "/" + seg
                try:
                    sh(
                        node,
                        f"HOME=/tmp timeout 2s clickhouse keeper-client --host 127.0.0.1 --port {CLIENT_PORT} -q \"touch '{full}'\" || true",
                    )
                except Exception:
                    pass
    except Exception:
        pass


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

    def _debug_dir(self):
        try:
            if parse_bool(os.environ.get("KEEPER_DEBUG")):
                repo_root = Path(__file__).parents[4]
                odir = repo_root / "tests" / "stress" / "keeper" / "tests"
                odir.mkdir(parents=True, exist_ok=True)
                return odir
        except Exception:
            return None
        return None

    def _write_debug(self, name, content):
        try:
            od = self._debug_dir()
            if od is None:
                return
            p = od / name
            with open(p, "w", encoding="utf-8") as f:
                f.write(content)
        except Exception:
            pass

    def _bench_cmd(self):
        if has_bin(self.node, "keeper-bench"):
            return "keeper-bench"
        if has_bin(self.node, "clickhouse"):
            # Many images ship keeper-bench as a clickhouse subcommand
            return "clickhouse keeper-bench"
        raise AssertionError("keeper-bench tool not found on node")

    def _parse_output_json(self, out_text):
        s = {
            "ops": 0,
            "errors": 0,
            "p50_ms": 0,
            "p95_ms": 0,
            "p99_ms": 0,
            "has_latency": False,
        }
        try:
            data = json.loads(out_text or "{}")
            if isinstance(data, dict):
                try:
                    s["ops"] = int(
                        data.get("operations") or data.get("total_requests") or 0
                    )
                except Exception:
                    pass
                try:
                    s["errors"] = int(data.get("errors") or data.get("failed") or 0)
                except Exception:
                    pass

                def _to_float_ms(v):
                    try:
                        if isinstance(v, (int, float)):
                            return float(v)
                        t = str(v).strip().lower()
                        import re as _re

                        m = _re.search(r"([0-9]+(?:\.[0-9]+)?)\s*(us|ms|s)?", t)
                        if not m:
                            return None
                        val = float(m.group(1))
                        unit = m.group(2) or ""
                        if unit == "us":
                            return val / 1000.0
                        if unit == "s":
                            return val * 1000.0
                        return val
                    except Exception:
                        return None

                def _search_any(obj, keys):
                    stack = [obj]
                    while stack:
                        cur = stack.pop()
                        if isinstance(cur, dict):
                            for k, v in cur.items():
                                lk = str(k).lower().replace("_", "")
                                for kk in keys:
                                    if lk == kk or lk.endswith(kk) or kk in lk:
                                        val = _to_float_ms(v)
                                        if val is not None:
                                            return val
                            for v in cur.values():
                                stack.append(v)
                        elif isinstance(cur, list):
                            for it in cur:
                                stack.append(it)
                    return None

                k50 = ["p50", "50%", "median", "50th", "p50ms"]
                k95 = ["p95", "95%", "95th", "p95ms"]
                k99 = ["p99", "99%", "99th", "p99ms"]
                p50 = _search_any(data, k50)
                p95 = _search_any(data, k95)
                p99 = _search_any(data, k99)
                if p50 is not None:
                    s["p50_ms"] = float(p50)
                if p95 is not None:
                    s["p95_ms"] = float(p95)
                if p99 is not None:
                    s["p99_ms"] = float(p99)
                s["has_latency"] = any(
                    x is not None and float(x) > 0 for x in (p50, p95, p99)
                )
        except Exception:
            pass
        return s

    def _run_stage(self, base_cfg, clients, dur):
        import copy as _copy

        st_cfg = _copy.deepcopy(base_cfg)
        try:
            st_cfg["concurrency"] = int(clients)
        except Exception:
            pass
        st_dump = yaml.safe_dump(st_cfg, sort_keys=False)
        _write_cfg_to_tmp(self.node, st_dump)
        hard_cap = max(5, int(dur) + 30)
        prefix = _timeout_prefix(self.node, hard_cap)
        base = f"{self._bench_cmd()} --config /tmp/keeper_bench.yaml -t {int(dur)}"
        cmd = f"{prefix} {base}".strip()
        # Capture both stdout and stderr to aid parsing (some builds print stats on stderr)
        run_out = sh(self.node, cmd + " 2>&1", timeout=int(hard_cap) + 5)
        out = sh(
            self.node,
            "cat /tmp/keeper_bench_out.json 2>/dev/null || cat keeper_bench_results.json 2>/dev/null || cat /var/lib/clickhouse/keeper_bench_results.json 2>/dev/null",
            timeout=5,
        )
        st = {
            "ops": 0,
            "errors": 0,
            "p50_ms": 0,
            "p95_ms": 0,
            "p99_ms": 0,
            "duration_s": int(dur),
        }
        try:
            parsed = self._parse_output_json(out.get("out", ""))
            st.update(parsed)
        except Exception:
            pass
        return st

    def run(self):
        cfg_text = ""
        clients = 64
        try:
            cfg_text = _load_cfg_text(self.cfg_path)
            y = yaml.safe_load(cfg_text) or {}
            clients = int(y.get("concurrency", y.get("clients", clients)) or clients)
        except Exception:
            pass
        try:
            dbg = []
            dbg.append(f"cfg_path={self.cfg_path or ''}\n")
            try:
                dbg.append(
                    f"cfg_exists={(os.path.exists(self.cfg_path) if self.cfg_path else False)}\n"
                )
            except Exception:
                dbg.append("cfg_exists=error\n")
            dbg.append(f"servers='{self.servers}'\n")
            dbg.append(f"cfg_text_preview={cfg_text[:400]}\n")
            self._write_debug("keeper_bench_meta.txt", "".join(dbg))
        except Exception:
            pass
        # Override from explicit constructor or env
        try:
            if self.clients is not None:
                clients = int(self.clients)
            elif os.environ.get("KEEPER_BENCH_CLIENTS"):
                clients = int(os.environ.get("KEEPER_BENCH_CLIENTS"))
        except Exception:
            pass
        # Build base config with output section so we can parse JSON results
        bench_cfg = _translate_workload(cfg_text, self.servers, self.duration_s)
        # Honor explicit/env clients for generator mode by overriding concurrency
        try:
            if self.clients is not None or os.environ.get("KEEPER_BENCH_CLIENTS"):
                bench_cfg["concurrency"] = int(clients)
        except Exception:
            pass
        # Best-effort: pre-create base paths referenced by workload to avoid 'No node' at bench init
        try:
            ysrc = yaml.safe_load(cfg_text) or {}
        except Exception:
            ysrc = {}
        _precreate_paths(self.node, ysrc)
        try:
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
                        ls_out.append(f"$ {q}\n" + str((r or {}).get("out", "")) + "\n")
                    except Exception as _:
                        ls_out.append(f"$ {q}\n<error>\n")
                self._write_debug("keeper_pre_ls.txt", "\n".join(ls_out))
        except Exception:
            pass
        cfg_dump = yaml.safe_dump(bench_cfg, sort_keys=False)
        try:
            if parse_bool(os.environ.get("KEEPER_DEBUG")):
                self._write_debug("keeper_bench_config.yaml", cfg_dump)
        except Exception:
            pass
        # Write config inside the container
        _write_cfg_to_tmp(self.node, cfg_dump)
        try:
            w = int(getenv_int("KEEPER_BENCH_WARMUP_S", 0))
            if w > 0:
                wp = ""
                try:
                    if has_bin(self.node, "timeout"):
                        wp = f"timeout -s SIGKILL {max(5, int(w) + 5)}"
                except Exception:
                    wp = ""
                wcmd = f"{wp} {self._bench_cmd()} --config /tmp/keeper_bench.yaml -t {int(w)}".strip()
                try:
                    sh(self.node, wcmd, timeout=max(5, int(w) + 10))
                except Exception:
                    pass
        except Exception:
            pass
        # Prepare default summary early; used on skip
        summary = {
            "ops": 0,
            "errors": 0,
            "p50_ms": 0,
            "p95_ms": 0,
            "p99_ms": 0,
            "duration_s": self.duration_s,
            "has_latency": False,
        }
        try:
            adapt_env = os.environ.get("KEEPER_BENCH_ADAPTIVE") or os.environ.get(
                "KEEPER_ADAPTIVE"
            )
            adapt = str(adapt_env).strip().lower() in ("1", "true", "yes", "on")
        except Exception:
            adapt = False
        if adapt:
            try:
                summary["duration_s"] = 0
            except Exception:
                pass
            target_p99 = int(
                getenv_int("KEEPER_ADAPT_TARGET_P99_MS", int(DEFAULT_P99_MS))
            )
            max_err = float(
                getenv_float("KEEPER_ADAPT_MAX_ERROR", float(DEFAULT_ERROR_RATE))
            )
            stage_s = int(getenv_int("KEEPER_ADAPT_STAGE_S", 15))
            cmin = int(getenv_int("KEEPER_ADAPT_MIN_CLIENTS", 8))
            # Allow KEEPER_BENCH_CLIENTS to seed cmax if explicit max not set
            cmax = int(
                getenv_int(
                    "KEEPER_ADAPT_MAX_CLIENTS",
                    int(os.environ.get("KEEPER_BENCH_CLIENTS", "128") or 128),
                )
            )
            ccur = int(getenv_int("KEEPER_BENCH_CLIENTS", 64))
            # If explicit clients were passed via constructor, prefer them as the starting point
            try:
                if self.clients is not None:
                    ccur = int(self.clients)
            except Exception:
                pass
            ccur = max(cmin, min(cmax, max(1, int(ccur))))

            def _run_stage(cli, dur):
                return self._run_stage(bench_cfg, cli, dur)

            remaining = int(self.duration_s)
            try:
                if parse_bool(os.environ.get("KEEPER_DEBUG")):
                    repo_root = Path(__file__).parents[4]
                    odir = repo_root / "tests" / "stress" / "keeper" / "tests"
                    odir.mkdir(parents=True, exist_ok=True)
                    adapt_log = odir / "keeper_adapt_stages.jsonl"
                else:
                    adapt_log = None
            except Exception:
                adapt_log = None

            while remaining > 0:
                dur = min(stage_s, remaining)
                st = _run_stage(ccur, dur)
                try:
                    summary["ops"] += int(st.get("ops") or 0)
                    summary["errors"] += int(st.get("errors") or 0)
                    summary["duration_s"] += int(st.get("duration_s") or 0)
                    summary["p50_ms"] = max(
                        float(summary.get("p50_ms") or 0), float(st.get("p50_ms") or 0)
                    )
                    summary["p95_ms"] = max(
                        float(summary.get("p95_ms") or 0), float(st.get("p95_ms") or 0)
                    )
                    summary["p99_ms"] = max(
                        float(summary.get("p99_ms") or 0), float(st.get("p99_ms") or 0)
                    )
                    summary["has_latency"] = bool(summary.get("has_latency")) or bool(
                        st.get("has_latency")
                    )
                except Exception:
                    pass
                try:
                    if adapt_log is not None:
                        with open(adapt_log, "a", encoding="utf-8") as f:
                            f.write(json.dumps({"clients": ccur, **st}) + "\n")
                except Exception:
                    pass
                remaining -= dur
                ops = float(st.get("ops") or 0)
                errs = float(st.get("errors") or 0)
                p99 = float(st.get("p99_ms") or 0)
                err_ratio = (errs / ops) if ops > 0 else 0.0
                try:
                    if ops <= 0:
                        ccur = min(cmax, max(cmin, int(ccur * 1.1) + 1))
                    elif err_ratio > max_err or p99 > target_p99:
                        ccur = max(cmin, int(max(ccur - 1, ccur * 0.7)))
                    elif p99 < 0.5 * target_p99:
                        ccur = min(cmax, int(ccur * 1.3) + 1)
                    else:
                        ccur = min(cmax, int(ccur * 1.1) + 1)
                except Exception:
                    ccur = min(cmax, max(cmin, int(ccur) + 1))
            return summary
        # Execute the bench tool (replay mode or generator mode)
        # Hard-cap execution: duration + 30s, if `timeout` is available in container
        hard_cap = max(5, int(self.duration_s) + 30)
        prefix = _timeout_prefix(self.node, hard_cap)
        # Fallback metric: capture pre-run znode count from this node
        pre_zc = None
        try:
            pre_zc = int(mntr(self.node).get("zk_znode_count", "0") or 0)
        except Exception:
            pre_zc = None
        if self.replay_path:
            hosts = _parse_hosts(self.servers)
            hflags = " ".join(f"-h {h}" for h in hosts)
            base = f"{self._bench_cmd()} --input-request-log {self.replay_path} {hflags} -c {int(clients)} -t {int(self.duration_s)} --continue_on_errors --config /tmp/keeper_bench.yaml"
        else:
            base = f"{self._bench_cmd()} --config /tmp/keeper_bench.yaml -t {int(self.duration_s)}"
        cmd = f"{prefix} {base}".strip()
        run_out = sh(self.node, cmd + " 2>&1", timeout=int(hard_cap) + 5)
        try:
            if parse_bool(os.environ.get("KEEPER_DEBUG")):
                self._write_debug("keeper_bench_cmd.txt", cmd + "\n")
                self._write_debug(
                    "keeper_bench_stdout.txt", (run_out or {}).get("out", "")
                )
        except Exception:
            pass
        # Parse JSON output if present
        out = sh(
            self.node,
            "cat /tmp/keeper_bench_out.json 2>/dev/null || cat /tmp/keeper_bench_out*.json 2>/dev/null || cat keeper_bench_results.json 2>/dev/null || cat /var/lib/clickhouse/keeper_bench_results.json 2>/dev/null",
            timeout=5,
        )
        try:
            if parse_bool(os.environ.get("KEEPER_DEBUG")):
                self._write_debug("keeper_bench_out_raw.json", out.get("out", ""))
                # Also record whether any likely JSON files exist inside the container
                files = sh(
                    self.node,
                    "ls -l /tmp/keeper_bench_out.json 2>/dev/null; ls -l /tmp/keeper_bench_out*.json 2>/dev/null; ls -l keeper_bench_results.json 2>/dev/null; ls -l /var/lib/clickhouse/keeper_bench_results.json 2>/dev/null",
                ).get("out", "")
                self._write_debug("keeper_bench_files.txt", files)
        except Exception:
            pass
        try:
            parsed = self._parse_output_json(out.get("out", ""))
            for k, v in parsed.items():
                summary[k] = (
                    v
                    if k
                    in ("ops", "errors", "p50_ms", "p95_ms", "p99_ms", "has_latency")
                    else summary.get(k, v)
                )
            if not bool(summary.get("has_latency")):
                try:
                    txt = (run_out or {}).get("out", "")
                except Exception:
                    txt = ""
                if txt:

                    def _pick(text, keys):
                        for k in keys:
                            try:
                                m = re.search(
                                    rf"(?i)\\b{re.escape(k)}\\b[^0-9]*([0-9]+(?:\\.[0-9]+)?)",
                                    text,
                                )
                                if m:
                                    return float(m.group(1))
                            except Exception:
                                continue
                        return None

                    p50 = _pick(txt, ("p50", "50%", "median", "50th"))
                    p95 = _pick(txt, ("p95", "95%", "95th"))
                    p99 = _pick(txt, ("p99", "99%", "99th"))
                    if p50 is not None:
                        summary["p50_ms"] = float(p50)
                    if p95 is not None:
                        summary["p95_ms"] = float(p95)
                    if p99 is not None:
                        summary["p99_ms"] = float(p99)
                    summary["has_latency"] = any(x is not None for x in (p50, p95, p99))
        except Exception:
            pass
        # Fallback: if ops still zero, estimate by znode_count delta on this node
        if int(summary.get("ops") or 0) == 0:
            try:
                post_zc = int(mntr(self.node).get("zk_znode_count", "0") or 0)
                if pre_zc is not None and post_zc >= pre_zc:
                    dz = post_zc - pre_zc
                    if dz > 0:
                        summary["ops"] = dz
            except Exception:
                pass
        # Parse stdout lines for read/write counts if printed (replay mode)
        try:
            txt = (run_out or {}).get("out", "")
            if txt:
                writes = [int(m) for m in re.findall(r"Write requests:\s*(\d+)", txt)]
                reads = [int(m) for m in re.findall(r"Read requests:\s*(\d+)", txt)]
                if writes:
                    summary["writes"] = writes[-1]
                if reads:
                    summary["reads"] = reads[-1]
                rw_total = float(summary.get("reads", 0) or 0) + float(
                    summary.get("writes", 0) or 0
                )
                if rw_total > 0:
                    summary["read_ratio"] = (
                        float(summary.get("reads", 0) or 0) / rw_total
                    )
                    summary["write_ratio"] = (
                        float(summary.get("writes", 0) or 0) / rw_total
                    )
        except Exception:
            pass
        return summary
