import json
import os
import re

import yaml

from ..framework.core.util import has_bin, sh


def _parse_hosts(servers):
    return [p.strip() for p in (servers or "").split() if p.strip()]


def _translate_workload(cfg_text, servers, duration_s):
    try:
        src = yaml.safe_load(cfg_text) or {}
    except Exception:
        src = {}
    name = src.get("name") or "bench"
    clients = int(src.get("clients", 1) or 1)
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
        write_ratio = float(multi_cfg.get("write_ratio", 0.5) or 0.5)
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

    hosts = _parse_hosts(servers)
    default_host = hosts[0] if hosts else "localhost:9181"
    # Allocate sessions roughly evenly across hosts; minimum 1 per host
    sessions_total = max(1, clients)
    per_host = max(1, sessions_total // max(1, len(hosts) or 1))
    conn_list = [{"host": h, "sessions": per_host} for h in (hosts or [default_host])]
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
            "file": {"path": "/tmp/keeper_bench_out.json", "with_timestamp": False},
            "stdout": True,
        },
    }
    return cfg


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

    def _bench_cmd(self):
        if has_bin(self.node, "keeper-bench"):
            return "keeper-bench"
        if has_bin(self.node, "clickhouse"):
            # Many images ship keeper-bench as a clickhouse subcommand
            return "clickhouse keeper-bench"
        raise AssertionError("keeper-bench tool not found on node")

    def run(self):
        cfg_text = ""
        clients = 64
        try:
            if self.cfg_path and os.path.exists(self.cfg_path):
                with open(self.cfg_path, "r", encoding="utf-8") as f:
                    cfg_text = f.read()
                    try:
                        y = yaml.safe_load(cfg_text) or {}
                        clients = int(y.get("clients", clients) or clients)
                    except Exception:
                        pass
        except Exception:
            cfg_text = ""
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
        cfg_dump = yaml.safe_dump(bench_cfg, sort_keys=False)
        # Write config inside the container
        sh(self.node, "mkdir -p /tmp || true")
        sh(self.node, "cat > /tmp/keeper_bench.yaml <<'YAML'\n" + cfg_dump + "YAML\n")
        # Prepare default summary early; used on skip
        summary = {
            "ops": 0,
            "errors": 0,
            "p50_ms": 0,
            "p95_ms": 0,
            "p99_ms": 0,
            "duration_s": self.duration_s,
        }
        # Execute the bench tool (replay mode or generator mode)
        # Hard-cap execution: duration + 30s, if `timeout` is available in container
        hard_cap = max(5, int(self.duration_s) + 30)
        prefix = ""
        try:
            if has_bin(self.node, "timeout"):
                prefix = f"timeout -s SIGKILL {hard_cap}"
        except Exception:
            prefix = ""
        if self.replay_path:
            hosts = _parse_hosts(self.servers)
            hflags = " ".join(f"-h {h}" for h in hosts)
            base = f"{self._bench_cmd()} --input-request-log {self.replay_path} {hflags} -c {int(clients)} -t {int(self.duration_s)} --continue_on_errors --config /tmp/keeper_bench.yaml"
        else:
            base = f"{self._bench_cmd()} --config /tmp/keeper_bench.yaml -t {int(self.duration_s)}"
        cmd = f"{prefix} {base}".strip()
        run_out = sh(self.node, cmd)
        # Parse JSON output if present
        out = sh(
            self.node,
            "cat /tmp/keeper_bench_out.json 2>/dev/null || cat keeper_bench_results.json 2>/dev/null",
        )
        try:
            data = json.loads(out.get("out", "") or "{}")
            if isinstance(data, dict):
                summary["ops"] = int(
                    data.get("operations") or data.get("total_requests") or 0
                )
                summary["errors"] = int(data.get("errors") or data.get("failed") or 0)
                lat = data.get("latency") or data.get("latency_ms") or {}

                def _pick(d, *ks):
                    for k in ks:
                        if k in d:
                            return d.get(k)
                    return None

                p50 = _pick(lat, "p50", "50%", "median")
                p95 = _pick(lat, "p95", "95%")
                p99 = _pick(lat, "p99", "99%")
                if p50 is not None:
                    summary["p50_ms"] = int(float(p50))
                if p95 is not None:
                    summary["p95_ms"] = int(float(p95))
                if p99 is not None:
                    summary["p99_ms"] = int(float(p99))
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
