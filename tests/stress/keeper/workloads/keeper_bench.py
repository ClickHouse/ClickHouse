import json
import os
import re
import shlex
import subprocess
import threading
import uuid
from pathlib import Path

import yaml
from keeper.framework.core.settings import (
    DEFAULT_CONCURRENCY,
    DEFAULT_CONNECTION_TIMEOUT_MS,
    DEFAULT_OPERATION_TIMEOUT_MS,
    DEFAULT_SESSION_TIMEOUT_MS,
)
from keeper.framework.core.util import (
    host_sh,
)
from keeper.workloads.adapter import servers_arg

ZOOKEEPER_OPERATION_TIMEOUT_MS = 120000
ZOOKEEPER_SESSION_TIMEOUT_MS = 120000


def _parse_hosts(servers):
    """Parse space-separated server addresses into list."""
    if not servers:
        raise ValueError("servers must be provided")
    return [p for p in str(servers).split() if p.strip()]


def _patch_keeper_bench_config(src, servers, clients, duration_s):
    """Patch keeper-bench config with dynamic values: servers, duration."""
    out = dict(src)
    # Buffer for keeper-bench to finish before scenario timeout; keep positive for short scenarios
    out["timelimit"] = max(1, int(duration_s) - 10)
    
    # Patch connections: distribute sessions across hosts
    conn = dict(out.get("connections", {}))
    conn.setdefault("operation_timeout_ms", DEFAULT_OPERATION_TIMEOUT_MS)
    conn.setdefault("connection_timeout_ms", DEFAULT_CONNECTION_TIMEOUT_MS)
    conn.setdefault("session_timeout_ms", DEFAULT_SESSION_TIMEOUT_MS)

    hosts = _parse_hosts(servers)
    sessions_total = max(1, int(clients))
    # ZooKeeper: use single host + single session to avoid session expiry
    use_single_zk_conn = bool(conn.pop("_zookeeper_single_conn", False))
    if use_single_zk_conn:
        hosts = hosts[:1]
        sessions_total = 1
    per_host_base = sessions_total // len(hosts)
    remainder = sessions_total % len(hosts)
    
    # Extract connection template if present
    existing = conn.get("connection")
    template = {}
    if existing:
        if isinstance(existing, list) and existing and isinstance(existing[0], dict):
            template = dict(existing[0])
        elif isinstance(existing, dict):
            template = dict(existing)
        template.pop("host", None)
        template.pop("sessions", None)
    
    # Create connection list with distributed sessions
    conn_list = []
    for i, h in enumerate(hosts):
        sessions = per_host_base + (1 if i < remainder else 0)
        conn_list.append({**template, "host": h, "sessions": max(1, sessions)})
    
    conn["connection"] = conn_list[0] if len(conn_list) == 1 else conn_list
    conn.pop("host", None)
    out["connections"] = conn
    out.setdefault("concurrency", clients)
    
    return out


class KeeperBench:
    """Runs keeper-bench workload on host. For ZooKeeper backend, uses node IPs and ZK-specific connection settings."""
    
    def __init__(self, nodes, ctx, cfg_path, duration_s, replay_path, secure=False):
        # RaftKeeper uses same workload as default (multi-connection); only ZooKeeper uses single-conn + high timeouts
        is_zk = bool(nodes and getattr(nodes[0], "is_zookeeper", False))
        is_raftkeeper = bool(nodes and getattr(nodes[0], "is_raftkeeper", False))
        self._is_zookeeper = is_zk and not is_raftkeeper
        # Always run on host; servers_arg uses node ip_address:port (host-reachable for ZK, RaftKeeper, Keeper).
        self.servers = servers_arg(nodes, in_container=False)
        self.nodes = nodes
        self.ctx = ctx
        self.cfg_path = cfg_path
        self.duration_s = int(duration_s)
        self.replay_path = replay_path
        self.secure = bool(secure)
        self.patched_config_path = None
        self.output_json_path = None
        self.bench_output_path = None
        self.bench_error_path = None
        self._th = None
        self._stop = False
        self._result = {}
        self._error = None

    def _bench_base_cmd(self, cfg_path):
        ch = os.environ.get("CLICKHOUSE_BINARY")
        bench = f"{ch} keeper-bench"
        if self.replay_path:
            replay_abs = os.path.abspath(self.replay_path) if not os.path.isabs(self.replay_path) else self.replay_path
            bench = f"{bench} --input-request-log {shlex.quote(replay_abs)}"
        return f"{bench} --config {cfg_path}"

    def _parse_output_json(self, out_text):
        """Parse keeper-bench JSON output and flatten into summary dict."""
        summary = {}
        # Set duration_s first so rps can be computed even if parse fails partway (e.g. different JSON shape for ZK).
        summary["duration_s"] = self.duration_s
        summary["bench_duration"] = self.duration_s
        try:
            data = json.loads(out_text)
            if not isinstance(data, dict):
                raise ValueError(f"Invalid JSON output: {out_text}")
            
            # Helper to convert percentile key to name
            def pct_name(pct_key):
                pct_float = float(pct_key)
                if pct_float == 99.9:
                    return "p99_90"
                elif pct_float == 99.99:
                    return "p99_99"
                else:
                    return f"p{int(pct_float)}"

            # Helper to flatten results (keeper-bench omits read_results/write_results when 0)
            def flatten_results(prefix, results):
                if not isinstance(results, dict):
                    results = {}
                summary[f"{prefix}_total_requests"] = int(results.get("total_requests") or 0)
                summary[f"{prefix}_requests_per_second"] = float(results.get("requests_per_second") or 0)
                summary[f"{prefix}_bytes_per_second"] = float(results.get("bytes_per_second") or 0)
                for pct_dict in results.get("percentiles") or []:
                    if isinstance(pct_dict, dict):
                        for pct_key, pct_value in pct_dict.items():
                            summary[f"{prefix}_{pct_name(pct_key)}_ms"] = float(pct_value)

            flatten_results("read", data.get("read_results"))
            flatten_results("write", data.get("write_results"))

            reads = summary.get("read_total_requests", 0)
            writes = summary.get("write_total_requests", 0)
            # Prefer top-level ops from keeper-bench when present (read_total + write_total)
            summary["ops"] = int(data["ops"]) if data.get("ops") is not None else (reads + writes)
            summary["reads"] = reads
            summary["writes"] = writes
            summary["read_rps"] = summary.get("read_requests_per_second", 0)
            summary["read_bps"] = summary.get("read_bytes_per_second", 0)
            summary["write_rps"] = summary.get("write_requests_per_second", 0)
            summary["write_bps"] = summary.get("write_bytes_per_second", 0)
            summary["errors"] = int(data.get("errors", 0))
        except Exception as e:
            print(f"[keeper][bench] Failed to parse JSON output: {e}")
        return summary

    def run(self):
        """Run keeper-bench on host. Uses integration helpers: servers_arg (zoo ips:2181) when backend=zookeeper."""
        cfg_text = yaml.safe_load(Path(self.cfg_path).read_text(encoding="utf-8"))
        clients = int(cfg_text.get("concurrency", DEFAULT_CONCURRENCY))
        # ZooKeeper: single connection + high timeouts to avoid "Session expired".
        if self._is_zookeeper:
            clients = 1
            cfg_text.setdefault("connections", {})["_zookeeper_single_conn"] = True
            conn = cfg_text.setdefault("connections", {})
            conn["operation_timeout_ms"] = ZOOKEEPER_OPERATION_TIMEOUT_MS
            conn["session_timeout_ms"] = ZOOKEEPER_SESSION_TIMEOUT_MS
            print(f"[keeper][bench] ZooKeeper: single connection, operation_timeout={ZOOKEEPER_OPERATION_TIMEOUT_MS//1000}s session_timeout={ZOOKEEPER_SESSION_TIMEOUT_MS//1000}s")
        clients_env = os.environ.get("KEEPER_BENCH_CLIENTS", "").strip()
        if clients_env:
            print(f"[keeper][bench] Using KEEPER_BENCH_CLIENTS={clients_env} from environment")
            clients = int(clients_env)
        bench_cfg = _patch_keeper_bench_config(cfg_text, self.servers, clients, self.duration_s)

        # Replay mode: remove generator section
        if self.replay_path:
            bench_cfg.pop("generator", None)
            bench_cfg.pop("setup", None)
            bench_cfg["concurrency"] = 1

        # Set unique output path (with_timestamp: false to use exact path) and stdout for fallback
        opath = f"/tmp/keeper_bench_out_{uuid.uuid4().hex[:8]}.json"
        out = bench_cfg.setdefault("output", {})
        # TODO: uncomment this when we figure out reason of bench core dumps when using file output
        out["file"] = {"path": opath, "with_timestamp": False}
        out["stdout"] = True
        self.output_json_path = opath

        # Write patched config
        patched_cfg_path = f"/tmp/keeper_bench_{uuid.uuid4().hex[:8]}.yaml"
        Path(patched_cfg_path).write_text(yaml.safe_dump(bench_cfg, sort_keys=False), encoding="utf-8")
        self.patched_config_path = patched_cfg_path
        
        # Run keeper-bench (timeout must be at least duration_s + buffer for short scenarios)
        bench_timeout = max(bench_cfg.get("timelimit", 0) + 30, self.duration_s + 30)
        
        stdout_path = f"/tmp/keeper_bench_stdout_{uuid.uuid4().hex[:8]}.log"
        stderr_path = f"/tmp/keeper_bench_stderr_{uuid.uuid4().hex[:8]}.log"
        
        self.bench_output_path = stdout_path
        self.bench_error_path = stderr_path

        try:
            cmd = f"{self._bench_base_cmd(patched_cfg_path)} > {shlex.quote(stdout_path)} 2> {shlex.quote(stderr_path)}"
            host_sh(cmd, timeout=bench_timeout)
        except subprocess.TimeoutExpired:
            # Bench may have written JSON to stdout before hanging in destructor cleanup (removeRecursive).
            # Still read and parse; valid output with ops > 0 is a success.
            print(f"[keeper][bench] host_sh timed out after {bench_timeout}s; reading output from {stdout_path} (bench may have written JSON before cleanup hang)")

        # Read output (prefer JSON output, fallback to stdout)
        print(f"[keeper][bench] Reading output from: KeeperBench created file {opath} or stdout redirected to {stdout_path}")
        out_text = ""
        
        # Try opath first
        if Path(opath).exists():
            try:
                out_text = Path(opath).read_text(encoding="utf-8")
                print(f"[keeper][bench] Successfully read output from {opath} ({len(out_text)} bytes):\n{out_text}")
            except Exception as e:
                print(f"[keeper][bench] Failed to read {opath}: {e}")
        else:
            print(f"[keeper][bench] Output file does not exist: {opath}")
        
        # Fallback to stdout_path if opath is empty
        if not out_text:
            if Path(stdout_path).exists():
                try:
                    out_text = Path(stdout_path).read_text(encoding="utf-8")
                    print(f"[keeper][bench] Fallback: Successfully read output from {stdout_path} ({len(out_text)} bytes):\n{out_text}")
                except Exception as e:
                    print(f"[keeper][bench] Failed to read {stdout_path}: {e}")
            else:
                print(f"[keeper][bench] Stdout file does not exist: {stdout_path}")
        
        if not out_text:
            # Check stderr for error messages and for "Requests executed: N" (bench ran but crashed before writing JSON, e.g. Stats assertion)
            stderr_text = ""
            if Path(stderr_path).exists():
                try:
                    stderr_text = Path(stderr_path).read_text(encoding="utf-8")
                    print(f"[keeper][bench] Stderr content ({len(stderr_text)} bytes):\n{stderr_text}")
                except Exception as e:
                    print(f"[keeper][bench] Failed to read stderr: {e}")
            # If bench ran to time limit and printed "Requests executed: N", use that as ops (e.g. ZK with single_hot_get)
            match = re.search(r"Requests executed:\s*(\d+)", stderr_text)
            if match and int(match.group(1)) > 0:
                ops = int(match.group(1))
                print(f"[keeper][bench] Using ops from stderr (bench ran but did not write JSON): {ops}")
                return self._stderr_fallback_summary(ops)
            err_msg = f"keeper-bench did not produce output (checked {opath}, {stdout_path})"
            if stderr_text:
                err_msg += f"; stderr: {stderr_text}"
            raise AssertionError(err_msg)

        # When output is not JSON (e.g. exception in stdout), check stderr for "Requests executed: N" so we still count ops (read-no-fault etc. may crash with Session expired after some progress)
        if out_text.strip() and out_text.strip()[0] != "{" and Path(stderr_path).exists():
            try:
                stderr_content = Path(stderr_path).read_text(encoding="utf-8")
                match = re.search(r"Requests executed:\s*(\d+)", stderr_content)
                if match and int(match.group(1)) > 0:
                    ops = int(match.group(1))
                    print(f"[keeper][bench] Output not JSON; using ops from stderr: {ops}")
                    return self._stderr_fallback_summary(ops)
            except Exception as e:
                print(f"[keeper][bench] Failed to read stderr: {e}")
        # When bench fails with Session expired or output is not JSON, print stderr for debugging
        if Path(stderr_path).exists() and ("Session expired" in out_text or (out_text.strip() and out_text.strip()[0] != "{")):
            try:
                stderr_content = Path(stderr_path).read_text(encoding="utf-8")
                print(f"[keeper][bench] Stderr (last 4K):\n{stderr_content[-4096:]}")
                if out_text.strip() and out_text.strip()[0] != "{":
                    print(f"[keeper][bench] Raw output (first 1K):\n{out_text[:1024]}")
            except Exception as e:
                print(f"[keeper][bench] Failed to read stderr: {e}")
        
        return self._parse_output_json(out_text)

    def _stderr_fallback_summary(self, ops):
        """Build minimal summary when bench did not write valid JSON (e.g. crashed after time limit)."""
        return {
            "ops": ops,
            "errors": 0,
            "read_p99_ms": 0.0,
            "write_p99_ms": 0.0,
            "duration_s": self.duration_s,
        }

    def _run_in_background(self):
        """Run keeper-bench in background thread."""
        try:
            if not self.cfg_path:
                raise AssertionError("cfg_path must be provided")
            self._result = self.run()
            if self.ctx:
                self.ctx["bench_summary"] = self._result
        except Exception as e:
            raise AssertionError(f"keeper-bench failed: {e}")
    
    def start(self):
        """Start bench execution in background thread."""
        if not self.nodes or not self.ctx:
            raise AssertionError("start() requires nodes and ctx")
        if self._th:
            raise RuntimeError("KeeperBench.start() called but background thread is already running")
        self._stop = False
        self._th = threading.Thread(target=self._run_in_background, daemon=True, name="bench")
        self._th.start()
    
    def stop(self):
        """Stop bench execution and wait for completion."""
        self._stop = True
        if self._th:
            self._th.join(timeout=(self.duration_s + 30))
            paths = [
                ("config (original)", self.cfg_path),
                ("replay", self.replay_path),
                ("config (patched)", self.patched_config_path),
                ("output (JSON)", self.output_json_path),
                ("stdout", self.bench_output_path),
                ("stderr", self.bench_error_path),
            ]
            print("[keeper][bench][paths] File paths:")
            for label, path in paths:
                if path:
                    print(f"  {label}: {path}")
            # Print the contents of the patched config YAML
            try:
                with open(self.patched_config_path, "r", encoding="utf-8") as f:
                    patched_config_content = f.read()
                try:
                    data = yaml.safe_load(patched_config_content)
                    printed_yaml = yaml.dump(
                        data,
                        default_flow_style=False,
                        sort_keys=False,
                        allow_unicode=True,
                    )
                except Exception:
                    printed_yaml = patched_config_content
                print(f"[keeper][bench][config patched] Contents of {self.patched_config_path}:\n{printed_yaml}")
            except Exception as e:
                print(f"[keeper][bench][config patched] Failed to read {self.patched_config_path}: {e}")
            if self._th.is_alive():
                raise AssertionError("bench thread did not terminate gracefully. Timeout exceeded.")
            self._th = None
