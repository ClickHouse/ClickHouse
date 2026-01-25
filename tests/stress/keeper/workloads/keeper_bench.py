import json
import os
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
)
from keeper.framework.core.util import (
    host_sh,
)
from keeper.workloads.adapter import servers_arg


def _parse_hosts(servers):
    """Parse space-separated server addresses into list."""
    if not servers:
        raise ValueError("servers must be provided")
    return [p for p in str(servers).split() if p.strip()]


def _patch_keeper_bench_config(src, servers, clients, duration_s):
    """Patch keeper-bench config with dynamic values: servers, duration."""
    out = dict(src)
    out["timelimit"] = int(duration_s - 60) # Buffer for keeper-bench to finish before scenario timeout
    
    # Patch connections: distribute sessions across hosts
    conn = dict(out.get("connections", {}))
    conn.setdefault("operation_timeout_ms", DEFAULT_OPERATION_TIMEOUT_MS)
    conn.setdefault("connection_timeout_ms", DEFAULT_CONNECTION_TIMEOUT_MS)
    
    hosts = _parse_hosts(servers)
    sessions_total = max(1, int(clients))
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
    """Runs keeper-bench workload on the host machine."""
    
    def __init__(self, nodes, ctx, cfg_path, duration_s, replay_path, secure=False):
        self.servers = servers_arg(nodes)
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
            summary["ops"] = reads + writes
            summary["reads"] = reads
            summary["writes"] = writes
            summary["read_rps"] = summary.get("read_requests_per_second", 0)
            summary["read_bps"] = summary.get("read_bytes_per_second", 0)
            summary["write_rps"] = summary.get("write_requests_per_second", 0)
            summary["write_bps"] = summary.get("write_bytes_per_second", 0)
            summary["errors"] = int(data.get("errors", 0))
            summary["bench_duration"] = self.duration_s
            summary["duration_s"] = self.duration_s
        except Exception as e:
            print(f"[keeper][bench] Failed to parse JSON output: {e}")
        return summary

    def run(self):
        """Run keeper-bench on host."""
        cfg_text = yaml.safe_load(Path(self.cfg_path).read_text(encoding="utf-8"))
        clients = int(cfg_text.get("concurrency", DEFAULT_CONCURRENCY))
        bench_cfg = _patch_keeper_bench_config(cfg_text, self.servers, clients, self.duration_s)
        
        # Replay mode: remove generator section
        if self.replay_path:
            bench_cfg.pop("generator", None)

        # Set unique output path (with_timestamp: false to use exact path) and stdout for fallback
        opath = f"/tmp/keeper_bench_out_{uuid.uuid4().hex[:8]}.json"
        out = bench_cfg.setdefault("output", {})
        # TODO: uncomment this when we figure out reason of bench core dumps when using file output
        # out["file"] = {"path": opath, "with_timestamp": False}
        out["stdout"] = True
        self.output_json_path = opath

        # Write patched config
        patched_cfg_path = f"/tmp/keeper_bench_{uuid.uuid4().hex[:8]}.yaml"
        Path(patched_cfg_path).write_text(yaml.safe_dump(bench_cfg, sort_keys=False), encoding="utf-8")
        self.patched_config_path = patched_cfg_path
        
        # Run keeper-bench
        bench_timeout = bench_cfg.get("timelimit") + 30
        
        stdout_path = f"/tmp/keeper_bench_stdout_{uuid.uuid4().hex[:8]}.log"
        stderr_path = f"/tmp/keeper_bench_stderr_{uuid.uuid4().hex[:8]}.log"
        
        self.bench_output_path = stdout_path
        self.bench_error_path = stderr_path

        cmd = f"{self._bench_base_cmd(patched_cfg_path)} > {shlex.quote(stdout_path)} 2> {shlex.quote(stderr_path)}"
        try:
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
            # Check stderr for error messages
            stderr_text = ""
            if Path(stderr_path).exists():
                try:
                    stderr_text = Path(stderr_path).read_text(encoding="utf-8")
                    print(f"[keeper][bench] Stderr content ({len(stderr_text)} bytes):\n{stderr_text}")
                except Exception as e:
                    print(f"[keeper][bench] Failed to read stderr: {e}")
            
            err_msg = f"keeper-bench did not produce output (checked {opath}, {stdout_path})"
            if stderr_text:
                err_msg += f"; stderr: {stderr_text}"
            raise AssertionError(err_msg)
        
        return self._parse_output_json(out_text)
    
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
            self._th.join(timeout=(self.duration_s+30))
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
            if self._th.is_alive():
                raise AssertionError("bench thread did not terminate gracefully. Timeout exceeded.")
            self._th = None
