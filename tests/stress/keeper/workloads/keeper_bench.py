import json
import os
import re
import shlex
import socket
import subprocess
import threading
import time as _time
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
ZOOKEEPER_SESSION_TIMEOUT_MS = 300000

# Max sessions a single keeper-bench process can hold: every session runs a send and a
# receive thread on the process-wide global thread pool (capped at 10000 threads), which
# the bench worker threads also draw from.  Above this the harness splits the run into
# multiple bench subprocesses ("shards") and merges their summaries.
SESSIONS_PER_BENCH = 4000
# Printed by keeper-bench (stderr) once the setup tree has been created; shard 0 owns
# setup, and the other shards are only launched after this marker appears so they never
# race the recursive cleanup + re-create of the workload tree.
BENCH_SETUP_DONE_MARKER = "---- Created test data ----"


def _parse_hosts(servers):
    """Parse space-separated server addresses into list."""
    if not servers:
        raise ValueError("servers must be provided")
    return [p for p in str(servers).split() if p.strip()]


def _patch_keeper_bench_config(src, servers, clients, duration_s, concurrency=None):
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
    # Always override concurrency so KEEPER_BENCH_CLIENTS controls both sessions and
    # concurrency.  Using setdefault would leave the workload YAML's concurrency intact
    # (e.g. prod_mix.yaml has concurrency: 640), causing 640 workers to share `clients`
    # sessions and generating far more load than intended.
    # A scenario can decouple the two via workload.concurrency (worker threads): each
    # worker picks a random session per request, so all sessions stay alive and get
    # watch-armed while the request rate is bounded by the worker count.  Required for
    # very high session counts (thousands), where one worker per session is infeasible.
    out["concurrency"] = int(concurrency) if concurrency is not None else clients
    # Ensure bench prints periodic progress to stderr so the "Requests executed: N"
    # fallback works when Session expired prevents JSON output.
    # Use `is None` check (not truthiness) because workloads explicitly set report_delay: 0.0
    # to suppress periodic stats; overriding 0.0 would trigger Stats::report on a partially-filled
    # collector and hit the assert(requests != 0) in getThroughput.
    if out.get("report_delay") is None:
        out["report_delay"] = 10.0

    return out


class KeeperBench:
    """Runs keeper-bench workload on host. For ZooKeeper backend, uses node IPs and ZK-specific connection settings."""
    
    def __init__(self, nodes, ctx, cfg_path, duration_s, replay_path, secure=False, clients=None, concurrency=None):
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
        # Per-scenario client count (workload.clients); overrides the workload YAML's
        # concurrency, is overridden by KEEPER_BENCH_CLIENTS (and forced to 1 for ZooKeeper).
        self.clients = int(clients) if clients is not None else None
        # Per-scenario worker thread count (workload.concurrency); when set, decouples
        # bench workers from session count (sessions stay `clients`, load generation
        # uses this many threads).  Unset = one worker per session (previous behavior).
        self.concurrency = int(concurrency) if concurrency is not None else None
        # Computed in run(): subprocess timeout for the current bench attempt, scaled by
        # session count.  stop() uses it to size the thread-join timeout.
        self._bench_timeout_s = None
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

    @staticmethod
    def _check_server_zk_ready(host, port):
        """Return True if the server responds to the ZK 'ruok' 4-letter command.

        TCP up is not enough: Keeper accepts connections while still in recovery
        (Raft log replay), then immediately resets them at the ZK handshake stage.
        The 'ruok' 4-letter command only succeeds once Keeper is fully initialized.
        """
        try:
            with socket.create_connection((host, port), timeout=2) as s:
                s.sendall(b"ruok")
                response = s.recv(4)
                return response == b"imok"
        except Exception:
            return False

    def _wait_for_any_server(self, timeout_s=90):
        """Wait until a quorum (majority) of servers are ZK-ready (respond to 'ruok').

        TCP up is not enough: Keeper accepts connections during recovery but resets
        them at the ZK handshake stage.  The 'ruok' 4-letter command verifies that
        Keeper has finished Raft recovery and is ready to serve ZK requests.
        """
        hosts = _parse_hosts(self.servers)
        quorum = len(hosts) // 2 + 1  # majority: 2 of 3 for a 3-node cluster
        deadline = _time.time() + timeout_s
        attempt = 0
        while _time.time() < deadline:
            up = 0
            for hp in hosts:
                host, port_str = hp.rsplit(":", 1)
                if self._check_server_zk_ready(host, int(port_str)):
                    up += 1
            if up >= quorum:
                if attempt > 0:
                    print(f"[keeper][bench] {up}/{len(hosts)} servers ZK-ready after {attempt} retries")
                return True
            attempt += 1
            remaining = deadline - _time.time()
            if remaining <= 0:
                break
            _time.sleep(min(2.0, remaining))
        print(f"[keeper][bench] no quorum after {timeout_s}s ({up}/{len(hosts)} ZK-ready), proceeding anyway")
        return False

    @staticmethod
    def _total_sessions(bench_cfg):
        """Total sessions across all connection entries in a patched bench config."""
        conn = (bench_cfg.get("connections") or {}).get("connection")
        if isinstance(conn, dict):
            conn = [conn]
        if not isinstance(conn, list):
            return 0
        return sum(int(c.get("sessions", 1) or 1) for c in conn if isinstance(c, dict))

    def _run_bench_subprocess(self, bench_cfg, patched_cfg_path):
        """Run keeper-bench subprocess and return (out_text, stdout_path, stderr_path).

        Returns the JSON output string (may be empty on failure) and the log paths
        for the caller to inspect.  Never raises; errors are logged and empty string
        is returned so the caller can decide whether to retry.
        """
        # Give bench 180s extra past its timelimit so that if dm_delay kills all nodes near
        # the end of bench's run, bench has time to wait for nodes to come back up, write its
        # output file, and clean up test znodes before being killed by the subprocess timeout.
        # The bench timelimit clock only starts after all sessions are created, and session
        # creation is serial (one TCP connect + ZK handshake each) — observed at tens of
        # sessions per second, so setup takes minutes at thousands of sessions.  Scale the
        # buffer with the session count (~100ms budget per session on top of the flat 180s);
        # this is only a kill ceiling, so generous is safe.
        bench_timeout = bench_cfg.get("timelimit", 0) + 180 + self._total_sessions(bench_cfg) // 10
        # Track the max across concurrent shard subprocesses; stop() sizes its join
        # timeout from this.
        self._bench_timeout_s = max(self._bench_timeout_s or 0, bench_timeout)

        stdout_path = f"/tmp/keeper_bench_stdout_{uuid.uuid4().hex[:8]}.log"
        stderr_path = f"/tmp/keeper_bench_stderr_{uuid.uuid4().hex[:8]}.log"

        self.bench_output_path = stdout_path
        self.bench_error_path = stderr_path

        opath = bench_cfg.get("output", {}).get("file", {}).get("path", self.output_json_path or "")

        try:
            cmd = f"{self._bench_base_cmd(patched_cfg_path)} > {shlex.quote(stdout_path)} 2> {shlex.quote(stderr_path)}"
            host_sh(cmd, timeout=bench_timeout)
        except subprocess.TimeoutExpired:
            print(f"[keeper][bench] host_sh timed out after {bench_timeout}s; reading output from {stdout_path}")

        out_text = ""
        if opath and Path(opath).exists():
            try:
                out_text = Path(opath).read_text(encoding="utf-8")
                print(f"[keeper][bench] Successfully read output from {opath} ({len(out_text)} bytes):\n{out_text}")
            except Exception as e:
                print(f"[keeper][bench] Failed to read {opath}: {e}")
        else:
            if opath:
                print(f"[keeper][bench] Output file does not exist: {opath}")

        if not out_text and Path(stdout_path).exists():
            try:
                out_text = Path(stdout_path).read_text(encoding="utf-8")
                print(f"[keeper][bench] Fallback: Successfully read output from {stdout_path} ({len(out_text)} bytes):\n{out_text}")
            except Exception as e:
                print(f"[keeper][bench] Failed to read {stdout_path}: {e}")

        return out_text, stdout_path, stderr_path

    def run(self):
        """Run keeper-bench on host. Uses integration helpers: servers_arg (zoo ips:2181) when backend=zookeeper."""
        run_start = _time.monotonic()
        self._wait_for_any_server(timeout_s=90)
        cfg_text = yaml.safe_load(Path(self.cfg_path).read_text(encoding="utf-8"))
        clients = int(cfg_text.get("concurrency", DEFAULT_CONCURRENCY))
        if self.clients is not None:
            print(f"[keeper][bench] Using clients={self.clients} from scenario workload")
            clients = self.clients
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
        concurrency = self.concurrency if not self._is_zookeeper else None
        if concurrency is not None:
            print(f"[keeper][bench] Decoupled workers: sessions={clients} concurrency={concurrency}")
        # Above the per-process session ceiling, split a generator run into several
        # bench subprocesses and merge their summaries (fault-free saturation rungs
        # only).  Replay runs never shard: a replay executes the recorded request
        # log in a single process (the replay rewrite below forces concurrency 1).
        if not self._is_zookeeper and not self.replay_path and clients > SESSIONS_PER_BENCH:
            return self._run_sharded(cfg_text, clients, concurrency)
        bench_cfg = _patch_keeper_bench_config(cfg_text, self.servers, clients, self.duration_s, concurrency=concurrency)

        # Replay mode: remove generator section
        if self.replay_path:
            bench_cfg.pop("generator", None)
            bench_cfg.pop("setup", None)
            bench_cfg["concurrency"] = 1

        # Set unique output path (with_timestamp: false to use exact path) and stdout for fallback
        opath = f"/tmp/keeper_bench_out_{uuid.uuid4().hex[:8]}.json"
        out = bench_cfg.setdefault("output", {})
        out["file"] = {"path": opath, "with_timestamp": False}
        out["stdout"] = True
        self.output_json_path = opath

        # Write patched config
        patched_cfg_path = f"/tmp/keeper_bench_{uuid.uuid4().hex[:8]}.yaml"
        Path(patched_cfg_path).write_text(yaml.safe_dump(bench_cfg, sort_keys=False), encoding="utf-8")
        self.patched_config_path = patched_cfg_path
        
        out_text, stdout_path, stderr_path = self._run_bench_subprocess(bench_cfg, patched_cfg_path)

        # Retry loop: bench fails with a startup exception ("All connection tries failed",
        # "Failed to get feature flags", etc.) when dm_delay kills all nodes simultaneously
        # during fault setup.  Keep retrying until we get JSON output (bench ran to some
        # completion) or the scenario duration expires.  Each retry updates timelimit to the
        # remaining budget.
        for _retry in range(8):
            if out_text and out_text.strip().startswith("{"):
                break  # Got JSON results (bench ran to completion)
            elapsed = _time.monotonic() - run_start
            remaining_s = int(self.duration_s - elapsed)
            if remaining_s <= 10:
                break
            print(f"[keeper][bench] Connection failure on attempt {_retry + 1}; waiting for quorum and retrying with {remaining_s}s budget")
            self._wait_for_any_server(timeout_s=min(90, max(5, remaining_s - 5)))
            elapsed2 = _time.monotonic() - run_start
            remaining_s2 = int(self.duration_s - elapsed2)
            if remaining_s2 <= 5:
                break
            retry_opath = f"/tmp/keeper_bench_out_{uuid.uuid4().hex[:8]}.json"
            bench_cfg["timelimit"] = max(1, remaining_s2 - 10)
            bench_cfg["output"]["file"] = {"path": retry_opath, "with_timestamp": False}
            self.output_json_path = retry_opath
            retry_cfg_path = f"/tmp/keeper_bench_{uuid.uuid4().hex[:8]}.yaml"
            Path(retry_cfg_path).write_text(yaml.safe_dump(bench_cfg, sort_keys=False), encoding="utf-8")
            self.patched_config_path = retry_cfg_path
            out_text, stdout_path, stderr_path = self._run_bench_subprocess(bench_cfg, retry_cfg_path)

        # When output is not JSON, inspect stderr for two recovery paths:
        #   1. Periodic "Requests executed: N" lines from report_delay — use ops directly.
        #   2. "Stopping launch of queries" marker — bench ran to timelimit but Session expired
        #      during post-timelimit pool teardown prevented JSON from being written.  In that
        #      case wait for servers and run a short recovery bench to get valid metrics.
        _RAN_TO_TIMELIMIT = "Stopping launch of queries. Requested time limit is exhausted."
        _out_is_json = bool(out_text and out_text.strip().startswith("{"))
        if not _out_is_json and Path(stderr_path).exists():
            try:
                _stderr_check = Path(stderr_path).read_text(encoding="utf-8")
            except Exception as _e:
                print(f"[keeper][bench] Failed to read stderr for recovery check: {_e}")
                _stderr_check = ""
            # Path 1: periodic report_delay stats
            _match = re.search(r"Requests executed:\s*(\d+)", _stderr_check)
            if _match and int(_match.group(1)) > 0:
                _ops = int(_match.group(1))
                print(f"[keeper][bench] Output not JSON; using ops from periodic stderr stats: {_ops}")
                return self._stderr_fallback_summary(_ops)
            # Path 2: bench ran to timelimit — Session expired during cleanup.
            # Do a short recovery run after servers come back up.
            if _RAN_TO_TIMELIMIT in _stderr_check:
                print(
                    "[keeper][bench] Bench ran to timelimit but Session expired prevented JSON "
                    "write.  Waiting for servers and running short recovery bench (timelimit=30s)..."
                )
                self._wait_for_any_server(timeout_s=120)
                recovery_opath = f"/tmp/keeper_bench_out_{uuid.uuid4().hex[:8]}.json"
                bench_cfg["timelimit"] = 30
                bench_cfg["output"]["file"] = {"path": recovery_opath, "with_timestamp": False}
                self.output_json_path = recovery_opath
                recovery_cfg_path = f"/tmp/keeper_bench_{uuid.uuid4().hex[:8]}.yaml"
                Path(recovery_cfg_path).write_text(
                    yaml.safe_dump(bench_cfg, sort_keys=False), encoding="utf-8"
                )
                self.patched_config_path = recovery_cfg_path
                out_text, stdout_path, stderr_path = self._run_bench_subprocess(bench_cfg, recovery_cfg_path)

        if not out_text:
            # Check stderr for "Requests executed: N" (bench ran but crashed before writing JSON)
            stderr_text = ""
            if Path(stderr_path).exists():
                try:
                    stderr_text = Path(stderr_path).read_text(encoding="utf-8")
                    print(f"[keeper][bench] Stderr content ({len(stderr_text)} bytes):\n{stderr_text}")
                except Exception as e:
                    print(f"[keeper][bench] Failed to read stderr: {e}")
            match = re.search(r"Requests executed:\s*(\d+)", stderr_text)
            if match and int(match.group(1)) > 0:
                ops = int(match.group(1))
                print(f"[keeper][bench] Using ops from stderr (bench ran but did not write JSON): {ops}")
                return self._stderr_fallback_summary(ops)
            err_msg = f"keeper-bench did not produce output (checked {self.output_json_path}, {stdout_path})"
            if stderr_text:
                err_msg += f"; stderr: {stderr_text}"
            raise AssertionError(err_msg)

        # When output is not JSON (e.g. exception in stdout), check stderr for "Requests executed: N"
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
        # Print stderr for debugging when output is non-JSON or contains Session expired
        if Path(stderr_path).exists() and ("Session expired" in out_text or (out_text.strip() and out_text.strip()[0] != "{")):
            try:
                stderr_content = Path(stderr_path).read_text(encoding="utf-8")
                print(f"[keeper][bench] Stderr (last 4K):\n{stderr_content[-4096:]}")
                if out_text.strip() and out_text.strip()[0] != "{":
                    print(f"[keeper][bench] Raw output (first 1K):\n{out_text[:1024]}")
            except Exception as e:
                print(f"[keeper][bench] Failed to read stderr: {e}")

        return self._parse_output_json(out_text)

    def _run_sharded(self, cfg_text, clients, concurrency):
        """Split a high-session run across several keeper-bench subprocesses.

        A single bench process cannot hold much more than SESSIONS_PER_BENCH sessions
        (two global-pool threads per session, 10000-thread pool), so `clients` sessions
        are spread over ceil(clients / SESSIONS_PER_BENCH) concurrent subprocesses:

        - shard 0 keeps the workload's `setup` tree: it alone wipes + creates the tree
          on startup and removes it on exit; the other shards get `setup` stripped;
        - shards 1+ launch only after shard 0 prints the setup-done marker, and get a
          shorter timelimit so they finish before shard 0's exit cleanup removes the
          tree from under them;
        - per-shard summaries are merged: counters/rates sum, latency percentiles take
          the max across shards (a conservative upper bound).

        Intended for fault-free saturation rungs; the fault-recovery retry paths of the
        single-process mode are deliberately not replicated here.
        """
        n_shards = (clients + SESSIONS_PER_BENCH - 1) // SESSIONS_PER_BENCH
        base, rem = divmod(clients, n_shards)
        shard_sessions = [base + (1 if i < rem else 0) for i in range(n_shards)]
        if concurrency is None:
            shard_conc = [None] * n_shards
        else:
            # Split the worker total across shards the same way as sessions, so the
            # cluster-wide total matches the configured concurrency exactly.
            wbase, wrem = divmod(int(concurrency), n_shards)
            shard_conc = [wbase + (1 if i < wrem else 0) for i in range(n_shards)]
            if wbase == 0:
                # keeper-bench needs at least one worker per process; with fewer
                # workers than shards the configured total cannot be preserved.
                shard_conc = [1] * n_shards
                print(
                    f"[keeper][bench] concurrency={concurrency} is below {n_shards} "
                    f"shards; running one worker per shard (effective total {n_shards})"
                )
        print(
            f"[keeper][bench] Sharded run: {clients} sessions over {n_shards} bench "
            f"processes {shard_sessions}, workers per shard "
            f"{shard_conc if concurrency is not None else 'one per session'}"
        )

        results = [None] * n_shards
        failures = [None] * n_shards
        launch_t0 = _time.monotonic()

        def _one_shard(i, stagger_s):
            try:
                shard_cfg = _patch_keeper_bench_config(
                    cfg_text, self.servers, shard_sessions[i], self.duration_s, concurrency=shard_conc[i]
                )
                if i > 0:
                    shard_cfg.pop("setup", None)
                    shard_cfg["timelimit"] = max(1, int(shard_cfg["timelimit"]) - int(stagger_s) - 60)
                opath = f"/tmp/keeper_bench_out_{uuid.uuid4().hex[:8]}.json"
                out = shard_cfg.setdefault("output", {})
                out["file"] = {"path": opath, "with_timestamp": False}
                out["stdout"] = True
                cfg_path = f"/tmp/keeper_bench_shard{i}_{uuid.uuid4().hex[:8]}.yaml"
                Path(cfg_path).write_text(yaml.safe_dump(shard_cfg, sort_keys=False), encoding="utf-8")
                if i == 0:
                    self.patched_config_path = cfg_path
                    self.output_json_path = opath
                out_text, _stdout_path, stderr_path = self._run_bench_subprocess(shard_cfg, cfg_path)
                if out_text and out_text.strip().startswith("{"):
                    results[i] = self._parse_output_json(out_text)
                    print(f"[keeper][bench] shard {i}: ops={results[i].get('ops')} errors={results[i].get('errors')}")
                else:
                    failures[i] = f"shard {i}: no JSON output (stderr: {stderr_path})"
            except Exception as e:
                failures[i] = f"shard {i}: {e}"

        th0 = threading.Thread(target=_one_shard, args=(0, 0), daemon=True, name="bench-shard-0")
        th0.start()

        # Launch the remaining shards only once shard 0 has created the setup tree,
        # so they cannot race its recursive wipe + re-create.  Session setup is
        # serial at roughly tens of sessions per second, so the wait scales with
        # shard 0's session count.
        marker_wait_s = 120 + shard_sessions[0] // 10
        marker_deadline = _time.monotonic() + marker_wait_s
        while _time.monotonic() < marker_deadline and th0.is_alive():
            p = self.bench_error_path
            try:
                if p and Path(p).exists() and BENCH_SETUP_DONE_MARKER in Path(p).read_text(errors="ignore"):
                    break
            except Exception:
                pass
            _time.sleep(2)
        else:
            if th0.is_alive():
                print(
                    f"[keeper][bench] WARNING: setup-done marker not seen within {marker_wait_s}s; "
                    "launching remaining shards anyway"
                )

        shard_threads = [th0]
        for i in range(1, n_shards):
            # Small launch stagger: each shard opens thousands of TCP connections as
            # fast as it can, and starting them all at once turns session setup into
            # an accept/handshake storm on the nodes.
            _time.sleep(2)
            stagger_s = _time.monotonic() - launch_t0
            th = threading.Thread(target=_one_shard, args=(i, stagger_s), daemon=True, name=f"bench-shard-{i}")
            th.start()
            shard_threads.append(th)

        join_deadline = _time.monotonic() + (self._bench_timeout_s or self.duration_s + 300) + 120
        for th in shard_threads:
            th.join(timeout=max(1, join_deadline - _time.monotonic()))

        for msg in filter(None, failures):
            print(f"[keeper][bench] SHARD FAILURE: {msg}")
        summaries = [r for r in results if r]
        # A lost shard means the advertised session count never materialized, so a
        # partial merge would silently pass the gates against a much smaller run.
        if len(summaries) < n_shards:
            raise AssertionError(
                f"{n_shards - len(summaries)} of {n_shards} bench shards failed: "
                f"{[f for f in failures if f]}"
            )

        merged = self._merge_summaries(summaries)
        merged["shards"] = n_shards
        print(f"[keeper][bench] Merged {n_shards} shards: ops={merged.get('ops')} errors={merged.get('errors')}")
        return merged

    def _merge_summaries(self, summaries):
        """Merge per-shard bench summaries: sum counters and rates, max percentiles."""
        merged = {"duration_s": self.duration_s, "bench_duration": self.duration_s}
        sum_keys = {"ops", "reads", "writes", "errors", "read_rps", "read_bps", "write_rps", "write_bps"}
        sum_suffixes = ("_total_requests", "_requests_per_second", "_bytes_per_second")
        for s in summaries:
            for k, v in s.items():
                if k in ("duration_s", "bench_duration"):
                    continue
                if k in sum_keys or k.endswith(sum_suffixes):
                    merged[k] = merged.get(k, 0) + v
                elif k.endswith("_ms"):
                    merged[k] = max(merged.get(k, 0.0), v)
                else:
                    merged.setdefault(k, v)
        return merged

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
            self._error = e  # re-raised from stop() after join
    
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
            # Join timeout: bench may wait for dm_delay to finish before writing output.
            # Allow the bench subprocess timeout (session-count-scaled, see
            # _run_bench_subprocess) + extra buffer; fall back to duration + 300s when
            # the bench never got as far as computing it.
            join_timeout = (self._bench_timeout_s + 120) if self._bench_timeout_s else (self.duration_s + 300)
            self._th.join(timeout=join_timeout)
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
        if self._error is not None:
            raise AssertionError(f"keeper-bench failed: {self._error}") from self._error
