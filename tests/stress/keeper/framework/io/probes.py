import json
import os
import shlex
import subprocess

from keeper.framework.core.settings import (
    CLIENT_PORT,
    KEEPER_CH_QUERY_TIMEOUT,
    PROM_PORT,
)
from keeper.framework.core.util import _exec, sh


def four(node, cmd):
    """Execute 4LW command on keeper node. For ZooKeeper/RaftKeeper, runs keeper-client from host."""
    if getattr(node, "is_raftkeeper", False):
        bin_path = os.environ.get("CLICKHOUSE_BINARY", "clickhouse")
        try:
            proc = subprocess.run(
                [
                    bin_path,
                    "keeper-client",
                    "--host",
                    str(node.ip_address),
                    "--port",
                    str(node.client_port),
                    "-q",
                    cmd,
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            return (proc.stdout or proc.stderr or "").strip()
        except Exception as e:
            print(f"[keeper][four] keeper-client failed for {node.name}: {e}. Skipping.")
        return ""

    if getattr(node, "is_zookeeper", False):
        cluster = getattr(node, "_cluster", None)
        if cluster:
            # Use zk_name when set (ZKBackedNode: 4LW goes to ZK container, not CH)
            zk_target = getattr(node, "zk_name", node.name)
            try:
                proc = subprocess.run(
                    [
                        cluster.server_bin_path,
                        "keeper-client",
                        "--host",
                        str(cluster.get_instance_ip(zk_target)),
                        "--port",
                        str(cluster.zookeeper_port),
                        "-q",
                        cmd,
                    ],
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                return (proc.stdout or proc.stderr or "").strip()
            except Exception as e:
                print(f"[keeper][four] keeper-client failed for {node.name}: {e}. Skipping.")
        return ""

    methods = [
        f"HOME=/tmp clickhouse keeper-client --host 127.0.0.1 --port {CLIENT_PORT} -q {shlex.quote(cmd)} 2>&1",
        f"HOME=/tmp clickhouse keeper-client -p {CLIENT_PORT} -q {shlex.quote(cmd)} 2>&1",
        f'bash -lc "exec 3<>/dev/tcp/127.0.0.1/{CLIENT_PORT}; printf \'%s\\n\' {shlex.quote(cmd)} >&3; cat <&3; exec 3<&-; exec 3>&-" 2>&1',
        # Raw 4LW over TCP (works when keeper-client is missing or -q does not support 4LW)
        f"printf '%s\\n' {shlex.quote(cmd)} | timeout 2 nc 127.0.0.1 {CLIENT_PORT} 2>&1",
    ]
    for method in methods:
        try:
            out = _exec(node, method, nothrow=True, timeout=5)
            if isinstance(out, str) and out.strip():
                return out
        except Exception as e:
            print(f"[keeper][four] error executing command {cmd} for node {node.name}: {e}. Skipping.")
    print(f"[keeper][four] error executing command {cmd} for node {node.name}: all methods returned empty. Skipping.")
    return ""


def is_leader(node):
    """Check if node is the leader using multiple fallback methods."""
    def _has_mode(text, mode):
        return f"mode: {mode}" in str(text or "").lower()
    
    # Try 'stat' first (most reliable)
    stat_out = four(node, "stat")
    if _has_mode(stat_out, "leader"):
        return True
    if _has_mode(stat_out, "follower") or _has_mode(stat_out, "standalone"):
        return False
    
    # Fallback to 'srvr'
    srvr_out = four(node, "srvr")
    if _has_mode(srvr_out, "leader"):
        return True
    
    # Final fallback: parse mntr
    try:
        return mntr(node).get("zk_server_state", "").strip().lower() == "leader"
    except Exception:
        return False


def count_leaders(nodes):
    return sum(1 for n in nodes if is_leader(n))

def _get_current_leader(nodes):
    """Get current leader node, return None if not found."""
    try:
        return next(n for n in nodes if is_leader(n))
    except Exception:
        print(f"[keeper] no leader found in nodes: {nodes}")
        return None

def ready(node):
    try:
        out = four(node, "ruok")
        return "imok" in str(out or "").lower()
    except Exception:
        return False


def mntr(node):
    kv = {}
    out = four(node, "mntr")
    if not out:
        print(f"[keeper][mntr] error getting mntr for node {node.name}: (empty). Skipping.")
        return {}
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        if "\t" in line:
            k, v = line.split("\t", 1)
            kv[k.strip()] = v.strip()
        else:
            parts = line.split(None, 1)
            if len(parts) == 2:
                k, v = parts
                k, v = k.strip(), v.strip()
                if k:
                    kv[k] = v
    return kv


def wchs_total(node):
    out = four(node, "wchs")
    for line in out.splitlines():
        if line.lower().startswith("total watches"):
            try:
                return int(line.split(":")[1].strip())
            except Exception:
                pass
    try:
        return int(mntr(node).get("zk_watch_count", "0"))
    except Exception:
        return 0


def wchp_paths(node):
    try:
        out = four(node, "wchp")
        counts = {}
        for line in out.splitlines():
            s = line.strip()
            if not s or not s.startswith("/"):
                continue
            parts = s.split()
            path = parts[0]
            num = None
            for tok in reversed(s.replace(",", " ").split()):
                try:
                    num = int(tok)
                    break
                except Exception:
                    continue
            if num is None:
                num = 1
            counts[path] = counts.get(path, 0) + int(num)
        return counts
    except Exception:
        return {}


def any_ephemerals(node):
    return "Sessions with Ephemerals" in four(node, "dump")


def lgif(node):
    out = four(node, "lgif")
    kv = {}
    for line in out.splitlines():
        p = line.split()
        if len(p) >= 2:
            try:
                kv[p[0]] = int(p[1])
            except Exception:
                pass
    return kv


def srvr_kv(node):
    out = four(node, "srvr")
    keys = ("connections", "outstanding", "received", "sent")
    kv = {}
    for line in out.splitlines():
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        k_clean = k.strip().lower()
        v_clean = v.strip().split()[0]
        try:
            val = float(v_clean)
        except Exception:
            continue
        for key in keys:
            if k_clean.startswith(key):
                kv[key] = val
                break
    return kv


def prom_metrics(node):
    """Fetch Prometheus metrics from node using curl (guaranteed by preflight)."""
    url = f"http://127.0.0.1:{PROM_PORT}/metrics"
    # 2>&1: merge stderr so connection refused, timeouts, etc. are visible when stdout is empty
    result = sh(node, f"curl -sf --max-time 2 {url} 2>&1", timeout=4)["out"]
    if not result or not result.strip():
        msg = (result or "").strip() or "empty response"
        raise AssertionError(f"Failed to fetch prometheus metrics for node {node.name}: {msg}")
    return result


def ch_metrics(node):
    return _query_json_each_row(
        node, "SELECT name, value FROM system.metrics FORMAT JSONEachRow"
    )


def ch_async_metrics(node):
    return _query_json_each_row(
        node,
        "SELECT name, value FROM system.asynchronous_metrics FORMAT JSONEachRow",
    )


def _query_json_each_row(node, sql):
    try:
        txt = node.query(sql, timeout=KEEPER_CH_QUERY_TIMEOUT, ignore_error=True)
        return [json.loads(l) for l in txt.strip().splitlines() if l.strip()]
    except Exception:
        return []


def ch_trace_log(node, limit_rows=500):
    try:
        q = f"SELECT * FROM system.trace_log ORDER BY event_time DESC LIMIT {int(limit_rows)} FORMAT JSONEachRow"
        return node.query(q, timeout=KEEPER_CH_QUERY_TIMEOUT, ignore_error=True)
    except Exception:
        return ""


def dirs(node):
    """Return raw output of 4LW 'dirs' command (best-effort)."""
    return four(node, "dirs")


def _exec_for_container_stats(node, cmd, nothrow=True, timeout=2):
    """Run cmd in the container we want to measure: ZK container for ZKBackedNode (workload hits ZK), else node's container."""
    exec_zk = getattr(node, "exec_in_container_zk", None)
    if callable(exec_zk):
        args = ["bash", "-lc", cmd] if isinstance(cmd, str) else list(cmd)
        return exec_zk(args, nothrow=nothrow, timeout=timeout)
    return _exec(node, cmd, nothrow=nothrow, timeout=timeout)


def container_stats(node):
    """Return CPU and memory usage for the container (from cgroup, inside container).
    For ZKBackedNode uses the Zookeeper container (zoo1/2/3) since workload is directed there.
    Returns dict with container_memory_bytes, container_cpu_usage_usec (may be missing if unreadable),
    and container_cpu_limit_cores when a CPU limit is set (cgroup quota); use with usage rate for CPU % of limit.
    """
    out = {}
    try:
        # Memory: cgroup v2 memory.current (bytes) or v1 memory.usage_in_bytes
        mem_cmd = (
            "cat /sys/fs/cgroup/memory.current 2>/dev/null || "
            "cat /sys/fs/cgroup/memory/memory.usage_in_bytes 2>/dev/null"
        )
        r = _exec_for_container_stats(node, mem_cmd, nothrow=True, timeout=2)
        if r and str(r).strip().isdigit():
            out["container_memory_bytes"] = int(str(r).strip())
    except Exception as e:
        print(f"[keeper][container_stats] error getting container memory for node {node.name}: {e}")
    try:
        # CPU: cgroup v2 cpu.stat (usage_usec) or v1 cpuacct.usage (nanoseconds).
        # We always store microseconds so queries can use rate/1e6 = cores.
        cpu_sources = [
            ("grep '^usage_usec' /sys/fs/cgroup/cpu.stat 2>/dev/null | awk '{print $2}'", "usec"),
            ("cat /sys/fs/cgroup/cpuacct/cpuacct.usage 2>/dev/null", "ns"),
            ("cat /sys/fs/cgroup/cpu,cpuacct/cpuacct.usage 2>/dev/null", "ns"),
        ]
        val = None
        for cmd, unit in cpu_sources:
            r = _exec_for_container_stats(node, cmd, nothrow=True, timeout=2)
            s = (r and str(r).strip()).strip() if r else ""
            s = s.splitlines()[0].strip() if s else ""
            if s and s.isdigit():
                val = int(s)
                if unit == "ns":
                    val = val // 1000  # ns -> usec
                break
        if val is not None:
            out["container_cpu_usage_usec"] = val
    except Exception as e:
        print(f"[keeper][container_stats] error getting container cpu for node {node.name}: {e}")
    try:
        # CPU limit (effective cores): so one can compute CPU % of limit from usage rate.
        # cgroup v2: cpu.max is "quota period" (usec) or "max"; effective_cores = quota/period.
        # cgroup v1: cpu.cfs_quota_us, cpu.cfs_period_us; quota -1 = no limit; else effective_cores = quota/period.
        limit_cores = None
        r = _exec_for_container_stats(node, "cat /sys/fs/cgroup/cpu.max 2>/dev/null", nothrow=True, timeout=2)
        if r:
            parts = str(r).strip().split()
            if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
                quota, period = int(parts[0]), int(parts[1])
                if period > 0:
                    limit_cores = quota / period
        if limit_cores is None:
            r2 = _exec_for_container_stats(
                node,
                "quota=$(cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us 2>/dev/null); period=$(cat /sys/fs/cgroup/cpu/cpu.cfs_period_us 2>/dev/null); echo $quota $period",
                nothrow=True,
                timeout=2,
            )
            if not r2:
                r2 = _exec_for_container_stats(
                    node,
                    "quota=$(cat /sys/fs/cgroup/cpu,cpuacct/cpu.cfs_quota_us 2>/dev/null); period=$(cat /sys/fs/cgroup/cpu,cpuacct/cpu.cfs_period_us 2>/dev/null); echo $quota $period",
                    nothrow=True,
                    timeout=2,
                )
            if r2:
                parts = str(r2).strip().split()
                if len(parts) >= 2:
                    try:
                        quota, period = int(parts[0]), int(parts[1])
                        if period > 0 and quota > 0:
                            limit_cores = quota / period
                    except ValueError:
                        pass
        if limit_cores is not None and limit_cores > 0:
            out["container_cpu_limit_cores"] = limit_cores
    except Exception as e:
        print(f"[keeper][container_stats] error getting container cpu limit for node {node.name}: {e}")
    return out
