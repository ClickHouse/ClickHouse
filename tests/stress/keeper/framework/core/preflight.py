import os
from .util import has_bin, sh_root
from .settings import parse_bool


def _tools_for_faults(faults):
    req = set()
    if not faults:
        return req
    kinds = set(str((f or {}).get("kind", "")).strip().lower() for f in faults if isinstance(f, dict))
    # Network related
    if kinds & {"netem", "tbf", "partition_symmetric", "partition_oneway"}:
        req.update({"tc", "ip", "iptables"})
    if "dns_blackhole" in kinds:
        req.update({"iptables", "ip6tables"})
    # Disk related
    if kinds & {"dm_delay", "dm_error"}:
        req.update({"dmsetup", "losetup"})
    if "enospc" in kinds:
        req.update({"dd", "fallocate"})
    # Process / stress
    if "stress_ng" in kinds:
        req.update({"stress-ng"})
    # HTTP probes and shell are required; 'nc' is optional (four() falls back without it)
    req.update({"curl", "bash"})
    return req


def ensure_environment(nodes, scenario):
    faults = (scenario or {}).get("faults") or []
    req = _tools_for_faults(faults)
    if not req:
        req = set()
    # Keeper-bench presence (if workload is requested)
    if isinstance(scenario, dict) and scenario.get("workload") and nodes and not parse_bool(os.environ.get("KEEPER_DISABLE_WORKLOAD")):
        bench_ok = False
        n0 = nodes[0]
        try:
            if has_bin(n0, "keeper-bench"):
                bench_ok = True
            elif has_bin(n0, "clickhouse"):
                from .util import sh
                r = sh(n0, "clickhouse keeper-bench --help >/dev/null 2>&1; echo $?")
                bench_ok = str(r.get("out", " ")).strip().endswith("0")
        except Exception:
            bench_ok = False
        if not bench_ok:
            try:
                url = os.environ.get("KEEPER_BENCH_URL", "").strip()
                if url:
                    for n in (nodes or []):
                        sh_root(n, f"curl -sfL {url} -o /usr/local/bin/keeper-bench && chmod +x /usr/local/bin/keeper-bench")
                    if has_bin(n0, "keeper-bench"):
                        bench_ok = True
            except Exception:
                pass
        if not bench_ok:
            msg = f"keeper-bench not available on {getattr(n0, 'name', 'node')}: install utils/keeper-bench or provide clickhouse keeper-bench"
            raise AssertionError(msg)
        # If replay is requested, verify the file is accessible inside container
        try:
            wl = scenario.get("workload") or {}
            replay_path = wl.get("replay")
            if replay_path:
                from .util import sh
                r2 = sh(n0, f"test -f {replay_path} >/dev/null 2>&1; echo $?")
                if not str(r2.get("out", " ")).strip().endswith("0"):
                    msg = f"replay file not found inside container at {replay_path} (mount it, e.g. bind-mount host log to /artifacts)"
                    raise AssertionError(msg)
        except Exception:
            pass
    missing = {}
    for n in (nodes or []):
        miss_n = [t for t in req if not has_bin(n, t)]
        if miss_n:
            missing[n.name] = miss_n
    if missing:
        msg = ", ".join(f"{name}: {', '.join(tools)}" for name, tools in missing.items())
        raise AssertionError(f"Missing required tools on nodes: {msg}")
    return None
