import os

from .settings import parse_bool
from .util import has_bin, sh_root


def _fault_kinds(faults):
    kinds = set()
    for f in faults or []:
        if isinstance(f, dict):
            try:
                k = str(f.get("kind", "")).strip().lower()
            except Exception:
                k = ""
            if k:
                kinds.add(k)
    return kinds


def _tools_for_faults(faults):
    req = set()
    kinds = _fault_kinds(faults)
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


def _bench_present(node):
    try:
        if has_bin(node, "keeper-bench"):
            return True
        if has_bin(node, "clickhouse"):
            # Many images ship keeper-bench as a clickhouse subcommand; treat clickhouse presence as sufficient
            return True
    except Exception:
        return False
    return False


def _install_bench_from_url(nodes):
    try:
        url = os.environ.get("KEEPER_BENCH_URL", "").strip()
    except Exception:
        url = ""
    if not url:
        return False
    try:
        for n in nodes or []:
            sh_root(
                n,
                f"curl -sfL {url} -o /usr/local/bin/keeper-bench && chmod +x /usr/local/bin/keeper-bench",
            )
        return True
    except Exception:
        return False


def _ensure_replay_if_requested(node0, scenario):
    try:
        wl = (scenario or {}).get("workload") or {}
        replay_path = wl.get("replay")
        if replay_path:
            from .util import sh

            r2 = sh(node0, f"test -f {replay_path} >/dev/null 2>&1; echo $?")
            if not str(r2.get("out", " ")).strip().endswith("0"):
                msg = (
                    f"replay file not found inside container at {replay_path} "
                    f"(mount it, e.g. bind-mount host log to /artifacts)"
                )
                raise AssertionError(msg)
    except Exception:
        pass


def ensure_environment(nodes, scenario):
    faults = (scenario or {}).get("faults") or []
    req = _tools_for_faults(faults) or set()
    # Keeper-bench presence (if workload is requested)
    if (
        isinstance(scenario, dict)
        and scenario.get("workload")
        and nodes
        and not parse_bool(os.environ.get("KEEPER_DISABLE_WORKLOAD"))
    ):
        n0 = nodes[0]
        bench_ok = _bench_present(n0)
        if not bench_ok and _install_bench_from_url(nodes):
            bench_ok = _bench_present(n0)
        if not bench_ok:
            msg = (
                f"keeper-bench not available on {getattr(n0, 'name', 'node')}: "
                f"install utils/keeper-bench or provide clickhouse keeper-bench"
            )
            raise AssertionError(msg)
        # If replay is requested, verify the file is accessible inside container
        _ensure_replay_if_requested(n0, scenario)
    missing = {}
    for n in nodes or []:
        miss_n = [t for t in req if not has_bin(n, t)]
        if miss_n:
            missing[n.name] = miss_n
    if missing:
        msg = ", ".join(
            f"{name}: {', '.join(tools)}" for name, tools in missing.items()
        )
        raise AssertionError(f"Missing required tools on nodes: {msg}")
    return None
