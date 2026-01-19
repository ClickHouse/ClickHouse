import os

import pytest
from keeper.framework.core.settings import parse_bool
from keeper.framework.core.util import has_bin, sh_root

# Map tool names to apt packages that provide them
_TOOL_TO_PACKAGE = {
    "dmsetup": "dmsetup",
    "losetup": "mount",  # losetup is in util-linux, but 'mount' package on most systems
    "tc": "iproute2",
    "ip": "iproute2",
    "iptables": "iptables",
    "ip6tables": "iptables",
    "stress-ng": "stress-ng",
    "dd": "coreutils",
    "fallocate": "util-linux",
    "curl": "curl",
    "bash": "bash",
}


def _fault_kinds(faults):
    kinds = set()
    for f in faults or []:
        if not isinstance(f, dict):
            continue
        k = str(f.get("kind", "") or "").strip().lower()
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


def _install_missing_tools(nodes, missing_tools):
    """Attempt to install missing tools via apt-get on all nodes."""
    if not missing_tools or not nodes:
        return True
    # Dedupe packages to install
    packages = set()
    for tool in missing_tools:
        pkg = _TOOL_TO_PACKAGE.get(tool, tool)
        packages.add(pkg)
    if not packages:
        return True
    pkg_list = " ".join(sorted(packages))
    try:
        for n in nodes:
            # Update apt cache and install packages (non-interactive)
            sh_root(n, "apt-get update -qq >/dev/null 2>&1 || true")
            sh_root(
                n, f"DEBIAN_FRONTEND=noninteractive apt-get install -y -qq {pkg_list}"
            )
        return True
    except Exception as e:
        print(f"[keeper][preflight] Failed to install packages: {e}")
        return False


def _bench_present(node):
    try:
        if has_bin(node, "keeper-bench"):
            return True
        # Many images ship keeper-bench as a clickhouse subcommand; treat clickhouse presence as sufficient
        return has_bin(node, "clickhouse")
    except Exception:
        return False


def _install_bench_from_url(nodes):
    url = os.environ.get("KEEPER_BENCH_URL", "").strip()
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
    wl = (scenario or {}).get("workload") or {}
    replay_path = wl.get("replay")
    if not replay_path:
        return

    from .util import sh

    r2 = sh(node0, f"test -f {replay_path} >/dev/null 2>&1; echo $?", timeout=5)
    if not str(r2.get("out", " ")).strip().endswith("0"):
        raise AssertionError(
            f"replay file not found inside container at {replay_path} "
            f"(mount it, e.g. bind-mount host log to /artifacts)"
        )


def _ensure_bench(nodes, scenario):
    if parse_bool(os.environ.get("KEEPER_DISABLE_WORKLOAD")):
        return

    # keeper-bench runs on host; validate CLICKHOUSE_BINARY early.
    env_bin = str(os.environ.get("CLICKHOUSE_BINARY", "")).strip()
    if not env_bin:
        raise AssertionError(
            "keeper-bench must run on host: env var CLICKHOUSE_BINARY must point to clickhouse binary"
        )
    if not os.path.exists(env_bin) or not os.access(env_bin, os.X_OK):
        raise AssertionError(
            f"keeper-bench must run on host: CLICKHOUSE_BINARY is not an executable file: {env_bin}"
        )

    if isinstance(scenario, dict) and scenario.get("workload") and nodes:
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
        _ensure_replay_if_requested(n0, scenario)


def _collect_missing_tools(nodes, req):
    missing = {}
    for n in nodes or []:
        miss_n = [t for t in req if not has_bin(n, t)]
        if miss_n:
            missing[getattr(n, "name", "node")] = miss_n
    return missing


def _flatten_missing(missing):
    all_missing = set()
    for tools in missing.values():
        all_missing.update(tools)
    return all_missing


def ensure_environment(nodes, scenario):
    faults = (scenario or {}).get("faults") or []
    req = _tools_for_faults(faults) or set()
    _ensure_bench(nodes, scenario)

    # Check for missing tools
    missing = _collect_missing_tools(nodes, req)

    if missing:
        all_missing = _flatten_missing(missing)

        # Attempt to install missing tools
        print(
            f"[keeper][preflight] Missing tools: {all_missing}, attempting install..."
        )
        if _install_missing_tools(nodes, all_missing):
            # Re-check after installation
            still_missing = _collect_missing_tools(nodes, req)
            if not still_missing:
                print(f"[keeper][preflight] Successfully installed: {all_missing}")
                return None
            missing = still_missing

        # Still missing after install attempt - skip gracefully
        msg = ", ".join(
            f"{name}: {', '.join(tools)}" for name, tools in missing.items()
        )
        pytest.skip(f"Missing required tools (install failed): {msg}")
    return None
