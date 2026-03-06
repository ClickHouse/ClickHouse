import os

from keeper.framework.core.util import has_bin, sh_root

# Map tool names to apt packages that provide them
_TOOL_TO_PACKAGE = {
    "dmsetup": "dmsetup",
    "losetup": "mount",
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

    def _walk(obj):
        if isinstance(obj, dict):
            k = str(obj.get("kind", "")).strip().lower()
            if k:
                kinds.add(k)
            subs = obj.get("steps")
            if isinstance(subs, list):
                for s in subs:
                    _walk(s)
        elif isinstance(obj, list):
            for it in obj:
                _walk(it)

    _walk(faults or [])
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
    """Attempt to install missing tools via apt-get inside Docker containers.
    
    Installation happens INSIDE containers (not on host), so host OS (Mac/Ubuntu/ARM)
    doesn't matter. ClickHouse integration test containers use Ubuntu/Debian on all
    architectures (AMD64/ARM64), so apt-get should work everywhere.
    """
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
            # Check if apt-get is available (containers should be Ubuntu/Debian)
            if not has_bin(n, "apt-get"):
                print(f"[keeper][preflight] apt-get not available on {getattr(n, 'name', 'node')}, skipping package install")
                raise AssertionError(f"apt-get not available on {getattr(n, 'name', 'node')}")
            # Update apt cache and install packages (non-interactive)
            sh_root(n, "apt-get update -qq >/dev/null 2>&1 || true")
            sh_root(
                n, f"DEBIAN_FRONTEND=noninteractive apt-get install -y -qq {pkg_list}"
            )
        return True
    except Exception as e:
        print(f"[keeper][preflight] Failed to install packages: {e}")
        return False


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
    faults = scenario.get("faults")
    req = _tools_for_faults(faults) or set()

    # Check for missing tools
    missing = _collect_missing_tools(nodes, req)

    if missing:
        all_missing = _flatten_missing(missing)

        # Attempt to install missing tools
        print(
            f"[keeper][preflight] Missing tools: {all_missing}, attempting install..."
        )
        _install_missing_tools(nodes, all_missing)
    return None
