import threading
import time
from contextlib import contextmanager

from keeper.faults.registry import register_fault
from keeper.framework.core.settings import DEFAULT_FAULT_DURATION_S, RAFT_PORT
from keeper.framework.core.util import (
    for_each_target,
    has_bin,
    resolve_targets,
    sh,
    sh_root,
    sh_root_strict,
    sh_strict,
    ts_ms,
)


def _get_ipv4(node, hostname=None):
    """Get IPv4 address for a node or hostname."""
    if hostname:
        ip = sh(
            node,
            f"getent ahosts {hostname} | awk '{{print $1}}' | grep -v ':' | head -n1 || true",
        )["out"].strip()
        return ip if ip else None
    return sh(node, "hostname -i")["out"].split()[0]


def _get_ipv6_list(node, hostname):
    """Get list of IPv6 addresses for a hostname."""
    out = sh(
        node,
        f"getent ahosts {hostname} | awk '{{print $1}}' | grep ':' | sort -u || true",
    )
    return [line for line in out.get("out", "\n").split() if ":" in line]


def _create_iptables_chain(node, cname):
    """Create and hook iptables chain for both IPv4 and IPv6."""
    sh_root(node, f"iptables -w 2 -t filter -N {cname} || true")
    sh_root(
        node,
        f"(iptables -w 2 -t filter -S OUTPUT | grep -q -- ' -j {cname}') || iptables -w 2 -t filter -I OUTPUT 1 -j {cname}",
    )
    sh_root(node, f"ip6tables -w 2 -t filter -N {cname} || true")
    sh_root(
        node,
        f"(ip6tables -w 2 -t filter -S OUTPUT | grep -q -- ' -j {cname}') || ip6tables -w 2 -t filter -I OUTPUT 1 -j {cname}",
    )


def _check_iptables_chain(node, cname):
    """Check if iptables chain exists and is hooked."""
    v1 = sh(node, f"iptables -w 2 -t filter -S {cname} >/dev/null 2>&1; echo $?")
    v2 = sh(
        node,
        f"iptables -w 2 -t filter -S OUTPUT | grep -q -- ' -j {cname}' ; echo $?",
    )
    return str(v1.get("out", " ")).strip().endswith("0") and str(
        v2.get("out", " ")
    ).strip().endswith("0")


def _remove_iptables_chain(node, cname):
    """Remove iptables chain for both IPv4 and IPv6."""
    sh_root(
        node,
        f"iptables -w 2 -t filter -D OUTPUT -j {cname} || true; iptables -w 2 -t filter -F {cname} || true; iptables -w 2 -t filter -X {cname} || true",
    )
    sh_root(
        node,
        f"ip6tables -w 2 -t filter -D OUTPUT -j {cname} || true; ip6tables -w 2 -t filter -F {cname} || true; ip6tables -w 2 -t filter -X {cname} || true",
    )


def _check_cmd_success(result):
    """Check if command result indicates success (exit code 0)."""
    return str((result or {}).get("out", "")).strip().endswith("0")


@contextmanager
def netem(
    node,
    delay_ms=0,
    jitter_ms=0,
    loss_pct=0,
    reorder=None,
    duplicate=None,
    corrupt=None,
):
    applied = False
    try:
        args = []
        if delay_ms:
            args.append(
                f"delay {int(delay_ms)}ms {int(jitter_ms)}ms"
                if jitter_ms
                else f"delay {int(delay_ms)}ms"
            )
        if loss_pct:
            args.append(f"loss {int(loss_pct)}%")
        if reorder:
            args.append(f"reorder {int(reorder)}% 50%")
        if duplicate:
            args.append(f"duplicate {int(duplicate)}%")
        if corrupt:
            args.append(f"corrupt {int(corrupt)}%")
        
        sh_root_strict(
            node,
            f"tc qdisc replace dev eth0 root netem {' '.join(args) if args else 'delay 0ms'}",
            timeout=20,
        )
        applied = True
        v = sh_strict(
            node, "tc qdisc show dev eth0 | grep -q netem; echo $?", timeout=10
        )
        if not _check_cmd_success(v):
            raise AssertionError("netem verify failed")
        yield
    finally:
        if applied:
            try:
                sh_root(node, "tc qdisc del dev eth0 root || true", timeout=20)
            except Exception:
                pass


@contextmanager
def tbf(node, rate="10mbit"):
    """Apply token bucket filter for rate limiting."""
    sh_root_strict(
        node,
        f"tc qdisc replace dev eth0 root tbf rate {rate} burst 32kb latency 400ms",
        timeout=20,
    )
    v = sh_strict(node, "tc qdisc show dev eth0 | grep -q tbf; echo $?", timeout=10)
    if not _check_cmd_success(v):
        raise AssertionError("tbf verify failed")
    try:
        yield
    finally:
        sh_root(node, "tc qdisc del dev eth0 root || true", timeout=20)


@contextmanager
def partition_symmetric(leader, peers):
    try:
        ips = []
        ip_l = _get_ipv4(leader)
        chains = {}
        used_method = "iptables"
        routes = []
        routes6 = []
        tc_used = False
        
        # Setup iptables chains
        cname_l = f"CH_KEEPER_{int(time.time()*1000)}"
        chains[leader] = cname_l
        _create_iptables_chain(leader, cname_l)
        
        for p in peers:
            ip_p = _get_ipv4(leader, p.name) or _get_ipv4(p)
            ips.append((p, ip_p))
            sh_root(
                leader,
                f"iptables -w 2 -t filter -A {cname_l} -p tcp --dport {RAFT_PORT} -d {ip_p} -j DROP",
            )
            for ip6 in _get_ipv6_list(leader, p.name):
                sh_root(
                    leader,
                    f"ip6tables -w 2 -t filter -A {cname_l} -p tcp --dport {RAFT_PORT} -d {ip6} -j DROP",
                )
        
        if not _check_iptables_chain(leader, cname_l):
            used_method = "iproute"
        
        for p, ip_p in ips:
            cname_p = f"CH_KEEPER_{int(time.time()*1000)}"
            chains[p] = cname_p
            _create_iptables_chain(p, cname_p)
            ip_l_peer = _get_ipv4(p, leader.name) or ip_l
            sh_root(
                p,
                f"iptables -w 2 -t filter -A {cname_p} -p tcp --dport {RAFT_PORT} -d {ip_l_peer} -j DROP",
            )
            for ip6 in _get_ipv6_list(p, leader.name):
                sh_root(
                    p,
                    f"ip6tables -w 2 -t filter -A {cname_p} -p tcp --dport {RAFT_PORT} -d {ip6} -j DROP",
                )
            if not _check_iptables_chain(p, cname_p):
                used_method = "iproute"
        # Verify blocking
        all_blocked = all(
            not _tcp_connect_ok(leader, ip_p, RAFT_PORT, timeout_s=1)
            and not _tcp_connect_ok(p, ip_l, RAFT_PORT, timeout_s=1)
            for p, ip_p in ips
        )
        
        if used_method == "iptables" and not all_blocked:
            used_method = "iproute"
        
        if used_method == "iproute":
            # Clean up iptables chains
            for n, cname in chains.items():
                try:
                    _remove_iptables_chain(n, cname)
                except Exception:
                    pass
            
            # Add blackhole routes
            for p, ip_p in ips:
                sh_root(leader, f"ip route add blackhole {ip_p}/32 || true")
                routes.append((leader, f"{ip_p}/32"))
                for ip6 in _get_ipv6_list(leader, p.name):
                    sh_root(leader, f"ip -6 route add blackhole {ip6}/128 || true")
                    routes6.append((leader, f"{ip6}/128"))
            
            for p, ip_p in ips:
                sh_root(p, f"ip route add blackhole {ip_l}/32 || true")
                routes.append((p, f"{ip_l}/32"))
                for ip6 in _get_ipv6_list(p, leader.name):
                    sh_root(p, f"ip -6 route add blackhole {ip6}/128 || true")
                    routes6.append((p, f"{ip6}/128"))
            
            sh(leader, "sleep 0.2")
            all_blocked = all(
                not _tcp_connect_ok(leader, ip_p, RAFT_PORT, timeout_s=1)
                and not _tcp_connect_ok(p, ip_l, RAFT_PORT, timeout_s=1)
                for p, ip_p in ips
            )
            
            if not all_blocked:
                # Fallback to tc
                tc_used = True
                sh_root(leader, "tc qdisc add dev eth0 root handle 1: prio || true")
                for p, ip_p in ips:
                    sh_root(
                        leader,
                        f"tc filter add dev eth0 protocol ip parent 1: prio 1 u32 match ip dst {ip_p} flowid 1:1 police rate 1bit burst 1 drop flowid :1 || true",
                    )
                for p, _ip in ips:
                    ip_l_peer = _get_ipv4(p, leader.name) or ip_l
                    sh_root(p, "tc qdisc add dev eth0 root handle 1: prio || true")
                    sh_root(
                        p,
                        f"tc filter add dev eth0 protocol ip parent 1: prio 1 u32 match ip dst {ip_l_peer} flowid 1:1 police rate 1bit burst 1 drop flowid :1 || true",
                    )
                sh(leader, "sleep 0.2")
                all_blocked = all(
                    not _tcp_connect_ok(leader, ip_p, RAFT_PORT, timeout_s=1)
                    and not _tcp_connect_ok(p, ip_l, RAFT_PORT, timeout_s=1)
                    for p, ip_p in ips
                )
                if not all_blocked:
                    raise AssertionError(
                        "partition_symmetric verify failed: traffic still flows"
                    )
                used_method = "tc"
        yield
    finally:
        if tc_used and used_method == "tc":
            try:
                sh_root(leader, "tc qdisc del dev eth0 root || true")
            except Exception:
                pass
            for p, _ in ips:
                try:
                    sh_root(p, "tc qdisc del dev eth0 root || true")
                except Exception:
                    pass
        elif used_method == "iptables":
            for n, cname in chains.items():
                _remove_iptables_chain(n, cname)
        else:
            for n, cidr in routes:
                sh_root(n, f"ip route del blackhole {cidr} || true")
            for n, cidr6 in routes6:
                sh_root(n, f"ip -6 route del blackhole {cidr6} || true")


@contextmanager
def partition_oneway(src, dst):
    ip = _get_ipv4(src, dst.name) or _get_ipv4(dst)
    method = None
    cname = None
    has_ipt = has_bin(src, "iptables")
    has_ip = has_bin(src, "ip")
    has_tc = has_bin(src, "tc")
    try:
        if has_ipt:
            method = "iptables"
            cname = f"CH_KEEPER_{int(time.time()*1000)}"
            _create_iptables_chain(src, cname)
            sh_root(
                src,
                f"iptables -w 2 -t filter -A {cname} -p tcp --dport {RAFT_PORT} -d {ip} -j DROP",
            )
            for ip6 in _get_ipv6_list(src, dst.name):
                sh_root(
                    src,
                    f"ip6tables -w 2 -t filter -A {cname} -p tcp --dport {RAFT_PORT} -d {ip6} -j DROP",
                )
            if not _check_iptables_chain(src, cname):
                if has_ip:
                    method = "iproute"
                    host = f"{ip}/32"
                    sh_root(src, f"ip route add blackhole {host} || true")
                    sh(src, "sleep 0.2")
                    sh_root(
                        src,
                        f"for ip6 in $(getent ahosts {dst.name} | awk '{{print $1}}' | grep ':' | sort -u); do ip -6 route add blackhole $ip6/128 || true; done",
                    )
                    if _tcp_connect_ok(src, ip, RAFT_PORT, timeout_s=1):
                        if has_tc:
                            method = "tc"
                            sh_root(
                                src,
                                f"tc qdisc add dev eth0 root handle 1: prio || true",
                            )
                            sh_root(
                                src,
                                f"tc filter add dev eth0 protocol ip parent 1: prio 1 u32 match ip dst {ip} flowid 1:1 police rate 1bit burst 1 drop flowid :1 || true",
                            )
                            sh(src, "sleep 0.2")
                            if _tcp_connect_ok(src, ip, RAFT_PORT, timeout_s=1):
                                raise AssertionError(
                                    "partition_oneway verify failed: traffic still flows"
                                )
                        else:
                            raise AssertionError(
                                "partition_oneway verify failed: iproute"
                            )
                    yield
                    return
                elif has_tc:
                    method = "tc"
                    sh_root(src, f"tc qdisc replace dev eth0 root netem loss 100%")
                    v = sh(src, "tc qdisc show dev eth0 | grep -q netem; echo $?")
                    if not _check_cmd_success(v) or _tcp_connect_ok(src, ip, RAFT_PORT, timeout_s=1):
                        raise AssertionError("partition_oneway verify failed: tc")
                    yield
                    return
                else:
                    raise AssertionError(
                        "partition_oneway: no available method (iptables/ip route/tc)"
                    )
            if _tcp_connect_ok(src, ip, RAFT_PORT, timeout_s=1):
                try:
                    sh_root(src, "tc qdisc add dev eth0 root handle 1: prio || true")
                    sh_root(
                        src,
                        f"tc filter add dev eth0 protocol ip parent 1: prio 1 u32 match ip dst {ip} flowid 1:1 police rate 1bit burst 1 drop flowid :1 || true",
                    )
                    sh(src, "sleep 0.2")
                    if _tcp_connect_ok(src, ip, RAFT_PORT, timeout_s=1):
                        raise AssertionError(
                            "partition_oneway verify failed: traffic still flows"
                        )
                    method = "tc"
                except Exception:
                    raise
            yield
        elif has_ip:
            method = "iproute"
            host = f"{ip}/32"
            sh_root(src, f"ip route add blackhole {host} || true")
            sh(src, "sleep 0.2")
            sh_root(
                src,
                f"for ip6 in $(getent ahosts {dst.name} | awk '{{print $1}}' | grep ':' | sort -u); do ip -6 route add blackhole $ip6/128 || true; done",
            )
            if _tcp_connect_ok(src, ip, RAFT_PORT, timeout_s=1):
                raise AssertionError(
                    "partition_oneway verify failed: traffic still flows"
                )
            yield
        elif has_tc:
            method = "tc"
            sh_root(src, f"tc qdisc replace dev eth0 root netem loss 100%")
            v = sh(src, "tc qdisc show dev eth0 | grep -q netem; echo $?")
            if not _check_cmd_success(v):
                raise AssertionError("partition_oneway verify failed: tc")
            if _tcp_connect_ok(src, ip, RAFT_PORT, timeout_s=1):
                raise AssertionError(
                    "partition_oneway verify failed: traffic still flows"
                )
            yield
        else:
            raise AssertionError(
                "partition_oneway: no available method (iptables/ip route/tc)"
            )
    finally:
        if method == "iptables" and cname:
            _remove_iptables_chain(src, cname)
        elif method == "iproute":
            host = f"{ip}/32"
            sh_root(
                src,
                f"ip route del blackhole {host} || true; tc qdisc del dev eth0 root || true",
            )
        elif method == "tc":
            sh_root(src, f"tc qdisc del dev eth0 root || true")


@contextmanager
def dns_blackhole(node):
    try:
        cname = f"CH_KEEPER_{int(time.time()*1000)}"
        _create_iptables_chain(node, cname)
        sh_root(node, f"iptables -w 2 -t filter -A {cname} -p udp --dport 53 -j DROP")
        sh_root(node, f"iptables -w 2 -t filter -A {cname} -p tcp --dport 53 -j DROP")
        sh_root(node, f"ip6tables -w 2 -t filter -A {cname} -p udp --dport 53 -j DROP")
        sh_root(node, f"ip6tables -w 2 -t filter -A {cname} -p tcp --dport 53 -j DROP")
        c1 = sh(
            node,
            f"export PATH=\"$PATH:/usr/sbin:/sbin\"; iptables -w 2 -t filter -S {cname} | grep -q -- '--dport 53' ; echo $?",
        )
        c2 = sh(
            node,
            f"export PATH=\"$PATH:/usr/sbin:/sbin\"; iptables -w 2 -t filter -S OUTPUT | grep -q -- ' -j {cname}' ; echo $?",
        )
        ok = _check_cmd_success(c1) and _check_cmd_success(c2)
        if not ok:
            bkp = "/etc/resolv.conf.keep"
            try:
                sh(
                    node,
                    f"cp /etc/resolv.conf {bkp} || true",
                    user="root",
                    privileged=True,
                )
                sh(
                    node,
                    "printf 'nameserver 127.0.0.2\noptions timeout:1 attempts:1\n' > /etc/resolv.conf",
                    user="root",
                    privileged=True,
                )
                v = sh(
                    node,
                    "getent hosts this.should.never.exist.invalid >/dev/null 2>&1; echo $?",
                )
                if str(v.get("out", " ")).strip().endswith("0"):
                    raise AssertionError("dns_blackhole verify failed")
            except Exception:
                raise AssertionError("dns_blackhole verify failed")
        yield
    finally:
        _remove_iptables_chain(node, cname)
        try:
            sh_root(
                node,
                "[ -f /etc/resolv.conf.keep ] && mv -f /etc/resolv.conf.keep /etc/resolv.conf || true",
            )
        except Exception:
            pass


def _tcp_connect_ok(node, ip, port, timeout_s=1):
    """Check if TCP connection to ip:port succeeds."""
    r = sh(
        node,
        f"timeout {timeout_s} bash -c '</dev/tcp/{ip}/{port}' >/dev/null 2>&1; echo $?",
    )
    return _check_cmd_success(r)


_NETEM_KEYS = ("delay_ms", "jitter_ms", "loss_pct", "reorder", "duplicate", "corrupt")


def _create_emit_fn(kind, ctx):
    """Create an emit function for fault events."""
    lock = threading.Lock()
    
    def _emit(node, phase, extra=None):
        try:
            ev = {
                "ts": ts_ms(),
                "kind": kind,
                "node": str(getattr(node, "name", "")),
                "phase": str(phase),
            }
            if isinstance(extra, dict) and extra:
                ev.update(extra)
            with lock:
                (ctx.setdefault("fault_events", [])).append(ev)
        except Exception:
            pass
    return _emit


@register_fault("netem")
def _f_netem(ctx, nodes, leader, step):
    dur = int(step.get("duration_s", DEFAULT_FAULT_DURATION_S))
    netem_args = {k: v for k, v in step.items() if k in _NETEM_KEYS}
    _emit = _create_emit_fn("netem", ctx)

    def _qdisc_has_netem(node):
        r = sh_strict(
            node, "tc qdisc show dev eth0 | grep -q netem; echo $?", timeout=10
        )
        return _check_cmd_success(r)

    def _run_one(t):
        t0 = time.time()
        _emit(t, "start", {"duration_s": int(dur), **netem_args})
        with netem(t, **netem_args):
            _emit(t, "apply_ok")
            if dur >= 4:
                early = min(2.0, float(dur) / 4.0)
                time.sleep(max(0.0, early))
                if not _qdisc_has_netem(t):
                    _emit(t, "active_verify_failed", {"when": "early"})
                    raise AssertionError("netem active verify failed (early)")
                _emit(t, "active_ok", {"when": "early"})

                remaining = max(0.0, float(dur) - early)
                late = min(2.0, max(0.0, remaining / 2.0))
                time.sleep(max(0.0, remaining - late))
                if not _qdisc_has_netem(t):
                    _emit(t, "active_verify_failed", {"when": "late"})
                    raise AssertionError("netem active verify failed (late)")
                _emit(t, "active_ok", {"when": "late"})
                time.sleep(max(0.0, late))
            else:
                time.sleep(dur)
        if _qdisc_has_netem(t):
            _emit(t, "cleanup_verify_failed")
            raise AssertionError("netem cleanup verify failed")
        _emit(t, "cleanup_ok", {"elapsed_s": time.time() - t0})

    targets = resolve_targets(step.get("on", "leader"), nodes, leader)
    for_each_target(step, nodes, leader, _run_one)

    evs = (ctx or {}).get("fault_events") or []
    cleaned = {
        e.get("node")
        for e in evs
        if e.get("kind") == "netem" and e.get("phase") == "cleanup_ok"
    }
    expected = {str(getattr(t, "name", "")) for t in targets}
    missing = sorted([n for n in expected if n and n not in cleaned])
    if missing:
        raise AssertionError(
            "netem did not complete cleanup for targets: " + ", ".join(missing)
        )


@register_fault("tbf")
def _f_tbf(ctx, nodes, leader, step):
    dur = int(step.get("duration_s", DEFAULT_FAULT_DURATION_S))

    def _run_one(t):
        with tbf(t, step.get("rate", "10mbit")):
            time.sleep(dur)

    for_each_target(step, nodes, leader, _run_one)


@register_fault("partition_symmetric")
def _f_partition_symmetric(ctx, nodes, leader, step):
    target = resolve_targets(step.get("on", "leader"), nodes, leader)
    peers = [n for n in nodes if n.name != target[0].name]
    time_s = int(step.get("duration_s", DEFAULT_FAULT_DURATION_S))
    with partition_symmetric(target[0], peers):
        time.sleep(time_s)


@register_fault("partition_oneway")
def _f_partition_oneway(ctx, nodes, leader, step):
    dur = int(step.get("duration_s", DEFAULT_FAULT_DURATION_S))

    def _run_one(src):
        others = [n for n in nodes if n.name != src.name]
        if not others:
            # Skip if no other nodes (can't partition with only one node)
            return
        dst = others[0]
        with partition_oneway(src, dst):
            time.sleep(dur)

    for_each_target(step, nodes, leader, _run_one)


@register_fault("dns_blackhole")
def _f_dns_blackhole(ctx, nodes, leader, step):
    dur = int(step.get("duration_s", DEFAULT_FAULT_DURATION_S))

    def _run_one(t):
        with dns_blackhole(t):
            time.sleep(dur)

    for_each_target(step, nodes, leader, _run_one)


@register_fault("partition_symmetric_during")
def _f_ps_during(ctx, nodes, leader, step):
    from .base import apply_step  # local import to avoid cycles

    target = resolve_targets(step.get("on", "leader"), nodes, leader)
    peers = [n for n in nodes if n.name != target[0].name]
    with partition_symmetric(target[0], peers):
        for sub in step.get("steps", []) or []:
            apply_step(sub, nodes, leader, ctx)
