import os
import time
from contextlib import contextmanager

from ..framework.core.registry import register_fault
from ..framework.core.settings import DEFAULT_FAULT_DURATION_S, RAFT_PORT
from ..framework.core.util import has_bin, resolve_targets, sh, sh_root

# Always strict enforcement
STRICT_FAULTS = True


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
            args += [
                (
                    f"delay {int(delay_ms)}ms {int(jitter_ms)}ms"
                    if jitter_ms
                    else f"delay {int(delay_ms)}ms"
                )
            ]
        if loss_pct:
            args += [f"loss {int(loss_pct)}%"]
        if reorder:
            args += [f"reorder {int(reorder)}% 50%"]
        if duplicate:
            args += [f"duplicate {int(duplicate)}%"]
        if corrupt:
            args += [f"corrupt {int(corrupt)}%"]
        try:
            sh_root(
                node,
                f"tc qdisc replace dev eth0 root netem {' '.join(args) if args else 'delay 0ms'}",
            )
            applied = True
            v = sh(node, "tc qdisc show dev eth0 | grep -q netem; echo $?")
            ok = str(v.get("out", " ")).strip().endswith("0")
            if not ok:
                raise AssertionError("netem verify failed")
        except Exception as e:
            raise AssertionError(f"netem apply failed: {e}")
        yield
    finally:
        if applied:
            try:
                sh_root(node, "tc qdisc del dev eth0 root || true")
            except Exception:
                pass


@contextmanager
def tbf(node, rate="10mbit"):
    applied = False
    try:
        try:
            sh_root(
                node,
                f"tc qdisc replace dev eth0 root tbf rate {rate} burst 32kb latency 400ms",
            )
            applied = True
            v = sh(node, "tc qdisc show dev eth0 | grep -q tbf; echo $?")
            ok = str(v.get("out", " ")).strip().endswith("0")
            if not ok:
                raise AssertionError("tbf verify failed")
        except Exception as e:
            raise AssertionError(f"tbf apply failed: {e}")
        yield
    finally:
        if applied:
            try:
                sh_root(node, "tc qdisc del dev eth0 root || true")
            except Exception:
                pass


@contextmanager
def partition_symmetric(leader, peers):
    try:
        ips = []
        ip_l = sh(leader, "hostname -i")["out"].split()[0]
        chains = {}
        used_method = "iptables"
        routes = []
        routes6 = []
        cname_l = f"CH_KEEPER_{int(time.time()*1000)}"
        chains[leader] = cname_l
        sh_root(leader, f"iptables -w 2 -t filter -N {cname_l} || true")
        sh_root(
            leader,
            f"(iptables -w 2 -t filter -S OUTPUT | grep -q -- ' -j {cname_l}') || iptables -w 2 -t filter -I OUTPUT 1 -j {cname_l}",
        )
        sh_root(leader, f"ip6tables -w 2 -t filter -N {cname_l} || true")
        sh_root(
            leader,
            f"(ip6tables -w 2 -t filter -S OUTPUT | grep -q -- ' -j {cname_l}') || ip6tables -w 2 -t filter -I OUTPUT 1 -j {cname_l}",
        )
        for p in peers:
            ip_p = sh(
                leader,
                f"getent ahosts {p.name} | awk '{{print $1}}' | grep -v ':' | head -n1 || true",
            )["out"].strip()
            if not ip_p:
                ip_p = sh(p, "hostname -i")["out"].split()[0]
            ips.append((p, ip_p))
            sh_root(
                leader,
                f"iptables -w 2 -t filter -A {cname_l} -p tcp --dport {RAFT_PORT} -d {ip_p} -j DROP",
            )
            sh_root(
                leader,
                f"for ip6 in $(getent ahosts {p.name} | awk '{{print $1}}' | grep ':' | sort -u); do ip6tables -w 2 -t filter -A {cname_l} -p tcp --dport {RAFT_PORT} -d $ip6 -j DROP; done",
            )
        v1 = sh(
            leader, f"iptables -w 2 -t filter -S {cname_l} >/dev/null 2>&1; echo $?"
        )
        v2 = sh(
            leader,
            f"iptables -w 2 -t filter -S OUTPUT | grep -q -- ' -j {cname_l}' ; echo $?",
        )
        ok_l = str(v1.get("out", " ")).strip().endswith("0") and str(
            v2.get("out", " ")
        ).strip().endswith("0")
        if not ok_l:
            used_method = "iproute"
        for p, ip_p in ips:
            cname_p = f"CH_KEEPER_{int(time.time()*1000)}"
            chains[p] = cname_p
            sh_root(p, f"iptables -w 2 -t filter -N {cname_p} || true")
            sh_root(
                p,
                f"(iptables -w 2 -t filter -S OUTPUT | grep -q -- ' -j {cname_p}') || iptables -w 2 -t filter -I OUTPUT 1 -j {cname_p}",
            )
            ip_l_peer = (
                sh(
                    p,
                    f"getent ahosts {leader.name} | awk '{{print $1}}' | grep -v ':' | head -n1 || true",
                )["out"].strip()
                or ip_l
            )
            sh_root(
                p,
                f"iptables -w 2 -t filter -A {cname_p} -p tcp --dport {RAFT_PORT} -d {ip_l_peer} -j DROP",
            )
            sh_root(p, f"ip6tables -w 2 -t filter -N {cname_p} || true")
            sh_root(
                p,
                f"(ip6tables -w 2 -t filter -S OUTPUT | grep -q -- ' -j {cname_p}') || ip6tables -w 2 -t filter -I OUTPUT 1 -j {cname_p}",
            )
            sh_root(
                p,
                f"for ip6 in $(getent ahosts {leader.name} | awk '{{print $1}}' | grep ':' | sort -u); do ip6tables -w 2 -t filter -A {cname_p} -p tcp --dport {RAFT_PORT} -d $ip6 -j DROP; done",
            )
            w1 = sh(p, f"iptables -w 2 -t filter -S {cname_p} >/dev/null 2>&1; echo $?")
            w2 = sh(
                p,
                f"iptables -w 2 -t filter -S OUTPUT | grep -q -- ' -j {cname_p}' ; echo $?",
            )
            ok_p = str(w1.get("out", " ")).strip().endswith("0") and str(
                w2.get("out", " ")
            ).strip().endswith("0")
            if not ok_p:
                used_method = "iproute"
        all_blocked = True
        for p, ip_p in ips:
            if _tcp_connect_ok(leader, ip_p, RAFT_PORT, timeout_s=1):
                all_blocked = False
                break
            if _tcp_connect_ok(p, ip_l, RAFT_PORT, timeout_s=1):
                all_blocked = False
                break
        if used_method == "iptables" and not all_blocked:
            used_method = "iproute"
        if used_method == "iproute":
            for n, cname in chains.items():
                try:
                    sh_root(
                        n,
                        f"iptables -w 2 -t filter -D OUTPUT -j {cname} || true; iptables -w 2 -t filter -F {cname} || true; iptables -w 2 -t filter -X {cname} || true",
                    )
                    sh_root(
                        n,
                        f"ip6tables -w 2 -t filter -D OUTPUT -j {cname} || true; ip6tables -w 2 -t filter -F {cname} || true; ip6tables -w 2 -t filter -X {cname} || true",
                    )
                except Exception:
                    pass
            for p, ip_p in ips:
                sh_root(leader, f"ip route add blackhole {ip_p}/32 || true")
                routes.append((leader, f"{ip_p}/32"))
                sh_root(
                    leader,
                    f"for ip6 in $(getent ahosts {p.name} | awk '{{print $1}}' | grep ':' | sort -u); do ip -6 route add blackhole $ip6/128 || true; echo $ip6; done",
                )
                out6 = sh(
                    leader,
                    f"getent ahosts {p.name} | awk '{{print $1}}' | grep ':' | sort -u || true",
                )
                for line in out6.get("out", "\n").split():
                    if ":" in line:
                        routes6.append((leader, f"{line}/128"))
            for p, ip_p in ips:
                sh_root(p, f"ip route add blackhole {ip_l}/32 || true")
                routes.append((p, f"{ip_l}/32"))
                out6 = sh(
                    p,
                    f"getent ahosts {leader.name} | awk '{{print $1}}' | grep ':' | sort -u || true",
                )
                sh_root(
                    p,
                    f"for ip6 in $(getent ahosts {leader.name} | awk '{{print $1}}' | grep ':' | sort -u); do ip -6 route add blackhole $ip6/128 || true; done",
                )
                for line in out6.get("out", "\n").split():
                    if ":" in line:
                        routes6.append((p, f"{line}/128"))
            sh(leader, "sleep 0.2")
            all_blocked = True
            for p, ip_p in ips:
                if _tcp_connect_ok(leader, ip_p, RAFT_PORT, timeout_s=1):
                    all_blocked = False
                    break
                if _tcp_connect_ok(p, ip_l, RAFT_PORT, timeout_s=1):
                    all_blocked = False
                    break
            if not all_blocked:
                tc_used = True
                try:
                    sh_root(leader, "tc qdisc add dev eth0 root handle 1: prio || true")
                    for p, ip_p in ips:
                        sh_root(
                            leader,
                            f"tc filter add dev eth0 protocol ip parent 1: prio 1 u32 match ip dst {ip_p} flowid 1:1 police rate 1bit burst 1 drop flowid :1 || true",
                        )
                    for p, _ip in ips:
                        ip_l_peer = (
                            sh(
                                p,
                                f"getent ahosts {leader.name} | awk '{{print $1}}' | grep -v ':' | head -n1 || true",
                            )["out"].strip()
                            or ip_l
                        )
                        sh_root(p, "tc qdisc add dev eth0 root handle 1: prio || true")
                        sh_root(
                            p,
                            f"tc filter add dev eth0 protocol ip parent 1: prio 1 u32 match ip dst {ip_l_peer} flowid 1:1 police rate 1bit burst 1 drop flowid :1 || true",
                        )
                    sh(leader, "sleep 0.2")
                    all_blocked = True
                    for p, ip_p in ips:
                        if _tcp_connect_ok(leader, ip_p, RAFT_PORT, timeout_s=1):
                            all_blocked = False
                            break
                        if _tcp_connect_ok(p, ip_l, RAFT_PORT, timeout_s=1):
                            all_blocked = False
                            break
                    if not all_blocked:
                        raise AssertionError(
                            "partition_symmetric verify failed: traffic still flows"
                        )
                    used_method = "tc"
                except Exception:
                    raise
        yield
    finally:
        if "tc_used" in locals() and used_method == "tc":
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
                sh_root(
                    n,
                    f"iptables -w 2 -t filter -D OUTPUT -j {cname} || true; iptables -w 2 -t filter -F {cname} || true; iptables -w 2 -t filter -X {cname} || true",
                )
                sh_root(
                    n,
                    f"ip6tables -w 2 -t filter -D OUTPUT -j {cname} || true; ip6tables -w 2 -t filter -F {cname} || true; ip6tables -w 2 -t filter -X {cname} || true",
                )
        else:
            for n, cidr in routes:
                sh_root(n, f"ip route del blackhole {cidr} || true")
            for n, cidr6 in routes6:
                sh_root(n, f"ip -6 route del blackhole {cidr6} || true")


@contextmanager
def partition_oneway(src, dst):
    ip = sh(
        src,
        f"getent ahosts {dst.name} | awk '{{print $1}}' | grep -v ':' | head -n1 || true",
    )["out"].strip()
    if not ip:
        ip = sh(dst, "hostname -i")["out"].split()[0]
    method = None
    cname = None
    has_ipt = has_bin(src, "iptables")
    has_ip = has_bin(src, "ip")
    has_tc = has_bin(src, "tc")
    try:
        if has_ipt:
            method = "iptables"
            cname = f"CH_KEEPER_{int(time.time()*1000)}"
            sh_root(src, f"iptables -w 2 -t filter -N {cname} || true")
            sh_root(
                src,
                f"(iptables -w 2 -t filter -S OUTPUT | grep -q -- ' -j {cname}') || iptables -w 2 -t filter -I OUTPUT 1 -j {cname}",
            )
            sh_root(
                src,
                f"iptables -w 2 -t filter -A {cname} -p tcp --dport {RAFT_PORT} -d {ip} -j DROP",
            )
            sh_root(src, f"ip6tables -w 2 -t filter -N {cname} || true")
            sh_root(
                src,
                f"(ip6tables -w 2 -t filter -S OUTPUT | grep -q -- ' -j {cname}') || ip6tables -w 2 -t filter -I OUTPUT 1 -j {cname}",
            )
            sh_root(
                src,
                f"for ip6 in $(getent ahosts {dst.name} | awk '{{print $1}}' | grep ':' | sort -u); do ip6tables -w 2 -t filter -A {cname} -p tcp --dport {RAFT_PORT} -d $ip6 -j DROP; done",
            )
            c1 = sh(src, f"iptables -w 2 -t filter -S {cname} >/dev/null 2>&1; echo $?")
            c2 = sh(
                src,
                f"iptables -w 2 -t filter -S OUTPUT | grep -q -- ' -j {cname}' ; echo $?",
            )
            ok = str(c1.get("out", " ")).strip().endswith("0") and str(
                c2.get("out", " ")
            ).strip().endswith("0")
            if not ok:
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
                    ok_tc = str(v.get("out", " ")).strip().endswith("0")
                    if not ok_tc or _tcp_connect_ok(src, ip, RAFT_PORT, timeout_s=1):
                        raise AssertionError("partition_oneway verify failed: tc")
                    yield
                    return
                else:
                    if STRICT_FAULTS:
                        raise AssertionError(
                            "partition_oneway: no available method (iptables/ip route/tc)"
                        )
                    yield
                    return
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
            ok = str(v.get("out", " ")).strip().endswith("0")
            if not ok:
                raise AssertionError("partition_oneway verify failed: tc")
            if _tcp_connect_ok(src, ip, RAFT_PORT, timeout_s=1):
                raise AssertionError(
                    "partition_oneway verify failed: traffic still flows"
                )
            yield
        else:
            method = "none"
            if STRICT_FAULTS:
                raise AssertionError(
                    "partition_oneway: no available method (iptables/ip route/tc)"
                )
            yield
    finally:
        if method == "iptables" and cname:
            sh_root(
                src,
                f"iptables -w 2 -t filter -D OUTPUT -j {cname} || true; iptables -w 2 -t filter -F {cname} || true; iptables -w 2 -t filter -X {cname} || true",
            )
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
        sh_root(node, f"iptables -w 2 -t filter -N {cname} || true")
        sh_root(
            node,
            f"(iptables -w 2 -t filter -S OUTPUT | grep -q -- ' -j {cname}') || iptables -w 2 -t filter -I OUTPUT 1 -j {cname}",
        )
        sh_root(node, f"iptables -w 2 -t filter -A {cname} -p udp --dport 53 -j DROP")
        sh_root(node, f"iptables -w 2 -t filter -A {cname} -p tcp --dport 53 -j DROP")
        sh_root(node, f"ip6tables -w 2 -t filter -N {cname} || true")
        sh_root(
            node,
            f"(ip6tables -w 2 -t filter -S OUTPUT | grep -q -- ' -j {cname}') || ip6tables -w 2 -t filter -I OUTPUT 1 -j {cname}",
        )
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
        ok = str(c1.get("out", " ")).strip().endswith("0") and str(
            c2.get("out", " ")
        ).strip().endswith("0")
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
        sh_root(
            node,
            f"iptables -w 2 -t filter -D OUTPUT -j {cname} || true; iptables -w 2 -t filter -F {cname} || true; iptables -w 2 -t filter -X {cname} || true",
        )
        try:
            sh_root(
                node,
                "[ -f /etc/resolv.conf.keep ] && mv -f /etc/resolv.conf.keep /etc/resolv.conf || true",
            )
        except Exception:
            pass


def _tcp_connect_ok(node, ip, port, timeout_s=1):
    try:
        r = sh(
            node,
            f"timeout {int(timeout_s)} bash -c '</dev/tcp/{ip}/{int(port)}' >/dev/null 2>&1; echo $?",
        )
        return str(r.get("out", " ")).strip().endswith("0")
    except Exception:
        return False


def _for_each_target(step, nodes, leader, run_one):
    target = resolve_targets(step.get("on", "leader"), nodes, leader)
    if step.get("target_parallel"):
        import threading as _th

        errors = []

        def _wrap(t):
            try:
                run_one(t)
            except Exception as e:
                errors.append(e)

        ths = []
        for t in target:
            th = _th.Thread(target=_wrap, args=(t,), daemon=True)
            th.start()
            ths.append(th)
        for th in ths:
            th.join()
        if errors:
            raise errors[0]
    else:
        for t in target:
            run_one(t)


@register_fault("netem")
def _f_netem(ctx, nodes, leader, step):
    dur = int(step.get("duration_s", DEFAULT_FAULT_DURATION_S))

    def _run_one(t):
        with netem(
            t,
            **{
                k: v
                for k, v in step.items()
                if k
                in (
                    "delay_ms",
                    "jitter_ms",
                    "loss_pct",
                    "reorder",
                    "duplicate",
                    "corrupt",
                )
            },
        ):
            time.sleep(dur)

    _for_each_target(step, nodes, leader, _run_one)


@register_fault("tbf")
def _f_tbf(ctx, nodes, leader, step):
    dur = int(step.get("duration_s", DEFAULT_FAULT_DURATION_S))

    def _run_one(t):
        with tbf(t, step.get("rate", "10mbit")):
            time.sleep(dur)

    _for_each_target(step, nodes, leader, _run_one)


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
        dst = [n for n in nodes if n.name != src.name][0]
        with partition_oneway(src, dst):
            time.sleep(dur)

    _for_each_target(step, nodes, leader, _run_one)


@register_fault("dns_blackhole")
def _f_dns_blackhole(ctx, nodes, leader, step):
    dur = int(step.get("duration_s", DEFAULT_FAULT_DURATION_S))

    def _run_one(t):
        with dns_blackhole(t):
            time.sleep(dur)

    _for_each_target(step, nodes, leader, _run_one)


@register_fault("partition_symmetric_during")
def _f_ps_during(ctx, nodes, leader, step):
    from .base import apply_step  # local import to avoid cycles

    target = resolve_targets(step.get("on", "leader"), nodes, leader)
    peers = [n for n in nodes if n.name != target[0].name]
    with partition_symmetric(target[0], peers):
        for sub in step.get("steps", []) or []:
            apply_step(sub, nodes, leader, ctx)
