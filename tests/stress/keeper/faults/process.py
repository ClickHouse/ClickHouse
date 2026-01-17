import time

from keeper.framework.core.registry import register_fault
from keeper.framework.core.settings import CLIENT_PORT, DEFAULT_FAULT_DURATION_S
from keeper.framework.core.util import for_each_target, resolve_targets, sh, sh_root


def _proc_exists(node):
    out = sh(
        node,
        "pgrep -x clickhouse >/dev/null 2>&1 || pgrep -x clickhouse-server >/dev/null 2>&1; echo $?",
        timeout=10,
    )
    return out.get("out", " ").strip().endswith("0")


def kill(node):
    if _proc_exists(node):
        sh(node, "pkill -9 -x clickhouse || pkill -9 clickhouse-server", timeout=10)


def stop(node):
    if _proc_exists(node):
        sh(
            node,
            "pkill -STOP -x clickhouse || pkill -STOP clickhouse-server",
            timeout=10,
        )


def cont(node):
    if _proc_exists(node):
        sh(
            node,
            "pkill -CONT -x clickhouse || pkill -CONT clickhouse-server",
            timeout=10,
        )


def rcvr(node):
    sh(node, f"printf 'rcvr\n' | nc -w1 127.0.0.1 {CLIENT_PORT}", timeout=5)


def rqld(node):
    sh(node, f"printf 'rqld\n' | nc -w1 127.0.0.1 {CLIENT_PORT}", timeout=5)


def ydld(node):
    sh(node, f"printf 'ydld\n' | nc -w1 127.0.0.1 {CLIENT_PORT}", timeout=5)


def cpu_hog(node, seconds=60):
    sh(node, f"timeout {seconds} sh -c 'while :; do :; done' &", timeout=10)


def fd_pressure(node, fds=5000, seconds=60):
    sh(
        node,
        (
            "set -euo pipefail; "
            "cat > /tmp/fd_pressure.py <<'PY'\n"
            "import time, os\n"
            f"fs=[open('/tmp/fd'+str(i),'w') for i in range({int(fds)})]\n"
            "open('/tmp/fd_pressure.ready','w').write('ok')\n"
            f"time.sleep({int(seconds)})\n"
            "PY\n"
            "python3 /tmp/fd_pressure.py & echo $! > /tmp/fd_pressure.pid; "
            "for i in $(seq 1 20); do [ -f /tmp/fd_pressure.ready ] && break; sleep 0.1; done; "
            "[ -f /tmp/fd_pressure.ready ]"
        ),
        timeout=int(max(30, seconds + 60)),
    )


def mem_hog_block(node, mb=512, seconds=60):
    sh(
        node,
        f"python3 - <<'PY'\nimport time\n_ = bytearray({int( max(1, mb) )}*1024*1024)\ntime.sleep({int( max(1, seconds) )})\nPY",
        timeout=int(max(30, seconds + 30)),
    )


def clock_skew(node, seconds):
    sh_root(node, f"date -s '@$(( $(date +%s) + {int(seconds)} ))'")


def time_strobe(node, swings=6, step_s=500, interval_s=5):
    for i in range(swings):
        clock_skew(node, step_s if i % 2 == 0 else -step_s)
        sh(node, f"sleep {interval_s}", timeout=10)


def nic_flap(node, down_s=5):
    sh_root(node, "ip link set eth0 down")
    v = sh(node, "ip link show dev eth0 | grep -qi 'state down'; echo $?", timeout=10)
    ok = str(v.get("out", " ")).strip().endswith("0")
    if not ok:
        raise AssertionError("nic_flap down verify failed")
    sh(node, f"sleep {int(down_s)}", timeout=int(max(10, down_s + 5)))
    sh_root(node, "ip link set eth0 up")


@register_fault("kill")
def _f_kill(ctx, nodes, leader, step):
    for_each_target(step, nodes, leader, kill)


@register_fault("stop")
def _f_stop(ctx, nodes, leader, step):
    for_each_target(step, nodes, leader, stop)


@register_fault("cont")
def _f_cont(ctx, nodes, leader, step):
    for_each_target(step, nodes, leader, cont)


@register_fault("rcvr")
def _f_rcvr(ctx, nodes, leader, step):
    for_each_target(step, nodes, leader, rcvr)


@register_fault("rqld")
def _f_rqld(ctx, nodes, leader, step):
    for_each_target(step, nodes, leader, rqld)


@register_fault("ydld")
def _f_ydld(ctx, nodes, leader, step):
    for_each_target(step, nodes, leader, ydld)


@register_fault("cpu_hog")
def _f_cpu_hog(ctx, nodes, leader, step):
    secs = int(step.get("seconds", DEFAULT_FAULT_DURATION_S))
    for_each_target(step, nodes, leader, lambda t: cpu_hog(t, secs))


@register_fault("fd_pressure")
def _f_fd_pressure(ctx, nodes, leader, step):
    secs, fds = int(step.get("seconds", DEFAULT_FAULT_DURATION_S)), int(
        step.get("fds", 5000)
    )
    for_each_target(step, nodes, leader, lambda t: fd_pressure(t, fds, secs))


@register_fault("mem_hog")
def _f_mem_hog(ctx, nodes, leader, step):
    mb, secs = int(step.get("mb", 512)), int(
        step.get("seconds", DEFAULT_FAULT_DURATION_S)
    )
    for_each_target(step, nodes, leader, lambda t: mem_hog_block(t, mb, secs))


@register_fault("clock_skew")
def _f_clock_skew(ctx, nodes, leader, step):
    secs = int(step.get("seconds", 500))
    for_each_target(step, nodes, leader, lambda t: clock_skew(t, secs))


@register_fault("nic_flap")
def _f_nic_flap(ctx, nodes, leader, step):
    down_s = int(step.get("down_s", 5))
    for_each_target(step, nodes, leader, lambda t: nic_flap(t, down_s))


@register_fault("time_strobe")
def _f_time_strobe(ctx, nodes, leader, step):
    swings = int(step.get("swings", 6))
    step_s = int(step.get("step_s", 500))
    interval_s = int(step.get("interval_s", 5))

    def _run_one(t):
        time_strobe(t, swings=swings, step_s=step_s, interval_s=interval_s)

    for_each_target(step, nodes, leader, _run_one)


@register_fault("stop_cont")
def _f_stop_cont(ctx, nodes, leader, step):
    target = resolve_targets(step.get("on", "leader"), nodes, leader)
    sleep_s = step.get("sleep_s", 1.0)
    for _ in range(step.get("count", 10)):
        for t in target:
            stop(t)
        time.sleep(sleep_s)
        for t in target:
            cont(t)
        time.sleep(sleep_s)


@register_fault("stress_ng")
def _f_stress_ng(ctx, nodes, leader, step):
    secs = int(step.get("seconds", DEFAULT_FAULT_DURATION_S))
    stress_args = [
        f"--{k} {int(step.get(k, 0))}"
        for k in ("cpu", "vm", "io", "hdd", "sched")
        if int(step.get(k, 0)) > 0
    ]
    if step.get("vm_bytes"):
        stress_args.append(f"--vm-bytes {step['vm_bytes']}")
    if not stress_args:
        stress_args = ["--cpu 1"]
    cmd = f"TMPDIR=/tmp stress-ng {' '.join(stress_args)} --temp-path /tmp --timeout {secs}s --metrics-brief"

    def _run_one(t):
        sh(t, "mkdir -p /tmp && chmod 1777 /tmp || true", timeout=10)
        sh(t, cmd, timeout=int(max(30, secs + 60)))

    for_each_target(step, nodes, leader, _run_one)
