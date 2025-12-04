import time

from ..framework.core.registry import register_fault
from ..framework.core.settings import CLIENT_PORT, DEFAULT_FAULT_DURATION_S
from ..framework.core.util import resolve_targets, sh, sh_root


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


def _proc_exists(node):
    out = sh(
        node,
        "pgrep -x clickhouse >/dev/null 2>&1 || pgrep -x clickhouse-server >/dev/null 2>&1; echo $?",
    )
    return out.get("out", " ").strip().endswith("0")


def kill(node):
    if _proc_exists(node):
        sh(node, "pkill -9 -x clickhouse || pkill -9 clickhouse-server")


def stop(node):
    if _proc_exists(node):
        sh(node, "pkill -STOP -x clickhouse || pkill -STOP clickhouse-server")


def cont(node):
    if _proc_exists(node):
        sh(node, "pkill -CONT -x clickhouse || pkill -CONT clickhouse-server")


def rcvr(node):
    sh(node, f"printf 'rcvr\n' | nc -w1 127.0.0.1 {CLIENT_PORT}")


def rqld(node):
    sh(node, f"printf 'rqld\n' | nc -w1 127.0.0.1 {CLIENT_PORT}")


def ydld(node):
    sh(node, f"printf 'ydld\n' | nc -w1 127.0.0.1 {CLIENT_PORT}")


def cpu_hog(node, seconds=60):
    sh(node, f"timeout {seconds} sh -c 'while :; do :; done' &")


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
    )


def mem_hog_block(node, mb=512, seconds=60):
    sh(
        node,
        f"python3 - <<'PY'\nimport time\n_ = bytearray({int( max(1, mb) )}*1024*1024)\ntime.sleep({int( max(1, seconds) )})\nPY",
    )


def clock_skew(node, seconds):
    sh_root(node, f"date -s '@$(( $(date +%s) + {int(seconds)} ))'")


def time_strobe(node, swings=6, step_s=500, interval_s=5):
    for i in range(swings):
        clock_skew(node, step_s if i % 2 == 0 else -step_s)
        sh(node, f"sleep {interval_s}")


def nic_flap(node, down_s=5):
    sh_root(node, "ip link set eth0 down")
    v = sh(node, "ip link show dev eth0 | grep -qi 'state down'; echo $?")
    ok = str(v.get("out", " ")).strip().endswith("0")
    if not ok:
        raise AssertionError("nic_flap down verify failed")
    sh(node, f"sleep {int(down_s)}")
    sh_root(node, "ip link set eth0 up")


@register_fault("kill")
def _f_kill(ctx, nodes, leader, step):
    target = resolve_targets(step.get("on", "leader"), nodes, leader)
    [kill(t) for t in target]


@register_fault("stop")
def _f_stop(ctx, nodes, leader, step):
    target = resolve_targets(step.get("on", "leader"), nodes, leader)
    [stop(t) for t in target]


@register_fault("cont")
def _f_cont(ctx, nodes, leader, step):
    target = resolve_targets(step.get("on", "leader"), nodes, leader)
    [cont(t) for t in target]


@register_fault("rcvr")
def _f_rcvr(ctx, nodes, leader, step):
    target = resolve_targets(step.get("on", "leader"), nodes, leader)
    [rcvr(t) for t in target]


@register_fault("rqld")
def _f_rqld(ctx, nodes, leader, step):
    target = resolve_targets(step.get("on", "leader"), nodes, leader)
    [rqld(t) for t in target]


@register_fault("ydld")
def _f_ydld(ctx, nodes, leader, step):
    target = resolve_targets(step.get("on", "leader"), nodes, leader)
    [ydld(t) for t in target]


@register_fault("cpu_hog")
def _f_cpu_hog(ctx, nodes, leader, step):
    secs = int(step.get("seconds", DEFAULT_FAULT_DURATION_S))

    def _run_one(t):
        cpu_hog(t, secs)

    _for_each_target(step, nodes, leader, _run_one)


@register_fault("fd_pressure")
def _f_fd_pressure(ctx, nodes, leader, step):
    secs = int(step.get("seconds", DEFAULT_FAULT_DURATION_S))
    fds = int(step.get("fds", 5000))

    def _run_one(t):
        fd_pressure(t, fds, secs)

    _for_each_target(step, nodes, leader, _run_one)


@register_fault("mem_hog")
def _f_mem_hog(ctx, nodes, leader, step):
    mb = int(step.get("mb", 512))
    secs = int(step.get("seconds", DEFAULT_FAULT_DURATION_S))

    def _run_one(t):
        mem_hog_block(t, mb, secs)

    _for_each_target(step, nodes, leader, _run_one)


@register_fault("clock_skew")
def _f_clock_skew(ctx, nodes, leader, step):
    target = resolve_targets(step.get("on", "leader"), nodes, leader)
    secs = int(step.get("seconds", 500))
    for t in target:
        clock_skew(t, secs)


@register_fault("nic_flap")
def _f_nic_flap(ctx, nodes, leader, step):
    target = resolve_targets(step.get("on", "leader"), nodes, leader)
    down_s = int(step.get("down_s", 5))
    for t in target:
        nic_flap(t, down_s)


@register_fault("time_strobe")
def _f_time_strobe(ctx, nodes, leader, step):
    swings = int(step.get("swings", 6))
    step_s = int(step.get("step_s", 500))
    interval_s = int(step.get("interval_s", 5))

    def _run_one(t):
        time_strobe(t, swings=swings, step_s=step_s, interval_s=interval_s)

    _for_each_target(step, nodes, leader, _run_one)


@register_fault("stop_cont")
def _f_stop_cont(ctx, nodes, leader, step):
    target = resolve_targets(step.get("on", "leader"), nodes, leader)
    for _ in range(step.get("count", 10)):
        [stop(t) for t in target]
        time.sleep(step.get("sleep_s", 1.0))
        [cont(t) for t in target]
        time.sleep(step.get("sleep_s", 1.0))


@register_fault("stress_ng")
def _f_stress_ng(ctx, nodes, leader, step):
    secs = int(step.get("seconds", DEFAULT_FAULT_DURATION_S))
    stress_args = []
    for k in ("cpu", "vm", "io", "hdd", "sched"):
        try:
            v = int(step.get(k, 0))
        except Exception:
            v = 0
        if v and v > 0:
            stress_args += [f"--{k} {v}"]
    if step.get("vm_bytes"):
        stress_args += [f"--vm-bytes {step.get('vm_bytes')}"]
    if not stress_args:
        stress_args = ["--cpu 1"]
    cmd = f"TMPDIR=/tmp stress-ng {' '.join(stress_args)} --temp-path /tmp --timeout {secs}s --metrics-brief"

    def _run_one(t):
        sh(t, "mkdir -p /tmp && chmod 1777 /tmp || true")
        sh(t, cmd)

    _for_each_target(step, nodes, leader, _run_one)
