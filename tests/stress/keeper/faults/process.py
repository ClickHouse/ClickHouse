import time

from keeper.faults.registry import register_fault
from keeper.framework.core.settings import DEFAULT_FAULT_DURATION_S
from keeper.framework.core.util import for_each_target, sh_strict


def cpu_hog(node, seconds=60):
    # Run blocking (same pattern as mem_hog_block): sh_strict returns after timeout expires.
    # Backgrounding with & left the CPU-burning process running with no cleanup path.
    sh_strict(node, f"timeout {seconds} sh -c 'while :; do :; done'", timeout=seconds + 10)


def mem_hog_block(node, mb=512, seconds=60):
    sh_strict(
        node,
        f"python3 - <<'PY'\nimport time\n_ = bytearray({int(max(1, mb))}*1024*1024)\ntime.sleep({int(max(1, seconds))})\nPY",
        timeout=int(max(30, seconds + 30)),
    )


@register_fault("cpu_hog")
def _f_cpu_hog(ctx, nodes, leader, step):
    secs = int(step.get("seconds") or DEFAULT_FAULT_DURATION_S)
    for_each_target(step, nodes, leader, lambda t: cpu_hog(t, secs))


@register_fault("mem_hog")
def _f_mem_hog(ctx, nodes, leader, step):
    mb = int(step.get("mb") or 512)
    secs = int(step.get("seconds") or DEFAULT_FAULT_DURATION_S)
    for_each_target(step, nodes, leader, lambda t: mem_hog_block(t, mb, secs))
