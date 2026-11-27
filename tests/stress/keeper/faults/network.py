import threading
import time
from contextlib import contextmanager

from keeper.faults.registry import register_fault
from keeper.framework.core.settings import DEFAULT_FAULT_DURATION_S
from keeper.framework.core.util import (
    for_each_target,
    resolve_targets,
    sh_root,
    sh_root_strict,
    sh_strict,
    ts_ms,
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
            except Exception as _e:
                # Log so netem leakage is visible; cannot re-raise inside finally
                # without masking the original exception.
                print(f"[keeper][netem] WARNING: failed to remove qdisc on {getattr(node, 'name', '?')}: {_e}")


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
