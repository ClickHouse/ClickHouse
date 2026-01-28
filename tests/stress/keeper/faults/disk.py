import os
import re
import threading
import time as _time
from contextlib import contextmanager

from keeper.faults.registry import register_fault
from keeper.framework.core.settings import DEFAULT_FAULT_DURATION_S
from keeper.framework.core.util import (
    for_each_target,
    resolve_targets,
    sh,
    sh_root,
    sh_root_strict,
    sh_strict,
    ts_ms,
)


def _ram_block_device(node):
    return sh(node, "ls -1 /dev/ram* 2>/dev/null | head -n1 || true")["out"].strip()


def _detach_loop_for_image(node, img):
    sh_root(
        node,
        f'LOOP=$(losetup -j {img} | cut -d: -f1); [ -n "$LOOP" ] && losetup -d $LOOP || true',
        timeout=60,
    )


def _loop_device_for_image(node, img, size_mb):
    sh_root_strict(node, f"rm -f {img} || true", timeout=30)
    sh_root_strict(
        node, f"dd if=/dev/zero of={img} bs=1M count={int(size_mb)}", timeout=120
    )
    _detach_loop_for_image(node, img)
    return sh_root_strict(node, f"losetup -f --show {img}", timeout=60)["out"].strip()


def _safe_dm_token(s):
    if not s:
        return ""
    s = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(s))
    return s.strip("_")


def _dm_name(prefix, node):
    suffix = getattr(node, "name", "node")
    run = _safe_dm_token(os.environ.get("KEEPER_CLUSTER_NAME", ""))
    if run:
        return f"{prefix}_{run[:32]}_{suffix}"
    return f"{prefix}_{suffix}"


def _dm_img(prefix, node):
    suffix = getattr(node, "name", "node")
    run = _safe_dm_token(os.environ.get("KEEPER_CLUSTER_NAME", ""))
    if run:
        return f"/var/lib/keeper_{prefix}_{run[:32]}_{suffix}.img"
    return f"/var/lib/keeper_{prefix}_{suffix}.img"


def _kill_clickhouse(node, signal="STOP"):
    """Send signal to clickhouse processes."""
    try:
        sh(
            node,
            f'PIDS=$(pidof clickhouse clickhouse-server 2>/dev/null || true); [ -n "$PIDS" ] && kill -{signal} $PIDS || true',
            timeout=30,
        )
    except Exception:
        pass


def _wait_dm_removed(node, dm_name, max_iter=80):
    """Wait for dm device to be removed."""
    sh_root_strict(
        node,
        f"i=0; while dmsetup info {dm_name} >/dev/null 2>&1 && [ $i -lt {max_iter} ]; do sleep 0.1; i=$((i+1)); done; true",
        timeout=30,
    )


def _check_dm_exists(node, dm_name):
    """Check if dm device exists."""
    try:
        r = sh_root(node, f"dmsetup info {dm_name} >/dev/null 2>&1; echo $?", timeout=10)
        if not str((r or {}).get("out", "")).strip().endswith("0"):
            return False
        r2 = sh_root(node, f"test -e /dev/mapper/{dm_name}; echo $?", timeout=10)
        return str((r2 or {}).get("out", "")).strip().endswith("0")
    except Exception:
        return False


@contextmanager
def dm_delay(node, ms=3):
    try:
        dm_name = _dm_name("kdelay", node)
        img = _dm_img("delay", node)
        # Prefer RAM block device if present inside container
        ram = _ram_block_device(node)
        backend_dev = ram if ram else _loop_device_for_image(node, img, 256)
        if not backend_dev:
            raise AssertionError("dm_delay: cannot create loop device")
        sz = sh_root_strict(node, f"blockdev --getsz {backend_dev}", timeout=30)["out"].strip()
        
        _kill_clickhouse(node, "STOP")
        sh_root_strict(
            node,
            f"umount /var/lib/clickhouse/coordination || true; umount /mnt/{dm_name} || true",
            timeout=90,
        )
        sh_root_strict(node, f"dmsetup remove --retry -f {dm_name} || true", timeout=60)
        _wait_dm_removed(node, dm_name, 80)
        
        # Force remove if still exists
        if _check_dm_exists(node, dm_name):
            sh_root_strict(node, f"dmsetup remove -f {dm_name} || true", timeout=60)
            _wait_dm_removed(node, dm_name, 80)
            if _check_dm_exists(node, dm_name):
                raise AssertionError("dm_delay: dm device still exists after remove")
        
        _kill_clickhouse(node, "CONT")
        try:
            sh_root_strict(
                node,
                f"dmsetup create {dm_name} --table '0 {sz} delay {backend_dev} 0 {ms} {backend_dev} 0 {ms}'",
                timeout=60,
            )
        except Exception as e:
            raise AssertionError(f"dm_delay: dmsetup create failed: {e}") from e
        
        try:
            _kill_clickhouse(node, "STOP")
            sh_root_strict(node, f"dmsetup mknodes {dm_name} || dmsetup mknodes", timeout=60)
            sh(
                node,
                f"i=0; while [ ! -e /dev/mapper/{dm_name} ] && [ $i -lt 50 ]; do sleep 0.1; i=$((i+1)); done; [ -e /dev/mapper/{dm_name} ] && echo ok || (dmsetup info {dm_name} && false)",
                timeout=30,
            )
            sh_root_strict(node, "umount /var/lib/clickhouse/coordination || true", timeout=60)
            sh_root_strict(
                node,
                f"mkdir -p /var/lib/clickhouse/coordination /mnt/{dm_name} || true",
                timeout=30,
            )
            sh_root_strict(node, f"wipefs -a /dev/mapper/{dm_name} || true", timeout=60)
            sh_root_strict(
                node,
                f"mkfs.ext4 -E nodiscard,lazy_itable_init=0,lazy_journal_init=0 -F /dev/mapper/{dm_name}",
                timeout=120,
            )
            sh_root_strict(
                node,
                "sync || true; (command -v udevadm >/dev/null 2>&1 && udevadm settle) || true",
                timeout=60,
            )
            sh(
                node,
                f"i=0; while ! blkid /dev/mapper/{dm_name} >/dev/null 2>&1 && [ $i -lt 50 ]; do sleep 0.1; i=$((i+1)); done",
                timeout=30,
            )
            sh(node, "sleep 0.3")
            sh_root_strict(
                node,
                f"mount -t ext4 -o rw,nodelalloc,data=writeback,noatime /dev/mapper/{dm_name} /mnt/{dm_name}",
                timeout=60,
            )
            sh_root(
                node,
                f"mkdir -p /mnt/{dm_name} /var/lib/clickhouse/coordination || true; "
                f"sync || true; "
                f"cp -a /var/lib/clickhouse/coordination/. /mnt/{dm_name}/ 2>/dev/null || true; "
                f"sync || true",
                timeout=180,
            )
            sh_root_strict(
                node,
                f"mount --bind /mnt/{dm_name} /var/lib/clickhouse/coordination",
                timeout=30,
            )
        except Exception as e:
            raise AssertionError(f"dm_delay: mount failed: {e}") from e
        finally:
            _kill_clickhouse(node, "CONT")
        yield
    finally:
        dm_name = _dm_name("kdelay", node)
        img = _dm_img("delay", node)
        _kill_clickhouse(node, "STOP")
        sh_root(
            node,
            (
                f"sync || true; "
                f"umount /var/lib/clickhouse/coordination 2>/dev/null || umount -l /var/lib/clickhouse/coordination 2>/dev/null || true; "
                f"mountpoint -q /var/lib/clickhouse/coordination && umount -l /var/lib/clickhouse/coordination 2>/dev/null || true; "
                f"mkdir -p /var/lib/clickhouse/coordination || true; "
                f"cp -a /mnt/{dm_name}/. /var/lib/clickhouse/coordination/ 2>/dev/null || true; "
                f"sync || true; "
                f"umount /mnt/{dm_name} 2>/dev/null || umount -l /mnt/{dm_name} 2>/dev/null || true; "
                f"dmsetup remove --retry -f {dm_name} || true"
            ),
            timeout=180,
        )
        _kill_clickhouse(node, "CONT")
        _detach_loop_for_image(node, img)
        sh_root(node, f"rm -f {img} || true", timeout=30)


@contextmanager
def dm_error(node):
    try:
        dm_name = _dm_name("kerr", node)
        img = _dm_img("err", node)
        ram = _ram_block_device(node)
        backend_dev = ram if ram else _loop_device_for_image(node, img, 128)
        if not backend_dev:
            raise AssertionError("dm_error: cannot create loop device")
        sz = sh_root_strict(node, f"blockdev --getsz {backend_dev}", timeout=30)["out"].strip()
        sh_root_strict(node, "umount /var/lib/clickhouse/coordination || true", timeout=60)
        sh_root_strict(node, f"dmsetup remove --retry -f {dm_name} || true", timeout=60)
        _wait_dm_removed(node, dm_name, 50)
        try:
            sh_root_strict(
                node, f"dmsetup create {dm_name} --table '0 {sz} error'", timeout=60
            )
        except Exception as e:
            raise AssertionError(f"dm_error: dmsetup create failed: {e}") from e
        sh_root_strict(node, "umount /var/lib/clickhouse/coordination || true", timeout=60)
        yield
    finally:
        dm_name = _dm_name("kerr", node)
        img = _dm_img("err", node)
        sh_root(
            node,
            f"umount /var/lib/clickhouse/coordination || true; dmsetup remove --retry -f {dm_name} || true",
            timeout=90,
        )
        _detach_loop_for_image(node, img)
        sh_root(node, f"rm -f {img} || true", timeout=30)


def fill_enospc(node, size_mb=2048):
    path = "/var/lib/clickhouse/coordination/snapshots/fill.bin"
    sh(
        node,
        f"fallocate -l {int(size_mb)}M {path} || dd if=/dev/zero of={path} bs=1M count={int(size_mb)} conv=fsync",
    )


def free_enospc(node):
    sh(node, "rm -f /var/lib/clickhouse/coordination/snapshots/fill.bin || true")


def volume_detach(node):
    sh_root(
        node,
        "if mountpoint -q /var/lib/clickhouse/coordination; then umount -l /var/lib/clickhouse/coordination; fi; losetup -D || true",
    )


def volume_attach(node):
    sh_root(node, "mkdir -p /var/lib/clickhouse/coordination && mount -o remount,rw /")


def _coord_mounted(node):
    """Check if coordination directory is mounted."""
    try:
        r = sh_root(
            node,
            "mountpoint -q /var/lib/clickhouse/coordination >/dev/null 2>&1; echo $?",
            timeout=10,
        )
        return str((r or {}).get("out", "")).strip().endswith("0")
    except Exception:
        return False


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


@register_fault("dm_delay")
def _f_dm_delay(ctx, nodes, leader, step):
    dur = int(step.get("duration_s", DEFAULT_FAULT_DURATION_S))
    ms = step.get("ms", 3)
    _emit = _create_emit_fn("dm_delay", ctx)

    def _run_one(t):
        dm_name = _dm_name("kdelay", t)
        t0 = _time.time()
        _emit(t, "start", {"duration_s": int(dur), "ms": ms})
        with dm_delay(t, ms):
            _emit(t, "apply_ok")
            if dur >= 4:
                early = min(2.0, float(dur) / 4.0)
                _time.sleep(max(0.0, early))
                if not _check_dm_exists(t, dm_name):
                    _emit(t, "active_verify_failed", {"when": "early"})
                    raise AssertionError("dm_delay active verify failed (early)")
                _emit(t, "active_ok", {"when": "early"})

                remaining = max(0.0, float(dur) - early)
                late = min(2.0, max(0.0, remaining / 2.0))
                _time.sleep(max(0.0, remaining - late))
                if not _check_dm_exists(t, dm_name):
                    _emit(t, "active_verify_failed", {"when": "late"})
                    raise AssertionError("dm_delay active verify failed (late)")
                _emit(t, "active_ok", {"when": "late"})
                _time.sleep(max(0.0, late))
            else:
                _time.sleep(dur)
        if _check_dm_exists(t, dm_name):
            _emit(t, "cleanup_verify_failed")
            raise AssertionError("dm_delay cleanup verify failed")
        if _coord_mounted(t):
            _emit(t, "cleanup_verify_failed", {"reason": "coordination_still_mounted"})
            raise AssertionError("dm_delay cleanup verify failed: coordination still mounted")
        _emit(t, "cleanup_ok", {"elapsed_s": _time.time() - t0})

    for_each_target(step, nodes, leader, _run_one)

    try:
        targets = resolve_targets(step.get("on", "leader"), nodes, leader)
    except Exception:
        targets = []
    evs = (ctx or {}).get("fault_events") or []
    cleaned = {
        e.get("node")
        for e in evs
        if e.get("kind") == "dm_delay" and e.get("phase") == "cleanup_ok"
    }
    expected = {str(getattr(t, "name", "")) for t in (targets or []) if str(getattr(t, "name", ""))}
    missing = sorted([n for n in expected if n and n not in cleaned])
    if missing:
        raise AssertionError(
            "dm_delay did not complete cleanup for targets: " + ", ".join(missing)
        )


@register_fault("dm_error")
def _f_dm_error(ctx, nodes, leader, step):
    dur = int(step.get("duration_s", DEFAULT_FAULT_DURATION_S))
    _emit = _create_emit_fn("dm_error", ctx)

    def _run_one(t):
        dm_name = _dm_name("kerr", t)
        t0 = _time.time()
        _emit(t, "start", {"duration_s": int(dur)})
        with dm_error(t):
            _emit(t, "apply_ok")
            if dur >= 4:
                early = min(2.0, float(dur) / 4.0)
                _time.sleep(max(0.0, early))
                if not _check_dm_exists(t, dm_name):
                    _emit(t, "active_verify_failed", {"when": "early"})
                    raise AssertionError("dm_error active verify failed (early)")
                _emit(t, "active_ok", {"when": "early"})

                remaining = max(0.0, float(dur) - early)
                late = min(2.0, max(0.0, remaining / 2.0))
                _time.sleep(max(0.0, remaining - late))
                if not _check_dm_exists(t, dm_name):
                    _emit(t, "active_verify_failed", {"when": "late"})
                    raise AssertionError("dm_error active verify failed (late)")
                _emit(t, "active_ok", {"when": "late"})
                _time.sleep(max(0.0, late))
            else:
                _time.sleep(dur)
        if _check_dm_exists(t, dm_name):
            _emit(t, "cleanup_verify_failed")
            raise AssertionError("dm_error cleanup verify failed")
        _emit(t, "cleanup_ok", {"elapsed_s": _time.time() - t0})

    for_each_target(step, nodes, leader, _run_one)

    try:
        targets = resolve_targets(step.get("on", "leader"), nodes, leader)
    except Exception:
        targets = []
    evs = (ctx or {}).get("fault_events") or []
    cleaned = {
        e.get("node")
        for e in evs
        if e.get("kind") == "dm_error" and e.get("phase") == "cleanup_ok"
    }
    expected = {str(getattr(t, "name", "")) for t in (targets or []) if str(getattr(t, "name", ""))}
    missing = sorted([n for n in expected if n and n not in cleaned])
    if missing:
        raise AssertionError(
            "dm_error did not complete cleanup for targets: " + ", ".join(missing)
        )


@register_fault("enospc")
def _f_enospc(ctx, nodes, leader, step):
    size_mb = step.get("size_mb", 2048)
    for_each_target(step, nodes, leader, lambda t: fill_enospc(t, size_mb))


@register_fault("free_enospc")
def _f_free_enospc(ctx, nodes, leader, step):
    for_each_target(step, nodes, leader, lambda t: free_enospc(t))


@register_fault("volume_detach")
def _f_volume_detach(ctx, nodes, leader, step):
    for_each_target(step, nodes, leader, lambda t: volume_detach(t))


@register_fault("volume_attach")
def _f_volume_attach(ctx, nodes, leader, step):
    for_each_target(step, nodes, leader, lambda t: volume_attach(t))
