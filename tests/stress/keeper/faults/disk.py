import threading
import time as _time
from contextlib import contextmanager

from keeper.framework.core.registry import register_fault
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


@contextmanager
def dm_delay(node, ms=3):
    try:
        suffix = getattr(node, "name", "node")
        dm_name = f"kdelay_{suffix}"
        img = f"/var/lib/keeper_delay_{suffix}.img"
        backend_dev = ""
        # Prefer RAM block device if present inside container
        ram = _ram_block_device(node)
        if ram:
            backend_dev = ram
        else:
            backend_dev = _loop_device_for_image(node, img, 256)
        if not backend_dev:
            raise AssertionError("dm_delay: cannot create loop device")
        sz = sh_root_strict(node, f"blockdev --getsz {backend_dev}", timeout=30)[
            "out"
        ].strip()
        try:
            sh(
                node,
                "pkill -STOP -x clickhouse || pkill -STOP clickhouse-server || true",
                timeout=30,
            )
        except Exception:
            pass
        sh_root_strict(
            node,
            f"umount /var/lib/clickhouse/coordination || true; umount /mnt/{dm_name} || true",
            timeout=90,
        )
        sh_root_strict(node, f"dmsetup remove --retry -f {dm_name} || true", timeout=60)
        sh_root_strict(
            node,
            f"i=0; while dmsetup info {dm_name} >/dev/null 2>&1 && [ $i -lt 80 ]; do sleep 0.1; i=$((i+1)); done; true",
            timeout=30,
        )
        try:
            still = sh_root(
                node, f"dmsetup info {dm_name} >/dev/null 2>&1; echo $?", timeout=10
            )
            if not str((still or {}).get("out", "")).strip().endswith("0"):
                still = None
        except Exception:
            still = None
        if still is not None:
            sh_root_strict(node, f"dmsetup remove -f {dm_name} || true", timeout=60)
            sh_root_strict(
                node,
                f"i=0; while dmsetup info {dm_name} >/dev/null 2>&1 && [ $i -lt 80 ]; do sleep 0.1; i=$((i+1)); done; true",
                timeout=30,
            )
            chk = sh_root(
                node, f"dmsetup info {dm_name} >/dev/null 2>&1; echo $?", timeout=10
            )
            if str((chk or {}).get("out", "")).strip().endswith("0"):
                raise AssertionError("dm_delay: dm device still exists after remove")
        try:
            sh(
                node,
                "pkill -CONT -x clickhouse || pkill -CONT clickhouse-server || true",
                timeout=30,
            )
        except Exception:
            pass
        try:
            sh_root_strict(
                node,
                f"dmsetup create {dm_name} --table '0 {sz} delay {backend_dev} 0 {ms} {backend_dev} 0 {ms}'",
                timeout=60,
            )
        except Exception as e:
            raise AssertionError(f"dm_delay: dmsetup create failed: {e}") from e
        try:
            sh(
                node,
                "pkill -STOP -x clickhouse || pkill -STOP clickhouse-server || true",
                timeout=30,
            )
            sh_root_strict(
                node, f"dmsetup mknodes {dm_name} || dmsetup mknodes", timeout=60
            )
            sh(
                node,
                f"i=0; while [ ! -e /dev/mapper/{dm_name} ] && [ $i -lt 50 ]; do sleep 0.1; i=$((i+1)); done; [ -e /dev/mapper/{dm_name} ] && echo ok || (dmsetup info {dm_name} && false)",
                timeout=30,
            )
            sh_root_strict(
                node, "umount /var/lib/clickhouse/coordination || true", timeout=60
            )
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
            try:
                sh(
                    node,
                    "pkill -CONT -x clickhouse || pkill -CONT clickhouse-server || true",
                    timeout=30,
                )
            except Exception:
                pass
        yield
    finally:
        suffix = getattr(node, "name", "node")
        dm_name = f"kdelay_{suffix}"
        img = f"/var/lib/keeper_delay_{suffix}.img"
        try:
            sh(
                node,
                "pkill -STOP -x clickhouse || pkill -STOP clickhouse-server || true",
                timeout=30,
            )
        except Exception:
            pass
        sh_root(
            node,
            f"sync || true; "
            f"umount /var/lib/clickhouse/coordination || true; "
            f"mkdir -p /var/lib/clickhouse/coordination || true; "
            f"cp -a /mnt/{dm_name}/. /var/lib/clickhouse/coordination/ 2>/dev/null || true; "
            f"sync || true; "
            f"umount /mnt/{dm_name} || true; "
            f"dmsetup remove --retry -f {dm_name} || true",
            timeout=180,
        )
        try:
            sh(
                node,
                "pkill -CONT -x clickhouse || pkill -CONT clickhouse-server || true",
                timeout=30,
            )
        except Exception:
            pass
        # Detach only our loop device (if any) and remove image
        _detach_loop_for_image(node, img)
        sh_root(node, f"rm -f {img} || true", timeout=30)


@contextmanager
def dm_error(node):
    try:
        suffix = getattr(node, "name", "node")
        dm_name = f"kerr_{suffix}"
        img = f"/var/lib/keeper_err_{suffix}.img"
        backend_dev = ""
        ram = _ram_block_device(node)
        if ram:
            backend_dev = ram
        else:
            backend_dev = _loop_device_for_image(node, img, 128)
        if not backend_dev:
            raise AssertionError("dm_error: cannot create loop device")
        sz = sh_root_strict(node, f"blockdev --getsz {backend_dev}", timeout=30)[
            "out"
        ].strip()
        sh_root_strict(
            node, "umount /var/lib/clickhouse/coordination || true", timeout=60
        )
        sh_root_strict(node, f"dmsetup remove --retry -f {dm_name} || true", timeout=60)
        sh(
            node,
            f"i=0; while dmsetup info {dm_name} >/dev/null 2>&1 && [ $i -lt 50 ]; do sleep 0.1; i=$((i+1)); done",
            timeout=30,
        )
        try:
            sh_root_strict(
                node, f"dmsetup create {dm_name} --table '0 {sz} error'", timeout=60
            )
        except Exception as e:
            raise AssertionError(f"dm_error: dmsetup create failed: {e}") from e
        sh_root_strict(
            node, "umount /var/lib/clickhouse/coordination || true", timeout=60
        )
        yield
    finally:
        suffix = getattr(node, "name", "node")
        dm_name = f"kerr_{suffix}"
        img = f"/var/lib/keeper_err_{suffix}.img"
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


@register_fault("dm_delay")
def _f_dm_delay(ctx, nodes, leader, step):
    dur = int(step.get("duration_s", DEFAULT_FAULT_DURATION_S))
    ms = step.get("ms", 3)

    lock = threading.Lock()

    def _emit(node, phase, extra=None):
        try:
            ev = {
                "ts": ts_ms(),
                "kind": "dm_delay",
                "node": str(getattr(node, "name", "")),
                "phase": str(phase),
            }
            if isinstance(extra, dict) and extra:
                ev.update(extra)
            with lock:
                (ctx.setdefault("fault_events", [])).append(ev)
        except Exception:
            pass

    def _dm_present(node, dm_name):
        try:
            r = sh_root(
                node, f"dmsetup info {dm_name} >/dev/null 2>&1; echo $?", timeout=10
            )
            ok = str((r or {}).get("out", "")).strip().endswith("0")
            if not ok:
                return False
            r2 = sh_root(node, f"test -e /dev/mapper/{dm_name}; echo $?", timeout=10)
            return str((r2 or {}).get("out", "")).strip().endswith("0")
        except Exception:
            return False

    def _run_one(t):
        suffix = getattr(t, "name", "node")
        dm_name = f"kdelay_{suffix}"
        t0 = _time.time()
        _emit(t, "start", {"duration_s": int(dur), "ms": ms})
        with dm_delay(t, ms):
            _emit(t, "apply_ok")
            if dur >= 4:
                early = min(2.0, float(dur) / 4.0)
                _time.sleep(max(0.0, early))
                if not _dm_present(t, dm_name):
                    _emit(t, "active_verify_failed", {"when": "early"})
                    raise AssertionError("dm_delay active verify failed (early)")
                _emit(t, "active_ok", {"when": "early"})

                remaining = max(0.0, float(dur) - early)
                late = min(2.0, max(0.0, remaining / 2.0))
                _time.sleep(max(0.0, remaining - late))
                if not _dm_present(t, dm_name):
                    _emit(t, "active_verify_failed", {"when": "late"})
                    raise AssertionError("dm_delay active verify failed (late)")
                _emit(t, "active_ok", {"when": "late"})
                _time.sleep(max(0.0, late))
            else:
                _time.sleep(dur)
        if _dm_present(t, dm_name):
            _emit(t, "cleanup_verify_failed")
            raise AssertionError("dm_delay cleanup verify failed")
        _emit(t, "cleanup_ok", {"elapsed_s": _time.time() - t0})

    for_each_target(step, nodes, leader, _run_one)

    try:
        targets = resolve_targets(step.get("on", "leader"), nodes, leader)
    except Exception:
        targets = []
    try:
        evs = (ctx or {}).get("fault_events") or []
        cleaned = {
            e.get("node")
            for e in evs
            if e.get("kind") == "dm_delay" and e.get("phase") == "cleanup_ok"
        }
        expected = {
            str(getattr(t, "name", ""))
            for t in (targets or [])
            if str(getattr(t, "name", ""))
        }
        missing = sorted([n for n in expected if n and n not in cleaned])
        if missing:
            raise AssertionError(
                "dm_delay did not complete cleanup for targets: " + ", ".join(missing)
            )
    except Exception:
        raise


@register_fault("dm_error")
def _f_dm_error(ctx, nodes, leader, step):
    dur = int(step.get("duration_s", DEFAULT_FAULT_DURATION_S))

    lock = threading.Lock()

    def _emit(node, phase, extra=None):
        try:
            ev = {
                "ts": ts_ms(),
                "kind": "dm_error",
                "node": str(getattr(node, "name", "")),
                "phase": str(phase),
            }
            if isinstance(extra, dict) and extra:
                ev.update(extra)
            with lock:
                (ctx.setdefault("fault_events", [])).append(ev)
        except Exception:
            pass

    def _dm_present(node, dm_name):
        try:
            r = sh_root(
                node, f"dmsetup info {dm_name} >/dev/null 2>&1; echo $?", timeout=10
            )
            ok = str((r or {}).get("out", "")).strip().endswith("0")
            if not ok:
                return False
            r2 = sh_root(node, f"test -e /dev/mapper/{dm_name}; echo $?", timeout=10)
            return str((r2 or {}).get("out", "")).strip().endswith("0")
        except Exception:
            return False

    def _run_one(t):
        suffix = getattr(t, "name", "node")
        dm_name = f"kerr_{suffix}"
        t0 = _time.time()
        _emit(t, "start", {"duration_s": int(dur)})
        with dm_error(t):
            _emit(t, "apply_ok")
            if dur >= 4:
                early = min(2.0, float(dur) / 4.0)
                _time.sleep(max(0.0, early))
                if not _dm_present(t, dm_name):
                    _emit(t, "active_verify_failed", {"when": "early"})
                    raise AssertionError("dm_error active verify failed (early)")
                _emit(t, "active_ok", {"when": "early"})

                remaining = max(0.0, float(dur) - early)
                late = min(2.0, max(0.0, remaining / 2.0))
                _time.sleep(max(0.0, remaining - late))
                if not _dm_present(t, dm_name):
                    _emit(t, "active_verify_failed", {"when": "late"})
                    raise AssertionError("dm_error active verify failed (late)")
                _emit(t, "active_ok", {"when": "late"})
                _time.sleep(max(0.0, late))
            else:
                _time.sleep(dur)
        if _dm_present(t, dm_name):
            _emit(t, "cleanup_verify_failed")
            raise AssertionError("dm_error cleanup verify failed")
        _emit(t, "cleanup_ok", {"elapsed_s": _time.time() - t0})

    for_each_target(step, nodes, leader, _run_one)

    try:
        targets = resolve_targets(step.get("on", "leader"), nodes, leader)
    except Exception:
        targets = []
    try:
        evs = (ctx or {}).get("fault_events") or []
        cleaned = {
            e.get("node")
            for e in evs
            if e.get("kind") == "dm_error" and e.get("phase") == "cleanup_ok"
        }
        expected = {
            str(getattr(t, "name", ""))
            for t in (targets or [])
            if str(getattr(t, "name", ""))
        }
        missing = sorted([n for n in expected if n and n not in cleaned])
        if missing:
            raise AssertionError(
                "dm_error did not complete cleanup for targets: " + ", ".join(missing)
            )
    except Exception:
        raise


def _run_with_ctx(ctx_mgr, dur):
    with ctx_mgr:
        _time.sleep(dur)


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
