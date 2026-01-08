from contextlib import contextmanager
import os
from ..framework.core.util import sh, sh_root, resolve_targets
from ..framework.core.settings import DEFAULT_FAULT_DURATION_S
from ..framework.core.registry import register_fault

# Always strict enforcement
STRICT_FAULTS = True


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
            th.start(); ths.append(th)
        for th in ths:
            th.join()
        if errors:
            raise errors[0]
    else:
        for t in target:
            run_one(t)


@contextmanager
def dm_delay(node, ms=3):
    try:
        suffix = getattr(node, "name", "node")
        dm_name = f"kdelay_{suffix}"
        img = f"/var/lib/keeper_delay_{suffix}.img"
        backend_dev = ""
        # Prefer RAM block device if present inside container
        ram = sh(node, "ls -1 /dev/ram* 2>/dev/null | head -n1 || true")['out'].strip()
        if ram:
            backend_dev = ram
        else:
            sh_root(node, f"rm -f {img} || true")
            sh_root(node, f"dd if=/dev/zero of={img} bs=1M count=256")
            # Ensure prior loop for this image is detached
            sh_root(node, f"LOOP=$(losetup -j {img} | cut -d: -f1); [ -n \"$LOOP\" ] && losetup -d $LOOP || true")
            lo = sh_root(node, f"losetup -f --show {img}")['out'].strip()
            backend_dev = lo
        if not backend_dev:
            if STRICT_FAULTS:
                raise AssertionError("dm_delay: cannot create loop device")
            yield
            return
        sz = sh_root(node, f"blockdev --getsz {backend_dev}")['out'].strip()
        sh_root(node, "umount /var/lib/clickhouse/coordination || true")
        sh_root(node, f"dmsetup remove --retry -f {dm_name} || true")
        sh(node, f"i=0; while dmsetup info {dm_name} >/dev/null 2>&1 && [ $i -lt 50 ]; do sleep 0.1; i=$((i+1)); done")
        try:
            sh_root(node, f"dmsetup create {dm_name} --table '0 {sz} delay {backend_dev} 0 {ms} {backend_dev} 0 {ms}'")
        except Exception as e:
            if STRICT_FAULTS:
                raise AssertionError(f"dm_delay: dmsetup create failed: {e}")
            # Non-strict: skip the dm delay injection
            yield
            return
        try:
            sh(node, "pkill -STOP -x clickhouse || pkill -STOP clickhouse-server || true")
            sh_root(node, f"dmsetup mknodes {dm_name} || dmsetup mknodes")
            sh(node, f"i=0; while [ ! -e /dev/mapper/{dm_name} ] && [ $i -lt 50 ]; do sleep 0.1; i=$((i+1)); done; [ -e /dev/mapper/{dm_name} ] && echo ok || (dmsetup info {dm_name} && false)")
            sh_root(node, "umount /var/lib/clickhouse/coordination || true")
            sh_root(node, f"mkdir -p /var/lib/clickhouse/coordination /mnt/{dm_name} || true")
            sh_root(node, f"wipefs -a /dev/mapper/{dm_name} || true")
            sh_root(node, f"mkfs.ext4 -E nodiscard,lazy_itable_init=0,lazy_journal_init=0 -F /dev/mapper/{dm_name}")
            sh_root(node, "sync || true; (command -v udevadm >/dev/null 2>&1 && udevadm settle) || true")
            sh(node, f"i=0; while ! blkid /dev/mapper/{dm_name} >/dev/null 2>&1 && [ $i -lt 50 ]; do sleep 0.1; i=$((i+1)); done")
            sh(node, "sleep 0.3")
            sh_root(node, f"mount -t ext4 -o rw,nodelalloc,data=writeback,noatime /dev/mapper/{dm_name} /mnt/{dm_name}")
            sh_root(node, f"mount --bind /mnt/{dm_name} /var/lib/clickhouse/coordination")
        except Exception as e:
            if STRICT_FAULTS:
                raise AssertionError(f"dm_delay: mount failed: {e}")
            # Non-strict: proceed without the dm mount
        finally:
            try:
                sh(node, "pkill -CONT -x clickhouse || pkill -CONT clickhouse-server || true")
            except Exception:
                pass
        yield
    finally:
        suffix = getattr(node, "name", "node")
        dm_name = f"kdelay_{suffix}"
        img = f"/var/lib/keeper_delay_{suffix}.img"
        sh_root(node, f"umount /var/lib/clickhouse/coordination || true; umount /mnt/{dm_name} || true; dmsetup remove --retry -f {dm_name} || true")
        # Detach only our loop device (if any) and remove image
        sh_root(node, f"LOOP=$(losetup -j {img} | cut -d: -f1); [ -n \"$LOOP\" ] && losetup -d $LOOP || true")
        sh_root(node, f"rm -f {img} || true")


@contextmanager
def dm_error(node):
    try:
        suffix = getattr(node, "name", "node")
        dm_name = f"kerr_{suffix}"
        img = f"/var/lib/keeper_err_{suffix}.img"
        backend_dev = ""
        ram = sh(node, "ls -1 /dev/ram* 2>/dev/null | head -n1 || true")['out'].strip()
        if ram:
            backend_dev = ram
        else:
            sh_root(node, f"rm -f {img} || true")
            sh_root(node, f"dd if=/dev/zero of={img} bs=1M count=128")
            sh_root(node, f"LOOP=$(losetup -j {img} | cut -d: -f1); [ -n \"$LOOP\" ] && losetup -d $LOOP || true")
            lo = sh_root(node, f"losetup -f --show {img}")['out'].strip()
            backend_dev = lo
        if not backend_dev:
            if STRICT_FAULTS:
                raise AssertionError("dm_error: cannot create loop device")
            yield
            return
        sz = sh_root(node, f"blockdev --getsz {backend_dev}")['out'].strip()
        sh_root(node, "umount /var/lib/clickhouse/coordination || true")
        sh_root(node, f"dmsetup remove --retry -f {dm_name} || true")
        sh(node, f"i=0; while dmsetup info {dm_name} >/dev/null 2>&1 && [ $i -lt 50 ]; do sleep 0.1; i=$((i+1)); done")
        try:
            sh_root(node, f"dmsetup create {dm_name} --table '0 {sz} error'")
        except Exception as e:
            if STRICT_FAULTS:
                raise AssertionError(f"dm_error: dmsetup create failed: {e}")
            yield
            return
        sh_root(node, "umount /var/lib/clickhouse/coordination || true"); yield
    finally:
        suffix = getattr(node, "name", "node")
        dm_name = f"kerr_{suffix}"
        img = f"/var/lib/keeper_err_{suffix}.img"
        sh_root(node, f"umount /var/lib/clickhouse/coordination || true; dmsetup remove --retry -f {dm_name} || true")
        sh_root(node, f"LOOP=$(losetup -j {img} | cut -d: -f1); [ -n \"$LOOP\" ] && losetup -d $LOOP || true")
        sh_root(node, f"rm -f {img} || true")


def fill_enospc(node, size_mb=2048):
    path = "/var/lib/clickhouse/coordination/snapshots/fill.bin"
    sh(node, f"fallocate -l {int(size_mb)}M {path} || dd if=/dev/zero of={path} bs=1M count={int(size_mb)} conv=fsync")


def free_enospc(node):
    sh(node, "rm -f /var/lib/clickhouse/coordination/snapshots/fill.bin || true")


def volume_detach(node):
    sh_root(node, "if mountpoint -q /var/lib/clickhouse/coordination; then umount -l /var/lib/clickhouse/coordination; fi; losetup -D || true")


def volume_attach(node):
    sh_root(node, "mkdir -p /var/lib/clickhouse/coordination && mount -o remount,rw /")


@register_fault("dm_delay")
def _f_dm_delay(ctx, nodes, leader, step):
    dur = int(step.get("duration_s", DEFAULT_FAULT_DURATION_S))
    def _run_one(t):
        with dm_delay(t, step.get("ms", 3)):
            import time as _t
            _t.sleep(dur)
    _for_each_target(step, nodes, leader, _run_one)


@register_fault("dm_error")
def _f_dm_error(ctx, nodes, leader, step):
    dur = int(step.get("duration_s", DEFAULT_FAULT_DURATION_S))
    def _run_one(t):
        with dm_error(t):
            import time as _t
            _t.sleep(dur)
    _for_each_target(step, nodes, leader, _run_one)


@register_fault("enospc")
def _f_enospc(ctx, nodes, leader, step):
    target = resolve_targets(step.get("on","leader"), nodes, leader)
    [fill_enospc(t, step.get("size_mb",2048)) for t in target]


@register_fault("free_enospc")
def _f_free_enospc(ctx, nodes, leader, step):
    target = resolve_targets(step.get("on","leader"), nodes, leader)
    [free_enospc(t) for t in target]


@register_fault("volume_detach")
def _f_volume_detach(ctx, nodes, leader, step):
    target = resolve_targets(step.get("on","leader"), nodes, leader)
    [volume_detach(t) for t in target]


@register_fault("volume_attach")
def _f_volume_attach(ctx, nodes, leader, step):
    target = resolve_targets(step.get("on","leader"), nodes, leader)
    [volume_attach(t) for t in target]
