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


def _restart_clickhouse(node):
    """Restart clickhouse process after it was killed. No-op for non-ClickHouse nodes."""
    if not hasattr(node, "start_clickhouse"):
        return
    try:
        node.start_clickhouse(start_wait_sec=90)
    except Exception as e:
        print(f"[dm_delay] _restart_clickhouse failed: {e}")


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


def _coord_mounted(node):
    """Check if /var/lib/clickhouse/coordination is still bind-mounted."""
    try:
        r = sh_root(node, "mountpoint -q /var/lib/clickhouse/coordination; echo $?", timeout=10)
        return str((r or {}).get("out", "")).strip().endswith("0")
    except Exception:
        return False


@contextmanager
def dm_delay(node, ms=3):
    # Declare before try so finally can always reference them safely.
    dm_name = _dm_name("kdelay", node)
    img = _dm_img("delay", node)
    backend_dev = None
    ram = None
    sz = None
    try:
        # Prefer RAM block device if present inside container
        ram = _ram_block_device(node)
        backend_dev = ram if ram else _loop_device_for_image(node, img, 1024)
        if not backend_dev:
            raise AssertionError("dm_delay: cannot create loop device")
        sz = sh_root_strict(node, f"blockdev --getsz {backend_dev}", timeout=30)["out"].strip()

        # --- PRE-STOP: format and create dm device before stopping keeper ---
        #
        # We format the loop device (not the dm device with delay) because formatting
        # through a delayed dm device takes O(num_blocks * delay_ms) time — with 3ms
        # delay and 256 MB / 4 KB blocks that is ~200 seconds, far exceeding the 30s
        # session_timeout_ms and causing all bench sessions to expire during setup.
        if not ram:
            sh_root_strict(node, f"wipefs -a {backend_dev} || true", timeout=60)
            sh_root_strict(
                node,
                f"mkfs.ext4 -E nodiscard,lazy_itable_init=0,lazy_journal_init=0 -F {backend_dev}",
                timeout=120,
            )
            sh_root_strict(
                node,
                "sync || true; (command -v udevadm >/dev/null 2>&1 && udevadm settle) || true",
                timeout=60,
            )

        # Remove any stale dm device from a previous fault (before STOP; coordination
        # is still mounted to original dir, so we only need to umount /mnt/{dm_name}).
        sh_root_strict(
            node,
            f"umount /mnt/{dm_name} 2>/dev/null || true; dmsetup remove --retry -f {dm_name} || true",
            timeout=90,
        )
        _wait_dm_removed(node, dm_name, 80)
        if _check_dm_exists(node, dm_name):
            sh_root_strict(node, f"dmsetup remove -f {dm_name} || true", timeout=60)
            _wait_dm_removed(node, dm_name, 80)
            if _check_dm_exists(node, dm_name):
                raise AssertionError("dm_delay: dm device still exists after remove")

        # NOTE: The dm device is created INSIDE the STOP window (below), not here.
        #
        # Reason: dmsetup create holds the backend loop device open.  If we created
        # the dm device here (PRE-STOP) and then tried to mount the loop device
        # directly in the STOP window (for the fast coordination copy), Linux would
        # refuse with EBUSY because the dm device already has the loop device open.
        # Creating the dm device after the fast copy avoids this conflict.

        # --- KILL window: kill keeper, set up dm, restart with fresh fds ---
        #
        # We use SIGKILL (not SIGSTOP/SIGCONT) to avoid the fd-split problem:
        # with SIGSTOP+SIGCONT the process resumes with existing open fds pointing
        # at the original docker overlay filesystem while new path-opens go to the
        # dm ext4.  After restart, Changelog detects the gap between the old fd-
        # written log entries (up to index N) and the new path-opened entries
        # (starting at N+100000+) and throws CORRUPTED_DATA.
        # SIGKILL guarantees the process has no open fds, so after restart every
        # write goes exclusively through dm ext4.
        _kill_clickhouse(node, "KILL")
        # Wait for the process to exit and release all fds before touching mounts.
        sh(
            node,
            "for i in $(seq 40); do pidof clickhouse clickhouse-server 2>/dev/null || break; sleep 0.5; done; true",
            timeout=25,
        )
        try:
            sh_root_strict(node, "umount /var/lib/clickhouse/coordination || true", timeout=60)
            sh_root_strict(node, f"mkdir -p /var/lib/clickhouse/coordination /mnt/{dm_name} || true", timeout=30)

            if not ram:
                # Mount the loop device DIRECTLY (bypassing dm delay) to copy data fast.
                # The dm device does not exist yet so the loop device is free to mount.
                sh_root_strict(
                    node,
                    f"mount -t ext4 -o rw,nodelalloc,data=writeback,noatime {backend_dev} /mnt/{dm_name}",
                    timeout=60,
                )
                sh_root(
                    node,
                    f"cp -a /var/lib/clickhouse/coordination/. /mnt/{dm_name}/ 2>/dev/null || true; sync || true",
                    timeout=180,
                )
                # Fix ext4 root ownership: mkfs.ext4 creates root:root root dir, but
                # the bind mount maps this root dir to /var/lib/clickhouse/coordination.
                # If the keeper process (non-root user) tries to create STATE_COPY_LOCK
                # or other files directly in coordination/, it gets Permission denied.
                # Copy ownership+mode from the original coordination dir to the ext4 root.
                sh_root(
                    node,
                    (
                        f"_coord_owner=$(stat -c '%u:%g' /var/lib/clickhouse/coordination 2>/dev/null || echo ''); "
                        f"_coord_mode=$(stat -c '%a' /var/lib/clickhouse/coordination 2>/dev/null || echo ''); "
                        f"[ -n \"$_coord_owner\" ] && chown \"$_coord_owner\" /mnt/{dm_name}/ 2>/dev/null || true; "
                        f"[ -n \"$_coord_mode\" ] && chmod \"$_coord_mode\" /mnt/{dm_name}/ 2>/dev/null || true"
                    ),
                    timeout=30,
                )
                sh_root_strict(node, f"umount /mnt/{dm_name}", timeout=60)

            # Create the delay dm device (fast — just writes a kernel table entry).
            try:
                sh_root_strict(
                    node,
                    f"dmsetup create {dm_name} --table '0 {sz} delay {backend_dev} 0 {ms} {backend_dev} 0 {ms}'",
                    timeout=60,
                )
            except Exception as e:
                raise AssertionError(f"dm_delay: dmsetup create failed: {e}") from e

            sh_root_strict(node, f"dmsetup mknodes {dm_name} || dmsetup mknodes", timeout=60)
            sh(
                node,
                f"i=0; while [ ! -e /dev/mapper/{dm_name} ] && [ $i -lt 50 ]; do sleep 0.1; i=$((i+1)); done; [ -e /dev/mapper/{dm_name} ] && echo ok || (dmsetup info {dm_name} && false)",
                timeout=30,
            )

            if ram:
                # RAM devices are already formatted; format via dm (RAM is instant anyway).
                sh_root_strict(node, f"wipefs -a /dev/mapper/{dm_name} || true", timeout=60)
                sh_root_strict(
                    node,
                    f"mkfs.ext4 -E nodiscard,lazy_itable_init=0,lazy_journal_init=0 -F /dev/mapper/{dm_name}",
                    timeout=120,
                )
                sh_root_strict(node, "sync || true", timeout=60)

            # Mount the dm device (with delay) and bind it to coordination.
            sh_root_strict(
                node,
                f"mount -t ext4 -o rw,nodelalloc,data=writeback,noatime /dev/mapper/{dm_name} /mnt/{dm_name}",
                timeout=60,
            )
            sh_root_strict(
                node,
                f"mount --bind /mnt/{dm_name} /var/lib/clickhouse/coordination",
                timeout=30,
            )
            # Restart keeper with fresh fds pointing entirely at dm ext4.
            # All coordination I/O now goes through the delayed device.
            try:
                node.start_clickhouse(start_wait_sec=90)
            except Exception as e:
                raise AssertionError(f"dm_delay: keeper failed to restart after setup: {e}") from e
        except Exception as e:
            raise AssertionError(f"dm_delay: setup failed: {e}") from e
        yield
    finally:
        # --- CLEANUP: SIGKILL instead of SIGSTOP to avoid stale fd EIO ---
        #
        # SIGSTOP + dmsetup remove -f leaves RocksDB and NuRaft file descriptors
        # pointing at the removed block device.  When the process resumes via SIGCONT
        # those stale fds return EIO, crashing the keeper and expiring all sessions.
        # SIGKILL causes the kernel to close all fds immediately, so the dm device
        # can be cleanly dismantled before restarting keeper with a fresh fd table.
        if backend_dev is not None:
            _kill_clickhouse(node, "KILL")
            # Wait for the process to actually exit so the kernel releases all dm-backed fds.
            sh(
                node,
                "for i in $(seq 40); do pidof clickhouse clickhouse-server 2>/dev/null || break; sleep 0.5; done; true",
                timeout=25,
            )

        # Replace dm delay table with a no-delay linear mapping before flushing.
        # Without this, sync+umount must push every dirty page through the 3ms
        # delay target, turning ~30s of buffered writes into ~90s of flush time
        # and blowing the for_each_target deadline.  A linear reload is instant
        # (just a kernel table swap) so sync and umount complete in <5s.
        if sz is not None and backend_dev is not None:
            sh_root(
                node,
                (
                    f"dmsetup suspend {dm_name} 2>/dev/null || true; "
                    f"dmsetup load {dm_name} --table '0 {sz} linear {backend_dev} 0' 2>/dev/null || true; "
                    f"dmsetup resume {dm_name} 2>/dev/null || true"
                ),
                timeout=30,
            )

        sh_root(
            node,
            (
                f"sync || true; "
                f"umount /var/lib/clickhouse/coordination 2>/dev/null || umount -l /var/lib/clickhouse/coordination 2>/dev/null || true; "
                f"mountpoint -q /var/lib/clickhouse/coordination && umount -l /var/lib/clickhouse/coordination 2>/dev/null || true; "
                f"umount /mnt/{dm_name} 2>/dev/null || umount -l /mnt/{dm_name} 2>/dev/null || true; "
                f"dmsetup remove --retry -f {dm_name} || true"
            ),
            timeout=90,
        )
        _wait_dm_removed(node, dm_name, 40)
        if _check_dm_exists(node, dm_name):
            raise AssertionError(
                f"dm_delay: cleanup failed: dm device {dm_name} still exists on {node.name} after removal attempt"
            )

        # Copy coordination data back by mounting the loop device directly (no dm delay).
        # This is fast even for large coordination directories.
        #
        # Notes:
        #  - Mount rw (not ro): ext4 journal recovery requires a read-write mount after
        #    an unclean shutdown.  A read-only mount fails silently (|| true) when the
        #    journal hasn't been replayed, resulting in an empty /mnt/{dm_name} and an
        #    empty coordination directory that prevents keeper from starting.
        #  - Clear destination first: cp -a does not delete stale files.  Leftover
        #    RocksDB SST/WAL files from before the fault confuse keeper on restart.
        if backend_dev and not ram:
            sh_root_strict(node, f"mkdir -p /var/lib/clickhouse/coordination /mnt/{dm_name}", timeout=10)
            # Strict mount: a silently-failed mount means cp reads an empty
            # mountpoint and restores an empty coordination directory, causing
            # keeper to fail to start without any visible error.
            sh_root_strict(node, f"mount -t ext4 -o rw {backend_dev} /mnt/{dm_name}", timeout=30)
            r_ls = sh_root(node, f"ls -la /mnt/{dm_name}/ 2>&1", timeout=10)
            print(f"[dm_delay][copyback][{node.name}] loop files: {(r_ls or {}).get('out', '')!r}")
            # Clear stale pre-fault files, then copy back — both strict so silent
            # failure cannot silently leave an empty coordination directory.
            sh_root_strict(node, f"find /var/lib/clickhouse/coordination -mindepth 1 -delete", timeout=30)
            sh_root_strict(node, f"cp -a /mnt/{dm_name}/. /var/lib/clickhouse/coordination/", timeout=60)
            r_coord = sh_root(node, f"ls -la /var/lib/clickhouse/coordination/ 2>&1", timeout=10)
            print(f"[dm_delay][copyback][{node.name}] coord files: {(r_coord or {}).get('out', '')!r}")
            sh_root(node, f"sync || true; umount /mnt/{dm_name} 2>/dev/null || true", timeout=30)
            # Restore coordination dir ownership from parent (/var/lib/clickhouse)
            # in case it was recreated as root:root during cleanup teardown.
            sh_root(
                node,
                f"chown $(stat -c '%u:%g' /var/lib/clickhouse 2>/dev/null || echo '0:0') "
                f"  /var/lib/clickhouse/coordination 2>/dev/null || true",
                timeout=10,
            )
        else:
            sh_root(node, "mkdir -p /var/lib/clickhouse/coordination || true", timeout=10)

        if backend_dev is not None:
            _restart_clickhouse(node)
        _detach_loop_for_image(node, img)
        sh_root(node, f"rm -f {img} || true", timeout=30)


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

    # dm_delay cleanup (kill keeper + flush + copy-back + restart) takes much
    # longer than the default 60s parallel slack.  Set a large slack so the
    # for_each_target deadline covers the full cleanup+restart cycle.
    step = {**step, "target_parallel_slack_s": step.get("target_parallel_slack_s", 300.0)}
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


