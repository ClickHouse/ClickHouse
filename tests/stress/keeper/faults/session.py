import random
import threading
import time
import uuid

from kazoo.client import KazooClient

from ..framework.core.registry import register_fault
from ..workloads.adapter import servers_arg


@register_fault("session_churn")
def _f_session_churn(ctx, nodes, leader, step):
    try:
        clients = int(step.get("clients", 128))
    except Exception:
        clients = 128
    try:
        rate = float(step.get("rate_per_s", 5.0))
    except Exception:
        rate = 5.0
    try:
        duration_s = int(step.get("duration_s", 60))
    except Exception:
        duration_s = 60
    try:
        eph_per_client = int(step.get("ephemerals_per_client", 0))
    except Exception:
        eph_per_client = 0
    path_prefix = str(step.get("path_prefix", "/churn")).strip() or "/churn"

    hosts = [h.strip() for h in (servers_arg(nodes) or "").split() if h.strip()]
    hostlist = ",".join(hosts) if hosts else "localhost:9181"

    stop_ts = time.time() + max(1, duration_s)
    launch_interval = 1.0 / max(0.1, rate)
    lock = threading.Lock()
    active = []
    sem = threading.Semaphore(max(1, clients))

    def _one_iter():
        zk = None
        try:
            zk = KazooClient(hosts=hostlist, timeout=5.0)
            zk.start(timeout=5.0)
            # optional ephemerals burst
            for _ in range(max(0, eph_per_client)):
                p = f"{path_prefix.rstrip('/')}/{uuid.uuid4().hex}"
                try:
                    base = p.rsplit("/", 1)[0]
                    zk.ensure_path(base)
                except Exception:
                    pass
                try:
                    zk.create(p, uuid.uuid4().hex.encode("utf-8"), ephemeral=True)
                except Exception:
                    pass
            # small jitter
            time.sleep(random.uniform(0.05, 0.2))
        except Exception:
            pass
        finally:
            try:
                if zk is not None:
                    zk.stop()
                    zk.close()
            except Exception:
                pass
            try:
                sem.release()
            except Exception:
                pass

    def _launcher():
        next_at = time.time()
        while time.time() < stop_ts:
            now = time.time()
            if now >= next_at:
                # Launch only if we can acquire a slot; otherwise back off briefly
                if sem.acquire(blocking=False):
                    th = threading.Thread(target=_one_iter, daemon=True)
                    th.start()
                    with lock:
                        active.append(th)
                    next_at = now + launch_interval
                else:
                    next_at = now + min(0.1, launch_interval)
            else:
                time.sleep(min(0.05, next_at - now))

    launchers = []
    launcher_threads = 1
    for _ in range(launcher_threads):
        t = threading.Thread(target=_launcher, daemon=True)
        t.start()
        launchers.append(t)

    # Wait for duration
    while time.time() < stop_ts:
        time.sleep(0.1)

    # Join launched threads
    for t in launchers:
        try:
            t.join(timeout=2.0)
        except Exception:
            pass
    with lock:
        for th in active:
            try:
                th.join(timeout=1.0)
            except Exception:
                pass

    return
