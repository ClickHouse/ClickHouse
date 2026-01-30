import random
import threading
import time
import uuid

from kazoo.client import KazooClient
from keeper.faults.registry import register_fault
from keeper.workloads.adapter import servers_arg


@register_fault("session_churn")
def _f_session_churn(ctx, nodes, leader, step):
    """Rapidly create/destroy ZK sessions to stress session management."""
    clients = int(step.get("clients", 128))
    rate = float(step.get("rate_per_s", 5.0))
    duration_s = int(step.get("duration_s", 60))
    eph_per_client = int(step.get("ephemerals_per_client", 0))
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
            # Create ephemerals
            for _ in range(max(0, eph_per_client)):
                p = f"{path_prefix.rstrip('/')}/{uuid.uuid4().hex}"
                try:
                    zk.ensure_path(p.rsplit("/", 1)[0])
                    zk.create(p, uuid.uuid4().hex.encode("utf-8"), ephemeral=True)
                except Exception:
                    pass
            time.sleep(random.uniform(0.05, 0.2))
        except Exception:
            pass
        finally:
            if zk:
                try:
                    zk.stop()
                    zk.close()
                except Exception:
                    pass
            sem.release()

    def _launcher():
        next_at = time.time()
        while time.time() < stop_ts:
            now = time.time()
            if now >= next_at and sem.acquire(blocking=False):
                th = threading.Thread(target=_one_iter, daemon=True)
                th.start()
                with lock:
                    active.append(th)
                next_at = now + launch_interval
            else:
                time.sleep(min(0.05, max(0, next_at - now)))

    launcher = threading.Thread(target=_launcher, daemon=True)
    launcher.start()

    # Wait for duration
    while time.time() < stop_ts:
        time.sleep(0.1)

    launcher.join(timeout=2.0)
    with lock:
        for th in active:
            th.join(timeout=1.0)
