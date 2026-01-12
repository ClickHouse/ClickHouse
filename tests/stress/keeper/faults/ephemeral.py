import uuid
from kazoo.client import KazooClient

from ..framework.core.registry import register_fault
from ..workloads.adapter import servers_arg


@register_fault("create_ephemerals")
def _f_create_ephemerals(ctx, nodes, leader, step):
    try:
        count = int(step.get("count", 1000))
    except Exception:
        count = 1000
    prefix = str(step.get("path_prefix", "/sem_ephem")).strip() or "/sem_ephem"

    hosts = [h.strip() for h in (servers_arg(nodes) or "").split() if h.strip()]
    hostlist = ",".join(hosts) if hosts else "localhost:9181"

    zk = None
    try:
        zk = KazooClient(hosts=hostlist, timeout=10.0)
        zk.start(timeout=10.0)
    except Exception:
        zk = None
    if zk is None:
        return

    try:
        base = prefix.rstrip("/")
        zk.ensure_path(base)
        for _ in range(max(1, count)):
            p = f"{base}/{uuid.uuid4().hex}"
            try:
                zk.create(p, uuid.uuid4().hex.encode("utf-8"), ephemeral=True)
            except Exception:
                pass
        arr = ctx.get("_ephem_clients") or []
        arr.append(zk)
        ctx["_ephem_clients"] = arr
    except Exception:
        try:
            zk.stop()
            zk.close()
        except Exception:
            pass


@register_fault("stop_ephemeral_client")
def _f_stop_ephemeral_client(ctx, nodes, leader, step):
    arr = ctx.get("_ephem_clients") or []
    if not arr:
        return
    # Stop one or all ephemeral-holding clients to trigger expiry
    try:
        if bool(step.get("all", False)):
            clients = list(arr)
            arr.clear()
        else:
            clients = [arr.pop(0)]
        for zk in clients:
            try:
                zk.stop()
                zk.close()
            except Exception:
                pass
        ctx["_ephem_clients"] = arr
    except Exception:
        pass
