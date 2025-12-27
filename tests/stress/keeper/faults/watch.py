import threading
import time
import uuid

from kazoo.client import KazooClient
from kazoo.recipe.watchers import DataWatch

from ..framework.core.registry import register_fault
from ..workloads.adapter import servers_arg


@register_fault("watch_flood")
def _f_watch_flood(ctx, nodes, leader, step):
    try:
        watchers = int(step.get("watchers", 1000))
    except Exception:
        watchers = 1000
    try:
        depth = int(step.get("depth", 1))
    except Exception:
        depth = 1
    prefix = str(step.get("path_prefix", "/watch")).strip() or "/watch"

    # Build host list for Kazoo
    hosts = [h.strip() for h in (servers_arg(nodes) or "").split() if h.strip()]
    hostlist = ",".join(hosts) if hosts else "localhost:9181"

    zk = KazooClient(hosts=hostlist, timeout=10.0)
    try:
        zk.start(timeout=10.0)
    except Exception:
        return

    # Helper to compute a nested path for i-th watcher
    def _path_for(i: int) -> str:
        base = prefix.rstrip("/")
        if depth > 1:
            for d in range(1, max(1, depth)):
                base = base + f"/d{(i % (d + 3))}"
        return base + f"/n{i}"

    # Ensure a reasonable number of parents exist to avoid heavy retries
    try:
        for i in range(min(64, max(1, watchers // 10))):
            p = _path_for(i)
            parent = p.rsplit("/", 1)[0]
            try:
                zk.ensure_path(parent)
            except Exception:
                pass
    except Exception:
        pass

    watches = []

    def _make_watch(p: str):
        try:
            def _cb(data, stat):
                return True
            w = DataWatch(zk, p, func=_cb)
            return w
        except Exception:
            return None

    for i in range(watchers):
        p = _path_for(i)
        w = _make_watch(p)
        if w is not None:
            watches.append(w)

    # Keep references and client to close in teardown
    try:
        arr = ctx.get("_watch_flood_clients") or []
        arr.append(zk)
        ctx["_watch_flood_clients"] = arr
    except Exception:
        pass
    try:
        keep = ctx.get("_watch_flood_watches") or []
        keep.extend(watches)
        ctx["_watch_flood_watches"] = keep
    except Exception:
        pass

    # Optionally touch nodes to generate events if requested
    if step.get("touch_once"):
        try:
            for i in range(min(128, watchers)):
                p = _path_for(i)
                parent = p.rsplit("/", 1)[0]
                try:
                    zk.ensure_path(parent)
                except Exception:
                    pass
                try:
                    zk.create(p, b"x", ephemeral=False, makepath=True)
                except Exception:
                    try:
                        zk.set(p, uuid.uuid4().hex.encode("utf-8"))
                    except Exception:
                        pass
        except Exception:
            pass

    # Return immediately; watchers persist while zk is alive
    return
