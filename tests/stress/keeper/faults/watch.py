import uuid

from kazoo.client import KazooClient
from keeper.framework.core.registry import register_fault
from keeper.workloads.adapter import servers_arg


@register_fault("watch_flood")
def _f_watch_flood(ctx, nodes, leader, step):
    """Create many watchers to stress watch management."""
    watchers = int(step.get("watchers", 1000))
    depth = int(step.get("depth", 1))
    prefix = str(step.get("path_prefix", "/watch")).strip() or "/watch"

    hosts = [h.strip() for h in (servers_arg(nodes) or "").split() if h.strip()]
    hostlist = ",".join(hosts) if hosts else "localhost:9181"

    zk = KazooClient(hosts=hostlist, timeout=10.0)
    try:
        zk.start(timeout=10.0)
    except Exception:
        return

    def _path_for(i):
        base = prefix.rstrip("/")
        for d in range(1, max(1, depth)):
            base = f"{base}/d{i % (d + 3)}"
        return f"{base}/n{i}"

    # Pre-create some parent paths
    for i in range(min(64, max(1, watchers // 10))):
        try:
            zk.ensure_path(_path_for(i).rsplit("/", 1)[0])
        except Exception:
            pass

    watches = []

    def _make_watch(p):
        def _cb(event):
            try:
                zk.exists(p, watch=_cb)
            except Exception:
                pass
            return True

        try:
            zk.exists(p, watch=_cb)
            return p
        except Exception:
            return None

    for i in range(watchers):
        w = _make_watch(_path_for(i))
        if w:
            watches.append(w)

    # Store references for teardown
    ctx.setdefault("_watch_flood_clients", []).append(zk)
    ctx.setdefault("_watch_flood_watches", []).extend(watches)

    # Touch nodes to generate events if requested
    if step.get("touch_once"):
        for i in range(min(128, watchers)):
            p = _path_for(i)
            try:
                zk.ensure_path(p.rsplit("/", 1)[0])
                try:
                    zk.create(p, b"x", ephemeral=False, makepath=True)
                except Exception:
                    zk.set(p, uuid.uuid4().hex.encode("utf-8"))
            except Exception:
                pass
