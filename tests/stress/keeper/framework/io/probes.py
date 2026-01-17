import json

from keeper.framework.core.settings import CLIENT_PORT, CONTROL_PORT, PROM_PORT
from keeper.framework.core.util import has_bin, sh


def four(node, cmd):
    try:
        if has_bin(node, "nc"):
            out = sh(
                node, f"printf '{cmd}\\n' | nc -w1 127.0.0.1 {CLIENT_PORT}", timeout=5
            )["out"]
            if str(out).strip():
                return out
    except Exception:
        pass
    fb = [
        f"HOME=/tmp timeout 2s clickhouse keeper-client --host 127.0.0.1 --port {CLIENT_PORT} -q '{cmd}' 2>/dev/null",
        f"HOME=/tmp timeout 2s clickhouse keeper-client -p {CLIENT_PORT} -q '{cmd}' 2>/dev/null",
    ]
    devtcp_inner = (
        f"exec 3<>/dev/tcp/127.0.0.1/{CLIENT_PORT}; "
        f"printf '{cmd}\\n' >&3; "
        f"cat <&3; "
        f"exec 3<&-; exec 3>&-"
    )
    fb.append(f'timeout 2s bash -lc "{devtcp_inner}"')
    for c in fb:
        try:
            out = sh(node, c, timeout=5)["out"]
            if str(out).strip():
                return out
        except Exception:
            continue
    return ""


def is_leader(node):
    # Prefer 'stat' which always includes Mode: <role>
    out = four(node, "stat")
    if "Mode: leader" in out:
        return True
    if "Mode: follower" in out or "Mode: standalone" in out:
        return False
    # Fallback to 'srvr' (some builds also include Mode)
    out2 = four(node, "srvr")
    if "Mode: leader" in out2:
        return True
    # Final fallback: parse mntr key
    try:
        state = mntr(node).get("zk_server_state", "").strip().lower()
        return state == "leader"
    except Exception:
        return False


def count_leaders(nodes):
    return sum(1 for n in nodes if is_leader(n))


def mntr(node):
    kv = {}
    for line in four(node, "mntr").splitlines():
        s = line.strip()
        if not s:
            continue
        if "\t" in s:
            k, v = s.split("\t", 1)
            kv[k.strip()] = v.strip()
            continue
        parts = s.split()
        if len(parts) >= 2:
            k = parts[0].strip()
            v = parts[1].strip()
            if k:
                kv[k] = v
    return kv


def wchs_total(node):
    out = four(node, "wchs")
    for line in out.splitlines():
        if line.lower().startswith("total watches"):
            try:
                return int(line.split(":")[1].strip())
            except Exception:
                pass
    try:
        return int(mntr(node).get("zk_watch_count", "0"))
    except Exception:
        return 0


def wchp_paths(node):
    try:
        out = four(node, "wchp")
        counts = {}
        for line in out.splitlines():
            s = line.strip()
            if not s or not s.startswith("/"):
                continue
            parts = s.split()
            path = parts[0]
            num = None
            for tok in reversed(s.replace(",", " ").split()):
                try:
                    num = int(tok)
                    break
                except Exception:
                    continue
            if num is None:
                num = 1
            counts[path] = counts.get(path, 0) + int(num)
        return counts
    except Exception:
        return {}


def any_ephemerals(node):
    return "Sessions with Ephemerals" in four(node, "dump")


def lgif(node):
    out = four(node, "lgif")
    kv = {}
    for line in out.splitlines():
        p = line.split()
        if len(p) >= 2:
            try:
                kv[p[0]] = int(p[1])
            except Exception:
                pass
    return kv


def srvr_kv(node):
    out = four(node, "srvr")
    kv = {}
    for line in out.splitlines():
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        k = k.strip().lower()
        v = v.strip().split()[0]
        try:
            val = float(v)
        except Exception:
            continue
        if k.startswith("connections"):
            kv["connections"] = val
        elif k.startswith("outstanding"):
            kv["outstanding"] = val
        elif k.startswith("received"):
            kv["received"] = val
        elif k.startswith("sent"):
            kv["sent"] = val
    return kv


def prom_metrics(node):
    return sh(
        node, f"curl -sf --max-time 2 http://127.0.0.1:{PROM_PORT}/metrics", timeout=4
    )["out"]


def ready(node):
    if not CONTROL_PORT:
        return False
    return (
        sh(
            node,
            f"curl -sf --max-time 2 -o /dev/null -w '%{{http_code}}' http://127.0.0.1:{CONTROL_PORT}/ready",
            timeout=4,
        )["out"].strip()
        == "200"
    )


def ch_metrics(node):
    return _query_json_each_row(
        node, "SELECT name, value FROM system.metrics FORMAT JSONEachRow"
    )


def ch_async_metrics(node):
    return _query_json_each_row(
        node,
        "SELECT name, value FROM system.asynchronous_metrics FORMAT JSONEachRow",
    )


def _query_json_each_row(node, sql):
    try:
        txt = node.query(sql)
        return [json.loads(l) for l in txt.strip().splitlines() if l.strip()]
    except Exception:
        return []


def ch_trace_log(node, limit_rows=500):
    try:
        q = f"SELECT * FROM system.trace_log ORDER BY event_time DESC LIMIT {int(limit_rows)} FORMAT JSONEachRow"
        return node.query(q)
    except Exception:
        return ""


def dirs(node):
    """Return raw output of 4LW 'dirs' command (best-effort)."""
    return four(node, "dirs")
