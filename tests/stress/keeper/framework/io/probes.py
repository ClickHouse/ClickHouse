import json

from ..core.settings import CLIENT_PORT, CONTROL_PORT, PROM_PORT
from ..core.util import has_bin, sh


def four(node, cmd):
    # Prefer nc when available
    try:
        if has_bin(node, "nc"):
            out = sh(node, f"printf '{cmd}\\n' | nc -w1 127.0.0.1 {CLIENT_PORT}")["out"]
            if str(out).strip():
                return out
    except Exception:
        pass
    # Fallbacks: keeper-client variants (avoid history file by setting HOME)
    for c in [
        f"HOME=/tmp timeout 2s clickhouse keeper-client --host 127.0.0.1 --port {CLIENT_PORT} -q '{cmd}'",
        f"HOME=/tmp timeout 2s clickhouse keeper-client -p {CLIENT_PORT} -q '{cmd}'",
    ]:
        try:
            out = sh(node, c + " 2>/dev/null")["out"]
            if str(out).strip():
                return out
        except Exception:
            continue
    # Last resort: use bash /dev/tcp to send 4lw and read reply
    try:
        devtcp_inner = (
            f"exec 3<>/dev/tcp/127.0.0.1/{CLIENT_PORT}; "
            f"printf '{cmd}\\n' >&3; "
            f"cat <&3; "
            f"exec 3<&-; exec 3>&-"
        )
        out = sh(node, f'timeout 2s bash -lc "{devtcp_inner}"')["out"]
        if str(out).strip():
            return out
    except Exception:
        pass
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
        if "\t" in line:
            k, v = line.split("\t", 1)
            kv[k.strip()] = v.strip()
    return kv


def wchs_total(node):
    out = four(node, "wchs")
    for line in out.splitlines():
        if line.lower().startswith("total watches"):
            try:
                return int(line.split(":")[1].strip())
            except:
                pass
    try:
        return int(mntr(node).get("zk_watch_count", "0"))
    except:
        return 0


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
            except:
                pass
    return kv


def prom_metrics(node):
    return sh(node, f"curl -sf http://127.0.0.1:{PROM_PORT}/metrics")["out"]


def ready(node):
    if not CONTROL_PORT:
        return False
    return (
        sh(
            node,
            f"curl -sf -o /dev/null -w '%{{http_code}}' http://127.0.0.1:{CONTROL_PORT}/ready",
        )["out"].strip()
        == "200"
    )


def ch_metrics(node):
    try:
        txt = node.query("SELECT name, value FROM system.metrics FORMAT JSONEachRow")
        return [json.loads(l) for l in txt.strip().splitlines() if l.strip()]
    except Exception:
        return []


def ch_async_metrics(node):
    try:
        txt = node.query(
            "SELECT name, value FROM system.asynchronous_metrics FORMAT JSONEachRow"
        )
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
    try:
        return four(node, "dirs")
    except Exception:
        return ""
