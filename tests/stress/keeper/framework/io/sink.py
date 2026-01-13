import json
import os

def _sanitize_filename_component(x: object) -> str:
    try:
        s = str(x)
    except Exception:
        s = ""
    s = s.strip().replace(" ", "_")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
    res = [ch if ch in allowed else "_" for ch in s]
    name = "".join(res) or "unknown"
    return name[:80]
 


def _get_helper():
    try:
        from tests.ci.clickhouse_helper import ClickHouseHelper  # type: ignore
    except Exception:
        return None

    url = os.environ.get("KEEPER_METRICS_CLICKHOUSE_URL") or None
    user = os.environ.get("CI_DB_USER") or None
    password = os.environ.get("CI_DB_PASSWORD") or None
    if url and user and password:
        try:
            return ClickHouseHelper(
                url=url,
                auth={
                    "X-ClickHouse-User": user,
                    "X-ClickHouse-Key": password,
                },
            )
        except Exception:
            return None
    return None


def has_ci_sink():
    """Return True if CI ClickHouse test-stat credentials are available."""
    try:
        if os.environ.get("KEEPER_METRICS_FILE"):
            return True
    except Exception:
        pass
    return _get_helper() is not None

def _write_jsonl_lines(path, rows):
    try:
        with open(path, "a", encoding="utf-8") as f:
            for r in rows:
                try:
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")
                except Exception:
                    pass
    except Exception:
        pass
def sink_clickhouse(_url_ignored, table, rows):
    """Write rows to sidecar JSONL for host-side ingestion.

    Supports per-test splitting when KEEPER_METRICS_SPLIT_PER_TEST is truthy.
    Files are grouped by (run_id, scenario) to limit file size and simplify ingestion.
    """
    if not rows:
        return
    try:
        sidecar = (os.environ.get("KEEPER_METRICS_FILE") or "").strip()
    except Exception:
        sidecar = ""
    if not sidecar:
        return
    # Ensure directory exists (host-side preflight)
    try:
        d = os.path.dirname(sidecar) or "."
        os.makedirs(d, exist_ok=True)
    except Exception:
        pass
    split = False
    try:
        split = os.environ.get(
            "KEEPER_METRICS_SPLIT_PER_TEST", "0"
        ).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        split = False
    if split:
        base = sidecar
        ext = ".jsonl"
        if base.lower().endswith(ext):
            base = base[: -len(ext)]
        groups = {}
        for r in rows:
            rid = _sanitize_filename_component((r or {}).get("run_id") or "run")
            scen = _sanitize_filename_component((r or {}).get("scenario") or "scenario")
            path = f"{base}__{rid}__{scen}{ext}"
            groups.setdefault(path, []).append(r)
        for path, rs in groups.items():
            _write_jsonl_lines(path, rs)
        return
    # Fallback: single file
    _write_jsonl_lines(sidecar, rows)
