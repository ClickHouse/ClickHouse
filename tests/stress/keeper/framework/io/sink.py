import json
import os


def _sanitize_filename_component(x):
    """Sanitize string for use in filename."""
    s = str(x).strip().replace(" ", "_")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
    name = "".join(ch if ch in allowed else "_" for ch in s) or "unknown"
    return name[:80]


def has_ci_sink():
    """Return True if metrics file path is configured."""
    return bool(os.environ.get("KEEPER_METRICS_FILE", "").strip())


def _write_jsonl_lines(path, rows):
    """Append rows as JSONL to path."""
    try:
        with open(path, "a", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
    except OSError:
        pass


def sink_clickhouse(_url_ignored, table, rows):
    """Write rows to sidecar JSONL file.

    Supports per-test splitting when KEEPER_METRICS_SPLIT_PER_TEST is truthy.
    Files are grouped by (run_id, scenario) to limit file size.
    """
    if not rows:
        return
    sidecar = os.environ.get("KEEPER_METRICS_FILE", "").strip()
    if not sidecar:
        return
    # Ensure directory exists
    try:
        os.makedirs(os.path.dirname(sidecar) or ".", exist_ok=True)
    except OSError:
        pass
    split = os.environ.get("KEEPER_METRICS_SPLIT_PER_TEST", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    if split:
        base, ext = sidecar, ".jsonl"
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
    else:
        _write_jsonl_lines(sidecar, rows)
