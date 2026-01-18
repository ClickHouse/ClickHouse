import json
import os


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
    """Write rows to sidecar JSONL file."""
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
    _write_jsonl_lines(sidecar, rows)
