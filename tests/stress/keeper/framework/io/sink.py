import json
import os

from keeper.framework.core.util import env_bool


def _write_jsonl_lines(path, rows):
    """Append rows as JSONL to path."""
    with open(path, "a", encoding="utf-8") as f:
        for r in rows:
            if env_bool("KEEPER_METRICS_DEBUG", False):
                print(f"writing row: {json.dumps(r, ensure_ascii=False)} to {path}")
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
        

def sink_clickhouse(rows):
    """Write rows to sidecar JSONL file."""
    if not rows:
        return
    sidecar = os.environ.get("KEEPER_METRICS_FILE", "").strip()
    if not sidecar:
        raise AssertionError("KEEPER_METRICS_FILE is not set")
    # Ensure directory exists
    try:
        os.makedirs(os.path.dirname(sidecar) or ".", exist_ok=True)
    except OSError:
        raise AssertionError(f"Failed to create directory for {sidecar}")
    _write_jsonl_lines(sidecar, rows)
