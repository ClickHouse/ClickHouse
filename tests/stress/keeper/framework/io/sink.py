import gzip
import json
import os
import random
import time

_SEEDED = False

def _autocreate_enabled() -> bool:
    try:
        v = os.environ.get("KEEPER_AUTOCREATE_SCHEMA", "").strip().lower()
        return v in ("1", "true", "yes", "on")
    except Exception:
        return False


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


def ensure_sink_schema(_url_ignored=None):
    """Ensure metrics DB/table exist using CI ClickHouseHelper credentials.

    Ignores any passed URL and uses the standard CI test-stat endpoint and
    credentials, same as other test frameworks.
    """
    global _SEEDED
    if _SEEDED:
        return
    # Align with other tests: do not auto-create unless explicitly enabled
    if not _autocreate_enabled():
        return
    try:
        import requests
    except Exception:
        return

    helper = _get_helper()
    if helper is None:
        return
    url = helper.url
    auth = helper.auth
    db = (
        os.environ.get("KEEPER_METRICS_DB", "keeper_stress_tests").strip()
        or "keeper_stress_tests"
    )
    ddls = [
        f"CREATE DATABASE IF NOT EXISTS {db}",
        f"""CREATE TABLE IF NOT EXISTS {db}.keeper_metrics_ts (
            ts DateTime DEFAULT now(),
            run_id String,
            commit_sha String,
            backend String,
            scenario String,
            topology Int32,
            node String,
            stage String,
            source LowCardinality(String),
            name LowCardinality(String),
            value Float64,
            labels_json String DEFAULT '{{}}'
        ) ENGINE=MergeTree
        ORDER BY (run_id, scenario, node, stage, name, ts)""",
    ]
    ok = True
    for ddl in ddls:
        d_ok = False
        for attempt in range(2):
            try:
                r = requests.post(url, params={"query": ddl}, headers=auth, timeout=20)
                r.raise_for_status()
                d_ok = True
                break
            except Exception:
                time.sleep(0.5 * (attempt + 1))
        ok = ok and d_ok
    if ok:
        _SEEDED = True


def sink_clickhouse(_url_ignored, table, rows):
    """Write rows to sidecar JSONL for host-side ingestion."""
    if not rows:
        return
    try:
        sidecar = (os.environ.get("KEEPER_METRICS_FILE") or "").strip()
    except Exception:
        sidecar = ""
    if not sidecar:
        return
    try:
        with open(sidecar, "a", encoding="utf-8") as f:
            for r in rows:
                try:
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")
                except Exception:
                    pass
    except Exception:
        pass
