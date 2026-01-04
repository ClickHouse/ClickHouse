import gzip
import json
import os
import random
import time

_SEEDED = False


def _get_helper():
    try:
        from tests.ci.clickhouse_helper import ClickHouseHelper  # type: ignore
    except Exception:
        return None

    url = (
        os.environ.get("CI_DB_URL")
        or os.environ.get("CLICKHOUSE_TEST_STAT_URL")
        or None
    )
    user = (
        os.environ.get("CI_DB_USER")
        or os.environ.get("CLICKHOUSE_TEST_STAT_LOGIN")
        or None
    )
    password = (
        os.environ.get("CI_DB_PASSWORD")
        or os.environ.get("CLICKHOUSE_TEST_STAT_PASSWORD")
        or None
    )
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
            pass

    try:
        return ClickHouseHelper()
    except Exception:
        return None


def has_ci_sink():
    """Return True if CI ClickHouse test-stat credentials are available."""
    return _get_helper() is not None


def ensure_sink_schema(_url_ignored=None):
    """Ensure metrics DB/table exist using CI ClickHouseHelper credentials.

    Ignores any passed URL and uses the standard CI test-stat endpoint and
    credentials, same as other test frameworks.
    """
    global _SEEDED
    if _SEEDED:
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
        ORDER BY (run_id, scenario, node, stage, name, ts)
        TTL ts + INTERVAL 30 DAY DELETE""",
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
    """Insert rows into metrics table using CI ClickHouse credentials.

    URL parameter is ignored; credentials are resolved like other tests.
    """
    if not rows:
        return
    helper = _get_helper()
    if helper is None:
        return
    ensure_sink_schema()
    db = os.environ.get("KEEPER_METRICS_DB", "keeper_stress_tests")
    t = table if "." in table else f"{db}.{table}"
    # Chunk rows to reduce request size and improve retry behavior
    chunk_size = 1000
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        body = "\n".join(json.dumps(r, ensure_ascii=False) for r in chunk)
        # ClickHouseHelper handles retries internally
        from tests.ci.clickhouse_helper import ClickHouseHelper as _CH  # type: ignore
        _CH.insert_json_str(helper.url, helper.auth, db, t, body)
