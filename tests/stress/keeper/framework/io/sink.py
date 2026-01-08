import json, time, random, gzip, os
_SEEDED = False

def ensure_sink_schema(url):
    if not url:
        return
    global _SEEDED
    if _SEEDED:
        return
    import requests
    def _auth_headers():
        u = os.environ.get("KEEPER_METRICS_CLICKHOUSE_USER", "").strip()
        p = os.environ.get("KEEPER_METRICS_CLICKHOUSE_PASSWORD", "").strip()
        return {"X-ClickHouse-User": u, "X-ClickHouse-Key": p} if u and p else {}
    db = os.environ.get("KEEPER_METRICS_DB", "keeper_stress_tests").strip() or "keeper_stress_tests"
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
    for ddl in ddls:
        last_exc = None
        for attempt in range(2):
            try:
                r = requests.post(url, params={"query": ddl}, headers=_auth_headers(), timeout=20)
                r.raise_for_status()
                last_exc = None
                break
            except Exception as e:
                last_exc = e
                time.sleep(0.5 * (attempt + 1))
    _SEEDED = True

def sink_clickhouse(url, table, rows):
    if not url or not rows: return
    import requests
    ensure_sink_schema(url)
    db = os.environ.get("KEEPER_METRICS_DB", "keeper_stress_tests").strip() or "keeper_stress_tests"
    t = table if "." in table else f"{db}.{table}"
    q=f"INSERT INTO {t} FORMAT JSONEachRow"
    # Chunk rows to reduce request size and improve retry behavior
    chunk_size = 1000
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i+chunk_size]
        body = "\n".join(json.dumps(r, ensure_ascii=False) for r in chunk).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        # Compress large bodies
        if len(body) > 1_000_000:
            body = gzip.compress(body)
            headers["Content-Encoding"] = "gzip"
        last_exc=None
        for attempt in range(4):
            try:
                auth = {}
                u = os.environ.get("KEEPER_METRICS_CLICKHOUSE_USER", "").strip()
                p = os.environ.get("KEEPER_METRICS_CLICKHOUSE_PASSWORD", "").strip()
                if u and p:
                    auth = {"X-ClickHouse-User": u, "X-ClickHouse-Key": p}
                # Merge headers (content-encoding) with auth headers
                h = dict(auth)
                h.update(headers)
                r=requests.post(url, params={"query": q}, data=body, headers=h, timeout=20)
                if r.status_code>=500 or r.status_code==429:
                    raise requests.HTTPError(f"{r.status_code} {r.text[:256]}")
                r.raise_for_status(); break
            except Exception as e:
                last_exc=e
                sleep = min(2**attempt, 8) * random.uniform(0.7, 1.3)
                time.sleep(sleep)
        if last_exc:
            raise last_exc
