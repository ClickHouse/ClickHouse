#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

python3 - <<'PY'
import concurrent.futures
import contextlib
import http.client
import json
import os
import shlex
import subprocess
import time
import urllib.parse

url = urllib.parse.urlsplit(os.environ["CLICKHOUSE_URL"])
client = shlex.split(os.environ["CLICKHOUSE_CLIENT"])
prefix = os.environ["CLICKHOUSE_TEST_UNIQUE_NAME"] + "_" + str(os.getpid())
base_params = dict(urllib.parse.parse_qsl(url.query))


def sql(query, *options):
    return subprocess.check_output(client + list(options) + ["--query", query], text=True).strip()


def connection():
    cls = http.client.HTTPSConnection if url.scheme == "https" else http.client.HTTPConnection
    return contextlib.closing(cls(url.hostname, url.port, timeout=60))


def request(conn, session, query, agent="sessions-first", **params):
    params = {**base_params, "session_id": session, **params}
    conn.request("POST", url.path + "?" + urllib.parse.urlencode(params), query,
                 {"User-Agent": agent, "X-Forwarded-For": "198.51.100.20"})
    response = conn.getresponse()
    body = response.read().decode()
    assert response.status == 200, body
    return body.strip()


def sessions():
    rows = sql("SELECT session_id, auth_id, event_time_microseconds, current_query_id, "
               "client_port, http_user_agent FROM system.sessions "
               f"WHERE startsWith(session_id, '{prefix}') FORMAT JSONEachRow")
    return {row["session_id"]: row for row in map(json.loads, rows.splitlines())}


def wait_for(predicate):
    deadline = time.monotonic() + 30
    while not predicate():
        assert time.monotonic() < deadline, "Timed out waiting for session state"
        time.sleep(0.05)


persistent, expired, idle, active, concurrent_session = [prefix + suffix for suffix in
                                              ("_persistent", "_expired", "_idle", "_active", "_concurrent")]
query_ids = [prefix + "_query_1", prefix + "_query_2"]
query_filter = "query_id IN (" + ",".join(repr(q) for q in query_ids) + ")"
try:
    with connection() as conn:
        request(conn, persistent, "CREATE TEMPORARY TABLE t (x UInt8) AS SELECT 42", session_timeout=60)
    wait_for(lambda: persistent in sessions() and not sessions()[persistent]["current_query_id"])
    original = sessions()[persistent]
    with connection() as conn:
        assert request(conn, persistent, "SELECT x FROM t", agent="sessions-reused", session_check=1) == "42"
    reused = sessions()[persistent]
    assert (reused["auth_id"], reused["event_time_microseconds"]) == (original["auth_id"], original["event_time_microseconds"])
    assert reused["http_user_agent"] == "sessions-reused"
    with connection() as conn:
        request(conn, persistent, "SELECT 1 FORMAT Null", close_session=1)
    wait_for(lambda: persistent not in sessions())
    print("named session: persistence, reuse and close OK")

    with connection() as conn:
        request(conn, expired, "SELECT 1 FORMAT Null", session_timeout=1)
    wait_for(lambda: expired not in sessions())
    with connection() as conn:
        conn.request("POST", url.path + "?" + urllib.parse.urlencode(
            {**base_params, "session_id": expired, "session_check": 1}), "SELECT 1")
        response = conn.getresponse()
        assert "SESSION_NOT_FOUND" in response.read().decode()
    print("named session: expiry OK")

    # The idle and active sessions deliberately share one keep-alive endpoint, even on servers
    # without forwarded-address authentication. With it enabled, all three share the forwarded IP.
    with connection() as first, connection() as second:
        request(first, idle, "SELECT 1 FORMAT Null", session_timeout=60)
        port = sessions()[idle]["client_port"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            def run(conn, name, query_id):
                conn.request("POST", url.path + "?" + urllib.parse.urlencode(
                    {**base_params, "session_id": name, "query_id": query_id, "session_timeout": 60}),
                    "SELECT sleepEachRow(0.1) FROM numbers(1000) SETTINGS max_block_size=1 FORMAT Null",
                    {"X-Forwarded-For": "198.51.100.20"})
                response = conn.getresponse()
                body = response.read().decode()
                assert "QUERY_WAS_CANCELLED" in body, body

            futures = [pool.submit(run, first, active, query_ids[0]),
                       pool.submit(run, second, concurrent_session, query_ids[1])]
            try:
                wait_for(lambda: sql(f"SELECT count() FROM system.processes WHERE {query_filter}") == "2")
                rows = sessions()
                assert rows[idle]["client_port"] == rows[active]["client_port"] == port
                assert rows[idle]["current_query_id"] == "", rows
                assert rows[active]["current_query_id"] == query_ids[0], rows
                assert rows[concurrent_session]["current_query_id"] == query_ids[1], rows
            finally:
                sql(f"KILL QUERY WHERE {query_filter} SYNC")
            for future in futures:
                future.result(timeout=30)
    wait_for(lambda: all(not row["current_query_id"] for row in sessions().values()))
    print("query attribution: shared endpoint and concurrent requests OK")
finally:
    sql(f"KILL QUERY WHERE {query_filter} SYNC", "--kill_throw_if_noop=0")
    for name in sessions():
        with connection() as conn:
            request(conn, name, "SELECT 1 FORMAT Null", close_session=1)
PY
