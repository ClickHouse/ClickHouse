#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: reading ZIP archives requires minizip, disabled in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -euo pipefail

mkdir -p "${CLICKHOUSE_TMP}/tmp"
work_dir=$(mktemp -d "${CLICKHOUSE_TMP}/tmp/${CLICKHOUSE_TEST_UNIQUE_NAME}.XXXXXX")
trap 'rm -rf "${work_dir}"' EXIT

# `clickhouse local` keeps the query log only when a configuration names it.
cat > "${work_dir}/config.yaml" <<'EOF'
query_log:
    database: system
    table: query_log
EOF

# The schema cache that carries the row counts (`use_cache_for_count_from_files`, on by default) and
# the inferred schema keys an object by its path within the storage. A Web failover group names
# several resources under one path, and the metadata probe picks one of them per read, so the path
# alone identifies the group rather than the resource: two endpoints serving different archives under
# the same path, with the same `Last-Modified` and the same ETag, would share one entry. Both halves
# of that entry have to belong to the endpoint they were read from - a row count of one endpoint must
# not answer for the other, and the schema of one must not be handed to a query that reads the other.
python3 - "${work_dir}" ${CLICKHOUSE_LOCAL} <<'PY'
import io
import subprocess
import sys
import threading
import urllib.request
import zipfile
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit


def make_archive(member):
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr(zipfile.ZipInfo("data.json"), member)
    return data.getvalue()


# The endpoints serve different contents under the same member path: one row of `a` against two rows
# of `b`. The ETag is the same on purpose, because it is scoped to one endpoint and cannot tell the
# two resources apart.
archives = {"primary": make_archive(b'{"a":1}\n'), "mirror": make_archive(b'{"b":2}\n{"b":3}\n')}
primary_enabled = True
data_reads = {"primary": 0, "mirror": 0}
primary_failures = 0


class Handler(BaseHTTPRequestHandler):
    def respond(self, head):
        global primary_enabled, primary_failures
        url = urlsplit(self.path)
        if url.path in ("/ready", "/disable-primary"):
            if url.path == "/disable-primary" and not head:
                primary_enabled = False
            body = b"ready\n" if url.path == "/ready" else b"primary disabled\n"
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if not head:
                self.wfile.write(body)
            return

        if url.path != "/data.zip":
            self.send_error(404)
            return
        replica = parse_qs(url.query)["replica"][0]
        if replica == "primary" and not primary_enabled:
            primary_failures += 1
            self.send_error(503)
            return

        body = archives[replica]
        begin, end = 0, len(body) - 1
        byte_range = self.headers.get("Range") if not head else None
        if byte_range is not None:
            first, last = byte_range.removeprefix("bytes=").split("-")
            if first:
                begin = int(first)
                if last:
                    end = min(int(last), end)
            else:
                begin = max(0, len(body) - int(last))
        self.send_response(206 if byte_range is not None else 200)
        self.send_header("Content-Length", str(end - begin + 1))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("ETag", '"same-version"')
        # A cached schema or row count is reused only while the modification time it was recorded with
        # still matches, so the two endpoints have to look equally old for the entry to be considered.
        self.send_header("Last-Modified", "Mon, 01 Jan 2024 00:00:00 GMT")
        if byte_range is not None:
            self.send_header("Content-Range", f"bytes {begin}-{end}/{len(body)}")
        self.end_headers()
        if not head:
            data_reads[replica] += 1
            try:
                self.wfile.write(body[begin:end + 1])
            except (BrokenPipeError, ConnectionResetError):
                # Archive seeks can close a lookahead response before consuming its whole body.
                pass

    def do_HEAD(self):
        self.respond(head=True)

    def do_GET(self):
        self.respond(head=False)

    def log_message(self, *args):
        pass


# Bind port zero without releasing it, and synchronize with an HTTP request, not a fixed delay.
server = HTTPServer(("127.0.0.1", 0), Handler)
server_thread = threading.Thread(target=server.serve_forever, daemon=True)
server_thread.start()
base_url = f"http://127.0.0.1:{server.server_port}"
work_dir = Path(sys.argv[1])
archive_url = f"{base_url}/data.zip?replica={{primary|mirror}}::data.json"
try:
    # This is a private loopback server; inherited proxy settings must not route readiness elsewhere.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(f"{base_url}/ready", timeout=10) as response:
        assert response.read() == b"ready\n"

    # The count cache is part of the subject, so it is enabled explicitly. The query log tells a served
    # row count (`SchemaInferenceCacheNumRowsHits`) from a read one, which the counts alone cannot:
    # reading the mirror every time would give the same numbers.
    queries = f"""
        SET allow_archive_path_syntax = 1,
            use_cache_for_count_from_files = 1,
            max_threads = 1,
            max_download_threads = 1,
            http_max_tries = 1;
        SELECT 'primary schema';
        SELECT * FROM url('{archive_url}', 'JSONEachRow') LIMIT 0 FORMAT TSVWithNamesAndTypes
            SETTINGS log_comment = 'primary_schema_cold';
        SELECT * FROM url('{archive_url}', 'JSONEachRow') LIMIT 0 FORMAT TSVWithNamesAndTypes
            SETTINGS log_comment = 'primary_schema_warm';
        SELECT 'primary cold', count() FROM url('{archive_url}', 'JSONEachRow', 'a UInt64')
            SETTINGS log_comment = 'primary_cold';
        SELECT 'primary warm', count() FROM url('{archive_url}', 'JSONEachRow', 'a UInt64')
            SETTINGS log_comment = 'primary_warm';
        SELECT * FROM url('{base_url}/disable-primary', 'TSV', 'status String');
        SELECT 'mirror count', count() FROM url('{archive_url}', 'JSONEachRow', 'b UInt64')
            SETTINGS log_comment = 'mirror_count';
        SELECT 'mirror schema';
        SELECT * FROM url('{archive_url}', 'JSONEachRow') LIMIT 0 FORMAT TSVWithNamesAndTypes;
        SYSTEM FLUSH LOGS query_log;
        SELECT 'warm hit', countIf(ProfileEvents['SchemaInferenceCacheNumRowsHits'] > 0) = 1
            FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'primary_warm';
        SELECT 'mirror miss', countIf(ProfileEvents['SchemaInferenceCacheNumRowsHits'] = 0) = 1
            FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'mirror_count';
        SELECT 'schema warm hit', countIf(ProfileEvents['SchemaInferenceCacheSchemaHits'] > 0) = 1
            FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'primary_schema_warm';
        SELECT 'schema cold miss', countIf(ProfileEvents['SchemaInferenceCacheSchemaHits'] = 0) = 1
            FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'primary_schema_cold';
    """
    subprocess.run(
        sys.argv[2:]
        + ["--path", str(work_dir / "db"), "--config-file", str(work_dir / "config.yaml"),
           "--log_queries", "1", "--multiquery", "--query", queries],
        check=True,
        timeout=120,
        stdin=subprocess.DEVNULL,
    )
    assert data_reads["primary"] > 0, "the cold query did not read the primary"
    assert primary_failures > 0, "the mirror queries did not fail over"
    assert data_reads["mirror"] > 0, "the mirror queries reused what the primary served"
finally:
    server.shutdown()
    server.server_close()
    server_thread.join()
PY
