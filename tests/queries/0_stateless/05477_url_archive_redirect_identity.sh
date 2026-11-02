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

# A redirect lets one URL name different resources over time, and `max_http_get_redirects` is what makes
# the metadata probe follow it. The resource the caches have to name is therefore the target of the
# redirect, not the URL the query wrote: with the URL alone, the row count and the schema of the first
# target would answer for the second one, which serves a different archive under the same path, the same
# `Last-Modified` and the same ETag.
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


# The two targets serve different contents: one row of `a` against two rows of `b`. The ETag and the
# modification time are the same on purpose, because neither can tell the two resources apart.
targets = {"1": make_archive(b'{"a":1}\n'), "2": make_archive(b'{"b":2}\n{"b":3}\n')}
current_target = "1"
target_reads = {"1": 0, "2": 0}


class Handler(BaseHTTPRequestHandler):
    def respond(self, head):
        global current_target
        url = urlsplit(self.path)
        if url.path in ("/ready", "/flip"):
            if url.path == "/flip" and not head:
                current_target = "2"
            body = b"ready\n" if url.path == "/ready" else b"flipped\n"
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if not head:
                self.wfile.write(body)
            return

        if url.path == "/archive.zip":
            # The URL the query names; it never serves content itself.
            self.send_response(302)
            self.send_header("Location", f"/target.zip?which={current_target}")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        if url.path != "/target.zip":
            self.send_error(404)
            return
        which = parse_qs(url.query)["which"][0]
        body = targets[which]
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
        # still matches, so the two targets have to look equally old for the entry to be considered.
        self.send_header("Last-Modified", "Mon, 01 Jan 2024 00:00:00 GMT")
        if byte_range is not None:
            self.send_header("Content-Range", f"bytes {begin}-{end}/{len(body)}")
        self.end_headers()
        if not head:
            target_reads[which] += 1
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
archive_url = f"{base_url}/archive.zip::data.json"
try:
    # This is a private loopback server; inherited proxy settings must not route readiness elsewhere.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(f"{base_url}/ready", timeout=10) as response:
        assert response.read() == b"ready\n"

    # The caches are the subject, so they are enabled explicitly. The query log tells a served row count
    # or schema (`SchemaInferenceCache*Hits`) from a read one, which the values alone cannot: reading the
    # target every time would give the same answers.
    queries = f"""
        SET allow_archive_path_syntax = 1,
            max_http_get_redirects = 1,
            use_cache_for_count_from_files = 1,
            max_threads = 1,
            max_download_threads = 1,
            http_max_tries = 1;
        SELECT 'schema cold';
        SELECT * FROM url('{archive_url}', 'JSONEachRow') LIMIT 0 FORMAT TSVWithNamesAndTypes
            SETTINGS log_comment = 'schema_cold';
        SELECT 'schema warm';
        SELECT * FROM url('{archive_url}', 'JSONEachRow') LIMIT 0 FORMAT TSVWithNamesAndTypes
            SETTINGS log_comment = 'schema_warm';
        SELECT 'count cold', count() FROM url('{archive_url}', 'JSONEachRow', 'a UInt64')
            SETTINGS log_comment = 'count_cold';
        SELECT 'count warm', count() FROM url('{archive_url}', 'JSONEachRow', 'a UInt64')
            SETTINGS log_comment = 'count_warm';
        SELECT * FROM url('{base_url}/flip', 'TSV', 'status String');
        SELECT 'schema after the redirect target changed';
        SELECT * FROM url('{archive_url}', 'JSONEachRow') LIMIT 0 FORMAT TSVWithNamesAndTypes
            SETTINGS log_comment = 'schema_flip';
        SELECT 'count after the redirect target changed', count() FROM url('{archive_url}', 'JSONEachRow', 'b UInt64')
            SETTINGS log_comment = 'count_flip';
        SYSTEM FLUSH LOGS query_log;
        SELECT 'schema warm hit', countIf(ProfileEvents['SchemaInferenceCacheSchemaHits'] > 0) = 1
            FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'schema_warm';
        SELECT 'schema flip miss', countIf(ProfileEvents['SchemaInferenceCacheSchemaHits'] = 0) = 1
            FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'schema_flip';
        SELECT 'count warm hit', countIf(ProfileEvents['SchemaInferenceCacheNumRowsHits'] > 0) = 1
            FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'count_warm';
        SELECT 'count flip miss', countIf(ProfileEvents['SchemaInferenceCacheNumRowsHits'] = 0) = 1
            FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'count_flip';
    """
    subprocess.run(
        sys.argv[2:]
        + ["--path", str(work_dir / "db"), "--config-file", str(work_dir / "config.yaml"),
           "--log_queries", "1", "--multiquery", "--query", queries],
        check=True,
        timeout=120,
        stdin=subprocess.DEVNULL,
    )
    assert target_reads["1"] > 0, "the first target was never read"
    assert target_reads["2"] > 0, "the second target reused what the first one served"
finally:
    server.shutdown()
    server.server_close()
    server_thread.join()
PY
