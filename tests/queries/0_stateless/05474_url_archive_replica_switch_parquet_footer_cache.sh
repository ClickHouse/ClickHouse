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

# The Parquet metadata (footer) cache is keyed by the path of the object and its ETag. For an archive
# member the path is the member path inside the archive and the ETag is the one of the archive, which
# is scoped to one endpoint: two endpoints of a failover group can serve different archives under the
# same path with the same ETag. The footer carries the row-group statistics the filter is pruned by,
# so a shared entry makes the second endpoint skip the row groups the first one excluded. The warm
# query below proves the cache is in use, and the mirror query must miss it.
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


def make_parquet(value):
    """One row group holding a single row, so its min/max statistics exclude the other value.

    `stdin` is closed because `clickhouse local` reads an inherited one as input, and a query without
    a `FROM` then runs once per block of it, which writes a file whose row count is not the one asked
    for.
    """
    result = subprocess.run(
        sys.argv[2:] + ["--query", f"SELECT toUInt64({value}) AS x FORMAT Parquet"],
        check=True,
        capture_output=True,
        stdin=subprocess.DEVNULL,
    )
    return result.stdout


def make_archive(member):
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr(zipfile.ZipInfo("data.parquet"), member)
    return data.getvalue()


# Equal length keeps the archives interchangeable for the byte-range logic; the ETag is the same on
# purpose, because an ETag is scoped to one endpoint and cannot tell the two resources apart.
archives = {"primary": make_archive(make_parquet(1)), "mirror": make_archive(make_parquet(100))}
assert len(archives["primary"]) == len(archives["mirror"])
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
archive_url = f"{base_url}/data.zip?replica={{primary|mirror}}::data.parquet"
try:
    # This is a private loopback server; inherited proxy settings must not route readiness elsewhere.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(f"{base_url}/ready", timeout=10) as response:
        assert response.read() == b"ready\n"

    # The count cache is off so that the footer cache is the only thing that can answer for the file. The
    # query condition cache is on: the same two queries also prove that the row groups one endpoint pruned
    # are not skipped for the other one.
    queries = f"""
        SET allow_archive_path_syntax = 1,
            use_parquet_metadata_cache = 1,
            use_query_condition_cache = 1,
            use_cache_for_count_from_files = 0,
            max_threads = 1,
            max_download_threads = 1,
            http_max_tries = 1;
        SELECT 'primary cold', count() FROM url('{archive_url}', 'Parquet', 'x UInt64') WHERE x = 100
            SETTINGS log_comment = 'primary_cold';
        SELECT 'primary warm', count() FROM url('{archive_url}', 'Parquet', 'x UInt64') WHERE x = 100
            SETTINGS log_comment = 'primary_warm';
        SELECT * FROM url('{base_url}/disable-primary', 'TSV', 'status String');
        SELECT 'mirror', count() FROM url('{archive_url}', 'Parquet', 'x UInt64') WHERE x = 100
            SETTINGS log_comment = 'mirror';
        SYSTEM FLUSH LOGS query_log;
        SELECT 'cold miss', countIf(ProfileEvents['ParquetMetadataCacheMisses'] > 0) = 1
            FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'primary_cold';
        SELECT 'warm hit', countIf(ProfileEvents['ParquetMetadataCacheHits'] > 0) = 1
            FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'primary_warm';
        SELECT 'mirror miss', countIf(ProfileEvents['ParquetMetadataCacheMisses'] > 0) = 1
            FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'mirror';
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
    assert primary_failures > 0, "the mirror query did not fail over"
    assert data_reads["mirror"] > 0, "the mirror query reused the primary's cached footer"
finally:
    server.shutdown()
    server.server_close()
    server_thread.join()
PY
