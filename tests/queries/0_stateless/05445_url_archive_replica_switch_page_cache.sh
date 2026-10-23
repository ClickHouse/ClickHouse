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

# A setting alone cannot enable an unallocated cache. Keep a private cache alive across all
# queries, with a fixed capacity large enough that neither resource needs to be evicted.
cat > "${work_dir}/config.yaml" <<'EOF'
page_cache_min_size: 16777216
page_cache_max_size: 16777216
query_log:
    database: system
    table: query_log
EOF

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


def make_archive(value):
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr(zipfile.ZipInfo("data.tsv"), (value + b"\n") * 100000)
    return data.getvalue()


# Equal-length stored ZIPs keep offsets, sizes and the strong ETag identical. Only the selected
# URL distinguishes the resources; an ETag is not a content identifier across different URLs.
archives = {"primary": make_archive(b"1"), "mirror": make_archive(b"2")}
assert len(archives["primary"]) == len(archives["mirror"])
assert archives["primary"] != archives["mirror"]
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
archive_url = f"{base_url}/data.zip?replica={{primary|mirror}}::data.tsv"
try:
    # This is a private loopback server; inherited proxy settings must not route readiness elsewhere.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(f"{base_url}/ready", timeout=10) as response:
        assert response.read() == b"ready\n"

    # The synchronous reader must reach the page cache rather than an async prefetch buffer.
    # Disable bypass/eviction injection and other caches so the warm query proves a real hit.
    # The control query completes before the next archive query starts, with the exact same
    # failover set and path as before. Both HEAD and GET on the primary then fail explicitly.
    queries = f"""
        SET allow_archive_path_syntax = 1,
            use_page_cache_for_object_storage = 1,
            remote_filesystem_read_method = 'read',
            page_cache_block_size = 65536,
            page_cache_inject_eviction = 0,
            read_from_page_cache_if_exists_otherwise_bypass_cache = 0,
            enable_filesystem_cache = 0,
            use_query_cache = 0,
            use_cache_for_count_from_files = 0,
            max_threads = 1,
            max_download_threads = 1,
            http_max_tries = 1;
        SELECT 'primary cold', count(), sum(x) FROM url('{archive_url}', 'TSV', 'x UInt64')
            SETTINGS log_comment = 'primary_cold';
        SELECT 'primary warm', count(), sum(x) FROM url('{archive_url}', 'TSV', 'x UInt64')
            SETTINGS log_comment = 'primary_warm';
        SELECT 'cache allocated', sum(value) > 0 FROM system.metrics WHERE metric = 'PageCacheBytes';
        SELECT * FROM url('{base_url}/disable-primary', 'TSV', 'status String');
        SELECT 'mirror', count(), sum(x) FROM url('{archive_url}', 'TSV', 'x UInt64')
            SETTINGS log_comment = 'mirror';
        SYSTEM FLUSH LOGS query_log;
        SELECT 'cold miss', countIf(ProfileEvents['PageCacheMisses'] > 0) = 1
            FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'primary_cold';
        SELECT 'warm hit', countIf(ProfileEvents['PageCacheHits'] > 0 AND ProfileEvents['PageCacheMisses'] = 0) = 1
            FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'primary_warm';
        SELECT 'mirror miss', countIf(ProfileEvents['PageCacheMisses'] > 0) = 1
            FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'mirror';
    """
    subprocess.run(
        sys.argv[2:]
        + ["--path", str(work_dir / "db"), "--config-file", str(work_dir / "config.yaml"),
           "--log_queries", "1", "--multiquery", "--query", queries],
        check=True,
        timeout=120,
    )
    assert data_reads["primary"] > 0, "the cold query did not read the primary"
    assert primary_failures > 0, "the mirror query did not fail over"
    assert data_reads["mirror"] > 0, "the mirror query reused the primary's cached bytes"
finally:
    server.shutdown()
    server.server_close()
    server_thread.join()
PY
