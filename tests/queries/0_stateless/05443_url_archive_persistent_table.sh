#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: reading ZIP archives requires minizip, disabled in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

WORKING_FOLDER=$(mktemp -d "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.XXXXXX")
HTTP_PID=
cleanup()
{
    if [[ -n "$HTTP_PID" ]]; then
        kill "$HTTP_PID" 2>/dev/null || true
        wait "$HTTP_PID" 2>/dev/null || true
    fi
    rm -rf "$WORKING_FOLDER"
}
trap cleanup EXIT

# Publish the port only after binding, avoiding port reservation races and readiness sleeps.
coproc ARCHIVE_HTTP {
    exec python3 -u - <<'PY'
import io
from http.server import BaseHTTPRequestHandler, HTTPServer
import zipfile

archive = io.BytesIO()
with zipfile.ZipFile(archive, 'w') as writer:
    writer.writestr('value.tsv', b'42\n')
data = archive.getvalue()

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def respond(self, with_body):
        if self.path != '/a.zip':
            self.send_error(404)
            return
        start, end = 0, len(data) - 1
        byte_range = self.headers.get('Range')
        if byte_range:
            first, last = byte_range[6:].split('-', 1)
            if first:
                start = int(first)
                if last:
                    end = min(int(last), end)
            else:
                start = max(0, len(data) - int(last))
            if start > end:
                self.send_error(416)
                return
        self.send_response(206 if byte_range else 200)
        self.send_header('Content-Type', 'application/zip')
        self.send_header('Accept-Ranges', 'bytes')
        self.send_header('Content-Length', str(end - start + 1))
        if byte_range:
            self.send_header('Content-Range', f'bytes {start}-{end}/{len(data)}')
        self.end_headers()
        if with_body:
            self.wfile.write(data[start:end + 1])

    def do_HEAD(self):
        self.respond(False)

    def do_GET(self):
        self.respond(True)

server = HTTPServer(('127.0.0.1', 0), Handler)
print(server.server_port, flush=True)
server.serve_forever()
PY
}
HTTP_PID=$ARCHIVE_HTTP_PID
read -r -t 30 HTTP_PORT <&"${ARCHIVE_HTTP[0]}"
ARCHIVE_URL="http://127.0.0.1:${HTTP_PORT}/a.zip :: value.tsv"

# Both explicit columns and schema inference must create an archive-aware persistent table.
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}/local" --multiquery --query "
    SET allow_archive_path_syntax = 1, http_max_tries = 1;
    CREATE DATABASE d;
    CREATE TABLE d.explicit_schema (x UInt64) AS url('${ARCHIVE_URL}', 'TSV', 'x UInt64');
    CREATE TABLE d.inferred_schema AS url('${ARCHIVE_URL}', 'TSV');
    SELECT 'created explicit', x FROM d.explicit_schema;
    SELECT 'created inferred', c1 FROM d.inferred_schema;
    INSERT INTO d.explicit_schema VALUES (99); -- { error NOT_IMPLEMENTED }
    INSERT INTO d.inferred_schema VALUES (99); -- { error NOT_IMPLEMENTED }
    INSERT INTO FUNCTION url('${ARCHIVE_URL}', 'TSV', 'x UInt64') VALUES (99); -- { error NOT_IMPLEMENTED }
    SELECT 'creation writes rejected';
" -- --max_server_memory_usage=10G --memory_worker_use_cgroup=0

# A fresh process replays the persisted metadata with cached columns, then reads and rejects writes.
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}/local" --multiquery --query "
    SET allow_archive_path_syntax = 1, http_max_tries = 1;
    SELECT 'reloaded explicit', x FROM d.explicit_schema;
    SELECT 'reloaded inferred', c1 FROM d.inferred_schema;
    INSERT INTO d.explicit_schema VALUES (99); -- { error NOT_IMPLEMENTED }
    INSERT INTO d.inferred_schema VALUES (99); -- { error NOT_IMPLEMENTED }
    CREATE NAMED COLLECTION archive_custom_method AS
        url = '${ARCHIVE_URL}', format = 'TSV', structure = 'x UInt64', http_method = 'PUT';
    INSERT INTO FUNCTION url(archive_custom_method) VALUES (99); -- { error BAD_ARGUMENTS }
    SELECT 'replay writes rejected';
" -- --max_server_memory_usage=10G --memory_worker_use_cgroup=0
