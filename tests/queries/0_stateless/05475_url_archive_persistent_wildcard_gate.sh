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


def make_archive(value):
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, 'w') as writer:
        writer.writestr('value.tsv', value)
    return archive.getvalue()


ARCHIVES = {'/archive1.zip': make_archive(b'1\n'), '/archive2.zip': make_archive(b'2\n')}
INDEX = b''.join(b'<a href="%s">%s</a>\n' % (name.encode(), name.encode()) for name in ARCHIVES)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def respond(self, with_body):
        path = self.path.split('?')[0]
        if path == '/':
            data, content_type = INDEX, 'text/html'
        elif path in ARCHIVES:
            data, content_type = ARCHIVES[path], 'application/zip'
        else:
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
        self.send_header('Content-Type', content_type)
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
# The outer `*` is expanded by listing the HTTP index page, which is what the experimental setting
# gates. An explicit column list skips schema inference, so only the storage construction can check it.
ARCHIVE_GLOB_URL="http://127.0.0.1:${HTTP_PORT}/archive*.zip :: value.tsv"

# Prints the rows an arm asked for, then whether the experimental gate refused anything. Counting the
# gate rather than echoing the error keeps the reference free of connection wording. An arm that the
# gate is expected to refuse exits non-zero, so the failure is read from the output, not from `set -e`.
run_local() {
    local out
    out=$(${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "$1" \
        -- --max_server_memory_usage=10G --memory_worker_use_cgroup=0 2>&1) || true
    echo "$out" | grep -E '^(created|read|reloaded|attached)\b' || true
    echo -n 'gate refused: '
    echo "$out" | grep -c 'SUPPORT_IS_DISABLED' || true
}

echo '--- explicit structure, the setting off'
run_local "
    SET allow_archive_path_syntax = 1, http_max_tries = 1;
    CREATE DATABASE gate_off;
    CREATE TABLE gate_off.t (x UInt64) AS url('${ARCHIVE_GLOB_URL}', 'TSV', 'x UInt64');
    SELECT 'created', engine FROM system.tables WHERE database = 'gate_off' AND name = 't';"

echo '--- explicit structure, the setting on'
run_local "
    SET allow_archive_path_syntax = 1, allow_url_wildcard_from_index_pages = 1, http_max_tries = 1;
    CREATE DATABASE gate_on;
    CREATE TABLE gate_on.t (x UInt64) AS url('${ARCHIVE_GLOB_URL}', 'TSV', 'x UInt64');
    SELECT 'created', engine FROM system.tables WHERE database = 'gate_on' AND name = 't';
    SELECT 'read', count(), sum(x) FROM gate_on.t;"

echo '--- reloaded, the index-page setting off'
# A definition read back from the stored metadata was validated when it was created, so the gate does
# not run again for it: refusing it here would block loading the table.
run_local "
    SET allow_archive_path_syntax = 1, http_max_tries = 1;
    SELECT 'reloaded', engine FROM system.tables WHERE database = 'gate_on' AND name = 't';
    SELECT 'read', count(), sum(x) FROM gate_on.t;"

echo '--- attached again, the index-page setting off'
# A short `ATTACH TABLE` reads the definition back from the metadata, so it is not a fresh definition and
# the gate does not run again for it either.
run_local "
    SET allow_archive_path_syntax = 1, http_max_tries = 1;
    DETACH TABLE gate_on.t;
    ATTACH TABLE gate_on.t;
    SELECT 'attached', engine FROM system.tables WHERE database = 'gate_on' AND name = 't';
    SELECT 'read', count(), sum(x) FROM gate_on.t;"

echo '--- attached again with the archive syntax off is rejected'
# A short `ATTACH TABLE` reads the definition back under this session, so a session value of
# `allow_archive_path_syntax` that differs from the server default would make the attached table read a
# different resource than the one its columns describe. That is rejected, like at creation time.
out=$(${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "
    SET allow_archive_path_syntax = 0, http_max_tries = 1;
    DETACH TABLE gate_on.t;
    ATTACH TABLE gate_on.t;
    SELECT 'attached', engine FROM system.tables WHERE database = 'gate_on' AND name = 't';" \
    -- --max_server_memory_usage=10G --memory_worker_use_cgroup=0 2>&1) || true
echo "$out" | grep -E '^(attached)\b' || true
echo -n 'archive syntax refused: '
echo "$out" | grep -c 'differs from the server default' || true
