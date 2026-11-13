#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: `DataLakeCatalog` requires the USE_AVRO and USE_PARQUET build options.

# An Iceberg REST namespace can end with an empty level: `["a", ""]` is `a.` in ClickHouse. Its children are listed
# with `parent` naming both levels, and a request to create such a namespace names both levels too.

# The CREATE TABLE below fails, keep its server logs off the client's stderr.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.port"
POSTED_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.posted"
rm -f "$PORT_FILE" "$POSTED_FILE"

# Resolves a namespace by its 0x1F-separated levels, an empty last level included, and answers
# `NoSuchNamespaceException` for an unknown one. A namespace creation is recorded, not performed.
python3 - "$PORT_FILE" "$POSTED_FILE" <<'EOF' &
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit

tables = {("a",): ["t_a"], ("a", ""): ["t_a_empty"], ("a", "", "b"): ["t_a_empty_b"]}


class Handler(BaseHTTPRequestHandler):
    def reply(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def no_such_namespace(self):
        self.reply(404, {"error": {"message": "Namespace does not exist", "type": "NoSuchNamespaceException", "code": 404}})

    def do_GET(self):
        url = urlsplit(self.path)
        parts = url.path.strip("/").split("/")
        if parts == ["ready"]:
            return self.reply(200, {})
        if parts == ["v1", "config"]:
            return self.reply(200, {"defaults": {}, "overrides": {}})
        if parts == ["v1", "namespaces"]:
            parent = parse_qs(url.query, keep_blank_values=True).get("parent")
            levels = tuple(parent[0].split("\x1f")) if parent else ()
            if levels and levels not in tables:
                return self.no_such_namespace()
            return self.reply(200, {"namespaces": sorted(list(ns) for ns in tables if ns[:-1] == levels)})
        if len(parts) >= 3 and parts[:2] == ["v1", "namespaces"]:
            levels = tuple(unquote(parts[2]).split("\x1f"))
            if levels not in tables:
                return self.no_such_namespace()
            if parts[3:] == ["tables"]:
                return self.reply(200, {"identifiers": [{"namespace": list(levels), "name": t} for t in tables[levels]]})
        self.reply(404, {"error": {"message": "Not found", "type": "NoSuchTableException", "code": 404}})

    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
        with open(sys.argv[2], "a") as f:
            f.write(json.dumps(request["namespace"]) + "\n")
        self.reply(200, {"namespace": request["namespace"], "properties": {}})

    def log_message(self, *args):
        pass


server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
with open(sys.argv[1] + ".tmp", "w") as f:
    f.write(str(server.server_address[1]))
os.replace(sys.argv[1] + ".tmp", sys.argv[1])
server.serve_forever()
EOF
HTTP_PID=$!

DB="${CLICKHOUSE_DATABASE}_catalog"
cleanup() {
    kill $HTTP_PID 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}" 2>/dev/null
    rm -f "$PORT_FILE" "$POSTED_FILE"
}
trap cleanup EXIT

for _ in {1..100}; do
    [ -s "$PORT_FILE" ] && break
    sleep 0.1
done
HTTP_PORT=$(cat "$PORT_FILE")
for _ in {1..100}; do
    [ "$(${CLICKHOUSE_CURL} -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:${HTTP_PORT}/ready")" = "200" ] && break
    sleep 0.1
done

${CLICKHOUSE_CLIENT} --allow_database_iceberg=1 --query "
    CREATE DATABASE ${DB} ENGINE = DataLakeCatalog('http://127.0.0.1:${HTTP_PORT}/v1')
    SETTINGS catalog_type = 'rest', warehouse = 'wh'"

# The table of `["a", "", "b"]` is found through the children of `["a", ""]`.
${CLICKHOUSE_CLIENT} --query "SHOW TABLES FROM ${DB}"

# `["c", ""]` does not exist, so a table in it makes the database create it first. The catalog gives no location
# either way, so the CREATE fails, and only the requested namespace is checked.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB}.\`c..t\` (x Int32)" 2>/dev/null
cat "$POSTED_FILE"
