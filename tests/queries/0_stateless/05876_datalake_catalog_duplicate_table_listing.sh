#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: `DataLakeCatalog` requires the USE_AVRO and USE_PARQUET build options.

# The catalog lists namespace `a` twice, table `a.x` twice in one page and table `b.y` on two pages.
# Every table must be listed once and its metadata must be loaded once.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.port"
rm -f "$PORT_FILE"

# Answers `NoSuchTableException` for every table, so `system.tables` keeps the row with a null storage.
# `/loads` returns how many times each table was requested and resets the counters.
python3 - "$PORT_FILE" <<'EOF' &
import json
import os
import sys
import threading
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit

pages = {"a": [(["x", "x"], None)], "b": [(["y"], "1"), (["y", "z"], None)]}
loads = Counter()
lock = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def reply(self, code, obj):
        body = obj.encode() if isinstance(obj, str) else json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        url = urlsplit(self.path)
        query = parse_qs(url.query)
        parts = url.path.strip("/").split("/")
        if parts == ["ready"]:
            return self.reply(200, {})
        if parts == ["loads"]:
            with lock:
                body = " ".join(f"{name}:{count}" for name, count in sorted(loads.items()))
                loads.clear()
            return self.reply(200, body)
        if parts == ["v1", "config"]:
            return self.reply(200, {"defaults": {}, "overrides": {}})
        if parts == ["v1", "namespaces"]:
            return self.reply(200, {"namespaces": [] if "parent" in query else [["a"], ["a"], ["b"]]})
        if len(parts) == 4 and parts[:2] == ["v1", "namespaces"] and parts[3] == "tables":
            namespace = unquote(parts[2])
            names, token = pages[namespace][int(query.get("pageToken", ["0"])[0])]
            response = {"identifiers": [{"namespace": [namespace], "name": name} for name in names]}
            if token:
                response["next-page-token"] = token
            return self.reply(200, response)
        if len(parts) == 5 and parts[:2] == ["v1", "namespaces"] and parts[3] == "tables":
            with lock:
                loads[unquote(parts[2]) + "." + unquote(parts[4])] += 1
        self.reply(404, {"error": {"message": "Table does not exist", "type": "NoSuchTableException", "code": 404}})

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
    rm -f "$PORT_FILE"
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

${CLICKHOUSE_CLIENT} --query "SHOW TABLES FROM ${DB}"
${CLICKHOUSE_CLIENT} --query "SHOW TABLES FROM ${DB} LIKE 'a.%'"

# Selecting `engine` resolves the storage of every listed table.
${CLICKHOUSE_CLIENT} --show_data_lake_catalogs_in_system_tables=1 --query "
    SELECT name, engine FROM system.tables WHERE database = '${DB}' ORDER BY name FORMAT CSV"
echo "loads: $(${CLICKHOUSE_CURL} -s "http://127.0.0.1:${HTTP_PORT}/loads")"
