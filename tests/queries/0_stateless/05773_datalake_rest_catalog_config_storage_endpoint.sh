#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: `DataLakeCatalog` requires the USE_AVRO and USE_PARQUET build options.

# An Iceberg REST catalog can set `s3.endpoint` for all its tables in the `defaults` and `overrides` of its
# configuration, `overrides` winning. The endpoint vended with the table takes precedence over both, and the
# `storage_endpoint` setting over all of them, and a table outside S3 keeps its location. The stub serves the catalog
# and the object storage: every endpoint names its case and its source, which the stub records from the storage
# requests and answers with 404.

# The failing table reads are logged by the server, keep them off the client's stderr.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.port"
rm -f "$PORT_FILE"

python3 - "$PORT_FILE" <<'EOF' &
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

recorded = {}
lock = threading.Lock()


def cases(port):
    def endpoint(case, source):
        return {"s3.endpoint": f"http://127.0.0.1:{port}/storage/{case}/{source}"}

    # <case>: (defaults, overrides, config of the load-table response, table location)
    return {
        "defaults": (endpoint("defaults", "defaults"), {}, {}, "s3://bucket/ns/t"),
        "overrides": (endpoint("overrides", "defaults"), endpoint("overrides", "overrides"), {}, "s3://bucket/ns/t"),
        "table": (endpoint("table", "defaults"), endpoint("table", "overrides"), endpoint("table", "table"), "s3://bucket/ns/t"),
        "setting": (endpoint("setting", "defaults"), endpoint("setting", "overrides"), {}, "s3://bucket/ns/t"),
        "local": (endpoint("local", "defaults"), {}, {}, "file:///nonexistent/ns/t"),
    }


def load_table(table_config, location):
    metadata = {
        "format-version": 2, "table-uuid": "4f3c2b1a-0d9e-4c8b-a7f6-e5d4c3b2a190", "location": location,
        "last-sequence-number": 0, "last-updated-ms": 1, "last-column-id": 1,
        "current-schema-id": 0,
        "schemas": [{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": False, "type": "int"}]}],
        "default-spec-id": 0, "partition-specs": [{"spec-id": 0, "fields": []}], "last-partition-id": 999,
        "default-sort-order-id": 0, "sort-orders": [{"order-id": 0, "fields": []}],
        "properties": {}, "snapshots": [], "snapshot-log": [], "metadata-log": [],
    }
    config = {"s3.access-key-id": "key", "s3.secret-access-key": "secret", **table_config}
    return {"metadata-location": f"{location}/metadata/v1.metadata.json", "metadata": metadata, "config": config}


class Handler(BaseHTTPRequestHandler):
    def reply(self, code, body=b""):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def storage_request(self, parts):
        if len(parts) >= 3 and parts[0] == "storage":
            with lock:
                recorded.setdefault(parts[1], set()).add(parts[2])
        self.reply(404)

    def do_HEAD(self):
        self.storage_request(self.path.partition("?")[0].strip("/").split("/"))

    def do_GET(self):
        parts = self.path.partition("?")[0].strip("/").split("/")
        known = cases(self.server.server_address[1])
        if parts == ["ready"]:
            return self.reply(200, b"{}")
        if len(parts) == 2 and parts[0] in known and parts[1] == "requests":
            with lock:
                return self.reply(200, " ".join(sorted(recorded.pop(parts[0], {"no request"}))).encode())
        if len(parts) >= 3 and parts[0] in known and parts[1] == "v1":
            defaults, overrides, table_config, location = known[parts[0]]
            if parts[2:] == ["config"]:
                return self.reply(200, json.dumps({"defaults": defaults, "overrides": overrides}).encode())
            if parts[2:] == ["namespaces", "ns", "tables", "t"]:
                return self.reply(200, json.dumps(load_table(table_config, location)).encode())
            return self.reply(404, b'{"error": {"message": "Not found", "type": "NoSuchTableException", "code": 404}}')
        self.storage_request(parts)

    def log_message(self, *args):
        pass


server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
with open(sys.argv[1] + ".tmp", "w") as f:
    f.write(str(server.server_address[1]))
os.replace(sys.argv[1] + ".tmp", sys.argv[1])
server.serve_forever()
EOF
HTTP_PID=$!

CASES="defaults overrides table setting local"
cleanup() {
    kill $HTTP_PID 2>/dev/null
    for c in $CASES; do
        ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_${c}" 2>/dev/null
    done
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

for c in $CASES; do
    settings="catalog_type = 'rest', warehouse = 'wh'"
    if [ "$c" = "setting" ]; then
        settings="${settings}, storage_endpoint = 'http://127.0.0.1:${HTTP_PORT}/storage/setting/setting'"
    fi
    ${CLICKHOUSE_CLIENT} --allow_database_iceberg=1 --query "
        CREATE DATABASE ${CLICKHOUSE_DATABASE}_${c} ENGINE = DataLakeCatalog('http://127.0.0.1:${HTTP_PORT}/${c}/v1')
        SETTINGS ${settings}"
    if [ "$c" = "local" ]; then
        # Not an S3 location, so the endpoint of the catalog configuration does not apply to it.
        echo "${c}: $(${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE ${CLICKHOUSE_DATABASE}_${c}.\`ns.t\` FORMAT TSVRaw" | grep -o "ENGINE = .*")"
        continue
    fi
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${CLICKHOUSE_DATABASE}_${c}.\`ns.t\`" >/dev/null 2>&1
    echo "${c}: $(${CLICKHOUSE_CURL} -s "http://127.0.0.1:${HTTP_PORT}/${c}/requests")"
done
