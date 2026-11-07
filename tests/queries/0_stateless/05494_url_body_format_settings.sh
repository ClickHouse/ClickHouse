#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")
python3 -u - "$PORT_FILE" <<'PY' &
import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        if self.headers.get("Transfer-Encoding") == "chunked":
            chunks = []
            while True:
                size = int(self.rfile.readline().strip(), 16)
                if not size:
                    while self.rfile.readline().strip():
                        pass
                    break
                chunks.append(self.rfile.read(size))
                self.rfile.read(2)
            body = b"".join(chunks)
        else:
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        response = json.dumps(
            {"body": body.decode(), "content_type": self.headers.get("Content-Type", "")}
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)


server = HTTPServer(("127.0.0.1", 0), Handler)
with open(sys.argv[1], "w") as port_file:
    port_file.write(str(server.server_port))
server.serve_forever()
PY
HTTP_PID=$!
trap 'kill "$HTTP_PID" 2>/dev/null || :; wait "$HTTP_PID" 2>/dev/null || :; rm -f "$PORT_FILE"' EXIT

for _ in {1..300}; do
    [[ -s "$PORT_FILE" ]] && break
    kill -0 "$HTTP_PID"
    sleep 0.1
done
HTTP_PORT=$(cat "$PORT_FILE")
[[ -n "$HTTP_PORT" ]]
URL="http://127.0.0.1:${HTTP_PORT}/"

# Both spellings of SETTINGS must control the serializer, independently of the enclosing query.
${CLICKHOUSE_CLIENT} --query "SELECT hex(body) FROM url('${URL}', JSONEachRow, 'body String, content_type String',
    body(SELECT 1 AS x, 2 AS y SETTINGS format_csv_delimiter = ';' FORMAT CSV)) SETTINGS format_csv_delimiter = ','"
${CLICKHOUSE_CLIENT} --query "SELECT hex(body) FROM url('${URL}', JSONEachRow, 'body String, content_type String',
    body(SELECT 1 AS x, 2 AS y FORMAT CSV SETTINGS format_csv_delimiter = ';'))"
${CLICKHOUSE_CLIENT} --query "SELECT hex(body) FROM url('${URL}', JSONEachRow, 'body String, content_type String',
    body(SELECT 1 AS x, 2 AS y UNION ALL SELECT 3, 4 FORMAT CSV SETTINGS format_csv_delimiter = ';', max_threads = 1))"


# The old planner receives an original AST instead of a resolved query tree.
${CLICKHOUSE_CLIENT} --query "SELECT hex(body) FROM url('${URL}', JSONEachRow, 'body String, content_type String',
    body(SELECT 1 AS x, 2 AS y UNION ALL SELECT 3, 4 FORMAT CSV SETTINGS format_csv_delimiter = ';', max_threads = 1)) SETTINGS enable_analyzer = 0"

# The same effective settings determine Content-Type as well as the bytes in the request.
${CLICKHOUSE_CLIENT} --query "SELECT JSONExtractUInt(body, 1, 'x'), content_type
    FROM url('${URL}', JSONEachRow, 'body String, content_type String',
        body(SELECT 1 AS x FORMAT JSONEachRow SETTINGS output_format_json_array_of_rows = 1))"
