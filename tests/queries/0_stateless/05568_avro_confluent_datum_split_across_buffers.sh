#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Avro format is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `format_binary_max_string_size` limits one `RowBinary` string and does not apply to `AvroConfluent`. A valid message must
# read the same whether it is inside one buffer of the input or is split between buffers.

SCHEMA='{"type":"record","name":"r","fields":[{"name":"a","type":"string"},{"name":"b","type":"string"}]}'

PORT_FILE=$(mktemp "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.XXXXXX.port")

# A schema registry that serves the schema above for any id. It binds to port 0 and reports the port
# it got, so parallel runs cannot collide.
python3 -c "
import json, os, sys
from http.server import HTTPServer, BaseHTTPRequestHandler
body = json.dumps({'schema': sys.argv[1]}).encode()
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *args):
        pass
server = HTTPServer(('127.0.0.1', 0), Handler)
with open(sys.argv[2] + '.tmp', 'w') as f:
    f.write(str(server.server_address[1]))
os.replace(sys.argv[2] + '.tmp', sys.argv[2])
server.serve_forever()
" "$SCHEMA" "$PORT_FILE" &
HTTP_PID=$!
DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.bin"
trap 'kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null; rm -f "$PORT_FILE" "$DATA_FILE"' EXIT

for _ in $(seq 1 300); do
    [[ -s "$PORT_FILE" ]] && break
    sleep 0.1
done
PORT=$(cat "$PORT_FILE")

# One message framed with magic byte 0 and schema id 1: a record of two 60-byte strings, a 122-byte datum.
python3 -c "
import struct, sys
out = b'\x00' + struct.pack('>I', 1)
for s in (b'x' * 60, b'y' * 60):
    out += bytes([len(s) << 1]) + s
sys.stdout.buffer.write(out)
" > "$DATA_FILE"

read_message()
{
    $CLICKHOUSE_LOCAL --input-format AvroConfluent --format_avro_schema_registry_url "http://127.0.0.1:$PORT" \
        -S "a String, b String" -q "SELECT length(a), length(b) FROM table" "$@" < "$DATA_FILE"
}

echo "contiguous, limit 100"
read_message --format_binary_max_string_size 100
echo "split into 30-byte buffers, limit 100"
read_message --format_binary_max_string_size 100 --storage_file_read_method read --max_read_buffer_size 30
echo "split into 30-byte buffers, no limit"
read_message --format_binary_max_string_size 0 --storage_file_read_method read --max_read_buffer_size 30
