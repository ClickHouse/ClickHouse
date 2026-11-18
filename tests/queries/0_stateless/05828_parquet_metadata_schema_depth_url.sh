#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the `ParquetMetadata` format needs Arrow, which is not built in the fast test.

# The depth check of the `ParquetMetadata` format and Arrow read the same copy of the footer, also for a URL
# whose server returns a shallow footer to short range requests and a deeply nested one to long range requests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

python3 -I -c '
import re, struct, sys
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

def varint(n):
    out = b""
    while True:
        b = n & 0x7f
        n >>= 7
        if not n:
            return out + bytes([b])
        out += bytes([b | 0x80])

def zigzag(n):
    return varint((n << 1) ^ (n >> 63))

def field(delta, thrift_type):
    return bytes([(delta << 4) | thrift_type])

I32, I64, BINARY, LIST, STRUCT = 5, 6, 8, 9, 12

def schema_element(name, physical_type=None, num_children=None):
    out = b""
    last = 0
    if physical_type is not None:
        out += field(1, I32) + zigzag(physical_type)
        last = 1
    out += field(3 - last, I32) + zigzag(1) + field(1, BINARY) + varint(len(name)) + name
    if num_children is not None:
        out += field(1, I32) + zigzag(num_children)
    return out + b"\x00"

def struct_list(items):
    header = bytes([(len(items) << 4) | STRUCT]) if len(items) < 15 else bytes([0xf0 | STRUCT]) + varint(len(items))
    return header + b"".join(items)

def parquet(depth, size=0):
    schema = [b"\x48\x06schema\x15\x02\x00"]
    schema += [schema_element(b"c", num_children=1) for _ in range(depth)]
    schema += [schema_element(b"c", physical_type=1)]
    metadata = field(1, I32) + zigzag(1) + field(1, LIST) + struct_list(schema) + field(1, I64) + zigzag(0) + field(1, LIST) + struct_list([]) + b"\x00"
    tail = metadata + struct.pack("<I", len(metadata)) + b"PAR1"
    return b"PAR1" + b"\x00" * max(0, size - 4 - len(tail)) + tail

deep = parquet(200000)
shallow = parquet(1, len(deep))

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Length", str(len(deep)))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()

    def do_GET(self):
        m = re.match(r"bytes=(\d+)-(\d+)", self.headers.get("Range", ""))
        begin, end = (int(m.group(1)), min(int(m.group(2)) + 1, len(deep))) if m else (0, len(deep))
        swap = self.path == "/swap.parquet" and end - begin >= 64
        data = (deep if swap else shallow)[begin:end]
        self.send_response(206 if m else 200)
        if m:
            self.send_header("Content-Range", "bytes %d-%d/%d" % (begin, end - 1, len(deep)))
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        self.wfile.write(data)

ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()
' "$HTTP_PORT" 2>/dev/null &
HTTP_PID=$!
trap 'kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null' EXIT

for _ in $(seq 1 100); do
    curl -s "http://127.0.0.1:$HTTP_PORT/shallow.parquet" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

$CLICKHOUSE_LOCAL -q "SELECT num_columns FROM url('http://127.0.0.1:$HTTP_PORT/shallow.parquet', ParquetMetadata)"
$CLICKHOUSE_LOCAL -q "SELECT num_columns FROM url('http://127.0.0.1:$HTTP_PORT/swap.parquet', ParquetMetadata)" 2>&1 | grep -o -m1 'TOO_DEEP_RECURSION'

# The size of the metadata in the footer exceeds the size of the file.
file="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
python3 -I -c 'import struct, sys; sys.stdout.buffer.write(b"PAR1" + bytes(70000) + struct.pack("<I", 1000000) + b"PAR1")' > "$file"
$CLICKHOUSE_LOCAL -q "SELECT num_columns FROM file('$file', ParquetMetadata)" 2>&1 | grep -o -m1 'CANNOT_READ_ALL_DATA'
rm -f "$file"
