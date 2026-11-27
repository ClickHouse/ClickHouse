#!/usr/bin/env bash

# The Kafka-compatible interface (`kafka_port`) closes the connection on a malformed request and on a request
# that stalls in the middle, and continues to serve the other connections.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PYTHONPATH="$CUR_DIR/helpers" python3 - "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_KAFKA}" <<'EOF'
import struct
import sys

from kafka_protocol_client import Client, string

host, port = sys.argv[1], int(sys.argv[2])

# ApiVersions of version 3 has the compact encoding: a string is prefixed by its length plus one as a varint.
# The lengths below do not fit into the request, and the first one does not fit into a signed 64-bit number.
for name, length in (("2^63", b"\x80" * 9 + b"\x01"), ("2^64 - 1", b"\xff" * 9 + b"\x01"), ("1000", b"\xe9\x07")):
    with Client(host, port) as client:
        client.send_message(client.header(18, 3) + b"\x00" + length + b"client")
        print(f"compact length {name}: closed {client.is_closed_by_server()}")

# The same for the length of an array, in Metadata of version 9.
with Client(host, port) as client:
    client.send_message(client.header(3, 9) + b"\x00" + b"\x80" * 9 + b"\x01")
    print(f"compact array length 2^63: closed {client.is_closed_by_server()}")

# A client that sends only a part of the size of a request, or only a part of the request, and stalls:
# the server closes the connection after `kafka_protocol.request_receive_timeout_ms`.
with Client(host, port) as stalled_in_size, Client(host, port) as stalled_in_body:
    stalled_in_size.sock.sendall(b"\x00\x00")
    stalled_in_body.sock.sendall(struct.pack(">i", 100) + stalled_in_body.header(18, 0))
    print(f"stalled in the size of a request: closed {stalled_in_size.is_closed_by_server()}")
    print(f"stalled in the body of a request: closed {stalled_in_body.is_closed_by_server()}")

with Client(host, port) as client:
    print("a correct request after that:", client.api_versions())
EOF
