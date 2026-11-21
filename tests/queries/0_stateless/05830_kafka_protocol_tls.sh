#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build is without SSL.

# The Kafka-compatible interface over TLS (`kafka_port_secure`): requests work as on `kafka_port`,
# and the cluster metadata advertises the port the client is connected to, so it stays on TLS.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 - "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_KAFKA}" "${CLICKHOUSE_PORT_KAFKA_SECURE}" <<'EOF'
import socket
import ssl
import struct
import sys

host, port, port_secure = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])


def connect(port, secure):
    sock = socket.create_connection((host, port), timeout=60)
    if secure:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        sock = context.wrap_socket(sock)
    return sock


def recv_exactly(sock, size):
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise RuntimeError("connection closed")
        data += chunk
    return data


def request(sock, api_key, api_version, correlation_id, body):
    client_id = b"test"
    message = struct.pack(">hhih", api_key, api_version, correlation_id, len(client_id)) + client_id + body
    sock.sendall(struct.pack(">i", len(message)) + message)
    (size,) = struct.unpack(">i", recv_exactly(sock, 4))
    response = recv_exactly(sock, size)
    (response_correlation_id,) = struct.unpack_from(">i", response, 0)
    assert response_correlation_id == correlation_id
    return response[4:]


def api_versions(sock):
    response = request(sock, 18, 0, 1, b"")
    error_code, count = struct.unpack_from(">hi", response, 0)
    keys = [struct.unpack_from(">h", response, 6 + i * 6)[0] for i in range(count)]
    return error_code, 18 in keys and 3 in keys


def broker_ports(sock):
    # Metadata v1 with an empty list of topics: only the brokers are needed.
    response = request(sock, 3, 1, 2, struct.pack(">i", 0))
    (count,) = struct.unpack_from(">i", response, 0)
    pos = 4
    ports = []
    for _ in range(count):
        pos += 4  # node id
        (host_size,) = struct.unpack_from(">h", response, pos)
        pos += 2 + host_size
        (broker_port,) = struct.unpack_from(">i", response, pos)
        pos += 4
        (rack_size,) = struct.unpack_from(">h", response, pos)
        pos += 2 + max(rack_size, 0)
        ports.append(broker_port)
    return ports


for name, current_port, secure in (("plaintext", port, False), ("tls", port_secure, True)):
    with connect(current_port, secure) as sock:
        print(name, "api versions:", *api_versions(sock))
        print(name, "advertises its own port:", broker_ports(sock) == [current_port])

# The TLS port does not accept a plaintext Kafka request.
try:
    with connect(port_secure, False) as sock:
        api_versions(sock)
    print("plaintext request to the TLS port: accepted")
except Exception:
    print("plaintext request to the TLS port: rejected")
EOF
