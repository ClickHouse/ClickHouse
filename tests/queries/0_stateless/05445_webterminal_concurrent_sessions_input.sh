#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Concurrent web terminal sessions run in the same server process and must not share
# the state of the line editor: the input of each session is sent byte by byte,
# interleaved with the other session, with multi-byte UTF-8 characters split
# between the sessions, and each session must decode its own query correctly.

python3 - "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_HTTP}" <<'PYEOF'
import base64
import json
import os
import socket
import struct
import sys
import time

host, port = sys.argv[1], int(sys.argv[2])


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("connection closed")
        buf += chunk
    return buf


def send_frame(sock, opcode, payload):
    mask = os.urandom(4)
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    if len(payload) < 126:
        header = bytes([0x80 | opcode, 0x80 | len(payload)])
    else:
        header = bytes([0x80 | opcode, 0x80 | 126]) + struct.pack(">H", len(payload))
    sock.sendall(header + mask + masked)


def read_frame(sock):
    header = recv_exact(sock, 2)
    opcode = header[0] & 0x0F
    length = header[1] & 0x7F
    if length == 126:
        length = struct.unpack(">H", recv_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack(">Q", recv_exact(sock, 8))[0]
    return opcode, recv_exact(sock, length)


def read_until(sock, needle, timeout):
    """Accumulate the terminal output until it contains `needle`."""
    deadline = time.monotonic() + timeout
    output = b""
    while needle not in output:
        if time.monotonic() > deadline:
            raise TimeoutError(f"{needle!r} not found in {output[-500:]!r}")
        opcode, payload = read_frame(sock)
        if opcode == 0x08:
            raise EOFError(f"closed by the server: {payload!r}")
        if opcode == 0x09:
            send_frame(sock, 0x0A, payload)
        elif opcode == 0x02:
            output += payload
    return output


def connect():
    sock = socket.create_connection((host, port), timeout=60)
    key = base64.b64encode(os.urandom(16)).decode()
    request = (
        f"GET /webterminal HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        f"Origin: http://{host}:{port}\r\n"
        f"Upgrade: websocket\r\n"
        f"Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        f"Sec-WebSocket-Version: 13\r\n\r\n"
    )
    sock.sendall(request.encode())
    response = b""
    while b"\r\n\r\n" not in response:
        response += recv_exact(sock, 1)
    assert response.startswith(b"HTTP/1.1 101"), response
    send_frame(sock, 0x01, json.dumps({"type": "auth", "user": "default", "password": ""}).encode())
    read_until(sock, b":) ", 60)
    return sock


# Different multi-byte characters in each session, so that a mixed-up decoder state
# produces a different string.
texts = ["привет", "€😀日本"]
sessions = [connect() for _ in texts]
queries = [f"SELECT hex('{text}') AS h_{i}\r".encode() for i, text in enumerate(texts)]

for pos in range(max(len(q) for q in queries)):
    for sock, query in zip(sessions, queries):
        if pos < len(query):
            send_frame(sock, 0x02, query[pos:pos + 1])

for i, (sock, text) in enumerate(zip(sessions, texts)):
    expected = text.encode().hex().upper().encode()
    read_until(sock, expected, 60)
    print(f"session {i}: ok")
    sock.close()
PYEOF
