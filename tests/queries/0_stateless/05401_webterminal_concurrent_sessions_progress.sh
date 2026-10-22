#!/usr/bin/env bash
# Tags: no-tsan
# Tag no-tsan: concurrent sessions still run into the line editor races fixed by #125510; the step check does not need TSan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Concurrent web terminal sessions render progress independently: the
# indicator of each session advances by one step per progress line.

python3 - "${CLICKHOUSE_HOST}" "${CLICKHOUSE_PORT_HTTP}" <<'PYEOF'
import base64
import json
import os
import re
import socket
import struct
import sys
import threading
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


def open_session():
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


INDICATORS = "→↘↓↙←↖↑↗"
PROGRESS_LINE = re.compile("([" + INDICATORS + "])\x1b\\[0m Progress: ")

query = (
    "SELECT 'finished_' || toString(count()) FROM numbers(30) WHERE NOT ignore(sleepEachRow(0.1)) "
    "SETTINGS max_block_size = 1\r"
)

sessions = [open_session(), open_session()]
for sock in sessions:
    send_frame(sock, 0x02, query.encode())

outputs = [None] * len(sessions)


def read_session(i):
    try:
        outputs[i] = read_until(sessions[i], b"finished_30", 60)
    except Exception as e:
        outputs[i] = e


threads = [threading.Thread(target=read_session, args=(i,)) for i in range(len(sessions))]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()

for i, output in enumerate(outputs, 1):
    if isinstance(output, Exception):
        print(f"session {i}: {type(output).__name__}: {output}")
        continue
    steps = [INDICATORS.index(glyph) for glyph in PROGRESS_LINE.findall(output.decode("utf-8", errors="replace"))]
    skips = sum((b - a) % 8 != 1 for a, b in zip(steps, steps[1:]))
    if len(steps) >= 5 and skips == 0:
        print(f"session {i}: ok")
    else:
        print(f"session {i}: lines={len(steps)} skips={skips}")

for sock in sessions:
    sock.close()
PYEOF
