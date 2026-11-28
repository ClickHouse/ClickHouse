#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Two pipelined requests sent over a compressed Keeper session: both must be answered.

python3 - <<'EOF'
import os
import shlex
import socket
import struct
import subprocess

compressor = shlex.split(os.environ["CLICKHOUSE_COMPRESSOR"])


def compress(data):
    return subprocess.run(compressor, input=data, stdout=subprocess.PIPE, check=True).stdout


def decompress(data):
    return subprocess.run(compressor + ["--decompress"], input=data, stdout=subprocess.PIPE, check=True).stdout


def exists(xid):
    # OpNum::Exists = 3, path "/", no watch
    body = struct.pack("!iii", xid, 3, 1) + b"/" + b"\x00"
    return struct.pack("!i", len(body)) + body


def recv_exact(s, n):
    data = b""
    while len(data) < n:
        chunk = s.recv(n - len(data))
        if not chunk:
            return None
        data += chunk
    return data


def run(name, payload):
    s = socket.create_connection((os.environ["CLICKHOUSE_HOST"], int(os.environ["CLICKHOUSE_PORT_KEEPER"])), timeout=120)
    # handshake length 45, protocol version 10 (compression), last zxid, timeout ms, session id, password, read only
    s.sendall(struct.pack("!iiqiqi", 45, 10, 0, 30000, 0, 16) + b"\x00" * 17)
    recv_exact(s, 40)
    s.sendall(payload)
    xids = []
    decoded = b""
    while len(xids) < 2:
        # checksum (16 bytes), method (1 byte), compressed size including the 9-byte header, decompressed size
        header = recv_exact(s, 25)
        if header is None:
            break
        block = recv_exact(s, struct.unpack("<I", header[17:21])[0] - 9)
        if block is None:
            break
        decoded += decompress(header + block)
        while len(decoded) >= 8 and len(decoded) >= 4 + struct.unpack("!i", decoded[:4])[0]:
            length, xid = struct.unpack("!ii", decoded[:8])
            xids.append(xid)
            decoded = decoded[4 + length:]
    s.close()
    print(name + ":", *xids)


requests = exists(1) + exists(2)
run("one block", compress(requests))
run("two blocks", compress(exists(1)) + compress(exists(2)))
run("split request", compress(requests[:22]) + compress(requests[22:]))
EOF
