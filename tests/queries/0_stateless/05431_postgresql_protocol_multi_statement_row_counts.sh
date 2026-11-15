#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the PostgreSQL compatibility port is not enabled in fasttest.

# Every statement of a multi-statement simple query reports its own row count in `CommandComplete`,
# whatever its position in the message.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The user name must be unique per test run, so that concurrent runs do not collide.
PG_USER="postgresql_user_05431_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
CREATE TABLE row_counts (x UInt64) ENGINE = Memory;
GRANT INSERT ON ${CLICKHOUSE_DATABASE}.row_counts TO ${PG_USER};
"

CLICKHOUSE_PORT_POSTGRESQL="$CLICKHOUSE_PORT_POSTGRESQL" PG_USER="$PG_USER" PG_DATABASE="$CLICKHOUSE_DATABASE" python3 - <<'PYTHON'
import os
import socket
import struct

sock = socket.create_connection(("127.0.0.1", int(os.environ["CLICKHOUSE_PORT_POSTGRESQL"])), timeout=60)
sock.settimeout(60)
buffer = b""


def read_exact(n):
    global buffer
    while len(buffer) < n:
        chunk = sock.recv(65536)
        if not chunk:
            raise RuntimeError("connection closed")
        buffer += chunk
    data, buffer = buffer[:n], buffer[n:]
    return data


def read_until_ready():
    """Returns the `CommandComplete` tags and the errors up to `ReadyForQuery`."""
    replies = []
    while True:
        kind = read_exact(1)
        body = read_exact(struct.unpack(">i", read_exact(4))[0] - 4)
        if kind == b"Z":
            return replies
        if kind == b"C":
            replies.append(body.rstrip(b"\x00").decode())
        elif kind == b"E":
            fields = dict((f[:1], f[1:]) for f in body.split(b"\x00") if f)
            replies.append("error: " + fields.get(b"M", b"").decode().splitlines()[0])
        elif kind == b"R" and struct.unpack(">i", body[:4])[0] == 3:
            sock.sendall(b"p" + struct.pack(">i", 5) + b"\x00")


payload = ("user\x00" + os.environ["PG_USER"] + "\x00database\x00" + os.environ["PG_DATABASE"] + "\x00\x00").encode()
sock.sendall(struct.pack(">ii", 8 + len(payload), 196608) + payload)
read_until_ready()

for text in [
    "SELECT number FROM numbers(9); SELECT number FROM numbers(9); SELECT number FROM numbers(4)",
    "INSERT INTO row_counts SELECT number FROM numbers(7); INSERT INTO row_counts SELECT number FROM numbers(2)",
]:
    body = text.encode() + b"\x00"
    sock.sendall(b"Q" + struct.pack(">i", 4 + len(body)) + body)
    print(", ".join(read_until_ready()))

sock.sendall(b"X" + struct.pack(">i", 4))
sock.close()
PYTHON

${CLICKHOUSE_CLIENT} -q "
DROP TABLE row_counts;
DROP USER ${PG_USER};
"
