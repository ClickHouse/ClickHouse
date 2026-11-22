#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the PostgreSQL compatibility port is not enabled in fasttest.

# `SQL_ASCII` disables character-set conversion in PostgreSQL and is sent by interactive `psql`
# under the `C`/`POSIX` locale. Verify `SQL_ASCII` (and lowercase/hyphenated variants) is accepted
# during PostgreSQL wire startup and executes simple queries, while transcoding encodings remain rejected.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PG_USER="postgresql_user_05781_${CLICKHOUSE_DATABASE}"
trap '${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${PG_USER};" >/dev/null 2>&1 || true' EXIT

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
"

CLICKHOUSE_PORT_POSTGRESQL="$CLICKHOUSE_PORT_POSTGRESQL" PG_USER="$PG_USER" PG_DATABASE="$CLICKHOUSE_DATABASE" python3 - <<'PYTHON'
import os
import socket
import struct

port = int(os.environ["CLICKHOUSE_PORT_POSTGRESQL"])
user = os.environ["PG_USER"]
database = os.environ["PG_DATABASE"]


def run_case(encoding):
    sock = socket.create_connection(("127.0.0.1", port), timeout=30)
    sock.settimeout(30)
    buf = b""

    def read_exact(n):
        nonlocal buf
        while len(buf) < n:
            chunk = sock.recv(65536)
            if not chunk:
                raise EOFError("connection closed unexpectedly")
            buf += chunk
        out, buf = buf[:n], buf[n:]
        return out

    def read_msg():
        kind = read_exact(1)
        length = struct.unpack(">i", read_exact(4))[0]
        return kind, read_exact(length - 4)

    try:
        payload = (
            f"user\x00{user}\x00database\x00{database}\x00client_encoding\x00{encoding}\x00\x00"
        ).encode("ascii")
        sock.sendall(struct.pack(">ii", 8 + len(payload), 196608) + payload)

        params = {}
        while True:
            kind, body = read_msg()
            if kind == b"E":
                fields = dict((f[:1], f[1:].decode("utf-8", "replace")) for f in body.split(b"\x00") if f)
                print(f"{encoding}: error {fields.get(b'C', '')} {fields.get(b'M', '').splitlines()[0]}")
                return
            if kind == b"R":
                auth_code = struct.unpack(">i", body[:4])[0]
                if auth_code == 3:
                    sock.sendall(b"p" + struct.pack(">i", 5) + b"\x00")
            elif kind == b"S":
                name, val, _ = body.split(b"\x00", 2)
                params[name.decode("utf-8")] = val.decode("utf-8")
            elif kind == b"Z":
                break

        query_body = b"SELECT 'ok'\x00"
        sock.sendall(b"Q" + struct.pack(">i", 4 + len(query_body)) + query_body)

        rows = []
        tag = ""
        while True:
            kind, body = read_msg()
            if kind == b"D":
                ncols = struct.unpack(">h", body[:2])[0]
                pos = 2
                row = []
                for _ in range(ncols):
                    flen = struct.unpack(">i", body[pos : pos + 4])[0]
                    pos += 4
                    if flen < 0:
                        row.append(None)
                    else:
                        row.append(body[pos : pos + flen].decode("utf-8"))
                        pos += flen
                rows.append(row)
            elif kind == b"C":
                tag = body.rstrip(b"\x00").decode("utf-8")
            elif kind == b"E":
                fields = dict((f[:1], f[1:].decode("utf-8", "replace")) for f in body.split(b"\x00") if f)
                print(f"{encoding}: query error {fields.get(b'C', '')} {fields.get(b'M', '')}")
                return
            elif kind == b"Z":
                break

        sock.sendall(b"X" + struct.pack(">i", 4))
        print(f"{encoding}: client_encoding={params.get('client_encoding')}, row={rows[0]}, tag={tag}")
    finally:
        sock.close()


for enc in ("SQL_ASCII", "sql_ascii", "sql-ascii", "LATIN1"):
    run_case(enc)
PYTHON
