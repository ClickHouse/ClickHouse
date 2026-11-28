#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires `psql`, and the PostgreSQL compatibility port is not enabled in fasttest.

# A client that resets the connection in the middle of the authentication makes the next write to the
# socket (`AuthenticationOk`) fail, which cancels the output buffer. The authentication code must then
# give up instead of trying to deliver `ErrorResponse` into the canceled buffer: that was a logical
# error, `Cannot write to canceled buffer`, which any client that can reach the port could cause.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PG_USER="postgresql_user_05843_${CLICKHOUSE_DATABASE}"
ATTEMPTS=10

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH plaintext_password BY 'pgpass';
"

# The `text_log` checks below must count only the rows written by this run: an earlier test on the
# same server may have left the same message behind. Microseconds since epoch are
# timezone-independent (the flaky check randomizes `session_timezone`).
RUN_START=$(${CLICKHOUSE_CLIENT} -q "SELECT toUnixTimestamp64Micro(now64(6))")

# Answer the password request with the right password and reset the connection at once, without
# reading the reply: closing a socket with `SO_LINGER` set to zero sends RST instead of FIN.
CLICKHOUSE_PORT_POSTGRESQL="$CLICKHOUSE_PORT_POSTGRESQL" PG_USER="$PG_USER" PG_DATABASE="$CLICKHOUSE_DATABASE" ATTEMPTS="$ATTEMPTS" python3 - <<'PYTHON'
import os
import socket
import struct
import sys

port = int(os.environ["CLICKHOUSE_PORT_POSTGRESQL"])
user = os.environ["PG_USER"]
database = os.environ["PG_DATABASE"]


def cstring(text):
    return text.encode() + b"\x00"


def receive_exact(sock, size):
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise RuntimeError("connection closed")
        data += chunk
    return data


for _ in range(int(os.environ["ATTEMPTS"])):
    sock = socket.create_connection(("127.0.0.1", port), timeout=60)

    # StartupMessage, protocol version 3.0.
    body = struct.pack(">i", 196608) + cstring("user") + cstring(user) + cstring("database") + cstring(database) + b"\x00"
    sock.sendall(struct.pack(">i", 4 + len(body)) + body)

    kind = receive_exact(sock, 1)
    (size,) = struct.unpack(">i", receive_exact(sock, 4))
    payload = receive_exact(sock, size - 4)
    if kind != b"R" or struct.unpack(">i", payload[:4])[0] != 3:
        sys.exit(f"expected AuthenticationCleartextPassword, got {kind!r} {payload!r}")

    password = cstring("pgpass")
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    sock.sendall(b"p" + struct.pack(">i", 4 + len(password)) + password)
    sock.close()
PYTHON

# Every attempt has reached the authentication: wait for the server to have seen all of them.
function count_attempts()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT count() FROM system.text_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600 AND toUnixTimestamp64Micro(event_time_microseconds) > ${RUN_START}
            AND message LIKE '%Authenticating user \\'${PG_USER}\\'%'
        SETTINGS max_rows_to_read = 0"
}

for _ in {1..100}
do
    [[ "$(count_attempts)" == "${ATTEMPTS}" ]] && break
    sleep 0.1
done

echo "--- every attempt reached the authentication"
count_attempts

echo "--- an ordinary login still works"
PGPASSWORD=pgpass psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" \
    --no-psqlrc --no-align --tuples-only --quiet -c "SELECT 1" 2>&1

echo "--- and the server did not try to write into the canceled socket buffer"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count() FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND toUnixTimestamp64Micro(event_time_microseconds) > ${RUN_START}
        AND logger_name = 'PostgreSQLHandler' AND message LIKE '%Cannot write to canceled buffer%'
    SETTINGS max_rows_to_read = 0"

${CLICKHOUSE_CLIENT} -q "DROP USER ${PG_USER}"
