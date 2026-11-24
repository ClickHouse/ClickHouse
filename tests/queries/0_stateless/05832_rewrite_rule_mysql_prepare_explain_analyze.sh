#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `COM_STMT_PREPARE` of the MySQL protocol only describes the result of the statement and must not run it.
# `EXPLAIN ANALYZE` runs its inner query while its interpreter is built, so a query rule that rewrites the
# prepared statement into `EXPLAIN ANALYZE` is refused at prepare time, before the inner query runs.
# The same holds for an `EXPLAIN ANALYZE` in a subquery, which the parser lowers to the `viewExplain` table function.
# It also holds for an `EXPLAIN ANALYZE` in a part of the query that is not among the children of its AST,
# such as the `WHERE` expression of `SHOW TABLES`.
# A rewrite into a plain `EXPLAIN` is still described.

RULE_ANALYZE="rule_prepare_analyze_${CLICKHOUSE_DATABASE}"
RULE_NESTED_ANALYZE="rule_prepare_nested_analyze_${CLICKHOUSE_DATABASE}"
RULE_SHOW_ANALYZE="rule_prepare_show_analyze_${CLICKHOUSE_DATABASE}"
RULE_EXPLAIN="rule_prepare_explain_${CLICKHOUSE_DATABASE}"
RULE_NESTED_EXPLAIN="rule_prepare_nested_explain_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP RULE IF EXISTS ${RULE_ANALYZE};
DROP RULE IF EXISTS ${RULE_NESTED_ANALYZE};
DROP RULE IF EXISTS ${RULE_SHOW_ANALYZE};
DROP RULE IF EXISTS ${RULE_EXPLAIN};
DROP RULE IF EXISTS ${RULE_NESTED_EXPLAIN};
CREATE RULE ${RULE_ANALYZE} AS (SELECT '${RULE_ANALYZE}') REWRITE TO (EXPLAIN ANALYZE SELECT throwIf(number = 0) FROM numbers(1));
CREATE RULE ${RULE_NESTED_ANALYZE} AS (SELECT '${RULE_NESTED_ANALYZE}') REWRITE TO (SELECT * FROM (EXPLAIN ANALYZE SELECT throwIf(number = 0) FROM numbers(1)));
CREATE RULE ${RULE_SHOW_ANALYZE} AS (SELECT '${RULE_SHOW_ANALYZE}') REWRITE TO (SHOW TABLES WHERE name IN (SELECT * FROM (EXPLAIN ANALYZE SELECT throwIf(number = 0) FROM numbers(1))));
CREATE RULE ${RULE_EXPLAIN} AS (SELECT '${RULE_EXPLAIN}') REWRITE TO (EXPLAIN SELECT 1);
CREATE RULE ${RULE_NESTED_EXPLAIN} AS (SELECT '${RULE_NESTED_EXPLAIN}') REWRITE TO (SELECT * FROM (EXPLAIN SELECT 1));
"

RULE_ANALYZE="${RULE_ANALYZE}" RULE_NESTED_ANALYZE="${RULE_NESTED_ANALYZE}" RULE_SHOW_ANALYZE="${RULE_SHOW_ANALYZE}" RULE_EXPLAIN="${RULE_EXPLAIN}" RULE_NESTED_EXPLAIN="${RULE_NESTED_EXPLAIN}" python3 - <<'PYTHON'
import os
import socket
import struct

host = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")
port = int(os.environ.get("CLICKHOUSE_PORT_MYSQL", 9004))
user = os.environ.get("MYSQL_CLIENT_CLICKHOUSE_USER", "default")
database = os.environ.get("CLICKHOUSE_DATABASE", "default")
rule_analyze = os.environ["RULE_ANALYZE"]
rule_nested_analyze = os.environ["RULE_NESTED_ANALYZE"]
rule_show_analyze = os.environ["RULE_SHOW_ANALYZE"]
rule_explain = os.environ["RULE_EXPLAIN"]
rule_nested_explain = os.environ["RULE_NESTED_EXPLAIN"]


def recv_exact(sock, n):
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("Connection closed by server")
        data += chunk
    return data


def read_packet(sock):
    header = recv_exact(sock, 4)
    return recv_exact(sock, header[0] | (header[1] << 8) | (header[2] << 16))


def send_packet(sock, sequence_id, payload):
    sock.sendall(struct.pack("<I", len(payload))[:3] + bytes([sequence_id]) + payload)


def is_err(payload):
    return payload[0] == 0xFF


def connect():
    sock = socket.create_connection((host, port), timeout=600)
    read_packet(sock)  # The server's initial Handshake packet.
    # CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH | CLIENT_CONNECT_WITH_DB
    payload = struct.pack("<II", 0x00000200 | 0x00008000 | 0x00080000 | 0x00000008, 0xFFFFFF)
    payload += bytes([33]) + b"\x00" * 23 + user.encode() + b"\x00" + bytes([0])
    payload += database.encode() + b"\x00" + b"mysql_native_password\x00"
    send_packet(sock, 1, payload)
    if is_err(read_packet(sock)):
        raise RuntimeError("Cannot connect")
    return sock


def set_query_rules(sock, rule):
    send_packet(sock, 0, bytes([0x03]) + f"SET query_rules = '{rule}'".encode())  # COM_QUERY
    if is_err(read_packet(sock)):
        raise RuntimeError("SET failed")


def prepare(sock, text):
    send_packet(sock, 0, bytes([0x16]) + text.encode())  # COM_STMT_PREPARE
    response = read_packet(sock)
    if is_err(response):
        message = response[9:].decode("utf-8", errors="replace")
        if "EXPLAIN ANALYZE" in message and "BAD_ARGUMENTS" in message:
            return "refused: EXPLAIN ANALYZE"
        return "error: " + message
    _, num_columns, _ = struct.unpack_from("<IHH", response, 1)
    for _ in range(num_columns):
        read_packet(sock)
    if num_columns:
        read_packet(sock)  # EOF
    return f"num_columns={num_columns}"


sock = connect()
set_query_rules(sock, rule_analyze)
print("EXPLAIN ANALYZE:", prepare(sock, f"SELECT '{rule_analyze}'"))
set_query_rules(sock, rule_nested_analyze)
print("Nested EXPLAIN ANALYZE:", prepare(sock, f"SELECT '{rule_nested_analyze}'"))
set_query_rules(sock, rule_show_analyze)
print("EXPLAIN ANALYZE in SHOW TABLES WHERE:", prepare(sock, f"SELECT '{rule_show_analyze}'"))
set_query_rules(sock, rule_explain)
print("EXPLAIN:", prepare(sock, f"SELECT '{rule_explain}'"))
set_query_rules(sock, rule_nested_explain)
print("Nested EXPLAIN:", prepare(sock, f"SELECT '{rule_nested_explain}'"))
sock.close()
PYTHON

${CLICKHOUSE_CLIENT} -q "
DROP RULE ${RULE_ANALYZE};
DROP RULE ${RULE_NESTED_ANALYZE};
DROP RULE ${RULE_SHOW_ANALYZE};
DROP RULE ${RULE_EXPLAIN};
DROP RULE ${RULE_NESTED_EXPLAIN};
"
