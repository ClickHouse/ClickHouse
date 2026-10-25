#!/usr/bin/env python3
"""Test the result metadata sent over the MySQL protocol.

The column definitions of a result set report the table column each result column comes from
(`schema`, `table`, `org_table`, `org_name`) and the display width of the type (`length`), as MySQL does.
Result columns that are not plain references to a table column have an empty table and `org_name`
equal to the name.

`COM_STMT_PREPARE` describes the result columns of the statement, as MySQL does, so that clients
know the shape of the result before executing it, and `COM_STMT_RESET` is answered with an OK packet.
The column definitions sent at prepare time and at execution time are the same.

https://github.com/ClickHouse/ClickHouse/issues/127599
"""

import os
import socket
import struct

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")
CLICKHOUSE_PORT_MYSQL = int(os.environ.get("CLICKHOUSE_PORT_MYSQL", 9004))
CLICKHOUSE_USER = os.environ.get("MYSQL_CLIENT_CLICKHOUSE_USER", "default")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "default")

# Capability flags (see src/Core/MySQL/PacketsGeneric.h).
CLIENT_CONNECT_WITH_DB = 0x00000008
CLIENT_PROTOCOL_41 = 0x00000200
CLIENT_SECURE_CONNECTION = 0x00008000
CLIENT_PLUGIN_AUTH = 0x00080000

COM_QUERY = 0x03
COM_STMT_PREPARE = 0x16
COM_STMT_EXECUTE = 0x17
COM_STMT_CLOSE = 0x19
COM_STMT_RESET = 0x1A


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
    length = header[0] | (header[1] << 8) | (header[2] << 16)
    return recv_exact(sock, length)


def send_packet(sock, sequence_id, payload):
    header = struct.pack("<I", len(payload))[:3] + bytes([sequence_id])
    sock.sendall(header + payload)


def send_command(sock, command, payload=b""):
    send_packet(sock, 0, bytes([command]) + payload)


def is_err(payload):
    return payload[0] == 0xFF


def is_eof(payload):
    return payload[0] == 0xFE and len(payload) < 9


def err_message(payload):
    # 0xFF, error_code (2 bytes), '#', sql_state (5 bytes), then the message.
    return payload[9:].decode("utf-8", errors="replace")


def read_lenenc_int(data, pos):
    first = data[pos]
    if first < 0xFB:
        return first, pos + 1
    if first == 0xFC:
        return struct.unpack_from("<H", data, pos + 1)[0], pos + 3
    if first == 0xFD:
        return int.from_bytes(data[pos + 1 : pos + 4], "little"), pos + 4
    return struct.unpack_from("<Q", data, pos + 1)[0], pos + 9


def read_lenenc_str(data, pos):
    length, pos = read_lenenc_int(data, pos)
    return data[pos : pos + length].decode(), pos + length


def parse_column_definition(payload):
    pos = 0
    fields = []
    for _ in range(6):  # catalog, schema, table, org_table, name, org_name
        value, pos = read_lenenc_str(payload, pos)
        fields.append(value)
    _, pos = read_lenenc_int(payload, pos)  # length of the fixed-length fields
    _charset, length, column_type = struct.unpack_from("<HIB", payload, pos)
    _catalog, schema, table, org_table, name, org_name = fields
    if schema == CLICKHOUSE_DATABASE:
        schema = "<database>"
    return (
        f"name={name} org_name={org_name} schema={schema} table={table} "
        f"org_table={org_table} type={column_type} length={length}"
    )


def connect():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(30)
    sock.connect((CLICKHOUSE_HOST, CLICKHOUSE_PORT_MYSQL))
    read_packet(sock)  # The server's initial Handshake packet.

    capability_flags = (
        CLIENT_PROTOCOL_41
        | CLIENT_SECURE_CONNECTION
        | CLIENT_PLUGIN_AUTH
        | CLIENT_CONNECT_WITH_DB
    )
    payload = bytearray()
    payload += struct.pack("<I", capability_flags)
    payload += struct.pack("<I", 0xFFFFFF)  # max_packet_size
    payload += bytes([33])  # character_set (utf8_general_ci)
    payload += b"\x00" * 23  # reserved
    payload += CLICKHOUSE_USER.encode() + b"\x00"
    payload += bytes([0])  # empty auth_response (no password)
    payload += CLICKHOUSE_DATABASE.encode() + b"\x00"
    payload += b"mysql_native_password\x00"
    send_packet(sock, 1, bytes(payload))

    response = read_packet(sock)
    if is_err(response):
        raise RuntimeError(f"Cannot connect: {err_message(response)}")
    return sock


def query(sock, text):
    send_command(sock, COM_QUERY, text.encode())
    response = read_packet(sock)
    if is_err(response):
        raise RuntimeError(f"Query {text!r} failed: {err_message(response)}")


def query_metadata(sock, text):
    """Runs a query returning a result set (text protocol) and prints its column definitions."""
    # The name of the test database is random, so it is masked in the output.
    print(f"--- query: {text.replace(CLICKHOUSE_DATABASE + '.', '<database>.')}")
    send_command(sock, COM_QUERY, text.encode())
    response = read_packet(sock)
    if is_err(response):
        raise RuntimeError(f"Query {text!r} failed: {err_message(response)}")

    num_columns, _ = read_lenenc_int(response, 0)
    for _ in range(num_columns):
        print(parse_column_definition(read_packet(sock)))

    # Without `CLIENT_DEPRECATE_EOF`, the column definitions are followed by an EOF packet, then the rows and another EOF packet.
    assert is_eof(read_packet(sock))
    while True:
        packet = read_packet(sock)
        if is_err(packet):
            raise RuntimeError(f"Query {text!r} failed: {err_message(packet)}")
        if is_eof(packet):
            break


def prepare(sock, text):
    """Returns the statement id and the column definitions, or None if the statement cannot be prepared."""
    print(f"--- prepare: {text}")
    send_command(sock, COM_STMT_PREPARE, text.encode())
    response = read_packet(sock)
    if is_err(response):
        print(f"error: {'UNKNOWN_TABLE' if 'UNKNOWN_TABLE' in err_message(response) else err_message(response)}")
        return None, None

    statement_id, num_columns, num_params = struct.unpack_from("<IHH", response, 1)
    print(f"num_columns={num_columns} num_params={num_params}")

    columns = [parse_column_definition(read_packet(sock)) for _ in range(num_columns)]
    for column in columns:
        print(column)

    # Without `CLIENT_DEPRECATE_EOF`, the column definitions are followed by an EOF packet.
    if num_columns:
        assert is_eof(read_packet(sock))

    return statement_id, columns


def reset(sock, statement_id):
    send_command(sock, COM_STMT_RESET, struct.pack("<I", statement_id))
    response = read_packet(sock)
    if is_err(response):
        message = err_message(response)
        print(f"reset: error: {'Unknown prepared statement' if 'Unknown prepared statement' in message else message}")
    else:
        print(f"reset: {'OK' if response[0] == 0x00 else response}")


def execute(sock, statement_id, prepared_columns):
    # statement_id, flags (no cursor), iteration_count
    send_command(sock, COM_STMT_EXECUTE, struct.pack("<IBI", statement_id, 0, 1))
    response = read_packet(sock)
    if is_err(response):
        print(f"execute: error: {err_message(response)}")
        return

    num_columns, _ = read_lenenc_int(response, 0)
    columns = [parse_column_definition(read_packet(sock)) for _ in range(num_columns)]

    num_rows = 0
    while True:
        packet = read_packet(sock)
        if is_eof(packet):
            break
        if is_err(packet):
            print(f"execute: error: {err_message(packet)}")
            return
        num_rows += 1

    same = "the same as" if columns == prepared_columns else "DIFFERENT from"
    print(f"execute: {num_rows} rows, column definitions are {same} the prepared ones")


def close(sock, statement_id):
    # No response is sent.
    send_command(sock, COM_STMT_CLOSE, struct.pack("<I", statement_id))


def main():
    sock = connect()
    try:
        query(sock, "DROP TABLE IF EXISTS t")
        query(sock, "DROP TABLE IF EXISTS types")
        query(sock, "CREATE TABLE t (id UInt64, flag UInt8, s String) ENGINE = MergeTree ORDER BY id")
        query(sock, "INSERT INTO t VALUES (1, 1, 'a'), (2, 0, 'b'), (3, 1, 'c')")
        query(
            sock,
            "CREATE TABLE types (i8 Int8, i16 Int16, i32 Int32, i64 Int64, u16 UInt16, u32 UInt32, "
            "f32 Float32, f64 Float64, d Decimal(10, 2), d0 Decimal(5, 0), date Date, dt DateTime, dt64 DateTime64(3), "
            "fs FixedString(3), n Nullable(UInt8), lc LowCardinality(String)) ENGINE = Memory",
        )

        query_metadata(sock, "SELECT * FROM t")
        query_metadata(sock, "SELECT * FROM types")
        query_metadata(sock, f"SELECT id FROM {CLICKHOUSE_DATABASE}.t")
        query_metadata(sock, "SELECT id AS x, a.s, flag + 1 AS expr, 1 AS const FROM t AS a")
        query_metadata(sock, "SELECT id FROM (SELECT id FROM t)")
        query_metadata(sock, "SELECT id FROM t UNION ALL SELECT id FROM t")

        for text in [
            "SELECT * FROM t ORDER BY id",
            "SELECT id AS x, a.s, flag + 1 AS expr FROM t AS a ORDER BY id LIMIT 2",
        ]:
            statement_id, columns = prepare(sock, text)
            reset(sock, statement_id)
            execute(sock, statement_id, columns)
            # The statement can be reset and executed again.
            reset(sock, statement_id)
            execute(sock, statement_id, columns)
            close(sock, statement_id)

        # A statement without a result set has no columns.
        statement_id, _ = prepare(sock, "INSERT INTO t VALUES (4, 0, 'd')")
        close(sock, statement_id)

        # Errors of the analysis are reported at prepare time.
        prepare(sock, "SELECT * FROM table_that_does_not_exist")

        # Resetting a statement that does not exist is an error.
        reset(sock, 1000000)

        query(sock, "DROP TABLE t")
        query(sock, "DROP TABLE types")
    finally:
        sock.close()


if __name__ == "__main__":
    main()
