"""Strict, uncompressed `Native` packets for the profile transport tests."""

import struct
import time


TRACE_REVISION = 54494
LEGACY_TIMEZONE_REVISION = 54464


def encode_varuint(value):
    result = bytearray()
    while value > 127:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def encode_string(value):
    data = value.encode() if isinstance(value, str) else value
    return encode_varuint(len(data)) + data


def read_exact(sock, size):
    result = bytearray()
    while len(result) < size:
        data = sock.recv(size - len(result))
        if not data:
            raise ConnectionError("Native connection closed inside a packet")
        result.extend(data)
    return bytes(result)


def read_varuint(sock):
    result = 0
    for shift in range(0, 70, 7):
        value = read_exact(sock, 1)[0]
        result |= (value & 127) << shift
        if value < 128:
            return result
    raise ValueError("Invalid Native VarUInt")


def read_string(sock):
    return read_exact(sock, read_varuint(sock)).decode()


def read_settings(sock):
    result = {}
    while name := read_string(sock):
        flags = read_varuint(sock)
        result[name] = (flags, read_string(sock))
    return result


def encode_settings(settings):
    return b"".join(encode_string(name) + encode_varuint(1) + encode_string(str(value)) for name, value in settings.items()) + b"\0"


def read_column(sock, type_name, rows, revision):
    if type_name.startswith("Array("):
        offsets = struct.unpack(f"<{rows}Q", read_exact(sock, rows * 8))
        assert all(left <= right for left, right in zip((0,) + offsets, offsets)), offsets
        values = read_column(sock, type_name[6:-1], offsets[-1] if rows else 0, revision)
        return [values[left:right] for left, right in zip((0,) + offsets, offsets)]
    if type_name == "String":
        if revision < 54492:
            return [read_string(sock) for _ in range(rows)]
        offsets = struct.unpack(f"<{rows}Q", read_exact(sock, rows * 8))
        assert all(left <= right for left, right in zip((0,) + offsets, offsets)), offsets
        data = read_exact(sock, offsets[-1] if rows else 0)
        return [data[left:right].decode() for left, right in zip((0,) + offsets, offsets)]
    if type_name.startswith("DateTime64("):
        code = "q"
    elif type_name.startswith("DateTime"):
        code = "I"
    elif type_name.startswith("Enum8("):
        code = "b"
    else:
        code = {"UInt8": "B", "UInt32": "I", "UInt64": "Q", "Int64": "q"}[type_name]
    return list(struct.unpack(f"<{rows}{code}", read_exact(sock, rows * struct.calcsize(code))))


def read_block(sock, revision):
    assert read_string(sock) == "", "unexpected external table name"
    while field := read_varuint(sock):
        if field == 3:
            assert revision >= 54480, "unexpected out-of-order bucket metadata"
            read_exact(sock, read_varuint(sock) * 4)
        else:
            assert field in (1, 2), field
            read_exact(sock, 1 if field == 1 else 4)
    columns, rows = read_varuint(sock), read_varuint(sock)
    result = {}
    for _ in range(columns):
        name, type_name = read_string(sock), read_string(sock)
        if revision >= 54454:
            assert read_exact(sock, 1) == b"\0", "unexpected custom serialization"
        result[name] = (type_name, read_column(sock, type_name, rows, revision))
    return rows, result


def encode_block(columns, rows, revision):
    result = bytearray(b"\0\0")  # External table name and `BlockInfo` terminator.
    result += encode_varuint(len(columns)) + encode_varuint(rows)
    for name, type_name, data in columns:
        result += encode_string(name) + encode_string(type_name)
        if revision >= 54454:
            result += b"\0"  # Default serialization.
        result += data
    return bytes(result)


def read_packet(sock, revision):
    packet = read_varuint(sock)
    if packet in (1, 14, 19):
        return packet, read_block(sock, revision)
    if packet == 3:
        names = ["read_rows", "read_bytes", "total_rows_to_read"]
        if revision >= 54463:
            names.append("total_bytes_to_read")
        names += ["written_rows", "written_bytes"]
        if revision >= 54460:
            names.append("elapsed_ns")
        return packet, {name: read_varuint(sock) for name in names}
    if packet == 17:
        return packet, read_string(sock)
    if packet in (4, 5):
        return packet, None
    if packet == 2:
        code = struct.unpack("<I", read_exact(sock, 4))[0]
        name, message, trace = read_string(sock), read_string(sock), read_string(sock)
        assert read_exact(sock, 1) == b"\0", "nested server exception"
        raise AssertionError((code, name, message, trace))
    raise AssertionError(f"Unexpected Native server packet {packet}")


def connect(sock, database):
    hello = encode_varuint(0) + encode_string("Native profile test")
    hello += encode_varuint(26) + encode_varuint(10) + encode_varuint(TRACE_REVISION)
    hello += encode_string(database) + encode_string("default") + encode_string("")
    sock.sendall(hello)
    assert read_varuint(sock) == 0
    read_string(sock)
    read_varuint(sock)
    read_varuint(sock)
    revision = min(TRACE_REVISION, read_varuint(sock))
    assert revision >= 54492, "unsupported protocol layout for the profile transport fixture"
    read_varuint(sock)  # Parallel replica protocol.
    read_string(sock)  # Timezone.
    read_string(sock)  # Display name.
    read_varuint(sock)  # Version patch.
    for _ in range(2):
        assert read_string(sock) in ("notchunked", "notchunked_optional", "chunked_optional")
    for _ in range(read_varuint(sock)):
        read_string(sock)
        read_string(sock)
    read_exact(sock, 8)  # Interserver nonce.
    read_settings(sock)
    read_varuint(sock)  # Query plan version.
    read_varuint(sock)  # Cluster function version.
    sock.sendall(encode_string("") + encode_string("notchunked") * 2 + encode_varuint(1))
    return revision


def send_query(sock, revision, query_id, query, settings):
    client_info = b"\1" + encode_string("default") + encode_string(query_id) + encode_string("127.0.0.1:0")
    client_info += struct.pack("<Q", int(time.time() * 1_000_000)) + b"\1"
    client_info += encode_string("test") + encode_string("localhost") + encode_string("Native profile test")
    client_info += encode_varuint(26) + encode_varuint(10) + encode_varuint(TRACE_REVISION)
    client_info += encode_string("") + encode_varuint(0) + encode_varuint(1) + b"\0"
    client_info += encode_varuint(0) * 5  # Parallel replicas and script position.
    client_info += b"\0" + encode_string("") + b"\0\0"  # JWT, agent, internal flag, roles.
    packet = encode_varuint(1) + encode_string(query_id) + client_info + encode_settings(settings)
    packet += encode_string("") * 2 + encode_varuint(2) + encode_varuint(0) + encode_string(query) + b"\0"
    sock.sendall(packet)
    sock.sendall(encode_varuint(2) + encode_block([], 0, revision))


def read_legacy_query(sock):
    assert read_varuint(sock) == 1
    read_string(sock)  # Query ID.
    assert read_exact(sock, 1) == b"\1"  # Initial query.
    for _ in range(3):
        read_string(sock)
    read_exact(sock, 8)
    assert read_exact(sock, 1) == b"\1"  # TCP interface.
    for _ in range(3):
        read_string(sock)
    for _ in range(3):
        read_varuint(sock)
    read_string(sock)  # Quota key.
    read_varuint(sock)  # Distributed depth.
    read_varuint(sock)  # Version patch.
    assert read_exact(sock, 1) == b"\0"  # No telemetry.
    for _ in range(3):
        read_varuint(sock)
    settings = read_settings(sock)
    assert read_string(sock) == ""  # Interserver secret.
    assert read_varuint(sock) == 2  # Complete stage.
    assert read_varuint(sock) == 0  # Uncompressed.
    query = read_string(sock)
    assert read_settings(sock) == {}  # Query parameters.
    assert read_varuint(sock) == 2
    assert read_block(sock, LEGACY_TIMEZONE_REVISION) == (0, {})
    return query, settings
