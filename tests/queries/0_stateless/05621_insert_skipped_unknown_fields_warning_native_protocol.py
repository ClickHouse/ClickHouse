#!/usr/bin/env python3

# A data block a native-protocol client sends for an INSERT with a column the table does not have: the column is
# skipped (`input_format_skip_unknown_fields`) and the server logs a warning naming it and the query.

import os
import socket
import urllib.parse
import urllib.request

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")
CLICKHOUSE_PORT_TCP = int(os.environ.get("CLICKHOUSE_PORT_TCP", "9000"))
CLICKHOUSE_PORT_HTTP = int(os.environ.get("CLICKHOUSE_PORT_HTTP", "8123"))
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "default")
SOCKET_TIMEOUT = float(os.environ.get("CLICKHOUSE_TEST_TIMEOUT", "600"))
CLIENT_NAME = "simple native protocol"
CLIENT_REVISION = 54449


def http_query(query):
    params = urllib.parse.urlencode({"database": CLICKHOUSE_DATABASE})
    url = "http://{}:{}/?{}".format(CLICKHOUSE_HOST, CLICKHOUSE_PORT_HTTP, params)
    with urllib.request.urlopen(url, data=query.encode("utf-8"), timeout=SOCKET_TIMEOUT) as response:
        return response.read().decode("utf-8")


def writeVarUInt(x, ba):
    for _ in range(0, 9):
        byte = x & 0x7F
        if x > 0x7F:
            byte |= 0x80

        ba.append(byte)

        x >>= 7
        if x == 0:
            return


def writeStringBinary(s, ba):
    b = bytes(s, "utf-8")
    writeVarUInt(len(b), ba)
    ba.extend(b)


def readStrict(s, size=1):
    res = bytearray()
    while size:
        cur = s.recv(size)
        if not cur:
            raise RuntimeError("Socket is closed")
        size -= len(cur)
        res.extend(cur)

    return res


def readUInt(s, size=1):
    res = readStrict(s, size)
    val = 0
    for i in range(len(res)):
        val += res[i] << (i * 8)
    return val


def readVarUInt(s):
    x = 0
    for i in range(9):
        byte = readStrict(s)[0]
        x |= (byte & 0x7F) << (7 * i)

        if not byte & 0x80:
            return x

    return x


def readStringBinary(s):
    size = readVarUInt(s)
    s = readStrict(s, size)
    return s.decode("utf-8")


def sendHello(s):
    ba = bytearray()
    writeVarUInt(0, ba)  # Hello
    writeStringBinary(CLIENT_NAME, ba)
    writeVarUInt(21, ba)
    writeVarUInt(9, ba)
    writeVarUInt(CLIENT_REVISION, ba)
    writeStringBinary("default", ba)  # database
    writeStringBinary("default", ba)  # user
    writeStringBinary("", ba)  # pwd
    s.sendall(ba)


def receiveHello(s):
    p_type = readVarUInt(s)
    assert p_type == 0  # Hello
    readStringBinary(s)  # server name
    readVarUInt(s)  # major
    readVarUInt(s)  # minor
    readVarUInt(s)  # revision
    readStringBinary(s)  # timezone
    readStringBinary(s)  # display name
    readVarUInt(s)  # patch


def serializeClientInfo(ba, query_id):
    writeStringBinary("default", ba)  # initial_user
    writeStringBinary(query_id, ba)  # initial_query_id
    writeStringBinary("127.0.0.1:9000", ba)  # initial_address
    ba.extend([0] * 8)  # initial_query_start_time_microseconds
    ba.append(1)  # TCP
    writeStringBinary("os_user", ba)  # os_user
    writeStringBinary("client_hostname", ba)  # client_hostname
    writeStringBinary(CLIENT_NAME, ba)  # client_name
    writeVarUInt(21, ba)
    writeVarUInt(9, ba)
    writeVarUInt(CLIENT_REVISION, ba)
    writeStringBinary("", ba)  # quota_key
    writeVarUInt(0, ba)  # distributed_depth
    writeVarUInt(1, ba)  # client_version_patch
    ba.append(0)  # No telemetry


def sendQuery(s, query, query_id):
    ba = bytearray()
    writeVarUInt(1, ba)  # query
    writeStringBinary(query_id, ba)
    ba.append(1)  # INITIAL_QUERY
    serializeClientInfo(ba, query_id)
    writeStringBinary("", ba)  # End of settings
    writeStringBinary("", ba)  # No interserver secret
    writeVarUInt(2, ba)  # Stage - Complete
    ba.append(0)  # No compression
    writeStringBinary(query, ba)
    s.sendall(ba)


def serializeBlockInfo(ba):
    writeVarUInt(1, ba)  # 1
    ba.append(0)  # is_overflows
    writeVarUInt(2, ba)  # 2
    writeVarUInt(0, ba)  # 0
    ba.extend([0] * 4)  # bucket_num


def sendEmptyBlock(s):
    ba = bytearray()
    writeVarUInt(2, ba)  # Data
    writeStringBinary("", ba)
    serializeBlockInfo(ba)
    writeVarUInt(0, ba)  # columns
    writeVarUInt(0, ba)  # rows
    s.sendall(ba)


def readException(s):
    readUInt(s, 4)  # code
    readStringBinary(s)  # name
    text = readStringBinary(s)
    readStringBinary(s)  # trace
    readUInt(s)  # has_nested
    return text


def readHeader(s):
    packet_type = readVarUInt(s)
    if packet_type == 2:  # Exception
        raise RuntimeError(readException(s))
    assert packet_type == 1, packet_type  # Data
    readStringBinary(s)  # external table name
    readVarUInt(s)  # 1
    readUInt(s)  # is_overflows
    readVarUInt(s)  # 2
    readUInt(s, 4)  # bucket_num
    readVarUInt(s)  # 0
    columns = readVarUInt(s)
    readVarUInt(s)  # rows
    for _ in range(columns):
        readStringBinary(s)  # name
        readStringBinary(s)  # type


def main():
    query_id = CLICKHOUSE_DATABASE + "_native"
    unknown = "u_native_" + CLICKHOUSE_DATABASE
    http_query("CREATE TABLE t_native (a UInt64, b String) ENGINE = Memory")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(SOCKET_TIMEOUT)
        s.connect((CLICKHOUSE_HOST, CLICKHOUSE_PORT_TCP))
        sendHello(s)
        receiveHello(s)
        sendQuery(
            s,
            "INSERT INTO {}.t_native SETTINGS input_format_defaults_for_omitted_fields = 0 FORMAT Native".format(
                CLICKHOUSE_DATABASE
            ),
            query_id,
        )
        sendEmptyBlock(s)  # external tables
        readHeader(s)

        ba = bytearray()
        writeVarUInt(2, ba)  # Data
        writeStringBinary("", ba)
        serializeBlockInfo(ba)
        writeVarUInt(3, ba)  # columns
        writeVarUInt(1, ba)  # rows
        writeStringBinary("a", ba)
        writeStringBinary("UInt64", ba)
        ba.extend([1] + [0] * 7)
        writeStringBinary("b", ba)
        writeStringBinary("String", ba)
        writeStringBinary("y", ba)
        writeStringBinary(unknown, ba)
        writeStringBinary("String", ba)
        writeStringBinary("x", ba)
        s.sendall(ba)
        sendEmptyBlock(s)

        packet_type = readVarUInt(s)
        if packet_type == 2:  # Exception
            raise RuntimeError(readException(s))
        assert packet_type == 5, packet_type  # EndOfStream

    http_query("SYSTEM FLUSH LOGS text_log")
    print(
        http_query(
            "SELECT count(), countIf(position(message, 'of the input data of query {} in format Native ') > 0) "
            "FROM system.text_log WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 1 HOUR AND level = 'Warning' "
            "AND position(message, 'Field `{}` ') > 0".format(query_id, unknown)
        ),
        end="",
    )
    print(http_query("SELECT * FROM t_native"), end="")
    http_query("DROP TABLE t_native")


if __name__ == "__main__":
    main()
