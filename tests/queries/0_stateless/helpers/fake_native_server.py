#!/usr/bin/env python3
"""A fake ClickHouse server that answers the query of clickhouse-client with a crafted reply.

It is used by the tests of what a reader of the native protocol does with a malformed result block:
a real server never sends one. The server announces `REVISION`, so the reply must use its layout.
"""

import os
import shlex
import socket
import struct
import subprocess
import threading

# DBMS_MIN_REVISION_WITH_REPLICATED_SERIALIZATION
REVISION = 54482
QUERY = "SELECT 'the reply of a fake server'"


def writeVarUInt(x, ba):
    while True:
        byte = x & 0x7F
        x >>= 7
        ba.append(byte | (0x80 if x else 0))
        if not x:
            return


def writeStringBinary(s, ba):
    b = bytes(s, "utf-8")
    writeVarUInt(len(b), ba)
    ba.extend(b)


def serverHello():
    ba = bytearray()
    writeVarUInt(0, ba)  # Hello
    writeStringBinary("fake server", ba)
    writeVarUInt(24, ba)  # major
    writeVarUInt(9, ba)  # minor
    writeVarUInt(REVISION, ba)
    writeVarUInt(1, ba)  # parallel replicas protocol version (>= 54471)
    writeStringBinary("UTC", ba)  # timezone (>= 54058)
    writeStringBinary("fake", ba)  # display name (>= 54372)
    writeVarUInt(0, ba)  # version patch (>= 54401)
    writeStringBinary("notchunked", ba)  # proto_send chunked (>= 54470)
    writeStringBinary("notchunked", ba)  # proto_recv chunked (>= 54470)
    writeVarUInt(0, ba)  # password complexity rules (>= 54461)
    ba.extend(struct.pack("<Q", 0))  # nonce (>= 54462)
    writeStringBinary("", ba)  # end of the server settings (>= 54474)
    writeVarUInt(0, ba)  # query plan serialization version (>= 54477)
    writeVarUInt(0, ba)  # cluster function protocol version (>= 54479)
    return bytes(ba)


def serveOneQuery(listener, reply):
    """Answers the query of one client with `reply`, without parsing what the client sends."""
    connection, _ = listener.accept()
    with connection:
        connection.settimeout(60)
        received = connection.recv(65536)  # the Hello of the client
        connection.sendall(serverHello())
        # The addendum and the Query packet follow. Once the text of the query is here, the
        # client is about to wait for the result.
        while QUERY.encode() not in received:
            chunk = connection.recv(65536)
            if not chunk:
                return
            received += chunk
        connection.sendall(reply)
        # The reply may be cut short: signal EOF so that the client gets a short read instead of
        # waiting for the missing bytes, then wait until it closes the connection.
        connection.shutdown(socket.SHUT_WR)
        try:
            while connection.recv(65536):
                pass
        except OSError:
            pass


def runClientAgainstFakeServer(reply):
    """The output of clickhouse-client whose server answers the query with `reply`."""
    client = shlex.split(os.environ["CLICKHOUSE_CLIENT_BINARY"])
    listener = socket.create_server(("127.0.0.1", 0))
    listener.settimeout(60)
    thread = threading.Thread(target=serveOneQuery, args=(listener, reply), daemon=True)
    thread.start()
    try:
        port = listener.getsockname()[1]
        result = subprocess.run(
            client
            + ["--host", "127.0.0.1", "--port", str(port), "--compression", "0"]
            + ["--query", QUERY],
            capture_output=True,
            timeout=120,
        )
        return (result.stdout + result.stderr).decode("utf-8", "replace")
    finally:
        thread.join(timeout=60)
        listener.close()
