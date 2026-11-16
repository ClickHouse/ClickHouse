#!/usr/bin/env bash
# A step of a `QueryPlan` packet (native-protocol packet type 13) needs as many children as it reads input
# headers, but the number of children comes from the packet. Steps used to read the input headers without
# checking it, so a plan with a step lacking its children made the server read out of bounds. Now such a
# plan is rejected with `INCORRECT_DATA` and the server keeps serving.
#
# The packet is only reachable for a query sent with the `QueryPlan` processing stage while the
# server has `process_query_plan_packet` enabled (see tests/config/config.d/process_query_plan_packet.xml).
# The client here speaks the raw native protocol pinned to revision 54482 so the revision-gated
# fields of the handshake are fixed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 - <<'PYEOF'
import os
import socket
import uuid

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT_TCP", "9000"))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "default")

CLIENT_NAME = "query plan missing inputs test"
REVISION = 54482

# Plan serialization version gates for header fields (see Core/ProtocolDefines.h).
WITH_EXECUTION_LIMITS = 11  # max_threads + concurrency_control
WITH_STEP_VERSIONS = 19     # per-step version


def writeVarUInt(x, ba):
    for _ in range(0, 9):
        byte = x & 0x7F
        if x > 0x7F:
            byte |= 0x80
        ba.append(byte)
        x >>= 7
        if x == 0:
            return


def varuint(x):
    ba = bytearray()
    writeVarUInt(x, ba)
    return bytes(ba)


def writeStringBinary(s, ba):
    b = bytes(s, "utf-8") if isinstance(s, str) else s
    writeVarUInt(len(b), ba)
    ba.extend(b)


def strbin(s):
    ba = bytearray()
    writeStringBinary(s, ba)
    return bytes(ba)


def readStrict(s, size=1):
    res = bytearray()
    while size:
        cur = s.recv(size)
        if not cur:
            raise EOFError("Connection closed by server")
        size -= len(cur)
        res.extend(cur)
    return res


def readUInt(s, size=1):
    res = readStrict(s, size)
    val = 0
    for i in range(len(res)):
        val += res[i] << (i * 8)
    return val


def readUInt32(s):
    return readUInt(s, 4)


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
    return readStrict(s, size).decode("utf-8")


def sendHello(s):
    ba = bytearray()
    writeVarUInt(0, ba)  # Hello
    writeStringBinary(CLIENT_NAME, ba)
    writeVarUInt(24, ba)  # major
    writeVarUInt(9, ba)  # minor
    writeVarUInt(REVISION, ba)
    writeStringBinary(CLICKHOUSE_DATABASE, ba)
    writeStringBinary(CLICKHOUSE_USER, ba)
    writeStringBinary(CLICKHOUSE_PASSWORD, ba)
    s.sendall(ba)


def receiveHello(s):
    assert readVarUInt(s) == 0  # Hello
    readStringBinary(s)  # server name
    readVarUInt(s)  # major
    readVarUInt(s)  # minor
    readVarUInt(s)  # revision
    readVarUInt(s)  # parallel replicas protocol version
    readStringBinary(s)  # timezone
    readStringBinary(s)  # display name
    readVarUInt(s)  # version patch
    readStringBinary(s)  # proto_send chunked
    readStringBinary(s)  # proto_recv chunked
    for _ in range(readVarUInt(s)):  # password complexity rules
        readStringBinary(s)
        readStringBinary(s)
    readStrict(s, 8)  # nonce
    while True:  # server settings
        if readStringBinary(s) == "":
            break
        readVarUInt(s)
        readStringBinary(s)
    query_plan_version = readVarUInt(s)  # query plan serialization version
    readVarUInt(s)  # cluster function protocol version
    return query_plan_version


def sendAddendum(s):
    ba = bytearray()
    writeStringBinary("", ba)  # quota key
    writeStringBinary("notchunked", ba)  # proto_send chunked
    writeStringBinary("notchunked", ba)  # proto_recv chunked
    writeVarUInt(0, ba)  # parallel replicas protocol version
    s.sendall(ba)


def serializeClientInfo(ba, query_id):
    ba.append(1)  # INITIAL_QUERY
    writeStringBinary(CLICKHOUSE_USER, ba)  # initial_user
    writeStringBinary(query_id, ba)  # initial_query_id
    writeStringBinary("127.0.0.1:9000", ba)  # initial_address
    ba.extend([0] * 8)  # initial_query_start_time_microseconds
    ba.append(1)  # interface = TCP
    writeStringBinary("os_user", ba)
    writeStringBinary("client_hostname", ba)
    writeStringBinary(CLIENT_NAME, ba)
    writeVarUInt(24, ba)  # client major
    writeVarUInt(9, ba)  # client minor
    writeVarUInt(REVISION, ba)  # client tcp protocol version
    writeStringBinary("", ba)  # quota key
    writeVarUInt(0, ba)  # distributed_depth
    writeVarUInt(1, ba)  # client version patch
    ba.append(0)  # opentelemetry: no trace id
    writeVarUInt(0, ba)  # parallel replicas: collaborate_with_initiator
    writeVarUInt(0, ba)  # parallel replicas: obsolete count
    writeVarUInt(0, ba)  # parallel replicas: number_of_current_replica
    writeVarUInt(0, ba)  # script query number
    writeVarUInt(0, ba)  # script line number
    ba.append(0)  # jwt: none


def sendQuery(s, query, stage):
    ba = bytearray()
    query_id = uuid.uuid4().hex
    writeVarUInt(1, ba)  # Query
    writeStringBinary(query_id, ba)
    serializeClientInfo(ba, query_id)
    writeStringBinary("", ba)  # empty per-query settings
    writeStringBinary("", ba)  # interserver externally granted roles
    writeStringBinary("", ba)  # interserver secret
    writeVarUInt(stage, ba)  # query processing stage
    ba.append(0)  # no compression
    writeStringBinary(query, ba)
    writeStringBinary("", ba)  # query parameters terminator
    s.sendall(ba)


def buildNode(plan_version, step_name, payload, children):
    """Serializes a plan node: its children first, then the step itself with an empty output header."""
    node = bytearray()
    writeVarUInt(len(children), node)  # num_children
    for child in children:
        node += child
    writeStringBinary(step_name, node)
    if plan_version >= WITH_STEP_VERSIONS:
        writeVarUInt(0, node)      # step_version
    writeStringBinary("", node)    # step_description
    writeVarUInt(0, node)          # output header: 0 columns
    writeVarUInt(0, node)          # settings: 0 changed
    node += payload
    return bytes(node)


def buildQueryPlanPacket(plan_version, step_name, payload, num_children):
    body = bytearray()
    writeVarUInt(plan_version, body)
    if plan_version >= WITH_EXECUTION_LIMITS:
        writeVarUInt(0, body)      # max_threads
        body.append(0)             # concurrency_control
    # Node tree: a root step with the given number of `ReadNothing` children.
    children = [buildNode(plan_version, "ReadNothing", b"", []) for _ in range(num_children)]
    body += buildNode(plan_version, step_name, payload, children)

    packet = bytearray()
    writeVarUInt(13, packet)       # Client::QueryPlan
    packet += body
    return bytes(packet)


def readException(s):
    code = readUInt32(s)
    readStringBinary(s)  # name
    text = readStringBinary(s)
    readStringBinary(s)  # trace
    readStrict(s)  # has_nested
    return code, text


def sendPlan(step_name, payload, num_children=0):
    """Sends a `QueryPlan` packet with a single step having `num_children` children and returns the exception code, or None if the query is not rejected."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(60)
        s.connect((CLICKHOUSE_HOST, CLICKHOUSE_PORT))
        sendHello(s)
        plan_version = receiveHello(s)
        sendAddendum(s)
        # Stage 7 == QueryProcessingStage::QueryPlan: the server then expects a QueryPlan packet.
        sendQuery(s, "SELECT 1", 7)
        s.sendall(buildQueryPlanPacket(plan_version, step_name, payload, num_children))

        # Only the first packet matters: either the exception or the start of the query result.
        packet_type = readVarUInt(s)
        if packet_type == 2:  # Exception
            return readException(s)[0]
        return None


def main():
    # 117 == INCORRECT_DATA
    for step_name, payload in [("Extremes", b""), ("Limit", b"\x00\x01\x00"), ("Union", b"\x00"), ("BlocksMarshalling", b"")]:
        print(step_name, sendPlan(step_name, payload))
    # A step with more children than it has inputs.
    print("Extremes with two children", sendPlan("Extremes", b"", num_children=2))


main()
PYEOF

# The server must still be alive and serving after rejecting the malicious packet.
${CLICKHOUSE_CLIENT} --query "SELECT 'server alive', 1"
