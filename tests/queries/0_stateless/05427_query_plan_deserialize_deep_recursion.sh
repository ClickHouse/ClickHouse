#!/usr/bin/env bash
# Regression test for unbounded recursion in `ActionsDAG::deserialize`.
#
# A `QueryPlan` packet (native-protocol packet type 13) can carry an `ActionsDAG` with a
# lambda-capture constant whose capture DAG is itself another lambda-capture constant, and so
# on. `ActionsDAG::deserialize` / `deserializeConstant` recurse once per nesting level, so a
# client that sends a few thousand nested captures used to exhaust the server stack and take the
# whole process down with SIGSEGV. The fix caps the nesting depth and adds `checkStackSize`,
# turning the attack into a `TOO_DEEP_RECURSION` exception while the server keeps serving.
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

CLIENT_NAME = "query plan deep recursion poc"
REVISION = 54482

# Plan serialization version gates for header fields (see Core/ProtocolDefines.h).
WITH_EXECUTION_LIMITS = 11  # max_threads + concurrency_control
WITH_STEP_VERSIONS = 19     # per-step version

# ActionsDAG::ActionType (Interpreters/ActionsDAG.h)
ACTION_COLUMN = 1

# BinaryTypeIndex (DataTypes/DataTypesBinaryEncoding.h)
TYPE_UINT8 = b"\x01"
# Function(0 args) -> UInt8 : 0x24 <num_args=0> <return_type>
TYPE_FUNCTION = b"\x24\x00" + TYPE_UINT8

DEPTH = 5000
SHALLOW_DEPTH = 10


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


def buildActionsDAG(depth):
    """An ActionsDAG with `depth` nested lambda-capture constants.

    Each level is a single COLUMN node of a Function type; its constant is a lambda capture
    whose capture DAG is the next level. The innermost level is a plain UInt8 constant, so a
    correct (shallow) deserializer would terminate.
    """
    # Terminal: one COLUMN node holding a UInt8 constant.
    dag = bytearray()
    dag += varuint(1)      # nodes_size
    dag += bytes([ACTION_COLUMN])
    dag += strbin("")      # result_name
    dag += TYPE_UINT8      # result_type
    dag += varuint(0)      # children
    dag += bytes([1])      # column_flags: has column
    dag += bytes([0])      # UInt8 constant value
    dag += varuint(0)      # inputs
    dag += varuint(0)      # outputs

    for _ in range(depth):
        unit = bytearray()
        unit += varuint(1)      # nodes_size
        unit += bytes([ACTION_COLUMN])
        unit += strbin("")      # result_name
        unit += TYPE_FUNCTION   # result_type -> isFunction() branch in deserializeConstant
        unit += varuint(0)      # children
        unit += bytes([1])      # column_flags: has column
        # deserializeCapture
        unit += strbin("")      # capture.return_name
        unit += TYPE_UINT8      # capture.return_type
        unit += varuint(0)      # num captured names
        unit += varuint(0)      # num captured types
        unit += varuint(0)      # num lambda arguments
        # recursive ActionsDAG::deserialize for the capture DAG
        unit += dag
        unit += varuint(0)      # num_captured_columns
        unit += varuint(0)      # inputs
        unit += varuint(0)      # outputs
        dag = unit

    return bytes(dag)


def buildQueryPlanPacket(plan_version, depth):
    body = bytearray()
    writeVarUInt(plan_version, body)
    if plan_version >= WITH_EXECUTION_LIMITS:
        writeVarUInt(0, body)      # max_threads
        body.append(0)             # concurrency_control
    # Node tree, single root Expression step with no children.
    writeVarUInt(0, body)          # num_children
    writeStringBinary("Expression", body)
    if plan_version >= WITH_STEP_VERSIONS:
        writeVarUInt(0, body)      # step_version
    writeStringBinary("", body)    # step_description
    writeVarUInt(0, body)          # output header: 0 columns
    writeVarUInt(0, body)          # settings: 0 changed
    # ExpressionStep::deserialize -> ActionsDAG::deserialize
    body += buildActionsDAG(depth)

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


def sendPlan(depth):
    """Sends a `QueryPlan` packet with `depth` nested lambda captures and returns the exception code and text, or None if the query is not rejected."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(60)
        s.connect((CLICKHOUSE_HOST, CLICKHOUSE_PORT))
        sendHello(s)
        plan_version = receiveHello(s)
        sendAddendum(s)
        # Stage 7 == QueryProcessingStage::QueryPlan: the server then expects a QueryPlan packet.
        sendQuery(s, "SELECT 1", 7)
        s.sendall(buildQueryPlanPacket(plan_version, depth))

        # Only the first packet matters: either the exception or the start of the query result.
        packet_type = readVarUInt(s)
        if packet_type == 2:  # Exception
            return readException(s)
        return None, "packet {}".format(packet_type)


def main():
    # A shallow plan must not hit the recursion limit: it distinguishes the depth limit from rejecting any plan.
    code, text = sendPlan(SHALLOW_DEPTH)
    if code == 306:
        print("UNEXPECTED TOO_DEEP_RECURSION for a shallow plan: {}".format(text))
    else:
        print("shallow plan: no TOO_DEEP_RECURSION")

    code, text = sendPlan(DEPTH)
    # 306 == TOO_DEEP_RECURSION. In sanitizer builds, `checkStackSize` may throw it before the depth limit is reached.
    if code == 306:
        print("TOO_DEEP_RECURSION")
    else:
        print("UNEXPECTED EXCEPTION {}: {}".format(code, text))


main()
PYEOF

# The server must still be alive and serving after rejecting the malicious packet.
${CLICKHOUSE_CLIENT} --query "SELECT 'server alive', 1"
