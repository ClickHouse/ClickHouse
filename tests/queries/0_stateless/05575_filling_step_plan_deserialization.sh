#!/usr/bin/env bash
# The rejection paths of `FillingStep::deserialize` (src/Processors/QueryPlan/FillingStep.cpp).
#
# A client can hand the server a query plan only in a `QueryPlan` packet (native protocol packet
# 13, for a query at stage `QueryPlan` against a server with `process_query_plan_packet` on, see
# tests/config/config.d/process_query_plan_packet.xml), so it is the only way to reach the checks
# below. Each case forges one plan whose root is a `Filling` step and has to be refused with
# INCORRECT_DATA instead of building a step that disagrees with its own stream, which
# `FillingTransform` would then read out of range; the last plan is well-formed, so the refusals
# are specific. Raw protocol client as in 05427_query_plan_deserialize_deep_recursion.sh.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 - <<'PYEOF'
import os
import socket
E = os.environ.get
HOST, PORT = E("CLICKHOUSE_HOST", "127.0.0.1"), int(E("CLICKHOUSE_PORT_TCP", "9000"))
USER, PASSWORD = E("CLICKHOUSE_USER", "default"), E("CLICKHOUSE_PASSWORD", "")
DATABASE = E("CLICKHOUSE_DATABASE", "default")
CLIENT = "filling step plan deserialization"
REVISION = 54482   # pinned, so the revision-gated fields of the handshake are fixed
VERSION = 21       # DBMS_QUERY_PLAN_SERIALIZATION_VERSION, pinned so the byte layout is fixed too
OLD = 17           # one below DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_FILLING_STEP
U8, U16 = b"\x01", b"\x02"            # BinaryTypeIndex
NUL, INPUT, ALIAS = b"\x00", 0, 2     # a Null Field or a zero flags byte, ActionsDAG::ActionType


def vu(x):
    out = bytearray()
    while True:
        out.append((x & 0x7F) | (0x80 if x > 0x7F else 0))
        x >>= 7
        if not x:
            return bytes(out)

def sb(s):
    return vu(len(s)) + s.encode()

def rv(f):
    x = shift = 0
    while True:
        c = f.read(1)[0]
        x |= (c & 0x7F) << shift
        shift += 7
        if not c & 0x80:
            return x

def skip_hello(f):
    assert rv(f) == 0
    for kind in "SVVVVSSVSS":   # name, 4 versions, timezone, display name, patch, 2 chunked modes
        rv(f) if kind == "V" else f.read(rv(f))
    for _ in range(rv(f) * 2):  # password complexity rules, two strings each
        f.read(rv(f))
    f.read(8)                   # nonce
    while f.read(rv(f)):        # server settings, an empty name terminates
        rv(f)
        f.read(rv(f))
    rv(f), rv(f)                # query plan serialization and cluster function versions

def header(columns):
    return vu(len(columns)) + b"".join(sb(n) + t for n, t in columns)

def interpolate(nodes, outputs, pairs):
    # The `flags & 2` payload: an ActionsDAG, then (input header key, DAG input name) pairs.
    out = vu(len(nodes))
    for kind, name, type_, children in nodes:
        out += bytes([kind]) + sb(name) + type_ + vu(len(children))
        out += b"".join(vu(c) for c in children) + NUL   # no constant column on the node
    out += vu(1) + vu(0) + vu(len(outputs)) + b"".join(vu(i) for i in outputs)   # inputs, outputs
    return bytes([2]) + out + vu(len(pairs)) + b"".join(sb(k) + sb(n) for k, n in pairs)

def forge(label, payload, children=(), version=VERSION, out_columns=()):
    plan = vu(version) + vu(0) + NUL + vu(len(children))   # version, max_threads, concurrency
    for columns in children:   # one `ReadNothing` child per header, read before its parent
        plan += vu(0) + sb("ReadNothing") + vu(0) + sb("") + header(columns) + vu(0)
    plan += sb("Filling") + (vu(0) if version >= 19 else b"")   # step name, per-step version
    plan += sb("") + header(out_columns) + vu(0) + payload   # description, header, settings
    qid = os.urandom(16).hex()
    info = (b"\x01" + sb(USER) + sb(qid) + sb("127.0.0.1:9000") + bytes(8) + b"\x01"
            + sb("os_user") + sb("host") + sb(CLIENT) + vu(24) + vu(9) + vu(REVISION) + sb("")
            + vu(0) + vu(1) + bytes(7))   # depth, patch, no tracing/replicas/script/jwt
    with socket.socket() as s:
        s.settimeout(60)
        s.connect((HOST, PORT))
        f = s.makefile("rb")
        s.sendall(vu(0) + sb(CLIENT) + vu(24) + vu(9) + vu(REVISION)
                  + sb(DATABASE) + sb(USER) + sb(PASSWORD))
        skip_hello(f)
        s.sendall(sb("") + sb("notchunked") + sb("notchunked") + vu(0)   # addendum
                  + vu(1) + sb(qid) + info + sb("") + sb("") + sb("")
                  + vu(7) + NUL + sb("SELECT 1") + sb("")   # 7 == QueryProcessingStage::QueryPlan
                  + vu(13) + plan)
        if rv(f) != 2:   # 2 == Protocol::Server::Exception
            print(label, "accepted")
            return
        print(label, "INCORRECT_DATA" if int.from_bytes(f.read(4), "little") == 117 else "other")

X8, X16 = (("x", U8),), (("x", U16),)
IN8 = [(INPUT, "x", U8, ())]
FILL = vu(1) + sb("x") + bytes([9]) + sb("") + NUL * 3 + b"\x01\x01" + NUL  # x ASC WITH FILL STEP 1

forge("pre-support plan version", b"", version=OLD)
forge("no input stream", b"")
forge("no WITH FILL column", vu(1) + sb("x") + bytes([1]), [X8])
forge("unknown step flags", FILL + bytes([4]), [X8])
forge("source not in header", FILL + interpolate(IN8, [], [("zz", "x")]), [X8])
forge("source not required", FILL + interpolate(IN8, [], [("x", "zz")]), [X8])
forge("source type differs", FILL + interpolate(IN8, [], [("x", "x")]), [X16])
forge("too few sources", FILL + interpolate(IN8, [], []), [X8])
forge("result not in header", FILL + interpolate(IN8 + [(ALIAS, "y", U8, (0,))], [1], [("x", "x")]), [X8])
forge("result type differs", FILL + interpolate(IN8 + [(ALIAS, "x", U16, (0,))], [1], [("x", "x")]), [X8])
forge("well-formed", FILL + NUL + vu(0), [X8], VERSION, X8)   # no INTERPOLATE, no sets
PYEOF

# The server has to be serving after each refusal.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM numbers(3)"
