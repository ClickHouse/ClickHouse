#!/usr/bin/env bash
# `ExpressionActions` takes the type of an `ALIAS` from its argument and the type of an `ARRAY_JOIN` from
# the nested column of its argument, ignoring the type declared in the DAG, while `FillingStep::deserialize`
# compares only the declared types of the `INTERPOLATE` outputs with the header. A forged plan could declare
# a type matching the header and have `FillingTransform` `insertFrom` a column of another type, reading
# memory out of bounds. `ActionsDAG::deserialize` now checks the declared types of `ALIAS` and `ARRAY_JOIN`
# nodes, and `FillingStep::deserialize` refuses `ARRAY_JOIN` in `INTERPOLATE`, which changes the number of
# rows of the single-row interpolation block (and the analyzer does not allow it anyway). A deserialized
# lambda body must not contain `ARRAY_JOIN` either, as the planner refuses to capture it, and its inputs must
# be the captured columns and the arguments of the lambda with the declared types: they are bound by name.
# Its single output must have the declared name and type of the result, the type of the lambda itself must be
# the one of its signature, and it must be created with exactly the captured columns it declares: a higher-order
# function trusts all of these, both for a `FunctionCapture` node and for a lambda folded into a constant.
# The raw protocol client is the one of 05575_filling_step_plan_deserialization.sh; the last plan is
# well-formed, so the refusals are specific.

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
CLIENT = "filling step interpolate forged types"
REVISION = 54482   # pinned, so the revision-gated fields of the handshake are fixed
VERSION = 21       # DBMS_QUERY_PLAN_SERIALIZATION_VERSION, pinned so the byte layout is fixed too
OLD = 17           # one below DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_FILLING_STEP
U8, U16 = b"\x01", b"\x02"            # BinaryTypeIndex
ARR8 = b"\x1e" + U8                   # Array(UInt8)
NUL, INPUT, COLUMN, ALIAS, ARRAY_JOIN, FUNCTION = b"\x00", 0, 1, 2, 3, 4   # a Null Field or a zero flags byte, ActionsDAG::ActionType


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

def dag(nodes, outputs):
    # A serialized ActionsDAG with node 0 as its only input. A node is either a tuple or raw bytes.
    out = vu(len(nodes))
    for node in nodes:
        if isinstance(node, bytes):
            out += node
            continue
        kind, name, type_, children = node
        out += bytes([kind]) + sb(name) + type_ + vu(len(children))
        out += b"".join(vu(c) for c in children) + NUL   # no constant column on the node
    return out + vu(1) + vu(0) + vu(len(outputs)) + b"".join(vu(i) for i in outputs)   # inputs, outputs

def interpolate(nodes, outputs, pairs):
    # The `flags & 2` payload: an ActionsDAG, then (input header key, DAG input name) pairs.
    return bytes([2]) + dag(nodes, outputs) + vu(len(pairs)) + b"".join(sb(k) + sb(n) for k, n in pairs)

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
        code = int.from_bytes(f.read(4), "little")
        print(label, {117: "INCORRECT_DATA", 48: "NOT_IMPLEMENTED"}.get(code, "other"))

FILL = vu(1) + sb("x") + bytes([9]) + sb("") + NUL * 3 + b"\x01\x01" + NUL  # x ASC WITH FILL STEP 1
H = [("x", U8), ("y", U16)]
HA = [("x", U8), ("a", ARR8), ("y", U16)]
HA8 = [("x", U8), ("a", ARR8), ("z", U8)]

# `y` is declared `UInt16` like in the header, but the alias passes through the `UInt8` input.
forge("alias type differs from its argument",
      FILL + interpolate([(INPUT, "x", U8, ()), (ALIAS, "y", U16, (0,))], [1], [("x", "x")]) + vu(0), [H], VERSION, H)
# `y` is declared `UInt16` like in the header, but `ARRAY JOIN` of `Array(UInt8)` yields `UInt8`.
forge("array join type differs from nested type",
      FILL + interpolate([(INPUT, "a", ARR8, ()), (ARRAY_JOIN, "y", U16, (0,))], [1], [("a", "a")]) + vu(0), [HA], VERSION, HA)
forge("array join of not an array",
      FILL + interpolate([(INPUT, "x", U8, ()), (ARRAY_JOIN, "z", U8, (0,))], [1], [("x", "x")]) + vu(0), [HA8], VERSION, HA8)
# Types agree, but an empty array would leave no row to copy.
forge("array join in interpolate",
      FILL + interpolate([(INPUT, "a", ARR8, ()), (ARRAY_JOIN, "z", U8, (0,))], [1], [("a", "a")]) + vu(0), [HA8], VERSION, HA8)
X8Z = [("x", U8), ("z", U8)]
def lambda_node(argument, input_, body_type, body_kind=ALIAS, returns=None, type_=None, captured=None, columns=None, folded=False):
    # `argument -> body`: a lambda with one argument (name, type), whose body is a single node `r` of `body_type`
    # over the only input `input_` (name, type). By default it captures nothing and its declarations agree with
    # the body. `returns` is the declared (name, type) of the result, `type_` the (argument, return) types of the
    # lambda itself (0x24 is `Function`), `captured` the declared (name, type) of a captured column, and `columns`
    # the number of the columns the lambda is created with, each one a `UInt8`.
    returns = returns or ("r", body_type)
    type_ = type_ or (argument[1], returns[1])
    captures = [captured] if captured else []
    columns = len(captures) if columns is None else columns
    head = sb("lambda") + b"\x24" + vu(1) + type_[0] + type_[1]
    signature = (sb(returns[0]) + returns[1]
                 + vu(len(captures)) + b"".join(sb(name) for name, _ in captures)
                 + vu(len(captures)) + b"".join(type for _, type in captures)
                 + vu(1) + sb(argument[0]) + argument[1])
    body = dag([(INPUT, input_[0], input_[1], ()), (body_kind, "r", body_type, (0,))], [1])
    if folded:
        # A lambda folded into a constant (the flags 1 | 2), which holds the captured columns as constants.
        return bytes([COLUMN]) + head + vu(0) + b"\x03" + signature + body + vu(columns) + (U8 + b"\x07") * columns
    # A `FunctionCapture` node (the flag 4), whose arguments are the captured columns: the node 0, `x UInt8`.
    return bytes([FUNCTION]) + head + vu(columns) + vu(0) * columns + b"\x04" + sb("lambda") + signature + body

def with_lambda(label, node):
    forge(label, FILL + interpolate([(INPUT, "x", U8, ()), (ALIAS, "z", U8, (0,)), node], [1], [("x", "x")]) + vu(0),
          [X8Z], VERSION, X8Z)

# `ARRAY JOIN` hidden in the body of a lambda that captures nothing: `e -> arrayJoin(e)`, `Array(UInt8) -> UInt8`.
# The planner refuses to capture such a body, and the check of `FillingStep` does not look into lambdas.
with_lambda("array join in a lambda body", lambda_node(("e", ARR8), ("e", ARR8), U8, ARRAY_JOIN))
# The lambda declares its argument `e` as `UInt8`, and it is called with such a column, but the body reads
# its input `e` as `UInt16`: the columns are passed to the body by name, without a check of the type.
with_lambda("lambda body input type differs from the argument", lambda_node(("e", U8), ("e", U16), U16))
with_lambda("lambda body input is not an argument", lambda_node(("e", U8), ("q", U8), U8))
# The body returns `r UInt8`, but the lambda declares another type or name of its result: `FunctionExpression`
# takes the result by the declared name and passes it on with the declared type.
with_lambda("lambda return type differs from its body", lambda_node(("e", U8), ("e", U8), U8, returns=("r", U16)))
with_lambda("lambda returns not the output of its body", lambda_node(("e", U8), ("e", U8), U8, returns=("q", U8)))
# The lambda is `UInt8 -> UInt8`, but its node declares another `Function` type, and a higher-order function
# derives its own result type from that one.
with_lambda("lambda type differs from its argument", lambda_node(("e", U8), ("e", U8), U8, type_=(U16, U8)))
with_lambda("lambda type differs from its return type", lambda_node(("e", U8), ("e", U8), U8, type_=(U8, U16)))
with_lambda("folded lambda return type differs from its body",
            lambda_node(("e", U8), ("e", U8), U8, returns=("r", U16), folded=True))
with_lambda("folded lambda type differs from its return type",
            lambda_node(("e", U8), ("e", U8), U8, type_=(U8, U16), folded=True))
# `e -> x` captures `x`, declared as `UInt16` in the lambda and in its body, but it is created with a `UInt8`
# column, or without the column, or with a column that it does not capture.
with_lambda("lambda captures a column of another type", lambda_node(("e", U8), ("x", U16), U16, captured=("x", U16)))
with_lambda("lambda is created without its captured column",
            lambda_node(("e", U8), ("x", U8), U8, captured=("x", U8), columns=0))
with_lambda("folded lambda captures a column of another type",
            lambda_node(("e", U8), ("x", U16), U16, captured=("x", U16), folded=True))
with_lambda("folded lambda holds a column that it does not capture",
            lambda_node(("e", U8), ("e", U8), U8, columns=1, folded=True))
with_lambda("well-formed lambda", lambda_node(("e", U8), ("e", U8), U8))
with_lambda("well-formed lambda with a captured column", lambda_node(("e", U8), ("x", U8), U8, captured=("x", U8)))
with_lambda("well-formed folded lambda", lambda_node(("e", U8), ("x", U8), U8, captured=("x", U8), folded=True))
forge("well-formed alias",
      FILL + interpolate([(INPUT, "x", U8, ()), (ALIAS, "z", U8, (0,))], [1], [("x", "x")]) + vu(0), [X8Z], VERSION, X8Z)
PYEOF

# The server has to be serving after each refusal.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM numbers(3)"
