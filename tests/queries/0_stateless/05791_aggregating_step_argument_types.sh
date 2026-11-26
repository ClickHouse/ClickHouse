#!/usr/bin/env bash
# The aggregate functions of a deserialized `Aggregating` step are resolved from the argument types written
# in the plan, but they are executed on the input columns of the same names. A plan with other types made a
# function interpret a column of another type. `AggregatingStep::deserialize` (and the steps merging or finalizing
# the states) now check the types against the input header. The steps merging the states read the keys and then the
# states by position, so their input must have them in the order of the plan, without duplicates.
# The well-formed plans are accepted, so the refusals are specific.
# The raw protocol client is the one of 05775_query_plan_step_missing_inputs.sh. The leaves of the plans are
# `ReadFromTable` steps reading the columns of their headers from the tables created below.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t (x UInt64, k UInt64, __grouping_set UInt64, a AggregateFunction(any, UInt64), b AggregateFunction(sum, UInt64)) ENGINE = Memory;
    CREATE TABLE q (a AggregateFunction(quantiles(0.5), UInt64)) ENGINE = Memory;
"

python3 - <<'PYEOF'
import os
import socket
import struct
E = os.environ.get
HOST, PORT = E("CLICKHOUSE_HOST", "127.0.0.1"), int(E("CLICKHOUSE_PORT_TCP", "9000"))
USER, PASSWORD = E("CLICKHOUSE_USER", "default"), E("CLICKHOUSE_PASSWORD", "")
DATABASE = E("CLICKHOUSE_DATABASE", "default")
CLIENT = "aggregating step argument types"
REVISION = 54482   # pinned, so the revision-gated fields of the handshake are fixed
VERSION = 0        # DBMS_QUERY_PLAN_SERIALIZATION_VERSION: no execution limits and no per-step versions
U64, STR = b"\x04", b"\x15"   # BinaryTypeIndex
NUL = b"\x00"   # a zero flags byte


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

def node(step, payload, out_columns, children=(), settings=()):
    # The children are read before their parent: the number of children, the children, then the step.
    out = vu(len(children)) + b"".join(children)
    out += sb(step) + sb("") + header(out_columns)   # step name, description, header
    out += vu(len(settings))
    for name, value in settings:   # the number of changed settings, then name, flags, binary value of each
        out += sb(name) + vu(0) + value
    return out + payload

def read_table(columns, table="t"):
    return node("ReadFromTable", sb(table) + NUL, columns)   # table name, no flags

def forge(label, payload, children=(), out_columns=(), step="Aggregating", settings=()):
    # One `ReadFromTable` child per header of `children`.
    forge_tree(label, node(step, payload, out_columns, [read_table(columns) for columns in children], settings))

def forge_tree(label, root):
    plan = vu(VERSION) + root + vu(0)   # version, the nodes, no sets
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
                  + vu(13) + plan
                  # an empty `Data` block ends the external tables: no table name, the block info, no columns and rows
                  + vu(2) + sb("") + vu(1) + NUL + vu(2) + struct.pack("<i", -1) + vu(0) + vu(0) + vu(0))
        code = None
        if rv(f) == 2:   # 2 == Protocol::Server::Exception, thrown while building the plan
            code = int.from_bytes(f.read(4), "little")
        else:
            # The plan is built, the header is sent, and the execution may still throw. The packets are not parsed:
            # an exception packet is found by the name of the exception after its code, and the query ends with
            # `EndOfStream` (5) when nothing comes after it.
            data = b""
            while True:
                try:
                    chunk = f.read1(65536)
                except socket.timeout:
                    break
                if not chunk:
                    break
                data += chunk
                position = data.find(b"\x0dDB::Exception")
                if position >= 4:
                    code = int.from_bytes(data[position - 4:position], "little")
                    break
                s.settimeout(1 if data.endswith(b"\x05") else 60)
        names = {117: "INCORRECT_DATA", 48: "NOT_IMPLEMENTED", 44: "ILLEGAL_COLUMN"}
        print(label, "accepted" if code is None else names.get(code, code))

FINAL = 1
AF = b"\x25"   # BinaryTypeIndex::AggregateFunction

def state(function, argument_type):   # AggregateFunction(function, argument_type), version 0
    return AF + vu(0) + sb(function) + vu(0) + vu(1) + argument_type

def aggregates_of(aggregates):   # name, arguments with their types, function, parameters
    out = vu(len(aggregates))
    for name, argument, a_type, function, *parameters in aggregates:
        out += sb(name) + vu(1) + sb(argument) + a_type + sb(function) + vu(len(parameters)) + b"".join(parameters)
    return out

def aggregating(flags, argument_type=None, keys=(), aggregates=None):
    # Flags, keys, then the aggregates. By default one aggregate `a` = any(x).
    if aggregates is None:
        aggregates = [("a", "x", argument_type, "any")]
    return bytes([flags]) + vu(len(keys)) + b"".join(sb(k) for k in keys) + aggregates_of(aggregates)

X = [("x", U64)]
A = [("a", U64)]
forge("argument type differs from the input", aggregating(FINAL, STR), [X], A)
forge("argument is not in the input", aggregating(FINAL, U64), [[("k", U64)]], A)
# The output of an ordinary aggregation has a column for every key and aggregate, finalized by name.
forge("aggregate is duplicated", aggregating(FINAL, aggregates=[("a", "x", U64, "groupArray")] * 2), [X], A * 2)
forge("well-formed", aggregating(FINAL, U64), [X], A)

# The merge reads the keys and then the states by position, so the input must have them in the order of the plan.
ANY_SUM = [("a", "x", U64, "any"), ("b", "x", U64, "sum")]
STATES = [("a", state("any", U64)), ("b", state("sum", U64))]
AB = [("a", U64), ("b", U64)]
def merging(keys, aggregates, flags=FINAL):   # flags, keys, aggregates, an empty sort description
    return bytes([flags]) + vu(len(keys)) + b"".join(sb(k) for k in keys) + aggregates_of(aggregates) + vu(0)

forge("merged column is not a state", merging([], ANY_SUM[:1]), [A], A, "MergingAggregated")
forge("merged states are reordered", merging([], ANY_SUM), [STATES[::-1]], AB, "MergingAggregated")
forge("merged state is duplicated", merging([], ANY_SUM[:1] * 2), [STATES[:1] * 2], A * 2, "MergingAggregated")
forge("merged key is after the states", merging(["k"], ANY_SUM[:1]), [STATES[:1] + [("k", U64)]],
      [("k", U64), ("a", U64)], "MergingAggregated")
forge("merged state is repeated after the states", merging([], ANY_SUM), [STATES + STATES[:1]], AB, "MergingAggregated")
forge("well-formed merge", merging([], ANY_SUM), [STATES], AB, "MergingAggregated")

# With grouping sets, the merge splits the rows by `__grouping_set`, read as `UInt64`, and keeps its type in the output.
GROUPING_SETS = 4
def merging_grouping_sets(keys, sets, aggregates, flags=FINAL | GROUPING_SETS, sort_description=vu(0)):
    out = bytes([flags]) + vu(len(keys)) + b"".join(sb(k) for k in keys) + vu(len(sets))
    for used_keys in sets:
        out += vu(len(used_keys)) + b"".join(sb(k) for k in used_keys)
    return out + aggregates_of(aggregates) + sort_description

GS = merging_grouping_sets(["k"], [["k"], []], ANY_SUM[:1])
K_STATE = [("k", U64), STATES[0]]
K_A = [("k", U64), ("a", U64)]
NOT_MEMORY_EFFICIENT = [("distributed_aggregation_memory_efficient", vu(0))]
forge("grouping set column is a String", GS, [[("__grouping_set", STR)] + K_STATE],
      [("__grouping_set", STR)] + K_A, "MergingAggregated")
forge("grouping set column is duplicated", GS, [[("__grouping_set", U64)] * 2 + K_STATE],
      [("__grouping_set", U64)] + K_A, "MergingAggregated")
# The memory-efficient merge (on by default in the settings of the step) does not support grouping sets.
forge("memory-efficient grouping sets merge", GS, [[("__grouping_set", U64)] + K_STATE],
      [("__grouping_set", U64)] + K_A, "MergingAggregated")
# The output header and the reconstruction of the missing keys take every key from the input by name.
forge("grouping set key is repeated after the states", GS, [[("__grouping_set", U64)] + K_STATE + [("k", U64)]],
      [("__grouping_set", U64)] + K_A, "MergingAggregated", NOT_MEMORY_EFFICIENT)
# The memory-bound merge (with a non-empty sort description) does not support grouping sets either.
MEMORY_BOUND = 32
GS_SORTED = merging_grouping_sets(["k"], [["k"], []], ANY_SUM[:1], FINAL | GROUPING_SETS | MEMORY_BOUND, vu(1) + sb("k") + b"\x03")
forge("memory-bound grouping sets merge", GS_SORTED, [[("__grouping_set", U64)] + K_STATE],
      [("__grouping_set", U64)] + K_A, "MergingAggregated", NOT_MEMORY_EFFICIENT)
forge("well-formed grouping sets merge", GS, [[("__grouping_set", U64)] + K_STATE],
      [("__grouping_set", U64)] + K_A, "MergingAggregated", NOT_MEMORY_EFFICIENT)

# Without grouping sets, the merge does not produce `__grouping_set`, so the input must not have it.
MERGING = merging(["k"], ANY_SUM[:1])
forge("grouping set column without grouping sets", MERGING, [K_STATE + [("__grouping_set", U64)]],
      [("__grouping_set", U64)] + K_A, "MergingAggregated")
forge("well-formed merge without grouping sets", MERGING, [K_STATE], K_A, "MergingAggregated")

# `TotalsHaving` finalizes the states it finds in the input by the names of its aggregates.
def totals_having(aggregates):   # flags (final), then the aggregates
    return bytes([FINAL]) + aggregates_of(aggregates)

forge("finalized column is not a state", totals_having(ANY_SUM[:1]), [A], A, "TotalsHaving")
forge("finalized state is not in the input", totals_having(ANY_SUM[:1]), [[("b", state("any", U64))]], [("b", U64)], "TotalsHaving")
forge("finalized state is of another function", totals_having(ANY_SUM[:1]), [[("a", state("sum", U64))]], A, "TotalsHaving")
forge("well-formed totals", totals_having(ANY_SUM), [STATES], AB, "TotalsHaving")

# `DataTypeAggregateFunction::equals` considers the states of `quantile` and `quantiles` equal, so a `Union`
# passes a column of `quantile` states under a header of `quantiles` states. They are finalized with the function
# of the column, to `Float64` instead of `Array(Float64)`. The aggregation without keys of an empty input gives one row.
F64 = b"\x03"   # Field::Types::Float64
HALF = F64 + struct.pack("<d", 0.5)
HALF_IN_TYPE = b"\x07" + struct.pack("<d", 0.5)   # the parameters of a type are encoded with `FieldBinaryTypeIndex`
def state_of(function, parameters, argument_type):
    return AF + vu(0) + sb(function) + vu(len(parameters)) + b"".join(parameters) + vu(1) + argument_type

ARRAY_F64 = b"\x1e" + b"\x0e"
def union_of_states(branch_function):
    branch = node("Aggregating", aggregating(0, aggregates=[("a", "x", U64, branch_function, HALF)]),
                  [("a", state_of(branch_function, [HALF_IN_TYPE], U64))], [read_table(X)])
    quantiles_state = [("a", state_of("quantiles", [HALF_IN_TYPE], U64))]
    return node("Union", b"", quantiles_state, [read_table(quantiles_state, "q"), branch])

for function in ("quantile", "quantiles"):
    forge_tree(f"TotalsHaving of a union with {function} states",
               node("TotalsHaving", totals_having([("a", "x", U64, "quantiles", HALF)]), [("a", ARRAY_F64)],
                    [union_of_states(function)]))
PYEOF

# The server has to be serving after each refusal.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM numbers(3)"
