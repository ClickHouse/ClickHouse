#!/usr/bin/env bash
# The aggregate functions of a deserialized `Aggregating` step are resolved from the argument types written
# in the plan, but they are executed on the input columns of the same names. A plan with other types made a
# function interpret a column of another type. `AggregatingStep::deserialize` (and the merging steps for the
# states) now check the types against the input header. The steps merging the states read the keys and then the
# states by position, so their input must have them in the order of the plan, without duplicates.
# The well-formed plans are accepted, so the refusals are specific.
# The raw protocol client is the one of 05575_filling_step_plan_deserialization.sh.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

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
VERSION = 21       # DBMS_QUERY_PLAN_SERIALIZATION_VERSION, pinned so the byte layout is fixed too
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

def forge(label, payload, children=(), version=VERSION, out_columns=(), step="Aggregating"):
    plan = vu(version) + vu(0) + NUL + vu(len(children))   # version, max_threads, concurrency
    for columns in children:   # one `ReadNothing` child per header, read before its parent
        plan += vu(0) + sb("ReadNothing") + vu(0) + sb("") + header(columns) + vu(0)
    plan += sb(step) + (vu(0) if version >= 19 else b"")   # step name, per-step version
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
        print(label, {117: "INCORRECT_DATA", 48: "NOT_IMPLEMENTED"}.get(code, code))

FINAL, ONLY_MERGE = 1, 128
AF = b"\x25"   # BinaryTypeIndex::AggregateFunction

def state(function, argument_type):   # AggregateFunction(function, argument_type), version 0
    return AF + vu(0) + sb(function) + vu(0) + vu(1) + argument_type

def aggregating(flags, argument_type=None, keys=(), aggregates=None):
    # Flags, keys, then the aggregates: name, arguments with their types, function, parameters.
    # The final `vu(0)` is the number of sets of the plan. By default one aggregate `a` = any(x).
    if aggregates is None:
        aggregates = [("a", "x", argument_type, "any")]
    out = bytes([flags]) + vu(len(keys)) + b"".join(sb(k) for k in keys) + vu(len(aggregates))
    for name, argument, a_type, function, *parameters in aggregates:
        out += sb(name) + vu(1) + sb(argument) + a_type + sb(function) + vu(len(parameters)) + b"".join(parameters)
    return out + vu(0)

X = [("x", U64)]
A = [("a", U64)]
forge("argument type differs from the input", aggregating(FINAL, STR), [X], VERSION, A)
forge("argument is not in the input", aggregating(FINAL, U64), [[("y", U64)]], VERSION, A)
forge("merged column is not a state", aggregating(FINAL | ONLY_MERGE, U64), [A], VERSION, A)
forge("well-formed", aggregating(FINAL, U64), [X], VERSION, A)

# The merge reads the keys and then the states by position, so the input must have them in the order of the plan.
ANY_SUM = [("a", "x", U64, "any"), ("b", "x", U64, "sum")]
STATES = [("a", state("any", U64)), ("b", state("sum", U64))]
AB = [("a", U64), ("b", U64)]
forge("merged states are reordered", aggregating(FINAL | ONLY_MERGE, aggregates=ANY_SUM), [STATES[::-1]], VERSION, AB)
forge("merged state is duplicated", aggregating(FINAL | ONLY_MERGE, aggregates=ANY_SUM[:1] * 2), [STATES[:1] * 2], VERSION, [("a", U64)] * 2)
forge("merged key is after the states", aggregating(FINAL | ONLY_MERGE, keys=["k"], aggregates=ANY_SUM[:1]),
      [STATES[:1] + [("k", U64)]], VERSION, [("k", U64), ("a", U64)])
forge("merged state is repeated after the states", aggregating(FINAL | ONLY_MERGE, aggregates=ANY_SUM),
      [STATES + STATES[:1]], VERSION, AB)
forge("well-formed merge", aggregating(FINAL | ONLY_MERGE, aggregates=ANY_SUM), [STATES], VERSION, AB)

# With grouping sets, the merge splits the rows by `__grouping_set`, read as `UInt64`, and keeps its type in the output.
GROUPING_SETS = 4
def merging_grouping_sets(keys, sets, aggregates):   # flags, keys, the used keys of every set, aggregates
    out = bytes([FINAL | GROUPING_SETS]) + vu(len(keys)) + b"".join(sb(k) for k in keys) + vu(len(sets))
    for used_keys in sets:
        out += vu(len(used_keys)) + b"".join(sb(k) for k in used_keys)
    return out + aggregating(FINAL, aggregates=aggregates)[2:-1] + vu(0) + vu(0)   # sort description, sets of the plan

GS = merging_grouping_sets(["k"], [["k"], []], ANY_SUM[:1])
K_STATE = [("k", U64), STATES[0]]
K_A = [("k", U64), ("a", U64)]
forge("grouping set column is a String", GS, [[("__grouping_set", STR)] + K_STATE], VERSION,
      [("__grouping_set", STR)] + K_A, "MergingAggregated")
forge("grouping set column is duplicated", GS, [[("__grouping_set", U64)] * 2 + K_STATE], VERSION,
      [("__grouping_set", U64)] + K_A, "MergingAggregated")
forge("well-formed grouping sets merge", GS, [[("__grouping_set", U64)] + K_STATE], VERSION,
      [("__grouping_set", U64)] + K_A, "MergingAggregated")

# Without grouping sets, the merge does not produce `__grouping_set`, so the input must not have it.
MERGING = bytes([FINAL]) + vu(1) + sb("k") + aggregating(FINAL, aggregates=ANY_SUM[:1])[2:-1] + vu(0) + vu(0)
forge("grouping set column without grouping sets", MERGING, [K_STATE + [("__grouping_set", U64)]], VERSION,
      [("__grouping_set", U64)] + K_A, "MergingAggregated")
forge("well-formed merge without grouping sets", MERGING, [K_STATE], VERSION, K_A, "MergingAggregated")

# `TotalsHaving` finalizes the states it finds in the input by the names of its aggregates.
def totals_having(aggregates):   # flags (final), then the aggregates as for `Aggregating`, without the keys
    return bytes([FINAL]) + aggregating(FINAL, aggregates=aggregates)[2:]

forge("finalized column is not a state", totals_having(ANY_SUM[:1]), [A], VERSION, A, "TotalsHaving")
forge("finalized state is not in the input", totals_having(ANY_SUM[:1]), [[("b", state("any", U64))]], VERSION, [("b", U64)], "TotalsHaving")
forge("finalized state is of another function", totals_having(ANY_SUM[:1]), [[("a", state("sum", U64))]], VERSION, A, "TotalsHaving")
forge("well-formed totals", totals_having(ANY_SUM), [STATES], VERSION, AB, "TotalsHaving")

# The output of an ordinary aggregation has a column for every key and aggregate, finalized by name.
forge("aggregate is duplicated", aggregating(FINAL, aggregates=[("a", "x", U64, "groupArray")] * 2), [X], VERSION, A * 2)
# The in-order merge addresses the whole input with a mask for the keys and the states only.
forge("merged input has another column", aggregating(FINAL | ONLY_MERGE, aggregates=ANY_SUM[:1]),
      [STATES[:1] + [("extra", U64)]], VERSION, A)

# `Rollup` and `Cube` always finalize, address the whole input with a mask for the keys and the states,
# and finalize a single chunk with the functions of its columns.
F64 = b"\x03"   # Field::Types::Float64
def group_by_modifier(flags, aggregates, keys=("k",)):   # aggregates: name, function, argument type, parameters
    out = bytes([flags]) + vu(len(keys)) + b"".join(sb(k) for k in keys) + vu(len(aggregates))
    for name, function, a_type, parameters in aggregates:
        out += sb(name) + sb(function) + vu(1) + a_type + vu(len(parameters)) + b"".join(parameters)
    return out + vu(0)   # the number of sets of the plan

SUM = group_by_modifier(FINAL, [("a", "sum", U64, [])])
K_SUM = [("k", U64), ("a", state("sum", U64))]
OUT = [("__grouping_set", U64), ("k", U64), ("a", U64)]
QUANTILES = group_by_modifier(FINAL, [("a", "quantiles", U64, [F64 + struct.pack("<d", 0.5)])])
for step in ("Rollup", "Cube"):
    forge(f"{step} input has another column", SUM, [K_SUM + [("extra", U64)]], VERSION, OUT, step)
    forge(f"{step} state is finalized to another type", QUANTILES, [[("k", U64), ("a", state("quantile", U64))]], VERSION,
          [("__grouping_set", U64), ("k", U64), ("a", b"\x1e" + b"\x0e")], step)   # Array(Float64)
    forge(f"{step} without final", group_by_modifier(0, [("a", "sum", U64, [])]), [K_SUM], VERSION,
          [("__grouping_set", U64), ("k", U64), ("a", state("sum", U64))], step)
    forge(f"well-formed {step}", SUM, [K_SUM], VERSION, OUT, step)

# The in-order aggregation takes the first columns of the `GROUP BY` sort description as those the input is sorted by
# (the sort description for merging) and compares the key columns at their positions.
IN_ORDER = 32
def sort_description(columns):   # ascending, nulls last, no collation or fill
    return vu(len(columns)) + b"".join(sb(c) + b"\x03" for c in columns)

def in_order(flags, merging, group_by, keys, aggregates):
    out = aggregating(flags | IN_ORDER, keys=keys, aggregates=aggregates)
    return out[:1] + sort_description(merging) + sort_description(group_by) + out[1:]

KX = [("k", U64), ("x", U64)]
forge("in-order sort description for merging is longer", in_order(FINAL, ["k"], [], ["k"], ANY_SUM[:1]), [KX], VERSION, K_A)
forge("in-order sort description for merging is not a prefix", in_order(FINAL, ["x"], ["k"], ["k"], ANY_SUM[:1]),
      [KX], VERSION, K_A)
forge("in-order sort description refers to an aggregate", in_order(FINAL, ["a"], ["a"], ["k"], ANY_SUM[:1]), [KX], VERSION, K_A)
forge("well-formed in-order", in_order(FINAL, ["k"], ["k"], ["k"], ANY_SUM[:1]), [KX], VERSION, K_A)

# The merge-only in-order aggregation finalizes with the functions of the input states, and the header has the plan's types.
QUANTILES_OF_X = [("a", "x", U64, "quantiles", F64 + struct.pack("<d", 0.5))]
forge("in-order merged state is finalized to another type",
      in_order(FINAL | ONLY_MERGE, ["k"], ["k"], ["k"], QUANTILES_OF_X), [[("k", U64), ("a", state("quantile", U64))]], VERSION,
      [("k", U64), ("a", b"\x1e" + b"\x0e")])   # Array(Float64)
forge("well-formed in-order merge",
      in_order(FINAL | ONLY_MERGE, ["k"], ["k"], ["k"], ANY_SUM[:1]), [K_STATE], VERSION, K_A)
PYEOF

# The server has to be serving after each refusal.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM numbers(3)"
