#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs pyarrow

# Arrow fields declared non-nullable that still contain nulls are read as nulls: with
# `input_format_null_as_default = 1` they become defaults (including column `DEFAULT` expressions) in non-nullable
# columns; with 0 they are rejected for non-nullable columns and kept as NULL for `Nullable` ones.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

python3 - "$TMP_DIR" <<'PYEOF'
import struct
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

out = sys.argv[1]
columns = {
    "i": pa.array([1, None, 3], pa.int64()),
    "s": pa.array(["a", None, "c"]),
    "d": pa.array(["x", None, "x"]).dictionary_encode(),
    "arr": pa.array([[1, None], [], None], pa.list_(pa.field("item", pa.int64(), nullable=False))),
    "tup": pa.array([{"x": 1}, {"x": None}, None], pa.struct([pa.field("x", pa.int64(), nullable=False)])),
}
schema = pa.schema([pa.field(name, arr.type, nullable=False) for name, arr in columns.items()])
table = pa.Table.from_arrays(list(columns.values()), schema=schema)
with ipc.new_file(f"{out}/data.arrow", schema) as w:
    w.write_table(table)
with ipc.new_stream(f"{out}/data.arrows", schema) as w:
    w.write_table(table)

item = pa.field("item", pa.int64(), nullable=False)
list_type = pa.list_(item)
lists = pa.array([[None], [7]], list_type)

# The nulls first appear in the second batch.
schema = pa.schema([pa.field("l", list_type, nullable=False)])
with ipc.new_stream(f"{out}/batches.arrows", schema) as w:
    w.write_batch(pa.record_batch([pa.array([[1, 2]], list_type)], schema=schema))
    w.write_batch(pa.record_batch([lists], schema=schema))

union_type = pa.sparse_union([pa.field("l", list_type, nullable=False)])
union = pa.Array.from_buffers(union_type, 2, [None, pa.array([0, 0], pa.int8()).buffers()[1]], children=[lists])
dict_of_lists = pa.DictionaryArray.from_arrays(pa.array([1, 0], pa.int32()), lists)
for name, arr in (("union", union), ("dictionary", dict_of_lists)):
    table = pa.Table.from_arrays([arr], names=["c"])
    with ipc.new_stream(f"{out}/{name}.arrows", table.schema) as w:
        w.write_table(table)

# pyarrow refuses nulls in map entries and keys, so write a list of structs and retag the field as a map.
def table_pos(b, table, slot):
    vtable = table - struct.unpack_from("<i", b, table)[0]
    return table + struct.unpack_from("<H", b, vtable + 4 + 2 * slot)[0]
def deref(b, pos):
    return pos + struct.unpack_from("<I", b, pos)[0]
entries_type = pa.struct([pa.field("key", pa.string(), nullable=False), pa.field("value", pa.int64())])
list_of_entries = pa.list_(pa.field("entries", entries_type, nullable=False))
for name, keys, mask in (("map_entries", ["a", "b"], [False, True]), ("map_keys", ["a", None], None)):
    entries = pa.StructArray.from_arrays([pa.array(keys), pa.array([1, 2])], fields=list(entries_type), mask=mask and pa.array(mask))
    schema = pa.schema([pa.field("c", list_of_entries)])
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, schema) as w:
        w.write_batch(pa.record_batch([pa.ListArray.from_arrays(pa.array([0, 2], pa.int32()), entries, type=list_of_entries)], schema=schema))
    data = bytearray(sink.getvalue().to_pybytes())
    message = deref(data, 8)
    fields = deref(data, table_pos(data, deref(data, table_pos(data, message, 2)), 1))
    type_type = table_pos(data, deref(data, fields + 4), 2)
    assert data[type_type] == 12  # List
    data[type_type] = 17  # Map
    open(f"{out}/{name}.arrows", "wb").write(data)
PYEOF

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_arrow_null_as_default
    (
        i Int64 DEFAULT 42,
        s String DEFAULT 'default',
        d LowCardinality(String) DEFAULT 'none',
        arr Array(Int64),
        tup Tuple(x Int64)
    ) ENGINE = Memory"

for format in Arrow ArrowStream; do
    file="$TMP_DIR/data.arrow"
    [ "$format" = ArrowStream ] && file="$TMP_DIR/data.arrows"

    echo "$format"
    $CLICKHOUSE_CLIENT -q "INSERT INTO t_arrow_null_as_default SETTINGS input_format_null_as_default = 1 FORMAT $format" < "$file"
    $CLICKHOUSE_CLIENT -q "SELECT * FROM t_arrow_null_as_default ORDER BY i"
    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t_arrow_null_as_default"

    $CLICKHOUSE_CLIENT -q "INSERT INTO t_arrow_null_as_default SETTINGS input_format_null_as_default = 0 FORMAT $format" < "$file" 2>&1 \
        | grep -o -m1 "CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN"
    $CLICKHOUSE_LOCAL -q "SELECT i, s, d, toTypeName(d) FROM file('$file', $format, 'i Nullable(Int64), s Nullable(String), d Nullable(String)') ORDER BY i SETTINGS input_format_null_as_default = 0"
    $CLICKHOUSE_LOCAL -q "SELECT groupArray(tuple(d)) FROM file('$file', $format, 'd LowCardinality(Nullable(String))') SETTINGS input_format_null_as_default = 0"
    $CLICKHOUSE_LOCAL -q "SELECT d FROM file('$file', $format, 'd String') SETTINGS input_format_null_as_default = 0" 2>&1 \
        | grep -o -m1 "CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN"
done

echo "Batches"
$CLICKHOUSE_LOCAL -q "SELECT l FROM file('$TMP_DIR/batches.arrows', ArrowStream) ORDER BY l"
$CLICKHOUSE_LOCAL -q "SELECT l FROM file('$TMP_DIR/batches.arrows', ArrowStream, 'l Array(Nullable(Int64))') ORDER BY l SETTINGS input_format_null_as_default = 0"
$CLICKHOUSE_LOCAL -q "SELECT l FROM file('$TMP_DIR/batches.arrows', ArrowStream, 'l Array(Int64)') SETTINGS input_format_null_as_default = 0" 2>&1 \
    | grep -o -m1 "CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN"

# Nulls in non-nullable children of unions and of dictionary values, and in map entries and keys, are rejected.
echo "Rejected"
for name in union dictionary map_entries map_keys; do
    $CLICKHOUSE_LOCAL -q "SELECT c FROM file('$TMP_DIR/$name.arrows', ArrowStream)" 2>&1 | grep -o -m1 "declared non-nullable but its FieldNode reports [0-9]* nulls"
done

$CLICKHOUSE_CLIENT -q "DROP TABLE t_arrow_null_as_default"
