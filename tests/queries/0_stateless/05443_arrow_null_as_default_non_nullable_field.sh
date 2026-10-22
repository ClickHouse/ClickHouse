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

# A union alternative and dictionary values with non-nullable children that contain nulls.
item = pa.field("item", pa.int64(), nullable=False)
lists = pa.array([[None], [7]], pa.list_(item))
union_type = pa.sparse_union([pa.field("l", lists.type, nullable=False)])
union = pa.Array.from_buffers(union_type, 2, [None, pa.array([0, 0], pa.int8()).buffers()[1]], children=[lists])
dict_of_lists = pa.DictionaryArray.from_arrays(pa.array([1, 0], pa.int32()), lists)
nested = pa.Table.from_arrays([union, dict_of_lists], names=["u", "dl"])
with ipc.new_stream(f"{out}/nested.arrows", nested.schema) as w:
    w.write_table(nested)
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

echo "Nested"
$CLICKHOUSE_LOCAL -q "SELECT u, dl FROM file('$TMP_DIR/nested.arrows', ArrowStream, 'u Variant(Array(Nullable(Int64))), dl Array(Nullable(Int64))')"
$CLICKHOUSE_LOCAL -q "SELECT dl FROM file('$TMP_DIR/nested.arrows', ArrowStream, 'dl Array(Int64)')"
$CLICKHOUSE_LOCAL -q "SELECT u FROM file('$TMP_DIR/nested.arrows', ArrowStream, 'u Variant(Array(Int64))')" 2>&1 \
    | grep -o -m1 "CANNOT_CONVERT_TYPE"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_arrow_null_as_default"
