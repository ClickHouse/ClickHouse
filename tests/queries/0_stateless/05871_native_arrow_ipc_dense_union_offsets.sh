#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the pyarrow Python module to build the Arrow files.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT

# The offsets of a dense union do not have to refer to the values of a child one by one: several rows can refer
# to the same value, and a child can have values that no row refers to (as the child of a sliced union has). A
# `Variant` column needs each of its variants to have exactly the values of its rows, in the order of the rows,
# so the reader has to gather the values in these cases, also when the children have as many values in total as
# the union has rows. Reading only a part of the rows shows the difference.

python3 - "$TMP_DIR" <<'PYEOF'
import sys

import pyarrow as pa
import pyarrow.ipc as ipc

directory = sys.argv[1]


def write(name, union_type, type_ids, offsets, children, valid=True):
    array = pa.Array.from_buffers(
        union_type,
        len(type_ids),
        [None, pa.array(type_ids, pa.int8()).buffers()[1], pa.array(offsets, pa.int32()).buffers()[1]],
        children=children,
    )
    if valid:
        array.validate(full=True)
    table = pa.table({"v": array})
    with ipc.new_file(f"{directory}/{name}.arrow", table.schema) as writer:
        writer.write_table(table)


int_child = pa.field("i", pa.int32(), nullable=False)
string_child = pa.field("s", pa.string(), nullable=False)

# Both rows refer to the second value of the child, and no row refers to the first one.
write("repeated_offset", pa.dense_union([int_child]), [0, 0], [1, 1], [pa.array([11, 22], pa.int32())])

# The same with two children: a value of each is referred to twice, and the other one is not referred to.
write(
    "repeated_offsets_of_two_children",
    pa.dense_union([int_child, string_child]),
    [0, 1, 0, 1],
    [1, 0, 1, 0],
    [pa.array([11, 22], pa.int32()), pa.array(["a", "b"], pa.string())],
)

# A child with a value that no row refers to, and more values of the children than rows.
write(
    "unreferenced_value",
    pa.dense_union([int_child, string_child]),
    [0, 1, 0],
    [1, 0, 2],
    [pa.array([11, 22, 33], pa.int32()), pa.array(["a"], pa.string())],
)

# The offsets of a child must not decrease in Arrow, but a file can have anything.
write("decreasing_offsets", pa.dense_union([int_child]), [0, 0, 0], [2, 1, 0], [pa.array([11, 22, 33], pa.int32())], valid=False)
PYEOF

for name in repeated_offset repeated_offsets_of_two_children unreferenced_value decreasing_offsets
do
    echo "$name"
    file="$TMP_DIR/$name.arrow"
    $CLICKHOUSE_LOCAL --query "SELECT toTypeName(v), groupArray(v) FROM file('$file', Arrow)"
    $CLICKHOUSE_LOCAL --query "SELECT v FROM file('$file', Arrow) LIMIT 1"
    $CLICKHOUSE_LOCAL --query "SELECT v FROM file('$file', Arrow) LIMIT 1 OFFSET 1"
    $CLICKHOUSE_LOCAL --query "SELECT v FROM file('$file', Arrow) ORDER BY rowNumberInAllBlocks() DESC LIMIT 1"
done
