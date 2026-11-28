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

# A list whose child is `null` (or a fixed-size list of empty structs) references child rows that no buffer backs,
# so a forged child length together with a forged offset or size can ask for a huge number of rows. Such a file is
# bad data, not an internal error.

python3 - "$TMP_DIR" <<'PYEOF'
import struct
import sys

import pyarrow as pa
import pyarrow.ipc as ipc

directory = sys.argv[1]
n = 1 << 30


def write(name, array, forged):
    table = pa.table({"a": array})
    path = f"{directory}/{name}.arrow"
    with ipc.new_file(path, table.schema) as writer:
        writer.write_table(table)
    data = bytearray(open(path, "rb").read())
    positions = [i for i in range(0, len(data) - 7, 8) if struct.unpack_from("<q", data, i)[0] == n]
    assert len(positions) == 3, positions
    for value, suffix in forged:
        patched = bytearray(data)
        for i in positions:
            struct.pack_into("<q", patched, i, value)
        open(f"{directory}/{name}_{suffix}.arrow", "wb").write(patched)


forged = [((1 << 63) - 1, "int64_max"), (1 << 62, "two_to_62")]
write("large_list", pa.LargeListArray.from_arrays(pa.array([0, n], pa.int64()), pa.nulls(n)), forged)
write(
    "large_list_view",
    pa.LargeListViewArray.from_arrays(pa.array([0], pa.int64()), pa.array([n], pa.int64()), pa.nulls(n)),
    forged)

# Empty structs allocate nothing, so only the offsets of the fixed-size list, 8 bytes per row, are left to allocate.
fixed_size_list_type = pa.list_(pa.field("item", pa.struct([]), nullable=False), 1)
fixed_size_list = pa.FixedSizeListArray.from_arrays(
    pa.StructArray.from_buffers(pa.struct([]), n, [None]), type=fixed_size_list_type)
write(
    "large_list_fixed_size_list",
    pa.LargeListArray.from_arrays(
        pa.array([0, n], pa.int64()), fixed_size_list, type=pa.large_list(pa.field("item", fixed_size_list_type, nullable=False))),
    [(1 << 60, "two_to_60")])

table = pa.table({"a": pa.LargeListArray.from_arrays(pa.array([0, 3, 7], pa.int64()), pa.nulls(7))})
with ipc.new_file(f"{directory}/small.arrow", table.schema) as writer:
    writer.write_table(table)
PYEOF

for name in large_list_int64_max large_list_two_to_62 large_list_view_int64_max large_list_view_two_to_62 \
    large_list_fixed_size_list_two_to_60
do
    echo "$name"
    $CLICKHOUSE_LOCAL --query "SELECT length(a) FROM file('$TMP_DIR/$name.arrow', Arrow)" 2>&1 \
        | grep -o -m1 'INCORRECT_DATA\|LOGICAL_ERROR\|MEMORY_LIMIT_EXCEEDED\|CANNOT_ALLOCATE_MEMORY'
done

$CLICKHOUSE_LOCAL --query "SELECT length(a) FROM file('$TMP_DIR/small.arrow', Arrow, 'a Array(Nullable(Nothing))')"
