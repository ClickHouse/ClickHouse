#!/usr/bin/env bash
# Tags: no-fasttest
# `ListView` and `LargeListView` columns are read as `Array`. Their rows reference ranges of the child by an offset
# and a size, which can come in any order and overlap; a null row is an empty array. Also covers a list view nested
# in a list view, a dictionary-encoded child, and a child of the `null` type.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -f "${DATA_FILE}".*' EXIT

python3 - "$DATA_FILE" <<'EOF'
import sys
import pyarrow as pa

base = sys.argv[1]


def view(offsets, sizes, values, mask=None, large=False):
    cls = pa.LargeListViewArray if large else pa.ListViewArray
    index_type = pa.int64() if large else pa.int32()
    return cls.from_arrays(pa.array(offsets, index_type), pa.array(sizes, index_type), values,
                           mask=None if mask is None else pa.array(mask))


columns = {
    "id": pa.array([1, 2, 3, 4, 5], pa.int32()),
    # Out of order and overlapping ranges; row 4 is null (an empty array) although its range is not empty.
    "lv": view([2, 0, 1, 1, 3], [2, 3, 0, 2, 1], pa.array([10, 20, 30, 40], pa.int32()),
               mask=[False, False, False, True, False]),
    "llv": view([1, 1, 0, 2, 0], [2, 1, 3, 0, 1], pa.array(["a", "b", "c"]), large=True),
    "nested": view([0, 1, 0, 2, 1], [1, 1, 2, 0, 2], view([1, 0, 2], [2, 1, 0], pa.array([7, 8, 9], pa.int8()))),
    "dict": view([1, 0, 2, 0, 1], [1, 2, 1, 3, 0], pa.array(["x", "y", "x"]).dictionary_encode()),
}
nulls = {"id": columns["id"], "nulls": view([0, 1, 0, 0, 2], [2, 1, 0, 3, 1], pa.nulls(3))}
for name, data in (("main", columns), ("nulls", nulls)):
    batch = pa.record_batch(list(data.values()), names=list(data))
    batch.validate(full=True)
    for fmt, opener in [("Arrow", pa.ipc.new_file), ("ArrowStream", pa.ipc.new_stream)]:
        with pa.OSFile(f"{base}.{name}.{fmt}", "wb") as sink:
            with opener(sink, batch.schema) as writer:
                writer.write_batch(batch)
EOF

for FMT in Arrow ArrowStream; do
    echo "--- ${FMT} ---"
    ${CLICKHOUSE_LOCAL} --query "
        DESC file('${DATA_FILE}.main.${FMT}', '${FMT}');
        SELECT * FROM file('${DATA_FILE}.main.${FMT}', '${FMT}') ORDER BY id;
        SELECT id, llv FROM file('${DATA_FILE}.main.${FMT}', '${FMT}', 'id Int32, llv Array(String)') ORDER BY id;
        SELECT * FROM file('${DATA_FILE}.nulls.${FMT}', '${FMT}', 'id Int32, nulls Array(Nullable(String))') ORDER BY id;"
done

# Paths the cases above don't reach. "mask" and "mask_unordered": dictionary index 99 is invalid, and only the null
# row's range points at it, so the read succeeds only if child rows used by no visible row are skipped. The ranges are
# contiguous in "mask" and out of order in "mask_unordered", where the unused rows are found by counting. "cut":
# contiguous ranges that don't start at child position 0. "oor": a range past the end of the child. The "mask",
# "mask_unordered" and "oor" files are invalid on purpose and are not validated by pyarrow.
python3 - "$DATA_FILE" <<'EOF'
import sys
import pyarrow as pa

base = sys.argv[1]
offsets_sizes = lambda o, s: (pa.array(o, pa.int32()), pa.array(s, pa.int32()))
cases = {
    "mask": pa.ListViewArray.from_arrays(
        *offsets_sizes([0, 1, 2], [1, 1, 2]),
        pa.DictionaryArray.from_arrays(pa.array([0, 1, 99, 99], pa.int32()), pa.array(["x", "y"]), safe=False),
        mask=pa.array([False, False, True])),
    "mask_unordered": pa.ListViewArray.from_arrays(
        *offsets_sizes([2, 0, 1], [1, 1, 1]),
        pa.DictionaryArray.from_arrays(pa.array([0, 99, 1], pa.int32()), pa.array(["x", "y"]), safe=False),
        mask=pa.array([False, False, True])),
    "cut": pa.ListViewArray.from_arrays(*offsets_sizes([3, 5, 6], [2, 1, 1]), pa.array([100, 101, 102, 3, 4, 5, 6], pa.int32())),
    "oor": pa.ListViewArray.from_arrays(*offsets_sizes([0, 3], [1, 2]), pa.array([1, 2, 3, 4], pa.int32())),
}
for name, values in cases.items():
    batch = pa.record_batch([values], names=["v"])
    with pa.OSFile(f"{base}.{name}.ArrowStream", "wb") as sink:
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
EOF

echo "--- unused child rows are not validated ---"
${CLICKHOUSE_LOCAL} --query "SELECT v FROM file('${DATA_FILE}.mask.ArrowStream', 'ArrowStream')"
echo "--- unused child rows of out-of-order ranges are not validated ---"
${CLICKHOUSE_LOCAL} --query "SELECT v FROM file('${DATA_FILE}.mask_unordered.ArrowStream', 'ArrowStream')"
echo "--- contiguous ranges after the start of the child ---"
${CLICKHOUSE_LOCAL} --query "SELECT v FROM file('${DATA_FILE}.cut.ArrowStream', 'ArrowStream')"
echo "--- a range past the end of the child ---"
${CLICKHOUSE_LOCAL} --query "SELECT v FROM file('${DATA_FILE}.oor.ArrowStream', 'ArrowStream')" 2>&1 | grep -o "INCORRECT_DATA" | head -1
