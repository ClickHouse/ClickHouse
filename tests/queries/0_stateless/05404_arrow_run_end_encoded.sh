#!/usr/bin/env bash
# Tags: no-fasttest
# Arrow `RunEndEncoded` columns are read as their values type, with each run's value repeated for the rows of the
# run. Covers Int16, Int32 and Int64 run ends, a null value, dictionary-encoded values (materialized) and a
# run-end-encoded field of a struct.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -f "${DATA_FILE}".*' EXIT

python3 - "$DATA_FILE" <<'EOF'
import sys
import pyarrow as pa

base = sys.argv[1]
ree = pa.RunEndEncodedArray.from_arrays
columns = {
    "id": pa.array(range(1, 7), pa.int32()),
    # Int32 run ends; the second run is a null value.
    "i": ree(pa.array([2, 3, 6], pa.int32()), pa.array([10, None, 30], pa.int64())),
    # Int16 run ends with a single row in the first run.
    "s": ree(pa.array([1, 4, 6], pa.int16()), pa.array(["a", "bb", "c"])),
    # Int64 run ends over dictionary-encoded values.
    "d": ree(pa.array([3, 6], pa.int64()), pa.array(["x", "y"]).dictionary_encode()),
    # A struct with a run-end-encoded field.
    "t": pa.StructArray.from_arrays(
        [ree(pa.array([4, 6], pa.int32()), pa.array([1.5, 2.5])), pa.array(range(6), pa.int8())], names=["r", "n"]),
}
batch = pa.record_batch(list(columns.values()), names=list(columns))
batch.validate(full=True)
for fmt, opener in [("Arrow", pa.ipc.new_file), ("ArrowStream", pa.ipc.new_stream)]:
    with pa.OSFile(f"{base}.{fmt}", "wb") as sink:
        with opener(sink, batch.schema) as writer:
            writer.write_batch(batch)
EOF

for FMT in Arrow ArrowStream; do
    echo "--- ${FMT} ---"
    ${CLICKHOUSE_LOCAL} --query "
        DESC file('${DATA_FILE}.${FMT}', '${FMT}');
        SELECT * FROM file('${DATA_FILE}.${FMT}', '${FMT}') ORDER BY id;
        SELECT id, i, t.r FROM file('${DATA_FILE}.${FMT}', '${FMT}', 'id Int32, i Nullable(Int64), t Tuple(r Float64)') ORDER BY id;"
done
