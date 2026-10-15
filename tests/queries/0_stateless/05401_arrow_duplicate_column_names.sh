#!/usr/bin/env bash
# Tags: no-fasttest
# Arrow allows several top-level fields with the same name. A repeated name gets the first free numeric suffix,
# skipping the names of other fields: here the second `ints` becomes `ints_2` because `ints_1` exists, and the
# second dictionary-encoded `d` becomes `d_1` (with its own dictionary).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -f "${DATA_FILE}.Arrow" "${DATA_FILE}.ArrowStream"' EXIT

python3 - "$DATA_FILE" <<'EOF'
import sys
import pyarrow as pa

base = sys.argv[1]
columns = [
    pa.array([1, 2], type=pa.int32()),
    pa.array([3, 4], type=pa.int64()),
    pa.array(["a", "b"]),
    pa.array(["p", "q"]).dictionary_encode(),
    pa.array(["r", "s"]).dictionary_encode(),
]
batch = pa.record_batch(columns, names=["ints", "ints", "ints_1", "d", "d"])
for fmt, opener in [("Arrow", pa.ipc.new_file), ("ArrowStream", pa.ipc.new_stream)]:
    with pa.OSFile(f"{base}.{fmt}", "wb") as sink:
        with opener(sink, batch.schema) as writer:
            writer.write_batch(batch)
EOF

for FMT in Arrow ArrowStream; do
    echo "--- ${FMT} ---"
    ${CLICKHOUSE_LOCAL} --query "
        DESC file('${DATA_FILE}.${FMT}', '${FMT}');
        SELECT * FROM file('${DATA_FILE}.${FMT}', '${FMT}') ORDER BY ints;
        SELECT ints_2, d_1 FROM file('${DATA_FILE}.${FMT}', '${FMT}', 'ints_2 Int64, d_1 String') ORDER BY ints_2;"
done
