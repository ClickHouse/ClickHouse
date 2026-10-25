#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: pyarrow is not available in fasttest builds
# Schema inference reads an Arrow column of the `null` type as `Nullable(Nothing)`: alone, next to other columns,
# inside a list or a union, and in every `schema_inference_make_columns_nullable` mode. When
# `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference` skips every column, the error says so.
# `data_arrow/unknown_type.arrows` is a schema-only stream whose single field `i` has a type id (99) that no Arrow
# version defines (a `large_utf8` field with the `Field.type_type` byte patched), because the reader maps every
# type that `pyarrow` can write.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -f "${DATA_FILE}".*' EXIT

python3 - "$DATA_FILE" <<'PY'
import sys
import pyarrow as pa

base = sys.argv[1]
batches = {
    "only_null": pa.record_batch([pa.nulls(2), pa.nulls(2)], names=["a", "b"]),
    "mixed": pa.record_batch(
        [pa.nulls(2), pa.array([1, 2], pa.int32()), pa.array([[None, None], []], pa.list_(pa.null()))],
        names=["x", "n", "l"]),
    "union": pa.record_batch(
        [pa.UnionArray.from_sparse(
            pa.array([0, 1], pa.int8()),
            [pa.array([[None], []], pa.list_(pa.null())), pa.array([1, 2], pa.int32())])],
        names=["u"]),
}
for name, batch in batches.items():
    with pa.OSFile(f"{base}.{name}.arrows", "wb") as sink:
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
PY

echo "--- only null columns"
${CLICKHOUSE_LOCAL} --query "DESC file('${DATA_FILE}.only_null.arrows', ArrowStream); SELECT * FROM file('${DATA_FILE}.only_null.arrows', ArrowStream)"

for SETTING in "" "SETTINGS schema_inference_make_columns_nullable = 0" "SETTINGS schema_inference_make_columns_nullable = 1"
do
    echo "--- ${SETTING:-default settings}"
    ${CLICKHOUSE_LOCAL} --query "
        DESC file('${DATA_FILE}.mixed.arrows', ArrowStream) ${SETTING};
        SELECT * FROM file('${DATA_FILE}.mixed.arrows', ArrowStream) ${SETTING};"
done

echo "--- inside a union with schema_inference_make_columns_nullable = 0"
${CLICKHOUSE_LOCAL} --query "
    DESC file('${DATA_FILE}.union.arrows', ArrowStream) SETTINGS schema_inference_make_columns_nullable = 0;
    SELECT u, variantType(u) FROM file('${DATA_FILE}.union.arrows', ArrowStream) SETTINGS schema_inference_make_columns_nullable = 0;"

echo "--- every column skipped"
${CLICKHOUSE_LOCAL} --query "
    DESC file('${CUR_DIR}/data_arrow/unknown_type.arrows', ArrowStream)
    SETTINGS input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference = 1" 2>&1 \
    | grep -o "skipped all of them" | head -1
