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

# A few physical runs can describe many rows: one run of 1000 rows is far more rows than the body has bytes, also
# inside a struct, a list or a fixed-size list. Rows hidden by a null struct row decode as defaults, even when they share
# a run with visible rows.
# A null run of a list value inside a union stays null. An opaque-tagged value (written by ClickHouse with
# `output_format_arrow_unsupported_types = 'binary'`) is read back through the run-end encoding.
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DATA_FILE}.opaque', 'ArrowStream') SELECT '{\"a\":1}'::JSON AS j
    SETTINGS output_format_arrow_compression_method = 'none', engine_file_truncate_on_insert = 1,
             output_format_arrow_unsupported_types = 'binary'"

python3 - "$DATA_FILE" <<'EOF'
import sys
import pyarrow as pa

base = sys.argv[1]
ree = pa.RunEndEncodedArray.from_arrays


def write(name, columns):
    batch = pa.record_batch(list(columns.values()), names=list(columns))
    with pa.OSFile(f"{base}.{name}", "wb") as sink:
        with pa.ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)


long_run = ree(pa.array([1000], pa.int32()), pa.array([7], pa.int32()))
write("long", {"v": long_run})
long_struct = pa.StructArray.from_arrays([long_run], names=["r"])
write("long_struct", {"t": long_struct})
write("long_struct_null", {"t": pa.StructArray.from_arrays([long_run, pa.nulls(1000)], names=["r", "n"])})
write("long_lists", {
    "l": pa.ListArray.from_arrays(pa.array([0, 1000], pa.int32()), long_struct),
    "f": pa.FixedSizeListArray.from_arrays(long_struct, 1000),
})

# The nulls of a run-end-encoded field are those of its values, even when the field itself is declared non-nullable.
null_run = ree(pa.array([2, 4], pa.int32()), pa.array([None, 7], pa.int32()))
batch = pa.record_batch([null_run], schema=pa.schema([pa.field("v", null_run.type, nullable=False)]))
with pa.OSFile(f"{base}.nonnullable", "wb") as sink:
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)

# Map keys are never nullable, even when they are run-end encoded with (nullable) values.
keys = ree(pa.array([1, 2], pa.int32()), pa.array([1, 2], pa.int32()))
write("map_keys", {"m": pa.MapArray.from_arrays(pa.array([0, 2], pa.int32()), keys, pa.array([10, 20], pa.int32()))})

# Rows hidden by a null struct row decode as defaults, even when they share a run with visible rows.
masked = ree(pa.array([2], pa.int32()), pa.array([7], pa.int32()))
write("masked", {
    "t": pa.StructArray.from_arrays([masked], names=["r"], mask=pa.array([True, False])),
    "p": pa.StructArray.from_arrays([pa.array([7, 7], pa.int32())], names=["r"], mask=pa.array([True, False])),
})

# The same for a struct-valued run: the elements of the hidden default are not null either.
struct_run = ree(pa.array([2], pa.int32()), pa.StructArray.from_arrays([pa.array([7], pa.int32())], names=["a"]))
write("masked_struct", {
    "t": pa.StructArray.from_arrays([struct_run], names=["r"], mask=pa.array([True, False])),
    "p": pa.StructArray.from_arrays(
        [pa.StructArray.from_arrays([pa.array([7, 7], pa.int32())], names=["a"])], names=["r"], mask=pa.array([True, False])),
})

null_list_run = ree(pa.array([2, 4], pa.int32()), pa.array([None, [7]], pa.list_(pa.int32())))
write("union", {"u": pa.UnionArray.from_dense(
    pa.array([0, 0, 0, 0], pa.int8()), pa.array([0, 1, 2, 3], pa.int32()), [null_list_run], ["l"])})

opaque = pa.ipc.open_stream(open(f"{base}.opaque", "rb").read()).read_all()
tag = opaque.schema.field("j").metadata


class Opaque(pa.ExtensionType):
    def __init__(self):
        super().__init__(pa.binary(), tag[b"ARROW:extension:name"].decode())

    def __arrow_ext_serialize__(self):
        return tag[b"ARROW:extension:metadata"]

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return cls()


values = pa.ExtensionArray.from_storage(Opaque(), opaque.column("j").chunk(0))
write("opaque_ree", {"j": ree(pa.array([3], pa.int32()), values)})
EOF

echo "--- one run of 1000 rows ---"
${CLICKHOUSE_LOCAL} --query "SELECT count(), sum(v) FROM file('${DATA_FILE}.long', 'ArrowStream')"
echo "--- one run of 1000 rows in a struct, a list and a fixed-size list ---"
${CLICKHOUSE_LOCAL} --query "SELECT count(), sum(t.r) FROM file('${DATA_FILE}.long_struct', 'ArrowStream')"
${CLICKHOUSE_LOCAL} --query "SELECT length(l), arrayReduce('sum', l.r), length(f), arrayReduce('sum', f.r) FROM file('${DATA_FILE}.long_lists', 'ArrowStream')"
echo "--- one run of 1000 rows in a struct with a null field ---"
${CLICKHOUSE_LOCAL} --query "SELECT count(), sum(t.r), countIf(t.n IS NULL) FROM file('${DATA_FILE}.long_struct_null', 'ArrowStream', 't Tuple(r Int32, n Nullable(Int32))')"
echo "--- a null run of a non-nullable field ---"
${CLICKHOUSE_LOCAL} --query "
    DESC file('${DATA_FILE}.nonnullable', 'ArrowStream') SETTINGS schema_inference_make_columns_nullable = 3;
    SELECT * FROM file('${DATA_FILE}.nonnullable', 'ArrowStream') SETTINGS schema_inference_make_columns_nullable = 3"
echo "--- run-end-encoded map keys ---"
${CLICKHOUSE_LOCAL} --query "DESC file('${DATA_FILE}.map_keys', 'ArrowStream'); SELECT * FROM file('${DATA_FILE}.map_keys', 'ArrowStream')"
echo "--- rows hidden by a null struct row ---"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DATA_FILE}.masked', 'ArrowStream')"
${CLICKHOUSE_LOCAL} --query "
    SELECT * FROM file('${DATA_FILE}.masked', 'ArrowStream', '\`t.r\` Nullable(Int32), \`p.r\` Nullable(Int32)')
    SETTINGS enable_nullable_tuple_type = 0"
echo "--- rows hidden by a null struct row in a struct-valued run ---"
for STRUCTURE in "'\`t.r.a\` Nullable(Int32), \`p.r.a\` Nullable(Int32)'" "'\`t.r.a\` Int32, \`p.r.a\` Int32'"; do
    ${CLICKHOUSE_LOCAL} --query "
        SELECT * FROM file('${DATA_FILE}.masked_struct', 'ArrowStream', ${STRUCTURE})
        SETTINGS enable_nullable_tuple_type = 0, input_format_null_as_default = 0"
done
echo "--- a null list run inside a union ---"
${CLICKHOUSE_LOCAL} --max_threads 1 --query "SELECT u FROM file('${DATA_FILE}.union', 'ArrowStream')"
echo "--- an opaque-tagged value ---"
${CLICKHOUSE_LOCAL} --query "SELECT j FROM file('${DATA_FILE}.opaque_ree', 'ArrowStream', 'j JSON')"
