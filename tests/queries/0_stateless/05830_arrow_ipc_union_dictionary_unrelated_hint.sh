#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the pyarrow Python module, as the ClickHouse writer does not dictionary-encode inside a union.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The dictionary of a field below an Arrow union is decoded before any record batch, for the position of the
# field. The children of a union restart the dotted path, so the struct child `s` of a union child has the path
# `s`, which is also the name of an unrelated column of the header. Its requested type must not reach the
# dictionary: before the fix the dictionary values were decoded as `IPv6` while the union declared them as
# `String`, and the cast of the union read the `IPv6` column as a `String` one.

DATA="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

python3 - "${DATA}" <<'PY'
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

prefix = sys.argv[1]
raw = [bytes.fromhex("20010db8000000000000000000000001"), bytes.fromhex("20010db8000000000000000000000002")]
child = pa.StructArray.from_arrays(
    [pa.DictionaryArray.from_arrays(pa.array([0, 1], type=pa.int32()), pa.array(raw, type=pa.binary()))], names=["s"])
# Rows 0 and 2 select the struct child, row 1 the integer one.
u = pa.UnionArray.from_dense(
    pa.array([0, 1, 0], type=pa.int8()), pa.array([0, 0, 1], type=pa.int32()), [child, pa.array([7], type=pa.uint64())])
batch = pa.record_batch([u, pa.array(raw + raw[:1], type=pa.binary())], names=["u", "s"])
for name, new_writer in (("arrow", ipc.new_file), ("arrows", ipc.new_stream)):
    with new_writer(f"{prefix}.{name}", batch.schema) as writer:
        writer.write_batch(batch)
PY

for FORMAT in Arrow ArrowStream; do
    FILE="${DATA}.arrow"
    [ "${FORMAT}" = "ArrowStream" ] && FILE="${DATA}.arrows"
    echo "-- ${FORMAT}: union String and unrelated column IPv6"
    $CLICKHOUSE_LOCAL -q "SELECT hex(u), s FROM file('${FILE}', ${FORMAT}, 'u String, s IPv6')"
done

rm -f "${DATA}.arrow" "${DATA}.arrows"
