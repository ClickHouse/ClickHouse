#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Arrow format is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query="SELECT number, multiIf(number = 0, 42::Variant(UInt8, String, Array(UInt8)), number = 1, 'a\"b', number = 2, [1, 2], NULL) AS v, [v, v] AS arr, tuple(v) AS t FROM numbers(4)"

for format in Arrow ArrowStream; do
    echo "${format}"
    $CLICKHOUSE_LOCAL -q "${query} FORMAT ${format} SETTINGS output_format_arrow_variant_as_json_string = 1" \
        | $CLICKHOUSE_LOCAL --input-format ${format} -q "DESCRIBE TABLE table SETTINGS describe_compact_output = 1; SELECT v, arr, t FROM table ORDER BY number"
done

echo "default"
$CLICKHOUSE_LOCAL -q "${query} FORMAT Arrow" | $CLICKHOUSE_LOCAL --input-format Arrow -q "SELECT v, toTypeName(v) FROM table ORDER BY number"

# An Arrow map key must not be nullable: a NULL Variant key is written as the JSON text `null`.
echo "map key"
$CLICKHOUSE_LOCAL -q "SELECT map(42::UInt64::Variant(UInt64, String), 'x', NULL::Variant(UInt64, String), 'y') AS m FORMAT ArrowStream SETTINGS output_format_arrow_variant_as_json_string = 1" \
    | $CLICKHOUSE_LOCAL --input-format ArrowStream -q "DESCRIBE TABLE table SETTINGS describe_compact_output = 1; SELECT m FROM table"

# Record batches are sized by the JSON text, so a small limit splits the block, and every batch stays within it.
# Compression is off because older pyarrow cannot read the "uncompressed" sentinel.
echo "batch size"
FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.arrows"
trap 'rm -f "$FILE"' EXIT
$CLICKHOUSE_LOCAL -q "
    SELECT number, number::UInt8::Variant(UInt8, String) AS v FROM numbers(64)
    SETTINGS max_block_size = 64, output_format_arrow_variant_as_json_string = 1,
        output_format_arrow_max_record_batch_size_bytes = 16, output_format_arrow_compression_method = 'none'
    FORMAT ArrowStream" > "$FILE"
python3 - "$FILE" <<'PY'
import sys
import pyarrow as pa
import pyarrow.ipc as ipc

with pa.OSFile(sys.argv[1], "rb") as src:
    batches = list(ipc.open_stream(src))
sizes = [sum(len(x.encode()) for x in b.column("v").to_pylist() if x is not None) for b in batches]
print(f"batches>1={int(len(batches) > 1)} max_bytes<=16={int(max(sizes) <= 16)}")
PY
$CLICKHOUSE_LOCAL -q "SELECT count(), sum(number), max(toUInt64(v)) FROM file('$FILE', ArrowStream)"
