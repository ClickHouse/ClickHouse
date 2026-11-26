#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -euo pipefail

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

$CLICKHOUSE_LOCAL -q "SELECT 5::Dynamic AS v FORMAT Parquet" > "$WORK_DIR/annotated.parquet"
$CLICKHOUSE_LOCAL -q "
    SELECT (unhex('010000'), unhex('00'))::Tuple(metadata String, value String) AS v
    SETTINGS output_format_parquet_string_as_string = 0 FORMAT Parquet" > "$WORK_DIR/structural.parquet"

# Change only a leaf's annotation in the footer, leaving its physical type and pages intact.
# The writer emits these blob leaves as required `BYTE_ARRAY` with no annotations.
python3 - "$WORK_DIR" <<'PY'
import pathlib
import struct
import sys

work = pathlib.Path(sys.argv[1])
annotations = {
    # `SchemaElement` fields 6/7/8: `converted_type` DECIMAL, scale 0, precision 9.
    'converted_decimal': bytes.fromhex('25 0a 15 00 15 12'),
    # `SchemaElement` field 10: `logicalType.DECIMAL`, scale 0, precision 9; no `converted_type`.
    'logical_decimal': bytes.fromhex('6c 5c 15 00 15 12 00 00'),
    # `SchemaElement` field 10: `logicalType.JSON`, no `converted_type`.
    'logical_json': bytes.fromhex('6c cc 00 00'),
}
for base in ('annotated', 'structural'):
    data = (work / (base + '.parquet')).read_bytes()
    footer_size = struct.unpack('<I', data[-8:-4])[0]
    footer_start = len(data) - 8 - footer_size
    footer = data[footer_start:-8]
    for leaf in (b'metadata', b'value'):
        # Fields 1/3/4: type BYTE_ARRAY, repetition REQUIRED, name, followed by STOP.
        original = bytes.fromhex('15 0c 25 00 18') + bytes([len(leaf)]) + leaf + b'\0'
        assert footer.count(original) == 1, (base, leaf)
        for name, annotation in annotations.items():
            # These binary blobs are not JSON documents; exercise JSON only in explicit variants.
            if base == 'structural' and name == 'logical_json':
                continue
            patched = footer.replace(original, original[:-1] + annotation + b'\0')
            path = work / (base + '_' + leaf.decode() + '_' + name + '.parquet')
            path.write_bytes(data[:footer_start] + patched + struct.pack('<I', len(patched)) + b'PAR1')
PY

$CLICKHOUSE_LOCAL -q "SELECT v FROM file('$WORK_DIR/annotated.parquet', Parquet)"
$CLICKHOUSE_LOCAL -q "SELECT isNull(v) FROM file('$WORK_DIR/structural.parquet', Parquet)"

expect_error() {
    local expected=$1 query=$2 output
    if output=$($CLICKHOUSE_LOCAL -q "$query" 2>&1); then
        echo "Unexpected success: $query"
        return 1
    fi
    if [[ "$output" != *"($expected)"* ]]; then
        echo "$output"
        return 1
    fi
    echo "$expected"
}

for leaf in metadata value; do
    for annotation in converted_decimal logical_decimal logical_json; do
        path="$WORK_DIR/annotated_${leaf}_${annotation}.parquet"
        # An explicit `VARIANT` must be rejected, not silently inferred as a `Tuple`.
        expect_error INCORRECT_DATA "DESCRIBE file('$path', Parquet)"
        expect_error INCORRECT_DATA "SELECT v FROM file('$path', Parquet, 'v Dynamic')"
    done
    for annotation in converted_decimal logical_decimal; do
        path="$WORK_DIR/structural_${leaf}_${annotation}.parquet"
        # A typed leaf makes this an ordinary `Tuple`, not a structure-detected variant.
        $CLICKHOUSE_LOCAL -q "SELECT startsWith(toTypeName(v), 'Tuple(') FROM file('$path', Parquet)"
        expect_error TYPE_MISMATCH "SELECT v FROM file('$path', Parquet, 'v Dynamic')"
    done
done
