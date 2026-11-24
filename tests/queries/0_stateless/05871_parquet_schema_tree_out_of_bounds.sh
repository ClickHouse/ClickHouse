#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -euo pipefail

# The `num_children` fields of a Parquet schema come from the file. A schema tree that needs more
# elements than the file has must be rejected, not read past the end of the schema.

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

$CLICKHOUSE_LOCAL -q "SELECT 5::Dynamic AS v FORMAT Parquet" > "$WORK_DIR/variant.parquet"
$CLICKHOUSE_LOCAL -q "
    SELECT ('x', 'y')::Tuple(a String, b String) AS v
    SETTINGS output_format_parquet_string_as_string = 0 FORMAT Parquet" > "$WORK_DIR/tuple.parquet"

# Change only the schema in the footer. `SchemaElement` fields: 1 = type, 3 = repetition_type,
# 4 = name, 5 = num_children. A field header is (field id delta << 4) | type, with I32 = 5 and
# BINARY = 8; an I32 value is a zigzag varint.
python3 - "$WORK_DIR" <<'PY'
import pathlib
import struct
import sys

work = pathlib.Path(sys.argv[1])

def write_patched(base, name, original, replacement):
    data = (work / (base + '.parquet')).read_bytes()
    footer_size = struct.unpack('<I', data[-8:-4])[0]
    footer_start = len(data) - 8 - footer_size
    footer = data[footer_start:-8]
    assert footer.count(original) == 1, (base, name)
    patched = footer.replace(original, replacement)
    path = work / (base + '_' + name + '.parquet')
    path.write_bytes(data[:footer_start] + patched + struct.pack('<I', len(patched)) + b'PAR1')

def leaf(name):
    # A required `BYTE_ARRAY` leaf.
    return bytes.fromhex('15 0c 25 00 18') + bytes([len(name)]) + name + b'\0'

def group_with_one_child(name):
    return bytes.fromhex('35 00 18') + bytes([len(name)]) + name + bytes.fromhex('15 02 00')

# The first child becomes a group whose only child is the second child, so `v` has no element left
# for its second child.
write_patched('variant', 'nested_first_child', leaf(b'metadata'), group_with_one_child(b'metadata'))
write_patched('tuple', 'nested_first_child', leaf(b'a'), group_with_one_child(b'a'))

# `v` declares 3, 2^31 - 1 and -1 children, but only 2 elements follow it.
v_header = bytes.fromhex('18 01') + b'v'
for name, num_children in (('extra_child', '06'), ('huge_num_children', 'fe ff ff ff 0f'), ('negative_num_children', '01')):
    write_patched('variant', name, v_header + bytes.fromhex('15 04'), v_header + bytes.fromhex('15 ' + num_children))
PY

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

for file in variant_nested_first_child tuple_nested_first_child variant_extra_child variant_huge_num_children variant_negative_num_children; do
    echo "$file"
    path="$WORK_DIR/${file}.parquet"
    type='Dynamic'
    [[ $file == tuple_* ]] && type='Tuple(a String, b String)'
    expect_error INCORRECT_DATA "DESCRIBE file('$path', Parquet)"
    expect_error INCORRECT_DATA "SELECT v FROM file('$path', Parquet, 'v $type')"
done
