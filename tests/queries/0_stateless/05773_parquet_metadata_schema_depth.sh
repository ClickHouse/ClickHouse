#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the `ParquetMetadata` format needs Arrow, which is not built in the fast test.

# The `ParquetMetadata` format limits the nesting depth of the Parquet schema with `max_parser_depth`,
# and always with a hard ceiling, because Arrow builds the schema tree recursively.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Writes a Parquet file without row groups, with a column nested in the given number of groups.
# It is written directly, because ClickHouse does not parse such deep types.
function write_deep_parquet()
{
    python3 -I -c '
import struct, sys

def varint(n):
    out = b""
    while True:
        b = n & 0x7f
        n >>= 7
        if not n:
            return out + bytes([b])
        out += bytes([b | 0x80])

def zigzag(n):
    return varint((n << 1) ^ (n >> 63))

def field(delta, thrift_type):
    return bytes([(delta << 4) | thrift_type])

I32, I64, BINARY, LIST, STRUCT = 5, 6, 8, 9, 12

def schema_element(name, physical_type=None, repetition=None, num_children=None):
    # Field ids: 1 type, 3 repetition_type, 4 name, 5 num_children.
    out = b""
    last = 0
    if physical_type is not None:
        out += field(1 - last, I32) + zigzag(physical_type)
        last = 1
    if repetition is not None:
        out += field(3 - last, I32) + zigzag(repetition)
        last = 3
    out += field(4 - last, BINARY) + varint(len(name)) + name
    last = 4
    if num_children is not None:
        out += field(5 - last, I32) + zigzag(num_children)
    return out + b"\x00"

def struct_list(items):
    header = bytes([(len(items) << 4) | STRUCT]) if len(items) < 15 else bytes([0xf0 | STRUCT]) + varint(len(items))
    return header + b"".join(items)

depth = int(sys.argv[1])
schema = [schema_element(b"schema", num_children=1)]
schema += [schema_element(b"c", repetition=1, num_children=1) for _ in range(depth)]
schema += [schema_element(b"c", physical_type=1, repetition=1)]
# FileMetaData: 1 version, 2 schema, 3 num_rows, 4 row_groups.
metadata = field(1, I32) + zigzag(1) + field(1, LIST) + struct_list(schema) + field(1, I64) + zigzag(0) + field(1, LIST) + struct_list([]) + b"\x00"
sys.stdout.buffer.write(b"PAR1" + metadata + struct.pack("<I", len(metadata)) + b"PAR1")
' "$1" > "$2"
}

file="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

write_deep_parquet 100 "$file"
$CLICKHOUSE_LOCAL -q "SELECT num_columns FROM file('$file', ParquetMetadata)"
$CLICKHOUSE_LOCAL --max_parser_depth 50 -q "SELECT num_columns FROM file('$file', ParquetMetadata)" 2>&1 | grep -o -m1 'TOO_DEEP_RECURSION'

# Raising `max_parser_depth` or setting it to 0 (unlimited) does not remove the hard ceiling.
write_deep_parquet 100000 "$file"
$CLICKHOUSE_LOCAL --max_parser_depth 0 -q "SELECT num_columns FROM file('$file', ParquetMetadata)" 2>&1 | grep -o -m1 'TOO_DEEP_RECURSION'
$CLICKHOUSE_LOCAL --max_parser_depth 1000000 -q "SELECT num_columns FROM file('$file', ParquetMetadata)" 2>&1 | grep -o -m1 'TOO_DEEP_RECURSION'

rm -f "$file"
