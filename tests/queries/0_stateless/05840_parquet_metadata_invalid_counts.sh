#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The footer of a Parquet file gives the number of values of a column chunk and the number of nulls in its
# statistics. They are read from the file as they are, so they have to be checked before the metadata is shown:
# `1 - INT64_MIN` would overflow. Only the footer is read, so the files have no data pages.

# Writes a Parquet file with one INT32 column and one row group, whose column chunk has the given counts.
write_file()
{
    python3 - "$1" "$2" "$3" <<'EOF'
import struct
import sys

path, num_values, null_count = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])


def varint(n):
    out = bytearray()
    while True:
        byte = n & 0x7F
        n >>= 7
        if n:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def zigzag(n):
    return varint((n << 1) ^ (n >> 63))


# The Thrift compact protocol: fields with a short delta header, then the value.
I32, I64, BINARY, LIST, STRUCT = 5, 6, 8, 9, 12


class Struct:
    def __init__(self):
        self.data = bytearray()
        self.last = 0

    def field(self, field_id, field_type, value):
        self.data.append(((field_id - self.last) << 4) | field_type)
        self.last = field_id
        self.data += value
        return self

    def bytes(self):
        return bytes(self.data) + b"\x00"


def string(s):
    s = s.encode()
    return varint(len(s)) + s


def list_of(element_type, elements):
    return bytes([(len(elements) << 4) | element_type]) + b"".join(elements)


root = Struct().field(4, BINARY, string("schema")).field(5, I32, zigzag(1)).bytes()
leaf = Struct().field(1, I32, zigzag(1)).field(3, I32, zigzag(0)).field(4, BINARY, string("x")).bytes()
statistics = Struct().field(3, I64, zigzag(null_count)).bytes()
column_metadata = (
    Struct()
    .field(1, I32, zigzag(1))
    .field(2, LIST, list_of(I32, [zigzag(0)]))
    .field(3, LIST, list_of(BINARY, [string("x")]))
    .field(4, I32, zigzag(0))
    .field(5, I64, zigzag(num_values))
    .field(6, I64, zigzag(0))
    .field(7, I64, zigzag(0))
    .field(9, I64, zigzag(4))
    .field(12, STRUCT, statistics)
    .bytes()
)
column_chunk = Struct().field(2, I64, zigzag(4)).field(3, STRUCT, column_metadata).bytes()
row_group = (
    Struct().field(1, LIST, list_of(STRUCT, [column_chunk])).field(2, I64, zigzag(0)).field(3, I64, zigzag(num_values)).bytes()
)
footer = (
    Struct()
    .field(1, I32, zigzag(1))
    .field(2, LIST, list_of(STRUCT, [root, leaf]))
    .field(3, I64, zigzag(num_values))
    .field(4, LIST, list_of(STRUCT, [row_group]))
    .bytes()
)

with open(path, "wb") as f:
    f.write(b"PAR1" + footer + struct.pack("<i", len(footer)) + b"PAR1")
EOF
}

FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

query()
{
    $CLICKHOUSE_LOCAL --query "
        SELECT tupleElement(statistics, 'num_values'), tupleElement(statistics, 'null_count')
        FROM (SELECT tupleElement(tupleElement(row_groups[1], 'columns')[1], 'statistics') AS statistics FROM file('$FILE', ParquetMetadata))" 2>&1 \
        | grep -o -m1 -E 'INCORRECT_DATA|^[0-9]+.[0-9]+$'
}

echo "valid"
write_file "$FILE" 3 1
query

echo "null count overflowing the subtraction"
write_file "$FILE" 1 -9223372036854775808
query

echo "negative null count"
write_file "$FILE" 1 -1
query

echo "null count greater than the number of values"
write_file "$FILE" 1 2
query

echo "negative number of values"
write_file "$FILE" -1 0
query

rm -f "$FILE"
