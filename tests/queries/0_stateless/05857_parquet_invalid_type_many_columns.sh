#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A corrupted footer may store any integer in the `type` of a schema element. With more than 1024 schema elements,
# the generated Thrift reader moves the elements that are already read while it reads the rest, before the reader of
# ClickHouse can check them: the enums of the generated code have a fixed underlying type, so this is well-defined,
# and the invalid type is reported afterwards. Only the footer is read, so the files have no data pages.

# Writes a Parquet file without row groups, with the given number of INT32 columns, the first of which has the given type.
write_file()
{
    python3 - "$1" "$2" "$3" <<'EOF'
import struct
import sys

path, num_columns, first_type = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])


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
    if len(elements) < 15:
        return bytes([(len(elements) << 4) | element_type]) + b"".join(elements)
    return bytes([0xF0 | element_type]) + varint(len(elements)) + b"".join(elements)


def leaf(name, physical_type):
    return Struct().field(1, I32, zigzag(physical_type)).field(3, I32, zigzag(0)).field(4, BINARY, string(name)).bytes()


root = Struct().field(4, BINARY, string("schema")).field(5, I32, zigzag(num_columns)).bytes()
leaves = [leaf("c" + str(i), first_type if i == 0 else 1) for i in range(num_columns)]
footer = (
    Struct()
    .field(1, I32, zigzag(1))
    .field(2, LIST, list_of(STRUCT, [root] + leaves))
    .field(3, I64, zigzag(0))
    .field(4, LIST, list_of(STRUCT, []))
    .bytes()
)

with open(path, "wb") as f:
    f.write(b"PAR1" + footer + struct.pack("<i", len(footer)) + b"PAR1")
EOF
}

FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

for physical_type in 1 -1 100
do
    echo "type of the first column: $physical_type"
    write_file "$FILE" 1100 "$physical_type"
    $CLICKHOUSE_LOCAL --query "SELECT num_columns FROM file('$FILE', ParquetMetadata)" 2>&1 | grep -o -m1 -E 'INCORRECT_DATA|^[0-9]+$'
    $CLICKHOUSE_LOCAL --query "SELECT count() FROM file('$FILE', Parquet)" 2>&1 | grep -o -m1 -E 'INCORRECT_DATA|^[0-9]+$'
done

rm -f "$FILE"
