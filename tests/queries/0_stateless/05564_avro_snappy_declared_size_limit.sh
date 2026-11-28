#!/usr/bin/env bash
# Tags: no-fasttest

# The snappy block below declares an uncompressed size of 4 GiB in its length prefix but carries only a few bytes.
# The reader must reject it as malformed without first allocating the declared size.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function make_file()
{
    python3 - <<'EOF'
import sys

def long(n):
    n = (n << 1) ^ (n >> 63)
    out = bytearray()
    while n > 0x7f:
        out.append((n & 0x7f) | 0x80)
        n >>= 7
    out.append(n)
    return bytes(out)

def string(b):
    return long(len(b)) + b

schema = b'{"type":"record","name":"r","fields":[{"name":"x","type":"int"}]}'
sync = bytes(range(16))

header = b'Obj\x01'
header += long(2)
header += string(b'avro.schema') + string(schema)
header += string(b'avro.codec') + string(b'snappy')
header += long(0)
header += sync

# Snappy length prefix (varint) of 2^32 - 1, then a few bytes, then the 4 byte big endian CRC32 of the uncompressed data.
data = bytes([0xff, 0xff, 0xff, 0xff, 0x0f]) + b'\x00\x00\x00' + b'\x00\x00\x00\x00'

block = long(1) + long(len(data)) + data + sync
sys.stdout.buffer.write(header + block)
EOF
}

# The reader reports malformed data as `INCORRECT_DATA` (`avro::Exception` caught in `AvroRowInputFormat`)
# or as `AVRO_EXCEPTION`. It must not hit the memory limit.
# Allocations made through the generic `operator new` only throw on the memory limit when they are at least
# `min_allocation_size_to_throw_on_memory_limit` bytes, so set it to make the 4 GiB snappy buffer fail without the fix.
error=$(make_file | $CLICKHOUSE_LOCAL --max_memory_usage='200Mi' --input-format Avro -S 'x Int32' -q 'SELECT * FROM table' -- --min_allocation_size_to_throw_on_memory_limit=1Mi 2>&1)

if grep -q 'MEMORY_LIMIT_EXCEEDED' <<< "$error"; then
    echo "FAIL: memory limit exceeded"
elif grep -q -E 'INCORRECT_DATA|AVRO_EXCEPTION' <<< "$error"; then
    echo "OK"
else
    echo "FAIL: $error"
fi
