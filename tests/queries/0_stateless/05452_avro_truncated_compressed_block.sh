#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Avro is not available in the fast test build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A compressed block whose payload is truncated, with the block header declaring the shorter length,
# must be rejected. The decoded records fill the reader's buffer exactly, so the decompressor error
# happens only when the reader checks for more data after the last record.

# Removes the last bytes of the payload of the first data block and fixes its declared length.
truncate_first_block()
{
    python3 -c '
import sys

data = sys.stdin.buffer.read()
pos = 0

def read_long():
    global pos
    shift = 0
    result = 0
    while True:
        b = data[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        shift += 7
        if not b & 0x80:
            break
    return (result >> 1) ^ -(result & 1)

def write_long(n):
    n = (n << 1) ^ (n >> 63)
    out = bytearray()
    while n & ~0x7F:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n)
    return bytes(out)

assert data[:4] == b"Obj\x01"
pos = 4
while True:
    count = read_long()
    if count == 0:
        break
    if count < 0:
        count = -count
        read_long()
    for _ in range(2 * count):
        length = read_long()
        pos += length
pos += 16

header_end = pos
count = read_long()
size = read_long()
payload_start = pos
new_size = size - 6
sys.stdout.buffer.write(data[:header_end] + write_long(count) + write_long(new_size)
    + data[payload_start:payload_start + new_size] + data[payload_start + size:])
'
}

for codec in bzip2 xz
do
    echo "$codec"
    $CLICKHOUSE_LOCAL --output_format_avro_codec "$codec" --output_format_avro_sync_interval 1000000 \
        -q "SELECT toInt64(0) AS x FROM numbers(16384) FORMAT Avro" > "$CLICKHOUSE_TMP/$codec.avro"
    $CLICKHOUSE_LOCAL --input-format Avro -q "SELECT count() FROM table" < "$CLICKHOUSE_TMP/$codec.avro"

    truncate_first_block < "$CLICKHOUSE_TMP/$codec.avro" > "$CLICKHOUSE_TMP/$codec.truncated.avro"
    $CLICKHOUSE_LOCAL --input-format Avro -q "SELECT count() FROM table" < "$CLICKHOUSE_TMP/$codec.truncated.avro" 2>&1 \
        | grep -q -E "(bzip2|lzma) error" && echo "rejected"
done
