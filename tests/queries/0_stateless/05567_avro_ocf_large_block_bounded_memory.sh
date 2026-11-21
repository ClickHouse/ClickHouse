#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Avro format is not available in the fast test build.

# A block of an Avro Object Container File is as large as its writer made it (the writer of ClickHouse allows sync
# intervals up to 1 GiB), however small its rows are. Reading it must cost memory for the rows that are read, not for the
# block: the payload of the null codec and the decompressed payload of `deflate` are consumed in bounded windows, a row
# that is larger than a window makes it grow only as far as that row, and a `LIMIT` or a narrow projection does not need the
# rest of the block. The files are valid, with one ~70 MB block each, and the limit is less than half of that.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

python3 - "$WORK_DIR" <<'PYEOF'
import json, sys, zlib

work = sys.argv[1]
SYNC = bytes(range(16))
ROWS = 1 << 19


def vlong(n):
    u = ((n << 1) ^ (n >> 63)) & ((1 << 64) - 1)
    out = bytearray()
    while True:
        b = u & 0x7F
        u >>= 7
        out.append(b | 0x80 if u else b)
        if not u:
            break
    return bytes(out)


def avstr(b):
    return vlong(len(b)) + b


def string_for(i, big_row, big_len):
    if i == big_row:
        return b"y" * big_len
    return b"%08d" % i + b"x" * 120


def plain_chunks(rows, big_row, big_len):
    """The plain payload of rows {id: long, s: string}, in pieces of 16384 rows."""
    for start in range(0, rows, 16384):
        yield b"".join(vlong(i) + avstr(string_for(i, big_row, big_len)) for i in range(start, min(rows, start + 16384)))


def ocf(name, codec, rows, big_row=-1, big_len=0, declared_extra=0, cut=0):
    """One block. `declared_extra` bytes are added to the declared size of the payload and `cut` bytes are cut from its end."""
    schema = json.dumps({
        "type": "record", "name": "r",
        "fields": [{"name": "id", "type": "long"}, {"name": "s", "type": "string"}],
    }).encode()
    meta = vlong(2) + avstr(b"avro.schema") + avstr(schema) + avstr(b"avro.codec") + avstr(codec.encode()) + vlong(0)
    if codec == "null":
        parts = list(plain_chunks(rows, big_row, big_len))
    else:
        compressor = zlib.compressobj(1, zlib.DEFLATED, -15)
        parts = [compressor.compress(chunk) for chunk in plain_chunks(rows, big_row, big_len)]
        parts.append(compressor.flush())
    payload = b"".join(parts)
    declared = len(payload) + declared_extra
    if cut:
        payload = payload[:-cut]
    with open(f"{work}/{name}.avro", "wb") as f:
        f.write(b"Obj\x01" + meta + SYNC + vlong(rows) + vlong(declared) + payload + SYNC)


for codec in ("null", "deflate"):
    # One block of small rows: ~70 MB of plain payload.
    ocf(f"large_{codec}", codec, ROWS)
    # One block with a 3 MiB row between small ones: a row larger than the window.
    ocf(f"wide_{codec}", codec, 1000, big_row=500, big_len=3 * 1024 * 1024)
    # A block that declares more payload than the file has, and a compressed one that is cut: invalid data.
    ocf(f"truncated_{codec}", codec, 1000, declared_extra=1 << 30 if codec == "null" else 0, cut=0 if codec == "null" else 100)
PYEOF

STRUCTURE="id Int64, s String"
LIMIT="SETTINGS max_memory_usage = 33554432, max_block_size = 8192"
ROWS=524288
# Every row of the large files, one value per row: the hash of its id and string. Both sides wrap around the same way.
EXPECTED_LARGE="(SELECT sum(cityHash64(number, concat(leftPad(toString(number), 8, '0'), repeat('x', 120)))) FROM numbers($ROWS))"
EXPECTED_WIDE="((SELECT sum(cityHash64(number, concat(leftPad(toString(number), 8, '0'), repeat('x', 120)))) FROM numbers(1000) WHERE number != 500) + cityHash64(toUInt64(500), repeat(repeat('y', 1024), 3 * 1024)))"

run()
{
    ${CLICKHOUSE_LOCAL} --input-format Avro -S "$STRUCTURE" --query "$1 $LIMIT" < "${WORK_DIR}/$2.avro"
}

for codec in null deflate
do
    echo "$codec: limit"
    run "SELECT id, length(s), substring(s, 1, 10) FROM table LIMIT 1" "large_$codec"
    echo "$codec: narrow projection"
    run "SELECT sum(id) FROM table" "large_$codec"
    echo "$codec: count"
    run "SELECT count() FROM table" "large_$codec"
    echo "$codec: all rows"
    run "SELECT count(), sum(length(s)), sum(cityHash64(toUInt64(id), s)) = $EXPECTED_LARGE FROM table" "large_$codec"
    echo "$codec: wide row"
    run "SELECT count(), sum(length(s)), sum(cityHash64(toUInt64(id), s)) = $EXPECTED_WIDE FROM table" "wide_$codec"

    echo "$codec: truncated"
    if out=$(run "SELECT count() FROM table" "truncated_$codec" 2>&1); then
        echo "UNEXPECTED success: $out"
    elif [[ "$out" == *MEMORY_LIMIT_EXCEEDED* ]]; then
        echo "UNEXPECTED memory limit: $out"
    else
        echo "rejected"
    fi
done
