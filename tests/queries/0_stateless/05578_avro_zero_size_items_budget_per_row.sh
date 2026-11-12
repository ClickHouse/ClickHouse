#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Avro format is not available in the fast test build.

# The items that occupy no bytes (`null`, empty records) that one row may hold are capped (16777216), and the cap is
# the row's: the same rows must read the same however the file puts them into OCF blocks. 1000 rows, each with an
# `array<null>` of 20000 items that the query does not read, are 12 kB of file and 20 million zero-size items in all:
# a budget shared by the block rejects them in one block and reads them in blocks of 50 rows. Positive and negative
# (with a byte size) array block counts, the null codec (the payload is decoded window by window from the input)
# and `deflate` (decoded from the decompressed block), one block and blocks of 50 rows, batches of one row.
# The last file has two rows that hold the whole cap each, in two array blocks.

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
ROWS = 1000
ITEMS = 20000
CAP = 1 << 24


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


def null_array(blocks, negative):
    """The blocks of an array of nulls, each with `count` items; a negative count is followed by the size of its items (0)."""
    out = b""
    for count in blocks:
        out += (vlong(-count) + vlong(0)) if negative else vlong(count)
    return out + vlong(0)


def row(i, blocks, negative):
    return vlong(i) + null_array(blocks, negative) + avstr(b"after")


def ocf(name, codec, rows, rows_per_block):
    schema = json.dumps({
        "type": "record", "name": "r",
        "fields": [{"name": "id", "type": "long"},
                   {"name": "ignored", "type": {"type": "array", "items": "null"}},
                   {"name": "tail", "type": "string"}],
    }).encode()
    meta = vlong(2) + avstr(b"avro.schema") + avstr(schema) + avstr(b"avro.codec") + avstr(codec.encode()) + vlong(0)
    with open(f"{work}/{name}.avro", "wb") as f:
        f.write(b"Obj\x01" + meta + SYNC)
        for start in range(0, len(rows), rows_per_block):
            chunk = rows[start:start + rows_per_block]
            payload = b"".join(chunk)
            if codec == "deflate":
                compressor = zlib.compressobj(6, zlib.DEFLATED, -15)
                payload = compressor.compress(payload) + compressor.flush()
            f.write(vlong(len(chunk)) + vlong(len(payload)) + payload + SYNC)


for codec in ("null", "deflate"):
    for sign, negative in (("pos", False), ("neg", True)):
        rows = [row(i, [ITEMS], negative) for i in range(ROWS)]
        ocf(f"one_block_{sign}_{codec}", codec, rows, ROWS)
        ocf(f"blocks_of_50_{sign}_{codec}", codec, rows, 50)
    # Two rows that hold the whole budget of a row each, in two blocks of the array.
    ocf(f"cap_{codec}", codec, [row(i, [CAP // 2, CAP // 2], i % 2 == 1) for i in range(2)], 2)
PYEOF

run()
{
    ${CLICKHOUSE_LOCAL} --input-format Avro -S "id Int64, tail String" \
        --query "SELECT count(), sum(id), sum(length(tail)) FROM table $2" < "${WORK_DIR}/$1.avro"
}

for codec in null deflate
do
    for sign in pos neg
    do
        echo "$codec, $sign: one block of 1000 rows"
        run "one_block_${sign}_${codec}"
        echo "$codec, $sign: blocks of 50 rows"
        run "blocks_of_50_${sign}_${codec}"
        echo "$codec, $sign: one block, batches of one row"
        run "one_block_${sign}_${codec}" "SETTINGS max_block_size = 1"
    done

    echo "$codec: two rows of the whole budget each"
    run "cap_${codec}"
done
