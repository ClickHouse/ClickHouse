#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Avro format is not available in the fast test build.

# Like 05574, for a large value inside an array or a map that the query does not read: the block of the collection is
# entered and its items are skipped as they arrive, so neither the value nor the payload that the count of the block
# announces has to be held. One row has an array of one 64 MiB `bytes`, or a map with one 64 MiB value, or an array of
# 16 million items of one byte each; the limit is 32 MiB. Narrow projection and `count()`, null codec and `deflate`.

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
BIG = 64 << 20
ITEMS = 16 << 20
PIECE = 1 << 20


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


def pieces(length, byte=b"x"):
    while length:
        n = min(length, PIECE)
        yield byte * n
        length -= n


def payload(kind):
    """The plain payload of the row {id: long, c: array<bytes> | map<bytes> | array<long>, t: string}, in pieces."""
    yield vlong(42)
    if kind == "array":
        yield vlong(1) + vlong(BIG)
        yield from pieces(BIG)
    elif kind == "map":
        yield vlong(1) + avstr(b"key") + vlong(BIG)
        yield from pieces(BIG)
    else:
        yield vlong(ITEMS)
        yield from pieces(ITEMS, b"\x00")
    yield vlong(0)
    yield avstr(b"tail")


def ocf(name, codec, kind, cut=0):
    """One block. `cut` bytes are cut from the end of its payload, which it still declares whole."""
    items = {"array": {"type": "array", "items": "bytes"}, "map": {"type": "map", "values": "bytes"},
             "longs": {"type": "array", "items": "long"}}[kind]
    schema = json.dumps({
        "type": "record", "name": "r",
        "fields": [{"name": "id", "type": "long"}, {"name": "c", "type": items}, {"name": "t", "type": "string"}],
    }).encode()
    meta = vlong(2) + avstr(b"avro.schema") + avstr(schema) + avstr(b"avro.codec") + avstr(codec.encode()) + vlong(0)
    if codec == "null":
        size = sum(len(p) for p in payload(kind))
        chunks = payload(kind)
    else:
        compressor = zlib.compressobj(1, zlib.DEFLATED, -15)
        chunks = [compressor.compress(p) for p in payload(kind)]
        chunks.append(compressor.flush())
        size = sum(len(p) for p in chunks)
    with open(f"{work}/{name}.avro", "wb") as f:
        f.write(b"Obj\x01" + meta + SYNC + vlong(1) + vlong(size))
        left = size - cut
        for piece in chunks:
            piece = piece[:left]
            f.write(piece)
            left -= len(piece)
        if not cut:
            f.write(SYNC)


for codec in ("null", "deflate"):
    for kind in ("array", "map", "longs"):
        ocf(f"{kind}_{codec}", codec, kind)
    # The payload ends inside the collection: invalid data.
    ocf(f"truncated_{codec}", codec, "array", cut=(1 << 20) if codec == "null" else 100)
PYEOF

LIMIT="SETTINGS max_memory_usage = 33554432, max_block_size = 1"

run()
{
    ${CLICKHOUSE_LOCAL} --input-format Avro -S "$1" --query "$2 $LIMIT" < "${WORK_DIR}/$3.avro"
}

for codec in null deflate
do
    for kind in array map longs
    do
        echo "$codec: $kind, narrow projection"
        run "id Int64, t String" "SELECT id, t FROM table" "${kind}_$codec"
        echo "$codec: $kind, count"
        run "id Int64, t String" "SELECT count() FROM table" "${kind}_$codec"
    done

    echo "$codec: truncated"
    for query in "SELECT id FROM table" "SELECT count() FROM table"
    do
        if out=$(run "id Int64" "$query" "truncated_$codec" 2>&1); then
            echo "UNEXPECTED success: $out"
        elif [[ "$out" == *MEMORY_LIMIT_EXCEEDED* ]]; then
            echo "UNEXPECTED memory limit: $out"
        else
            echo "rejected"
        fi
    done
done
