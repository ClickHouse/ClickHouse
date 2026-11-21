#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Avro format is not available in the fast test build.

# A `bytes`, `string` or `fixed` value that the query does not read must cost no memory, however large it is: the
# reader skips it as it arrives and never holds the row it is in. The files are valid, with one 128 MiB `bytes` (or one
# 64 MiB `fixed`) value among small ones, and the limit is less than a third of that. This holds for a narrow projection
# and for `count()` (which validates the payload of the block without reading any column), for the null codec and for
# `deflate`, whose payload is decompressed in windows, and for `max_block_size = 1`, where every row is a batch.

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
BIG_BYTES = 128 << 20
BIG_FIXED = 64 << 20
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


def pieces(length):
    """`length` bytes of `x` in pieces of at most 1 MiB."""
    while length:
        n = min(length, PIECE)
        yield b"x" * n
        length -= n


def payload(kind):
    """The plain payload of the rows {id: long, s: bytes | fixed, t: string}, in pieces."""
    if kind == "bytes":
        # Three rows: the big value is between small ones.
        for ident, length, tail in ((1, 10, b"first"), (42, BIG_BYTES, b"middle"), (3, 10, b"last")):
            yield vlong(ident) + vlong(length)
            yield from pieces(length)
            yield avstr(tail)
    else:
        yield vlong(42)
        yield from pieces(BIG_FIXED)
        yield avstr(b"tail")


def ocf(name, codec, kind, cut=0):
    """One block. `cut` bytes are cut from the end of its payload, which it still declares whole."""
    value = {"name": "s", "type": "bytes"} if kind == "bytes" else {"name": "s", "type": {"type": "fixed", "name": "f", "size": BIG_FIXED}}
    schema = json.dumps({
        "type": "record", "name": "r",
        "fields": [{"name": "id", "type": "long"}, value, {"name": "t", "type": "string"}],
    }).encode()
    meta = vlong(2) + avstr(b"avro.schema") + avstr(schema) + avstr(b"avro.codec") + avstr(codec.encode()) + vlong(0)
    rows = 3 if kind == "bytes" else 1
    if codec == "null":
        size = sum(len(p) for p in payload(kind))
        chunks = payload(kind)
    else:
        compressor = zlib.compressobj(1, zlib.DEFLATED, -15)
        chunks = [compressor.compress(p) for p in payload(kind)]
        chunks.append(compressor.flush())
        size = sum(len(p) for p in chunks)
    with open(f"{work}/{name}.avro", "wb") as f:
        f.write(b"Obj\x01" + meta + SYNC + vlong(rows) + vlong(size))
        left = size - cut
        for piece in chunks:
            piece = piece[:left]
            f.write(piece)
            left -= len(piece)
        if not cut:
            f.write(SYNC)


for codec in ("null", "deflate"):
    ocf(f"bytes_{codec}", codec, "bytes")
    ocf(f"fixed_{codec}", codec, "fixed")
    # The payload ends inside the big value: invalid data, which must not be read as a file that ends early or hang.
    ocf(f"truncated_{codec}", codec, "bytes", cut=(1 << 20) if codec == "null" else 100)
PYEOF

LIMIT="SETTINGS max_memory_usage = 33554432, max_block_size = 1"

run()
{
    ${CLICKHOUSE_LOCAL} --input-format Avro -S "$1" --query "$2 $LIMIT" < "${WORK_DIR}/$3.avro"
}

for codec in null deflate
do
    echo "$codec: bytes, narrow projection"
    run "id Int64" "SELECT id FROM table ORDER BY id" "bytes_$codec"
    echo "$codec: bytes, value after the skipped one"
    run "id Int64, t String" "SELECT id, t FROM table ORDER BY id" "bytes_$codec"
    echo "$codec: bytes, count"
    run "id Int64, s String, t String" "SELECT count() FROM table" "bytes_$codec"
    echo "$codec: fixed, narrow projection"
    run "id Int64, t String" "SELECT id, t FROM table" "fixed_$codec"
    echo "$codec: fixed, count"
    run "id Int64, t String" "SELECT count() FROM table" "fixed_$codec"

    echo "$codec: truncated"
    for query in "SELECT id FROM table ORDER BY id" "SELECT count() FROM table"
    do
        if out=$(run "id Int64, s String, t String" "$query" "truncated_$codec" 2>&1); then
            echo "UNEXPECTED success: $out"
        elif [[ "$out" == *MEMORY_LIMIT_EXCEEDED* ]]; then
            echo "UNEXPECTED memory limit: $out"
        else
            echo "rejected"
        fi
    done
done
