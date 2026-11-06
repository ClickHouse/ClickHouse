#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Avro format is not available in the fast test build.

# A block of an Avro Object Container File whose declared payload is longer than its compressed stream: the stream is
# followed by bytes that no row can come from.
#
# For `deflate` the file is valid. Writers like fastavro and pyiceberg (which write the Iceberg metadata files) produce
# the payload as `zlib.compress(data)[2:-1]`, which leaves the last 3 bytes of the adler32 trailer after the raw deflate
# stream, and the readers stop at the end of the stream and go on to the sync marker. Both decoders read the rows of such
# blocks, also when more blocks follow. The same holds for any other bytes after the stream.
#
# For `zstandard` the byte after the frame makes the file invalid for both decoders.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

python3 - "$WORK_DIR" <<'PYEOF'
import json, struct, sys, zlib

work = sys.argv[1]
SYNC = bytes(range(16))
ROWS = 5
BLOCKS = 2


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


def zstd(data):
    """A zstandard frame of one raw block: a single segment of less than 256 bytes, with no checksum."""
    assert len(data) < 256
    return b"\x28\xb5\x2f\xfd" + b"\x20" + bytes([len(data)]) + struct.pack("<I", (len(data) << 3) | 1)[:3] + data


def raw_deflate(data):
    compressor = zlib.compressobj(6, zlib.DEFLATED, -15)
    return compressor.compress(data) + compressor.flush()


def plain_rows(first):
    return b"".join(vlong(i) + avstr(b"row%d" % i) for i in range(first, first + ROWS))


schema = json.dumps({
    "type": "record", "name": "r",
    "fields": [{"name": "id", "type": "long"}, {"name": "s", "type": "string"}],
}).encode()


def ocf(name, codec, payloads):
    meta = vlong(2) + avstr(b"avro.schema") + avstr(schema) + avstr(b"avro.codec") + avstr(codec.encode()) + vlong(0)
    with open(f"{work}/{name}.avro", "wb") as f:
        f.write(b"Obj\x01" + meta + SYNC)
        for payload in payloads:
            f.write(vlong(ROWS) + vlong(len(payload)) + payload + SYNC)


blocks = [plain_rows(first) for first in range(0, ROWS * BLOCKS, ROWS)]

ocf("deflate_valid", "deflate", [raw_deflate(b) for b in blocks])
# What fastavro writes: the zlib stream without its 2 bytes header and the last byte of the adler32 trailer.
ocf("deflate_fastavro", "deflate", [zlib.compress(b)[2:-1] for b in blocks])
ocf("deflate_trailing", "deflate", [raw_deflate(b) + b"x" for b in blocks])
ocf("deflate_trailing_last_only", "deflate", [raw_deflate(blocks[0]), raw_deflate(blocks[1]) + b"xyz"])

ocf("zstandard_valid", "zstandard", [zstd(b) for b in blocks])
ocf("zstandard_trailing", "zstandard", [zstd(b) + b"x" for b in blocks])
PYEOF

STRUCTURE="id Int64, s String"
# The ten rows: their count, the sum of the ids and the strings in the order of the ids.
EXPECTED=$'10\t45\trow0,row1,row2,row3,row4,row5,row6,row7,row8,row9'

run()
{
    ${CLICKHOUSE_LOCAL} --input-format Avro -S "$STRUCTURE" --query "SELECT count(), sum(id), arrayStringConcat(arrayMap(x -> x.2, arraySort(groupArray((id, s)))), ',') FROM table" < "${WORK_DIR}/$1.avro" 2>&1
}

for name in deflate_valid deflate_fastavro deflate_trailing deflate_trailing_last_only zstandard_valid
do
    echo "$name"
    if out=$(run "$name") && [[ "$out" == "$EXPECTED" ]]; then
        echo "ok"
    else
        echo "UNEXPECTED: $out"
    fi
done

echo "zstandard_trailing"
if out=$(run zstandard_trailing); then
    echo "UNEXPECTED success: $out"
elif [[ "$out" == *AVRO_EXCEPTION* ]]; then
    echo "rejected"
else
    echo "UNEXPECTED failure: $out"
fi
