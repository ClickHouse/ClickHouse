#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Avro integration is not available in the fast test build.

# A `LIMIT` that is satisfied while the segmentator is in the middle of a large Avro block: the file has a tiny
# first block (10 rows) and a 16 MiB second block. `min_chunk_bytes_for_parallel_parsing = 1` makes the first
# block its own segment, so the pipeline stops while the segmentator copies the second one, and the partial
# block is dropped. The query must return the first rows and finish without an error. `max_threads = 1` keeps
# the rows in file order after the source.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

file="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.avro"
trap 'rm -f "$file"' EXIT

python3 - "$file" <<'PY'
import sys
out = sys.argv[1]
SYNC = bytes(range(16))
SCHEMA = b'{"type":"record","name":"r","fields":[{"name":"s","type":"string"}]}'

def enc_long(n):
    u = ((n << 1) ^ (n >> 63)) & 0xFFFFFFFFFFFFFFFF
    out_bytes = bytearray()
    while True:
        b = u & 0x7F
        u >>= 7
        out_bytes.append(b | 0x80 if u else b)
        if not u:
            return bytes(out_bytes)

def enc_bytes(b):
    return enc_long(len(b)) + b

def meta():
    entries = enc_bytes(b"avro.schema") + enc_bytes(SCHEMA) + enc_bytes(b"avro.codec") + enc_bytes(b"null")
    return enc_long(2) + entries + enc_long(0)

def block(values):
    data = b"".join(enc_bytes(v) for v in values)
    return enc_long(len(values)) + enc_long(len(data)) + data + SYNC

header = b"Obj\x01" + meta() + SYNC
small = block([b"row" + str(i).encode() for i in range(10)])  # satisfies LIMIT 1
big = block([b"x" * (16 * 1024 * 1024)])                      # being copied when the LIMIT is satisfied
open(out, "wb").write(header + small + big)
PY

settings="input_format_parallel_parsing = 1, max_parsing_threads = 4, min_chunk_bytes_for_parallel_parsing = 1, max_threads = 1"
${CLICKHOUSE_LOCAL} -q "SELECT s FROM file('$file', 'Avro', 's String') LIMIT 1 SETTINGS $settings"
${CLICKHOUSE_LOCAL} -q "SELECT groupArray(s) FROM (SELECT s FROM file('$file', 'Avro', 's String') LIMIT 10) SETTINGS $settings"
