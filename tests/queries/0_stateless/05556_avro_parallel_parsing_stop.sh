#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Avro integration is not available in the fast test build.

# Regression: when parallel parsing stops before the input is consumed (here a `LIMIT`), the segmentator
# must not keep draining the current Avro block. The file has a tiny first block (which satisfies
# `LIMIT 1`) and a 16 MiB second block that must be dropped, not read. `min_chunk_bytes_for_parallel_parsing = 1`
# makes the first block its own segment, so the pipeline stops while the segmentator is on the second
# block. We assert the bytes read from the file stay well below the file size; without the stop check the
# whole second block is drained (~17 MiB read).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

file_name="${CLICKHOUSE_DATABASE}_05556_stop.avro"
dst="${CLICKHOUSE_USER_FILES}/${file_name}"

python3 - "$dst" <<'PY'
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
big = block([b"x" * (16 * 1024 * 1024)])                      # must not be drained
open(out, "wb").write(header + small + big)
PY

qid="05556_stop_${CLICKHOUSE_DATABASE}_${RANDOM}"
${CLICKHOUSE_CLIENT} --query_id "$qid" -q "
    SELECT s FROM file('${file_name}', 'Avro', 's String') LIMIT 1
    SETTINGS input_format_parallel_parsing = 1, max_parsing_threads = 4,
             min_chunk_bytes_for_parallel_parsing = 1, storage_file_read_method = 'pread' FORMAT Null"

# The query_log row is written asynchronously, so flush and retry until it lands.
rb=""
for _ in {1..60}; do
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
    rb=$(${CLICKHOUSE_CLIENT} -q "
        SELECT ProfileEvents['ReadBufferFromFileDescriptorReadBytes']
        FROM system.query_log
        WHERE query_id = '$qid' AND type = 'QueryFinish' AND event_date >= yesterday()
          AND current_database = currentDatabase()
        ORDER BY event_time DESC LIMIT 1")
    [ -n "$rb" ] && break
    sleep 0.5
done

# The 16 MiB second block must not be drained: far fewer than the file's ~17 MiB are read.
${CLICKHOUSE_CLIENT} -q "SELECT ${rb:-0} < 8388608"

rm -f "$dst"
