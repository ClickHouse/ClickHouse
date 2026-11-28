#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Avro integration is not available in the fast test build.

# Robustness of the Avro OCF sync-marker segmentation (used for parallel parsing) against hand-crafted
# container bytes that the ClickHouse Avro writer never produces: a sync marker embedded inside a value,
# the negative-count metadata-map encoding, a block with an oversized declared length over truncated input,
# an overflowing varint, a negative object count, and a final block missing its trailing sync marker. The
# segmentator parses raw container bytes by itself, so it must handle the well-formed cases exactly as the
# sequential reader does and reject every malformed one cleanly (an exception, never a hang or a crash).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

D="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Build three OCF files with a *fixed* sync marker so the payload can be made to contain it on purpose.
python3 - "$D" <<'PY'
import sys
base = sys.argv[1]

SYNC = bytes(range(16))                 # a fixed, known 16-byte sync marker
SCHEMA = b'{"type":"record","name":"r","fields":[{"name":"s","type":"string"}]}'

def enc_long(n):                        # Avro long: ZigZag + base-128 varint
    u = ((n << 1) ^ (n >> 63)) & 0xFFFFFFFFFFFFFFFF
    out = bytearray()
    while True:
        b = u & 0x7F
        u >>= 7
        out.append(b | 0x80 if u else b)
        if not u:
            return bytes(out)

def enc_bytes(b):
    return enc_long(len(b)) + b

def meta_positive():
    entries = enc_bytes(b"avro.schema") + enc_bytes(SCHEMA) + enc_bytes(b"avro.codec") + enc_bytes(b"null")
    return enc_long(2) + entries + enc_long(0)

def meta_negative():
    # -count means `count` entries preceded by the block's byte length (the external-writer variant).
    entries = enc_bytes(b"avro.schema") + enc_bytes(SCHEMA) + enc_bytes(b"avro.codec") + enc_bytes(b"null")
    return enc_long(-2) + enc_long(len(entries)) + entries + enc_long(0)

def block(values):
    data = b"".join(enc_bytes(v) for v in values)
    return enc_long(len(values)) + enc_long(len(data)) + data + SYNC

def ocf(meta, blocks):
    return b"Obj\x01" + meta + SYNC + b"".join(blocks)

# 1) A value that literally contains the sync marker, spread over many blocks. Segmentation must jump by
#    the declared block length, not scan for the marker, so this must not cause a false split.
blocks = [block([b"aa" + SYNC + b"bb" + str(i).encode()]) for i in range(200)]
open(base + "_marker.avro", "wb").write(ocf(meta_positive(), blocks))

# 2) The same data but with negative-count metadata encoding in the header.
open(base + "_negcount.avro", "wb").write(ocf(meta_negative(), blocks))

# 3) A block whose declared byte length is far larger than the bytes that follow (truncated input).
good = block([b"x" * 10 for _ in range(50)])
header = b"Obj\x01" + meta_positive() + SYNC
bad = enc_long(5) + enc_long(10**9) + b"too short"          # declares 1e9 bytes, supplies 9
open(base + "_oversized.avro", "wb").write(header + good + bad)

# 4) A block count encoded as a varint longer than 10 bytes (the 10th byte still has the continuation bit).
#    The segmentator's varint reader must reject it, like `avro::BinaryDecoder::doDecodeLong` does.
too_long_varint = b"\x80" * 10 + b"\x01"
open(base + "_toolong.avro", "wb").write(header + too_long_varint)

# 5) A data block with a negative object (record) count. The segmentator must reject it, not spin forever
#    on a block it can neither advance past nor consume.
neg_obj = enc_long(-5) + enc_long(10) + (b"x" * 10) + SYNC
open(base + "_negobjcount.avro", "wb").write(header + neg_obj)

# 6) A container whose final block is missing its trailing 16-byte sync marker (truncated). The block body
#    is intact, so this must still be rejected (fail closed), not accepted as a clean end of file.
last = enc_long(3) + enc_long(3) + b"\x02ab"                 # one record "ab", no sync appended
open(base + "_truncsync.avro", "wb").write(header + good + last)
PY

# Parallel vs sequential must agree as a multiset for the two well-formed files.
check() {
    local file="$1" seq par seq_rc par_rc
    seq=$(${CLICKHOUSE_LOCAL} -q "SELECT s FROM file('$file', 'Avro', 's String') SETTINGS input_format_parallel_parsing=0 FORMAT TSV"); seq_rc=$?
    par=$(${CLICKHOUSE_LOCAL} -q "SELECT s FROM file('$file', 'Avro', 's String') SETTINGS input_format_parallel_parsing=1, max_threads=8, max_parsing_threads=8, min_chunk_bytes_for_parallel_parsing=64 FORMAT TSV"); par_rc=$?
    # Both reads must succeed and return rows, otherwise two empty outputs would hash equal and falsely pass.
    if [ "$seq_rc" -ne 0 ] || [ "$par_rc" -ne 0 ] || [ -z "$seq" ]; then
        echo "QUERY FAILED (seq_rc=$seq_rc par_rc=$par_rc)"; return
    fi
    [ "$(printf '%s' "$seq" | sort | md5sum)" = "$(printf '%s' "$par" | sort | md5sum)" ] && echo "OK" || echo "MISMATCH"
}

# A malformed file must be rejected with a specific `AVRO_EXCEPTION`, like the sequential reader -- not a
# different error, and never a crash. The exit status cannot tell a signal apart, as `clickhouse-local` exits
# with the error code modulo 256 (`AVRO_EXCEPTION` = 748 gives 236), so assert the exception text instead.
# Asserting the exact message also makes the too-long varint case fail unless the length guard, not the
# subsequent truncation, is what rejects it. `ParallelParsingBlockInputFormat` in the stack proves that the
# parallel path, not a sequential fallback, rejected the file.
expect_error() {
    local file="$1" expected="$2" out rc
    out=$(${CLICKHOUSE_LOCAL} -q "SELECT s FROM file('$file', 'Avro', 's String') SETTINGS input_format_parallel_parsing=1, max_threads=8, max_parsing_threads=8, min_chunk_bytes_for_parallel_parsing=64 FORMAT Null" 2>&1); rc=$?
    if [ "$rc" -ne 0 ] && printf '%s' "$out" | grep -F "$expected" | grep -F "(AVRO_EXCEPTION)" | grep -qF "ParallelParsingBlockInputFormat"; then echo "OK"; else echo "NOT REJECTED (rc=$rc)"; fi
}

echo -n "marker-in-payload: "; check "${D}_marker.avro"
echo -n "negative-count-metadata: "; check "${D}_negcount.avro"
echo -n "oversized-length rejected: "; expect_error "${D}_oversized.avro" "Unexpected end of Avro input"
echo -n "too-long-varint rejected: "; expect_error "${D}_toolong.avro" "Avro varint is too long"
echo -n "negative-object-count rejected: "; expect_error "${D}_negobjcount.avro" "Negative object count in block header"
echo -n "truncated-sync rejected: "; expect_error "${D}_truncsync.avro" "Unexpected end of Avro input"

rm -f "${D}"_*.avro
