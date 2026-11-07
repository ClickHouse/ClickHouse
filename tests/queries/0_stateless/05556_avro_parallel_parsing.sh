#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Avro integration is not available in the fast test build.

# Parallel parsing of Avro OCF (sync-marker segmentation) must produce exactly the same rows as the
# single-threaded reader. Files are written with a tiny sync interval so there are many blocks, and read
# back with a tiny min_chunk_bytes_for_parallel_parsing so the segmentator emits several segments.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

D="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

struct="i Int64, s String, arr Array(UInt64), n Nullable(UInt32)"

# Pin every setting that decides whether the parallel path is taken; `max_parsing_threads` is randomized
# and a value of 1 would silently make the "parallel" read sequential.
PAR="input_format_parallel_parsing=1, max_threads=8, max_parsing_threads=8"

# Compare a sequential read with a parallel read of the same Avro file. The pipeline below the format does
# not preserve row order for a `SELECT *` over `file` (parallel streams interleave -- this is true for every
# format, not just Avro), so the comparison is the md5 of the full output sorted as a multiset. That detects
# row loss, duplication and any wrong column association across every column -- not just aggregate totals.
check() {
    local file="$1" structure="$2" extra="${3:-}"
    local seq par seq_rc par_rc
    seq=$(${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('$file', 'Avro', '$structure') SETTINGS ${extra}input_format_parallel_parsing=0 FORMAT TSV"); seq_rc=$?
    par=$(${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('$file', 'Avro', '$structure') SETTINGS ${extra}${PAR}, min_chunk_bytes_for_parallel_parsing=4096 FORMAT TSV"); par_rc=$?
    # Both reads must succeed and return rows, otherwise two empty outputs would hash equal and falsely pass.
    if [ "$seq_rc" -ne 0 ] || [ "$par_rc" -ne 0 ] || [ -z "$seq" ]; then
        echo "QUERY FAILED (seq_rc=$seq_rc par_rc=$par_rc)"; return
    fi
    [ "$(printf '%s' "$seq" | sort | md5sum)" = "$(printf '%s' "$par" | sort | md5sum)" ] && echo "OK" || echo "MISMATCH"
}

# 1) round-trip equality, mixed types, many blocks
${CLICKHOUSE_LOCAL} -q "
    SELECT number::Int64 AS i, toString(number) AS s, range(number % 6) AS arr,
           if(number % 3 = 0, NULL, number)::Nullable(UInt32) AS n
    FROM numbers(6000)
    INTO OUTFILE '${D}_mixed.avro' FORMAT Avro SETTINGS output_format_avro_sync_interval = 512"
echo -n "mixed: "; check "${D}_mixed.avro" "$struct"

# 2) each codec
for codec in null deflate zstd snappy; do
    ${CLICKHOUSE_LOCAL} -q "
        SELECT number::Int64 AS i, toString(number) AS s, range(number % 6) AS arr,
               if(number % 3 = 0, NULL, number)::Nullable(UInt32) AS n
        FROM numbers(3000)
        INTO OUTFILE '${D}_${codec}.avro' FORMAT Avro
        SETTINGS output_format_avro_sync_interval = 256, output_format_avro_codec = '${codec}'"
    echo -n "codec ${codec}: "; check "${D}_${codec}.avro" "$struct"
done

# 3) single-block / tiny file (degenerate one-segment path)
${CLICKHOUSE_LOCAL} -q "SELECT number::Int64 AS i FROM numbers(3) INTO OUTFILE '${D}_tiny.avro' FORMAT Avro"
echo -n "tiny: "; check "${D}_tiny.avro" "i Int64"

# 4) schema evolution: read a subset plus a missing-with-default column, under parallel parsing
${CLICKHOUSE_LOCAL} -q "
    SELECT number::Int64 AS a, toString(number) AS b, (number * 2)::Int64 AS c
    FROM numbers(4000)
    INTO OUTFILE '${D}_evo.avro' FORMAT Avro SETTINGS output_format_avro_sync_interval = 300"
echo -n "schema-evolution: "; check "${D}_evo.avro" "a Int64, c Int64, d Int32" "input_format_avro_allow_missing_fields=1, "

# 5) wide random-byte values across many blocks. The segmentation jumps over each block by its declared
# byte length rather than scanning for the 16-byte sync marker, so a value whose bytes happen to contain
# the sync sequence must not cause a false split -- random 200-byte strings exercise that.
${CLICKHOUSE_LOCAL} -q "
    SELECT number::Int64 AS i, randomString(200) AS s
    FROM numbers(4000)
    INTO OUTFILE '${D}_wide.avro' FORMAT Avro SETTINGS output_format_avro_sync_interval = 256"
echo -n "wide-random-bytes: "; check "${D}_wide.avro" "i Int64, s String"

# 6) large multi-MiB blocks read with a tiny read buffer. This drives the stepped, amortized segment
# growth in the segmentator (each block is copied in bounded steps that repeatedly grow the buffer past
# its capacity) over many buffer refills; the parallel read must still match the sequential one.
${CLICKHOUSE_LOCAL} -q "
    SELECT number::Int64 AS i, randomString(2000) AS s
    FROM numbers(1500)
    INTO OUTFILE '${D}_bigblk.avro' FORMAT Avro SETTINGS output_format_avro_sync_interval = 2097152"
echo -n "large-blocks-small-buffer: "; check "${D}_bigblk.avro" "i Int64, s String" "max_read_buffer_size=4096, "

# 7) Segment limits. The rows are the same whichever limit closes a segment, so observe the boundaries
# instead: every segment is parsed into its own blocks, so with 20000 rows (a multiple of every
# `max_block_size` below) a single segment yields only full blocks, and any other block size marks a cut.
${CLICKHOUSE_LOCAL} -q "
    SELECT number::Int64 AS i, toString(number) AS s
    FROM numbers(20000)
    INTO OUTFILE '${D}_limits.avro' FORMAT Avro SETTINGS output_format_avro_sync_interval = 256"
block_sizes="SELECT blockSize() AS b FROM file('${D}_limits.avro', 'Avro', 'i Int64, s String')"
# The byte limit alone: rows are not limited, so more than one block means the 4 KiB limit cut the file.
echo -n "byte-limit split: "; ${CLICKHOUSE_LOCAL} -q "
    SELECT count() > 1 FROM (${block_sizes})
    SETTINGS ${PAR}, min_chunk_bytes_for_parallel_parsing=4096, max_block_size=20000"
# The row limit alone: the byte limit is far above the file size, so a block smaller than 100 rows means
# a segment was closed after reaching `max_block_size` rows, at the next Avro block boundary.
echo -n "row-limit split: "; ${CLICKHOUSE_LOCAL} -q "
    SELECT countIf(b != 100) > 0 FROM (${block_sizes})
    SETTINGS ${PAR}, min_chunk_bytes_for_parallel_parsing=104857600, max_block_size=100"

rm -f "${D}"_*.avro
