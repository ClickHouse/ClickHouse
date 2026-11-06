#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Avro integration is not available in the fast test build.

# Parallel parsing of Avro OCF (sync-marker segmentation) must produce exactly the same rows as the
# single-threaded reader. Files are written with a tiny sync interval so there are many blocks, and read
# back with a tiny min_chunk_bytes_for_parallel_parsing so the segmentator emits several segments.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

D="${CLICKHOUSE_TMP}/05556"

agg="count(), sum(i), sum(cityHash64(s)), sum(length(arr)), sum(ifNull(n, 0))"
struct="i Int64, s String, arr Array(UInt64), n Nullable(UInt32)"

# Compare a sequential read with a parallel read of the same Avro file.
check() {
    local file="$1" structure="$2" cols="$3"
    local seq par
    seq=$(${CLICKHOUSE_LOCAL} -q "SELECT $cols FROM file('$file', 'Avro', '$structure') SETTINGS input_format_parallel_parsing=0")
    par=$(${CLICKHOUSE_LOCAL} -q "SELECT $cols FROM file('$file', 'Avro', '$structure') SETTINGS input_format_parallel_parsing=1, max_threads=8, min_chunk_bytes_for_parallel_parsing=4096")
    [ "$seq" = "$par" ] && echo "OK" || echo "MISMATCH seq=[$seq] par=[$par]"
}

# 1) round-trip equality, mixed types, many blocks
${CLICKHOUSE_LOCAL} -q "
    SELECT number::Int64 AS i, toString(number) AS s, range(number % 6) AS arr,
           if(number % 3 = 0, NULL, number)::Nullable(UInt32) AS n
    FROM numbers(100000)
    INTO OUTFILE '${D}_mixed.avro' FORMAT Avro SETTINGS output_format_avro_sync_interval = 512"
echo -n "mixed: "; check "${D}_mixed.avro" "$struct" "$agg"

# 2) each codec
for codec in null deflate zstd; do
    ${CLICKHOUSE_LOCAL} -q "
        SELECT number::Int64 AS i, toString(number) AS s, range(number % 6) AS arr,
               if(number % 3 = 0, NULL, number)::Nullable(UInt32) AS n
        FROM numbers(60000)
        INTO OUTFILE '${D}_${codec}.avro' FORMAT Avro
        SETTINGS output_format_avro_sync_interval = 256, output_format_avro_codec = '${codec}'"
    echo -n "codec ${codec}: "; check "${D}_${codec}.avro" "$struct" "$agg"
done

# 3) single-block / tiny file (degenerate one-segment path)
${CLICKHOUSE_LOCAL} -q "SELECT number::Int64 AS i FROM numbers(3) INTO OUTFILE '${D}_tiny.avro' FORMAT Avro"
echo -n "tiny: "; check "${D}_tiny.avro" "i Int64" "count(), sum(i)"

# 4) schema evolution: read a subset plus a missing-with-default column, under parallel parsing
${CLICKHOUSE_LOCAL} -q "
    SELECT number::Int64 AS a, toString(number) AS b, (number * 2)::Int64 AS c
    FROM numbers(50000)
    INTO OUTFILE '${D}_evo.avro' FORMAT Avro SETTINGS output_format_avro_sync_interval = 300"
seq=$(${CLICKHOUSE_LOCAL} -q "SELECT count(), sum(a), sum(c), sum(d) FROM file('${D}_evo.avro', 'Avro', 'a Int64, c Int64, d Int32') SETTINGS input_format_avro_allow_missing_fields=1, input_format_parallel_parsing=0")
par=$(${CLICKHOUSE_LOCAL} -q "SELECT count(), sum(a), sum(c), sum(d) FROM file('${D}_evo.avro', 'Avro', 'a Int64, c Int64, d Int32') SETTINGS input_format_avro_allow_missing_fields=1, input_format_parallel_parsing=1, max_threads=8, min_chunk_bytes_for_parallel_parsing=4096")
echo -n "schema-evolution: "; [ "$seq" = "$par" ] && echo "OK" || echo "MISMATCH seq=[$seq] par=[$par]"

rm -f "${D}"_*.avro
