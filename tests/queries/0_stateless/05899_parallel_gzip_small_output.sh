#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

if ! command -v gzip &> /dev/null; then echo "gzip not found" 1>&2; exit 1; fi

# The parallel gzip deflater starts with a buffer of one block and grows it with the output, and a pass of one block
# is compressed without a thread pool job, so a small output costs what the single-threaded writer costs.
U="${CLICKHOUSE_TEST_UNIQUE_NAME}"
OUT="${CLICKHOUSE_TMP}/${U}.tsv.gz"
SMALL="SELECT number FROM numbers(10) SETTINGS max_threads = 1, output_format_parallel_formatting = 0 FORMAT TSV"
MEDIUM="SELECT number FROM numbers(100000) SETTINGS max_threads = 1, output_format_parallel_formatting = 0, max_block_size = 65536 FORMAT TSV"

function http_gzip()
{
    ${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: gzip' \
        "${CLICKHOUSE_URL}&enable_http_compression=1&max_generic_compression_threads=$2&query_id=${U}_$3" -d "$1" -o "$OUT"
    gzip -dc "$OUT" | wc -l
}

http_gzip "$SMALL" 1 small_1
http_gzip "$SMALL" 256 small_256
http_gzip "$MEDIUM" 1 medium_1
http_gzip "$MEDIUM" 256 medium_256
rm -f "$OUT"

# A file write larger than one block still grows its buffer and compresses its passes on the thread pool.
for threads in 1 8; do
    ${CLICKHOUSE_CLIENT} --query_id "${U}_file_${threads}" -q "INSERT INTO FUNCTION file('${U}_${threads}.tsv.gz', 'TSV', 'n UInt64') SELECT number FROM numbers(1000000) SETTINGS max_threads = 1, output_format_parallel_formatting = 0, engine_file_truncate_on_insert = 1, max_generic_compression_threads = ${threads}"
done
${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(n) FROM file('${U}_8.tsv.gz', 'TSV', 'n UInt64')"

# The query_log entry of an HTTP query is written after its response is sent.
for _ in {1..60}; do
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
    count=$(${CLICKHOUSE_CLIENT} -q "
        SELECT count() FROM system.query_log
        WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND startsWith(query_id, '${U}_')")
    [ "$count" -ge 6 ] && break
    sleep 0.5
done
${CLICKHOUSE_CLIENT} -q "
    SELECT
        count(),
        sumIf(memory_usage, query_id = '${U}_small_256') < sumIf(memory_usage, query_id = '${U}_small_1') + 32 * 1024 * 1024,
        sumIf(ProfileEvents['LocalThreadPoolJobs'], query_id = '${U}_small_256') = sumIf(ProfileEvents['LocalThreadPoolJobs'], query_id = '${U}_small_1'),
        sumIf(memory_usage, query_id = '${U}_medium_256') < sumIf(memory_usage, query_id = '${U}_medium_1') + 32 * 1024 * 1024,
        sumIf(ProfileEvents['LocalThreadPoolJobs'], query_id = '${U}_file_8') > sumIf(ProfileEvents['LocalThreadPoolJobs'], query_id = '${U}_file_1')
    FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND startsWith(query_id, '${U}_')"

rm -f "${CLICKHOUSE_USER_FILES_UNIQUE}_1.tsv.gz" "${CLICKHOUSE_USER_FILES_UNIQUE}_8.tsv.gz"
