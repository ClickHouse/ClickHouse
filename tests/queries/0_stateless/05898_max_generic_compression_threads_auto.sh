#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

if ! command -v gzip &> /dev/null; then echo "gzip not found" 1>&2; exit 1; fi

# `max_generic_compression_threads` is a `MaxThreads` setting, `auto` (the number of CPU cores) by default.
${CLICKHOUSE_CLIENT} -q "SELECT type, default LIKE 'auto(%)' FROM system.settings WHERE name = 'max_generic_compression_threads'"
${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.settings WHERE name = 'max_generic_compression_threads' SETTINGS max_generic_compression_threads = 1"
${CLICKHOUSE_CLIENT} -q "SELECT value LIKE 'auto(%)' FROM system.settings WHERE name = 'max_generic_compression_threads' SETTINGS max_generic_compression_threads = 0"
${CLICKHOUSE_CLIENT} -q "SELECT value LIKE 'auto(%)' FROM system.settings WHERE name = 'max_generic_compression_threads' SETTINGS max_generic_compression_threads = 'auto'"
# Versions before 26.12 compressed gzip on one thread.
${CLICKHOUSE_CLIENT} -q "SET compatibility = '26.11'; SELECT getSetting('max_generic_compression_threads')"

# The number of threads `auto` resolves to on this server, read without relying on the default.
threads=$(${CLICKHOUSE_CLIENT} -q "SELECT getSetting('max_generic_compression_threads') SETTINGS max_generic_compression_threads = 0")

# With the default, gzip output goes through the parallel deflater whenever `auto` is more than one thread: it is framed
# differently from the single-threaded writer and decompresses to the same bytes. One reading thread and no parallel
# formatting make the bytes and the flush points deterministic, so two single-threaded outputs would be byte-identical.
QUERY="SELECT number FROM numbers(100000) SETTINGS max_threads = 1, output_format_parallel_formatting = 0 FORMAT TSV"
DEF="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_default.tsv.gz"
ONE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_one.tsv.gz"
rm -f "$DEF" "$ONE"
${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: gzip' "${CLICKHOUSE_URL}&enable_http_compression=1" -d "$QUERY" -o "$DEF"
${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: gzip' "${CLICKHOUSE_URL}&enable_http_compression=1&max_generic_compression_threads=1" -d "$QUERY" -o "$ONE"
diff <(gzip -dc "$DEF") <(gzip -dc "$ONE") > /dev/null && echo "http: default and single-threaded responses decompress identically"
{ [ "$threads" -eq 1 ] || ! cmp -s "$DEF" "$ONE"; } && echo "http: the default response is compressed in parallel"
rm -f "$DEF" "$ONE"

# The same for a file written by the server.
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}_default.tsv.gz', 'TSV', 'n UInt64') SELECT number FROM numbers(100000) SETTINGS max_threads = 1, output_format_parallel_formatting = 0, engine_file_truncate_on_insert = 1"
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}_one.tsv.gz', 'TSV', 'n UInt64') SELECT number FROM numbers(100000) SETTINGS max_threads = 1, output_format_parallel_formatting = 0, engine_file_truncate_on_insert = 1, max_generic_compression_threads = 1"
${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(n) FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}_default.tsv.gz', 'TSV', 'n UInt64')"
${CLICKHOUSE_CLIENT} -q "SELECT ${threads} = 1 OR (SELECT raw FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}_default.tsv.gz', 'RawBLOB', 'raw String', 'none')) != (SELECT raw FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}_one.tsv.gz', 'RawBLOB', 'raw String', 'none'))"
