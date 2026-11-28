#!/usr/bin/env bash
# Tags: no-darwin
# - no-darwin - macOS has no /dev/full

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Prints the exit code of the client and what it reported after its output file failed.
function checkFailedSink()
{
    local err="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_$1.err"
    echo "exit code: $2"
    grep -Fq "CANNOT_WRITE_TO_FILE_DESCRIPTOR" "$err" && echo "write error reported"
    grep -Fc "Logical error" "$err"
    rm -f "$err"
}

echo "performing test: full_file"
${CLICKHOUSE_CLIENT} --query "SELECT number FROM numbers(1000000) INTO OUTFILE '/dev/full' TRUNCATE FORMAT JSON SETTINGS output_format_parallel_formatting = 0" 2>"${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_full_file.err"
checkFailedSink full_file "$?"

echo "performing test: full_file_gzip"
${CLICKHOUSE_CLIENT} --query "SELECT number FROM numbers(1000000) INTO OUTFILE '/dev/full' TRUNCATE COMPRESSION 'gzip' FORMAT TSV SETTINGS output_format_parallel_formatting = 0" 2>"${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_full_file_gzip.err"
checkFailedSink full_file_gzip "$?"
