#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `Pretty` formats write the squashed blocks from their own thread when `output_format_pretty_squash_consecutive_ms`
# is nonzero. The size of the written data is checked after every block for splitting, and it must not race with
# that thread (checked by TSan). Every block of 10 rows takes 20 ms, much longer than the squashing interval, so the
# blocks are written by that thread one by one. The number of the resulting files depends on the timing.

DIR="${CLICKHOUSE_TMP}/05568_split_on_write_pretty_squash"
rm -rf "${DIR}"
mkdir -p "${DIR}"

SETTINGS="max_threads = 1, max_insert_threads = 1, max_block_size = 10, min_insert_block_size_rows = 10, min_insert_block_size_bytes = 0, output_format_pretty_squash_consecutive_ms = 1, output_format_pretty_max_rows = 1000000, output_format_pretty_color = 0, output_format_pretty_row_numbers = 0"

echo '--- File'
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/data.txt', Pretty, 'x UInt64') SELECT number + sleepEachRow(0.002) FROM numbers(300) SETTINGS ${SETTINGS}, engine_file_split_on_write_by_size_bytes = 1000;
"
# Every row of the data is a line with its number between the borders.
cat "${DIR}"/data*.txt | grep -c -E '^│ +[0-9]+ │$'
ls "${DIR}" | wc -l | awk '{ print ($1 > 1) }'

echo '--- S3'
PREFIX="05568_split_on_write_pretty_squash/${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename='${PREFIX}/data.txt', format=Pretty, structure='x UInt64')
        SELECT number + sleepEachRow(0.002) FROM numbers(300) SETTINGS ${SETTINGS}, s3_split_on_write_by_size_bytes = 1000;
    SELECT countIf(match(line, '^│ +[0-9]+ │$')), uniqExact(_file) > 1 FROM s3(s3_conn, filename='${PREFIX}/data*.txt', format=LineAsString, structure='line String');
"

rm -rf "${DIR}"
