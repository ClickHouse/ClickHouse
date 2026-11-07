#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: bzip2, IcebergLocal and BACKUP to a zip archive are not in the fast-test build

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"

function cleanup()
{
    rm -rf "${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"_*
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}_zip"
}
cleanup
trap cleanup EXIT

function check()
{
    local out
    out=$(${CLICKHOUSE_CLIENT} --send_logs_level=fatal "$@" 2>&1) && echo "Unexpected success"
    grep -oE '(Compression level -?[0-9]+ is out of range for [a-z0-9]+|(bzip2 stream encoder|zstd_stream_decoder) init failed for (compression level|zstd_window_log_max =) -?[0-9]+: [A-Za-z ]+( \([A-Z_]+\))?)(: While executing File)?\. \([A-Z0-9_]+\)' <<< "$out"
}

function check_read()
{
    local out
    out=$(${CLICKHOUSE_CLIENT} --send_logs_level=fatal --query "$1" 2>&1) && echo "Unexpected success"
    grep -oE '(bzip2 stream decoder|lzma_stream_decoder) failed: [A-Za-z0-9 ]+ \([A-Z_]+\)' <<< "$out"
    grep -oE '\((BZIP2|LZMA)_STREAM_DECODER_FAILED\)$' <<< "$out"
}

check --query "INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}_1.csv.gz', 'CSV') SELECT 1 SETTINGS output_format_compression_level = 18"
check --query "INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}_2.csv.deflate', 'CSV') SELECT 1 SETTINGS output_format_compression_level = 18"
check --query "INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}_3.csv.xz', 'CSV') SELECT 1 SETTINGS output_format_compression_level = 10"
check --query "INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}_4.csv.bz2', 'CSV') SELECT 1 SETTINGS output_format_compression_level = 0"

# Iceberg metadata files are compressed with the same level.
check --iceberg_metadata_compression_method=gzip --output_format_compression_level=18 \
    --query "CREATE TABLE ${TABLE} (k Int64) ENGINE = IcebergLocal('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_5/', 'Parquet')"

${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}_6.csv.zst', 'CSV', 'x UInt64') SELECT 1"
check --query "SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}_6.csv.zst', 'CSV', 'x UInt64') SETTINGS zstd_window_log_max = 9"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE}_zip (k Int64) ENGINE = MergeTree ORDER BY k"
for method in deflate bzip2 lzma xz; do
    check --query "BACKUP TABLE ${TABLE}_zip TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_7_${method}.zip') SETTINGS compression_method = '${method}', compression_level = 10"
done
check --query "BACKUP TABLE ${TABLE}_zip TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_7_low.zip') SETTINGS compression_method = 'bzip2', compression_level = -2"

# A library status other than an invalid value is described too.
for ext in bz2 xz; do
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}_8.tsv.${ext}', 'TSV', 's String', 'none') SELECT 'not compressed'"
    check_read "SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}_8.tsv.${ext}', 'TSV', 's String')"
done
