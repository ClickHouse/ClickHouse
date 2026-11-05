#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: bzip2 and IcebergLocal are not in the fast-test build

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"

function cleanup()
{
    rm -rf "${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"_*
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
}
cleanup
trap cleanup EXIT

function check()
{
    local out
    out=$(${CLICKHOUSE_CLIENT} --send_logs_level=fatal "$@" 2>&1) && echo "Unexpected success"
    grep -oE 'Compression level -?[0-9]+ is out of range for [a-z0-9]+\. \([A-Z0-9_]+\)' <<< "$out"
}

check --query "INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}_1.csv.gz', 'CSV') SELECT 1 SETTINGS output_format_compression_level = 18"
check --query "INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}_2.csv.deflate', 'CSV') SELECT 1 SETTINGS output_format_compression_level = 18"
check --query "INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}_3.csv.xz', 'CSV') SELECT 1 SETTINGS output_format_compression_level = 10"
check --query "INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}_4.csv.bz2', 'CSV') SELECT 1 SETTINGS output_format_compression_level = 0"

# Iceberg metadata files are compressed with the same level.
check --iceberg_metadata_compression_method=gzip --output_format_compression_level=18 \
    --query "CREATE TABLE ${TABLE} (k Int64) ENGINE = IcebergLocal('${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_5/', 'Parquet')"
