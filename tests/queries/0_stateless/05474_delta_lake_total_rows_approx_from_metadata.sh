#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# A read of a Delta Lake table reports the `numRecords` of the data files it reads as the total
# number of rows to read, so that the progress of the query is known.
# https://github.com/ClickHouse/ClickHouse/issues/128400

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}"
TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}_delta"
rm -rf "${TABLE_PATH}"

DELTA_SETTINGS="--allow_experimental_delta_kernel_rs 1 --allow_delta_lake_writes 1 --allow_delta_lake_create_table 1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
# shellcheck disable=SC2086
${CLICKHOUSE_CLIENT} ${DELTA_SETTINGS} --query "CREATE TABLE ${TABLE} (x Int64) ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet)"
# shellcheck disable=SC2086
${CLICKHOUSE_CLIENT} ${DELTA_SETTINGS} --query "INSERT INTO ${TABLE} SELECT number FROM numbers(100)"
# shellcheck disable=SC2086
${CLICKHOUSE_CLIENT} ${DELTA_SETTINGS} --query "INSERT INTO ${TABLE} SELECT number FROM numbers(200)"

${CLICKHOUSE_CURL} -sS -D - -o /dev/null "${CLICKHOUSE_URL}&wait_end_of_query=1&use_query_condition_cache=0&allow_experimental_delta_kernel_rs=1" \
    -d "SELECT sum(x) FROM ${TABLE}" | grep -i '^X-ClickHouse-Summary' | grep -o '"total_rows_to_read":"[0-9]*"'

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH}"
