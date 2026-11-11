#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet

# The query condition cache entries of an `ORDER BY ... LIMIT` read of a `File` table must not be reused by a
# read of another table with a filter on `_table`. Tables without a UUID (here in an `Ordinary` database) share
# the synthetic `table_id` of their file, and a table with a UUID keeps it on `RENAME`, but the same filter on
# `_table` selects different rows in them and so makes a different TopK threshold.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${CLICKHOUSE_DATABASE}_05427"
ORDINARY_DB="${CLICKHOUSE_DATABASE}_05427_ordinary"
rm -rf "${USER_FILES_PATH:?}/${DATA_DIR}"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/rows.parquet', Parquet, 'k UInt64') SELECT number FROM numbers(100000)
    SETTINGS output_format_parquet_row_group_size = 1000"
# Backdate the file so its version token has settled and the cache engages.
touch -d '2020-01-01 00:00:00' "${USER_FILES_PATH}/${DATA_DIR}/rows.parquet"

SETTINGS="use_query_condition_cache = 1, use_query_condition_cache_for_top_k = 1, use_top_k_dynamic_filtering = 1,
    query_plan_max_limit_for_top_k_optimization = 1000, max_threads = 1, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0"

function run()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT groupArray(k) FROM (SELECT k FROM $1 WHERE (_table = 't1' OR k < 100) ORDER BY k DESC LIMIT 3) SETTINGS ${SETTINGS}"
}

echo "Ordinary database"
# `--send_logs_level=fatal` keeps the `Ordinary` engine deprecation warning out of the test's stderr.
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --allow_deprecated_database_ordinary 1 --query "CREATE DATABASE ${ORDINARY_DB} ENGINE = Ordinary"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${ORDINARY_DB}.t1 (k UInt64) ENGINE = File(Parquet, '${DATA_DIR}/rows.parquet')"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${ORDINARY_DB}.t2 (k UInt64) ENGINE = File(Parquet, '${DATA_DIR}/rows.parquet')"
run "${ORDINARY_DB}.t1"
run "${ORDINARY_DB}.t2"
run "${ORDINARY_DB}.t1"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${ORDINARY_DB}"

echo "RENAME"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t1 (k UInt64) ENGINE = File(Parquet, '${DATA_DIR}/rows.parquet')"
run t1
${CLICKHOUSE_CLIENT} --query "RENAME TABLE t1 TO t2"
run t2
${CLICKHOUSE_CLIENT} --query "DROP TABLE t2"

rm -rf "${USER_FILES_PATH:?}/${DATA_DIR}"
