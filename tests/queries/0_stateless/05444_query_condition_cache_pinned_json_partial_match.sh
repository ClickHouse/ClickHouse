#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet

# Two `File` tables without a UUID (in an `Ordinary` database) over the same Parquet file, with the same
# format, compression and structure, but with a different `type_json_use_partial_match_to_skip_paths_by_regexp`
# fixed at `CREATE`. It decides which paths of a `JSON` column decoded from a variant are skipped, so the
# query condition cache entries of one table must not be reused by the other.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${CLICKHOUSE_DATABASE}_05444"
mkdir -p "${USER_FILES_PATH:?}/${DATA_DIR}"
DB="${CLICKHOUSE_DATABASE}_ordinary"

cp "${CUR_DIR}/data_parquet/05299_variant_read_as_json.parquet" "${USER_FILES_PATH}/${DATA_DIR}/variant.parquet"
# Backdate the file so its version token has settled and the cache engages.
touch -d '2020-01-01 00:00:00' "${USER_FILES_PATH}/${DATA_DIR}/variant.parquet"

# `--send_logs_level=fatal` keeps the `Ordinary` engine deprecation warning out of the test's stderr.
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --allow_deprecated_database_ordinary 1 --query "CREATE DATABASE ${DB} ENGINE = Ordinary"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${DB}.partial (n Int64, v JSON(SKIP REGEXP 'c')) ENGINE = File(Parquet, '${DATA_DIR}/variant.parquet')
    SETTINGS type_json_use_partial_match_to_skip_paths_by_regexp = 1"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${DB}.full (n Int64, v JSON(SKIP REGEXP 'c')) ENGINE = File(Parquet, '${DATA_DIR}/variant.parquet')
    SETTINGS type_json_use_partial_match_to_skip_paths_by_regexp = 0"

SETTINGS="use_query_condition_cache = 1, use_columns_cache = 0, max_threads = 1"
CONDITION="position(toJSONString(v), '\"c\"') > 0"

${CLICKHOUSE_CLIENT} --query "SELECT 'partial', sum(n) FROM ${DB}.partial PREWHERE ${CONDITION} SETTINGS ${SETTINGS}"
${CLICKHOUSE_CLIENT} --query "SELECT 'full', sum(n) FROM ${DB}.full PREWHERE ${CONDITION} SETTINGS ${SETTINGS}"
${CLICKHOUSE_CLIENT} --query "SELECT 'full without cache', sum(n) FROM ${DB}.full PREWHERE ${CONDITION} SETTINGS ${SETTINGS}, use_query_condition_cache = 0"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB}"
rm -rf "${USER_FILES_PATH:?}/${DATA_DIR}"
