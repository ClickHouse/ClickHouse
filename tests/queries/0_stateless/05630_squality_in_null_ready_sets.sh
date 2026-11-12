#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
CLUSTER_FN="icebergLocalCluster('test_cluster_two_shards_localhost', '${TABLE_PATH}', 'Parquet')"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (k Nullable(UInt8)) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (NULL), (1)"

query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_${RANDOM}"
${CLICKHOUSE_CLIENT} --query_id="${query_id}" --query "SELECT count() FROM ${CLUSTER_FN} WHERE k NOT IN (256) SETTINGS in_null_as_unknown = 1"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${CLUSTER_FN} WHERE k NOT IN (256) SETTINGS in_null_as_unknown = 0 FORMAT Null"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${CLUSTER_FN} WHERE k NOT IN (SELECT CAST(NULL AS Nullable(UInt8))) SETTINGS in_null_as_unknown = 0 FORMAT Null"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${CLUSTER_FN} WHERE k NOT IN (SELECT CAST(NULL AS Nullable(UInt8))) SETTINGS in_null_as_unknown = 1"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() > 0 AND countIf(Settings['cluster_table_function_pushed_down_filter'] != '') = count()
    FROM system.query_log
    WHERE initial_query_id = '${query_id}' AND type = 'QueryFinish' AND is_initial_query = 0 AND query LIKE '%${TABLE}%'
        AND initial_query_id IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND is_initial_query)"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH}"
