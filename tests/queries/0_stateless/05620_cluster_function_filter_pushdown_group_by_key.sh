#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
CLUSTER_FN="icebergLocalCluster('test_shard_localhost', '${TABLE_PATH}', 'Parquet')"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (a Int64, b Int64) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1, 2), (2, 1), (1, 1), (3, 3)"

# A filter on a GROUP BY key must not reach the replicas as a filter on another column.
${CLICKHOUSE_CLIENT} --query "SELECT b, a + 0 AS x, count() FROM ${CLUSTER_FN} GROUP BY x, b HAVING x = 2 ORDER BY ALL"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM (SELECT b, a + 0 AS x, count() FROM ${CLUSTER_FN} GROUP BY x, b) WHERE x = 2 ORDER BY ALL"
${CLICKHOUSE_CLIENT} --query "SELECT a AS b, t.b AS c, count() FROM ${CLUSTER_FN} AS t GROUP BY 1, 2 HAVING t.b = 2 ORDER BY ALL"
# Nor as a filter on a column whose name is the alias of another one.
${CLICKHOUSE_CLIENT} --query "SELECT t.b AS c, t.a AS b FROM ${CLUSTER_FN} AS t SETTINGS additional_result_filter = 'c = 2'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"
