#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
CLUSTER_FN="icebergLocalCluster('test_cluster_two_shards_localhost', '${TABLE_PATH}', 'Parquet')"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (k Nullable(UInt8)) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (NULL), (1), (2)"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM (SELECT * FROM ${TABLE} SETTINGS in_null_as_unknown = 0) WHERE isNull(k IN (1, NULL)) SETTINGS in_null_as_unknown = 1"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM (SELECT * FROM ${CLUSTER_FN} SETTINGS in_null_as_unknown = 0) WHERE isNull(k IN (1, NULL)) SETTINGS in_null_as_unknown = 1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH}"
