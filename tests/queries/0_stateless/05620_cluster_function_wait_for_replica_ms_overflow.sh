#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs IcebergLocal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (x Int64) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1), (2), (3)"

# The largest value of the setting must not overflow the deadline of the read.
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), sum(x) FROM icebergLocalCluster('test_shard_localhost', '${TABLE_PATH}', 'Parquet')
    SETTINGS cluster_function_wait_for_replica_ms = 9223372036854775"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH}"
