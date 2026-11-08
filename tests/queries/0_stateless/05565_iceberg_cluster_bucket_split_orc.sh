#!/usr/bin/env bash
# Tags: no-fasttest

# With `cluster_table_function_split_granularity = 'bucket'` the bucket splitter is chosen by the format of each
# data file. A table with Parquet and ORC data files must be read in full: the Parquet file is split by row groups,
# and the ORC file, which has no splitter, is read as a whole.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
CHECK_QUERY="SELECT count(), sum(id)"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}', 'ORC')"

# One Parquet data file with several row groups, then one ORC data file.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO TABLE FUNCTION icebergLocal('${TABLE_PATH}', 'Parquet', 'id Int64') SELECT number FROM numbers(100000) SETTINGS output_format_parquet_row_group_size = 10000"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} SELECT number + 100000 FROM numbers(100000)"

echo "--- local"
${CLICKHOUSE_CLIENT} --query "${CHECK_QUERY} FROM ${TABLE}"
echo "--- cluster, bucket"
${CLICKHOUSE_CLIENT} --query "${CHECK_QUERY} FROM icebergLocalCluster('test_cluster_two_shards_localhost', '${TABLE_PATH}') SETTINGS cluster_table_function_split_granularity = 'bucket'"
echo "--- cluster, bucket, split offsets"
${CLICKHOUSE_CLIENT} --query "${CHECK_QUERY} FROM icebergLocalCluster('test_cluster_two_shards_localhost', '${TABLE_PATH}') SETTINGS cluster_table_function_split_granularity = 'bucket', cluster_table_function_split_by_split_offsets = 1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"
