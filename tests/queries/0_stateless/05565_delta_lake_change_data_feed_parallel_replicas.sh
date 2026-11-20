#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs and MinIO are not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# The data files of the change data feed of a Delta Lake table (`delta_lake_snapshot_start_version`) are
# not distributed between replicas. With parallel replicas the `deltaLakeS3` table function was replaced
# with its `Cluster` variant anyway, and every change was returned once per replica.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_TEST_UNIQUE_NAME}/t0"
DELTA_SETTINGS="--allow_experimental_delta_kernel_rs 1 --allow_delta_lake_writes 1"

# An empty table with the change data feed enabled: version 0 has only the protocol and the metadata.
SCHEMA='{\"type\":\"struct\",\"fields\":[{\"name\":\"c0\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}'
INITIAL_COMMIT="{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":7,\"writerFeatures\":[\"changeDataFeed\"]}}
{\"metaData\":{\"id\":\"${CLICKHOUSE_TEST_UNIQUE_NAME}\",\"format\":{\"provider\":\"parquet\",\"options\":{}},\"schemaString\":\"${SCHEMA}\",\"partitionColumns\":[],\"configuration\":{\"delta.enableChangeDataFeed\":\"true\"},\"createdTime\":1700000000000}}
"

echo -n "${INITIAL_COMMIT}" | ${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename = '${TABLE_PATH}/_delta_log/00000000000000000000.json', format = 'RawBLOB', structure = 'data String')
    FORMAT RawBLOB"

# Versions 1 and 2 add one data file each.
${CLICKHOUSE_CLIENT} $DELTA_SETTINGS --query "INSERT INTO FUNCTION deltaLakeS3(s3_conn, filename = '${TABLE_PATH}') VALUES (1), (2)"
${CLICKHOUSE_CLIENT} $DELTA_SETTINGS --query "INSERT INTO FUNCTION deltaLakeS3(s3_conn, filename = '${TABLE_PATH}') VALUES (3)"

PARALLEL_REPLICAS_SETTINGS="--enable_parallel_replicas 1 --max_parallel_replicas 3 --cluster_for_parallel_replicas test_cluster_one_shard_three_replicas_localhost --parallel_replicas_for_cluster_engines 1"

echo "-- change data feed from version 1"
${CLICKHOUSE_CLIENT} --allow_experimental_delta_kernel_rs 1 $PARALLEL_REPLICAS_SETTINGS --delta_lake_snapshot_start_version 1 --query "
    SELECT c0, _change_type, _commit_version FROM deltaLakeS3(s3_conn, filename = '${TABLE_PATH}') ORDER BY c0"

echo "-- change data feed of version 2 only"
${CLICKHOUSE_CLIENT} --allow_experimental_delta_kernel_rs 1 $PARALLEL_REPLICAS_SETTINGS --delta_lake_snapshot_start_version 2 --delta_lake_snapshot_end_version 2 --query "
    SELECT c0, _change_type, _commit_version FROM deltaLakeS3(s3_conn, filename = '${TABLE_PATH}') ORDER BY c0"

echo "-- the snapshot"
${CLICKHOUSE_CLIENT} --allow_experimental_delta_kernel_rs 1 $PARALLEL_REPLICAS_SETTINGS --query "
    SELECT c0 FROM deltaLakeS3(s3_conn, filename = '${TABLE_PATH}') ORDER BY c0"
