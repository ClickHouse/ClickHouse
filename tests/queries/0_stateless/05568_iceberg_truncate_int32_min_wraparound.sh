#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Fixture written by PyIceberg 0.11.1 with pyiceberg-core 0.8.0: `(id long, x int)` partitioned by `truncate[10](x)`,
# one append of x = -2147483648, -2147483641, -2147483640, -1, 7, 2147483647; it stores 2147483646 for the first two.
# `promoted`: the same rows, then `x` promoted to `long`, one more append that merges the manifests, and the first
# snapshot expired.
# `java_wide`: written by Spark 4.1 with Iceberg 1.11.0, `truncate[1073741825](x)`, x = -2147483648 and 1073741824; Java's
# 32-bit `(x % W) + W` stores both under 2147483646.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LAKE="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"
cp -r "${CUR_DIR}/data_minio/iceberg_truncate_int32_wraparound/t" "${LAKE}/t"
cp -r "${CUR_DIR}/data_minio/iceberg_truncate_int32_wraparound/promoted" "${LAKE}/promoted"
cp -r "${CUR_DIR}/data_minio/iceberg_truncate_int32_wraparound/java_wide" "${LAKE}/java_wide"

echo '--- ids with partition pruning on and off'
for predicate in 'x = -2147483648' 'x <= 7' 'x IN (-2147483648, 7)' 'x > 7'; do
    ${CLICKHOUSE_CLIENT} --query "
        SELECT '${predicate}',
            (SELECT groupArray(id) FROM (SELECT id FROM icebergLocal('${LAKE}/t') WHERE ${predicate} ORDER BY id)
                SETTINGS use_iceberg_partition_pruning = 1),
            (SELECT groupArray(id) FROM (SELECT id FROM icebergLocal('${LAKE}/t') WHERE ${predicate} ORDER BY id)
                SETTINGS use_iceberg_partition_pruning = 0)"
done

echo '--- files pruned by partition and by min/max, manifests pruned by the manifest list'
${CLICKHOUSE_CLIENT} --query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_eq" --use_iceberg_partition_pruning=1 \
    --query "SELECT id FROM icebergLocal('${LAKE}/t') WHERE x = -2147483648 FORMAT Null"
${CLICKHOUSE_CLIENT} --query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_gt" --use_iceberg_partition_pruning=1 \
    --query "SELECT id FROM icebergLocal('${LAKE}/t') WHERE x > 7 FORMAT Null"
${CLICKHOUSE_CLIENT} --query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_mf" --use_iceberg_partition_pruning=1 \
    --query "SELECT id FROM icebergLocal('${LAKE}/promoted') WHERE x = -2147483651 FORMAT Null"
${CLICKHOUSE_CLIENT} --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT
        replaceOne(query_id, '${CLICKHOUSE_TEST_UNIQUE_NAME}_', ''),
        ProfileEvents['IcebergPartitionPrunedFiles'],
        ProfileEvents['IcebergMinMaxIndexPrunedFiles'],
        ProfileEvents['IcebergPartitionPrunedManifestFiles']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
        AND query_id IN ('${CLICKHOUSE_TEST_UNIQUE_NAME}_eq', '${CLICKHOUSE_TEST_UNIQUE_NAME}_gt', '${CLICKHOUSE_TEST_UNIQUE_NAME}_mf')
    ORDER BY query_id"

echo '--- column promoted to long, adding snapshot expired'
for predicate in 'x = -2147483648' 'x IN (-2147483648, 7)'; do
    ${CLICKHOUSE_CLIENT} --query "
        SELECT '${predicate}',
            (SELECT groupArray(id) FROM (SELECT id FROM icebergLocal('${LAKE}/promoted') WHERE ${predicate} ORDER BY id)
                SETTINGS use_iceberg_partition_pruning = 1),
            (SELECT groupArray(id) FROM (SELECT id FROM icebergLocal('${LAKE}/promoted') WHERE ${predicate} ORDER BY id)
                SETTINGS use_iceberg_partition_pruning = 0)"
done

echo '--- truncate[1073741825] written by Java'
for predicate in 'x > 0' 'x = 1073741824' 'x = -2147483648'; do
    ${CLICKHOUSE_CLIENT} --query "
        SELECT '${predicate}',
            (SELECT groupArray(id) FROM (SELECT id FROM icebergLocal('${LAKE}/java_wide') WHERE ${predicate} ORDER BY id)
                SETTINGS use_iceberg_partition_pruning = 1),
            (SELECT groupArray(id) FROM (SELECT id FROM icebergLocal('${LAKE}/java_wide') WHERE ${predicate} ORDER BY id)
                SETTINGS use_iceberg_partition_pruning = 0)"
done

rm -rf "${LAKE}"
