#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
#
# Iceberg `time` is a time of day of microsecond precision, read as `Time64(6)`. `Time` and `Time64`
# columns are written as it into Parquet, ORC and Avro data files, the partition values and the column
# bounds of the manifests, and must be read back the same with partition and min/max pruning.
# Values outside of [00:00:00, 24:00:00) are rejected on write.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX="t_${CLICKHOUSE_DATABASE}_${RANDOM}_time"
trap 'rm -rf "${USER_FILES_PATH:?}/${PREFIX:?}"_* 2>/dev/null' EXIT

WRITE="--allow_insert_into_iceberg=1"

for format in Parquet ORC Avro; do
    for type in 'Time' 'Time64(3)' 'Nullable(Time64(6))'; do
        echo "--- ${format} ${type} ---"
        table="${PREFIX}_${format}_${RANDOM}"
        path="${USER_FILES_PATH}/${table}/"

        ${CLICKHOUSE_CLIENT} --query "
            CREATE TABLE ${table} (key ${type}, value ${type}, comment String)
            ENGINE = IcebergLocal('${path}', '${format}')
            PARTITION BY key"
        # One insert per row, so that min/max pruning has files to skip.
        for row in "('12:00:00', '12:10:00', 'a')" "('13:00:00', '13:10:00.5', 'b')" "('14:00:00', '14:10:00', 'c')"; do
            ${CLICKHOUSE_CLIENT} ${WRITE} --query "INSERT INTO ${table} VALUES ${row}"
        done

        ${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${table} ORDER BY key"
        for pruning in 0 1; do
            ${CLICKHOUSE_CLIENT} --query "
                SELECT comment FROM ${table} WHERE key = '13:00:00' SETTINGS use_iceberg_partition_pruning = ${pruning}"
            ${CLICKHOUSE_CLIENT} --query "
                SELECT groupArray(comment) FROM (SELECT comment FROM ${table} WHERE key >= '13:00:00' ORDER BY comment)
                SETTINGS use_iceberg_partition_pruning = ${pruning}"
            ${CLICKHOUSE_CLIENT} --query "
                SELECT comment FROM ${table} WHERE value = '13:10:00.5' SETTINGS use_iceberg_partition_pruning = ${pruning}"
            ${CLICKHOUSE_CLIENT} --query "
                SELECT groupArray(comment) FROM (SELECT comment FROM ${table} WHERE value <= '13:10:00.5' ORDER BY comment)
                SETTINGS use_iceberg_partition_pruning = ${pruning}"
        done

        # One file of three is read: the others are skipped by the partition values and the column bounds.
        ${CLICKHOUSE_CLIENT} --query_id "${table}_partition" --query "SELECT comment FROM ${table} WHERE key = '13:00:00' FORMAT Null"
        ${CLICKHOUSE_CLIENT} --query_id "${table}_minmax" --query "SELECT comment FROM ${table} WHERE value = '13:10:00.5' FORMAT Null"
        ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
        ${CLICKHOUSE_CLIENT} --query "
            SELECT ProfileEvents['IcebergPartitionPrunedFiles'] + ProfileEvents['IcebergMinMaxIndexPrunedFiles'] FROM system.query_log
            WHERE current_database = currentDatabase() AND query_id IN ('${table}_partition', '${table}_minmax') AND type = 'QueryFinish'
            ORDER BY query_id DESC"

        # The table function reads the schema of the metadata, where the column is `time`.
        ${CLICKHOUSE_CLIENT} --query "
            SELECT toTypeName(key), key, value FROM icebergLocal('${path}') WHERE key = '13:00:00'"
        # Iceberg `time` was read as `Int64` microseconds before; the setting brings it back.
        ${CLICKHOUSE_CLIENT} --query "
            SELECT toTypeName(key), key, value FROM icebergLocal('${path}') WHERE key = 46800000000 ORDER BY key
            SETTINGS iceberg_time_as_int64 = 1"

        ${CLICKHOUSE_CLIENT} ${WRITE} --query "INSERT INTO ${table} VALUES ('100:00:00', '12:00:00', 'x')" 2>&1 \
            | grep -o -m1 'VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE\|ICEBERG_SPECIFICATION_VIOLATION' | head -n1
        ${CLICKHOUSE_CLIENT} ${WRITE} --query "INSERT INTO ${table} VALUES ('12:00:00', '-01:00:00', 'x')" 2>&1 \
            | grep -o -m1 'VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE\|ICEBERG_SPECIFICATION_VIOLATION' | head -n1
        ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${table}"

        ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table}"
    done
done

echo "--- bucket ---"
table="${PREFIX}_bucket"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${table} (t Time64(6), id UInt32)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${table}/', 'Parquet')
    PARTITION BY icebergBucket(4, t)"
${CLICKHOUSE_CLIENT} ${WRITE} --query "INSERT INTO ${table} SELECT toTime64('00:00:00', 6) + number * 600, number FROM numbers(20)"
for pruning in 0 1; do
    ${CLICKHOUSE_CLIENT} --query "SELECT id FROM ${table} WHERE t = '01:10:00' SETTINGS use_iceberg_partition_pruning = ${pruning}"
done
# The Iceberg spec example: `bucket` of 22:31:08 hashes the microseconds to -662762989.
${CLICKHOUSE_CLIENT} --query "SELECT icebergBucket(4, toTime64('22:31:08', 6)) = bitAnd(-662762989, 2147483647) % 4"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table}"

echo "--- write with iceberg_time_as_int64 ---"
# A `time` field would be written as a plain integer, which Iceberg does not allow.
table="${PREFIX}_as_int64"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${table} (t Time64(6), l Array(Time64(6)))
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${table}/', 'Parquet')"
${CLICKHOUSE_CLIENT} ${WRITE} --query "
    INSERT INTO TABLE FUNCTION icebergLocal('${USER_FILES_PATH}/${table}/') SETTINGS iceberg_time_as_int64 = 1
    VALUES (46800000000, [46800000000])" 2>&1 | grep -o -m1 'Cannot write column t of type Int64 into the Iceberg `time` field' | head -n1
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${table}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table}"

echo "--- unsupported ---"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${PREFIX}_ns (t Time64(9)) ENGINE = IcebergLocal('${USER_FILES_PATH}/${PREFIX}_ns/', 'Parquet')" 2>&1 \
    | grep -o -m1 'Unsupported type for iceberg Time64(9)' | head -n1
