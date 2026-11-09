#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
#
# An Iceberg `timestamp` (without time zone) holds a wall-clock value: it must be shown as stored and
# compared with literals the same way in any session time zone, for tables written by Spark and by
# ClickHouse, with and without pruning. https://github.com/ClickHouse/ClickHouse/issues/130025
#
# Every statement pins `session_timezone`, because the test runner randomizes that setting.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ts_ntz` is a Spark `TIMESTAMP_NTZ` (Iceberg `timestamp`) column, row `id` holds `id * 3600000001` microseconds:
# Spark reads row 7 back as `1970-01-01 07:00:00.000007`. `icebergLocal` in `clickhouse-local` only reads
# paths inside the current directory.
echo "--- table written by Spark, Asia/Shanghai session ---"
cd "$CUR_DIR/data_lake_corpus" || exit 1
$CLICKHOUSE_LOCAL --session_timezone Asia/Shanghai -q "
    SELECT toTypeName(ts_ntz) FROM icebergLocal('iceberg/v2_all_types') LIMIT 1;
    SELECT ts_ntz FROM icebergLocal('iceberg/v2_all_types') WHERE id = 7;
    SELECT count() FROM icebergLocal('iceberg/v2_all_types') WHERE ts_ntz = '1970-01-01 07:00:00.000007'
        SETTINGS use_iceberg_partition_pruning = 1;
    SELECT count() FROM icebergLocal('iceberg/v2_all_types') WHERE ts_ntz = '1970-01-01 07:00:00.000007'
        SETTINGS use_iceberg_partition_pruning = 0;
    SELECT count() FROM icebergLocal('iceberg/v2_all_types') WHERE ts_ntz < '1970-01-01 08:00:00'
        SETTINGS use_iceberg_partition_pruning = 1;
    SELECT count() FROM icebergLocal('iceberg/v2_all_types') WHERE ts_ntz < '1970-01-01 08:00:00'
        SETTINGS use_iceberg_partition_pruning = 0;
"

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_ts"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

# Identity partitioning puts the timestamp into every manifest as a partition field, and the two INSERTs
# write two data files. Background Iceberg compaction would rewrite the manifests, so it is pinned off.
${CLICKHOUSE_CLIENT} --session_timezone UTC --query "
    CREATE TABLE ${TABLE} (ts DateTime64(6), id Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') PARTITION BY (ts)
    SETTINGS allow_experimental_iceberg_compaction = 0;
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO ${TABLE} SETTINGS session_timezone = 'Asia/Shanghai' VALUES ('2024-06-11 12:34:56.123456', 1);
    INSERT INTO ${TABLE} SETTINGS session_timezone = 'UTC' VALUES ('2024-06-12 01:00:00', 2);
"

echo "--- table written by ClickHouse, rows as inserted ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT ts, id FROM ${TABLE} ORDER BY id SETTINGS session_timezone = 'UTC' FORMAT TSV"

echo "--- table written by ClickHouse, Asia/Shanghai session ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT ts FROM ${TABLE} WHERE id = 1 SETTINGS session_timezone = 'Asia/Shanghai';
    SELECT count() FROM ${TABLE} WHERE ts = '2024-06-11 12:34:56.123456'
        SETTINGS session_timezone = 'Asia/Shanghai', use_iceberg_partition_pruning = 1;
    SELECT count() FROM ${TABLE} WHERE ts = '2024-06-11 12:34:56.123456'
        SETTINGS session_timezone = 'Asia/Shanghai', use_iceberg_partition_pruning = 0;
    SELECT count() FROM icebergLocal('${TABLE_PATH}') WHERE ts = '2024-06-11 12:34:56.123456'
        SETTINGS session_timezone = 'Asia/Shanghai', use_iceberg_partition_pruning = 1;
    SELECT count() FROM icebergLocal('${TABLE_PATH}') WHERE ts = '2024-06-11 12:34:56.123456'
        SETTINGS session_timezone = 'Asia/Shanghai', use_iceberg_partition_pruning = 0;"

# The Avro type of a `timestamp` partition field in a manifest is `timestamp-micros` with
# `adjust-to-utc: false`; `true` would declare it a `timestamptz`.
echo "--- manifest partition field type ---"
grep -aho '"adjust-to-utc":[a-z]*' "${TABLE_PATH}metadata/"*.avro | sort -u

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE} SYNC"
