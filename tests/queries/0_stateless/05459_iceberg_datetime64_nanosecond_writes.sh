#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: requires `IcebergLocal` (USE_AVRO).
# - no-replicated-database: `IcebergLocal` is non-replicated.

# Iceberg `timestamp_ns` / `timestamptz_ns` are v3 types. Mapping `DateTime64(9)` to
# `timestamp` while dumping nanosecond ticks over-prunes readers that treat the
# column as microseconds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"
mkdir -p "${CLICKHOUSE_USER_FILES_UNIQUE}"
trap 'rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"' EXIT

CLIENT="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --session_timezone UTC"
DB="${CLICKHOUSE_DATABASE}"

require_v3()
{
    grep -o 'requires format version 3' || echo 'NO_ERROR'
}

echo -n "create_v2: "
${CLIENT} --query "
CREATE TABLE t_v2_${DB} (ts DateTime64(9), id Int32)
ENGINE = IcebergLocal('${CLICKHOUSE_USER_FILES_UNIQUE}/v2/', 'Parquet')
SETTINGS iceberg_format_version = 2
" 2>&1 | require_v3

echo -n "create_default: "
${CLIENT} --query "
CREATE TABLE t_def_${DB} (ts DateTime64(9), id Int32)
ENGINE = IcebergLocal('${CLICKHOUSE_USER_FILES_UNIQUE}/def/', 'Parquet')
" 2>&1 | require_v3

echo -n "create_v1: "
${CLIENT} --query "
CREATE TABLE t_v1_${DB} (ts DateTime64(9), id Int32)
ENGINE = IcebergLocal('${CLICKHOUSE_USER_FILES_UNIQUE}/v1/', 'Parquet')
SETTINGS iceberg_format_version = 1
" 2>&1 | require_v3

echo -n "create_nested_v2: "
${CLIENT} --query "
CREATE TABLE t_nested_${DB} (ts Array(DateTime64(9)), id Int32)
ENGINE = IcebergLocal('${CLICKHOUSE_USER_FILES_UNIQUE}/nested/', 'Parquet')
SETTINGS iceberg_format_version = 2
" 2>&1 | require_v3

${CLIENT} --query "
CREATE TABLE t_v3_${DB} (ts DateTime64(9), value DateTime64(9), id Int32)
ENGINE = IcebergLocal('${CLICKHOUSE_USER_FILES_UNIQUE}/v3/', 'Parquet')
SETTINGS iceberg_format_version = 3
"

echo "describe_v3:"
${CLIENT} --query "DESCRIBE TABLE t_v3_${DB}" | awk -F'\t' '{print $1 "\t" $2}'

${CLIENT} --query "
INSERT INTO t_v3_${DB} VALUES
    (toDateTime64('2020-01-02 03:04:05.123456789', 9), toDateTime64('2021-02-03 04:05:06.987654321', 9), 1)
"

echo -n "select_v3: "
${CLIENT} --query "SELECT ts, value, id FROM t_v3_${DB}"

echo -n "prune_v3: "
${CLIENT} --query "
SELECT count() FROM t_v3_${DB}
WHERE ts = toDateTime64('2020-01-02 03:04:05.123456789', 9)
"

${CLIENT} --query "
CREATE TABLE t_tz_${DB} (ts DateTime64(9, 'UTC'), id Int32)
ENGINE = IcebergLocal('${CLICKHOUSE_USER_FILES_UNIQUE}/tz/', 'Parquet')
SETTINGS iceberg_format_version = 3
"
echo -n "describe_tz: "
${CLIENT} --query "DESCRIBE TABLE t_tz_${DB}" | awk -F'\t' 'NR==1 {print $1 "\t" $2}'

${CLIENT} --query "
CREATE TABLE t_part_${DB} (ts DateTime64(9), id Int32)
ENGINE = IcebergLocal('${CLICKHOUSE_USER_FILES_UNIQUE}/part/', 'Parquet')
PARTITION BY ts
SETTINGS iceberg_format_version = 3
"
${CLIENT} --query "
INSERT INTO t_part_${DB} VALUES (toDateTime64('2020-01-02 03:04:05.123456789', 9), 7)
"
echo -n "partitioned: "
${CLIENT} --query "SELECT ts, id FROM t_part_${DB}"

${CLIENT} --query "
CREATE TABLE t_alter_v2_${DB} (ts DateTime64(6), id Int32)
ENGINE = IcebergLocal('${CLICKHOUSE_USER_FILES_UNIQUE}/alter_v2/', 'Parquet')
SETTINGS iceberg_format_version = 2
"
echo -n "alter_add_v2: "
${CLIENT} --query "ALTER TABLE t_alter_v2_${DB} ADD COLUMN ts_ns Nullable(DateTime64(9))" 2>&1 | require_v3
echo -n "alter_modify_v2: "
${CLIENT} --query "ALTER TABLE t_alter_v2_${DB} MODIFY COLUMN ts DateTime64(9)" 2>&1 | require_v3

${CLIENT} --query "
CREATE TABLE t_alter_v3_${DB} (ts DateTime64(6), id Int32)
ENGINE = IcebergLocal('${CLICKHOUSE_USER_FILES_UNIQUE}/alter_v3/', 'Parquet')
SETTINGS iceberg_format_version = 3
"
${CLIENT} --query "ALTER TABLE t_alter_v3_${DB} ADD COLUMN ts_ns Nullable(DateTime64(9))"
echo -n "alter_add_v3: "
${CLIENT} --query "DESCRIBE TABLE t_alter_v3_${DB}" | awk -F'\t' '$1=="ts_ns" {print $1 "\t" $2}'

${CLIENT} --query "
DROP TABLE t_v3_${DB} SYNC;
DROP TABLE t_tz_${DB} SYNC;
DROP TABLE t_part_${DB} SYNC;
DROP TABLE t_alter_v2_${DB} SYNC;
DROP TABLE t_alter_v3_${DB} SYNC;
"
