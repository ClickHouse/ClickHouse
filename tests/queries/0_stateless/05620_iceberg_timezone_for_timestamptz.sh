#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_timestamptz"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}"
BERLIN_TABLE="${TABLE}_berlin"
IDENTITY_TABLE="${TABLE}_identity"
IDENTITY_TABLE_PATH="${USER_FILES_PATH}/${IDENTITY_TABLE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${BERLIN_TABLE}"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${IDENTITY_TABLE}"
    rm -rf "${TABLE_PATH}" "${IDENTITY_TABLE_PATH}"
}
trap cleanup EXIT

create_metadata()
{
    mkdir -p "$1/metadata"
    cat > "$1/metadata/v1.metadata.json" << EOF
{
  "format-version": 2,
  "table-uuid": "8f2c1a6e-3b0d-4c55-9a51-2d7f0e4b6c11",
  "location": "$1",
  "last-sequence-number": 0,
  "last-updated-ms": 1700000000000,
  "last-column-id": 5,
  "current-schema-id": 0,
  "schemas": [{"type": "struct", "schema-id": 0, "fields": [$2]}],
  "default-spec-id": 0,
  "partition-specs": [{"spec-id": 0, "fields": [$3]}],
  "last-partition-id": 1000,
  "default-sort-order-id": 0,
  "sort-orders": [{"order-id": 0, "fields": []}],
  "properties": {},
  "current-snapshot-id": -1,
  "snapshots": [],
  "snapshot-log": [],
  "metadata-log": []
}
EOF
}

create_metadata "${TABLE_PATH}" '
    {"id": 1, "name": "id", "required": false, "type": "int"},
    {"id": 2, "name": "ts", "required": false, "type": "timestamptz"},
    {"id": 3, "name": "local", "required": false, "type": "timestamp"},
    {"id": 4, "name": "arr", "required": false, "type": {"type": "list", "element-id": 5, "element": "timestamptz", "element-required": false}}' \
    '{"source-id": 2, "field-id": 1000, "name": "ts_day", "transform": "day"}'

BERLIN="iceberg_timezone_for_timestamptz = 'Europe/Berlin'"
NO_PRUNING="use_iceberg_partition_pruning = 0, use_query_condition_cache = 0, input_format_parquet_filter_push_down = 0"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} ENGINE = IcebergLocal('${TABLE_PATH}/')"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${BERLIN_TABLE} ENGINE = IcebergLocal('${TABLE_PATH}/') SETTINGS ${BERLIN}"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} SETTINGS allow_insert_into_iceberg = 1 VALUES (1, '2024-01-01 23:30:00', '2024-01-01 23:30:00', ['2024-01-01 23:30:00'])"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${BERLIN_TABLE} SETTINGS allow_insert_into_iceberg = 1 VALUES (2, '2024-01-02 00:30:00', '2024-01-02 00:30:00', ['2024-01-02 00:30:00'])"

echo '-- default'
${CLICKHOUSE_CLIENT} --query "SELECT id, ts, local, arr FROM ${TABLE} ORDER BY id"
${CLICKHOUSE_CLIENT} --query "DESCRIBE TABLE ${TABLE}" | cut -f1,2

echo '-- Europe/Berlin'
${CLICKHOUSE_CLIENT} --query "SELECT id, ts, local, arr FROM ${BERLIN_TABLE} ORDER BY id"
${CLICKHOUSE_CLIENT} --query "DESCRIBE TABLE ${BERLIN_TABLE}" | cut -f1,2
${CLICKHOUSE_CLIENT} --query "SELECT toTypeName(ts) FROM icebergLocal('${TABLE_PATH}/', SETTINGS ${BERLIN}) LIMIT 1"

echo '-- the setting of one table does not affect the other'
${CLICKHOUSE_CLIENT} --query "SELECT toTypeName(ts) FROM ${TABLE} LIMIT 1"

echo '-- an empty time zone'
{ ${CLICKHOUSE_CLIENT} --query "SELECT * FROM icebergLocal('${TABLE_PATH}/', SETTINGS iceberg_timezone_for_timestamptz = '')" 2>&1 || true; } | grep -o -m1 'BAD_ARGUMENTS'

check_filters()
{
    for SETTINGS in "use_query_condition_cache = 0" "${NO_PRUNING}"; do
        ${CLICKHOUSE_CLIENT} --query "
            SELECT
                (SELECT groupArray(id) FROM (SELECT id FROM ${BERLIN_TABLE} WHERE ts = '2024-01-02 00:30:00' ORDER BY id)),
                (SELECT groupArray(id) FROM (SELECT id FROM ${BERLIN_TABLE} WHERE ts >= '2024-01-02 00:00:00' ORDER BY id)),
                (SELECT groupArray(id) FROM (SELECT id FROM ${BERLIN_TABLE} WHERE toHour(ts) = 0 ORDER BY id)),
                (SELECT groupArray(id) FROM (SELECT id FROM ${BERLIN_TABLE} PREWHERE toHour(ts) = 0 ORDER BY id))
            SETTINGS ${SETTINGS}"
    done
    ${CLICKHOUSE_CLIENT} --query "SELECT groupArray(id) FROM (SELECT id FROM ${TABLE} WHERE ts >= '2024-01-02 00:00:00' ORDER BY id)"
}

echo '-- filters'
check_filters

echo '-- filters on data files written with an older schema'
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} RENAME COLUMN local TO local_renamed SETTINGS allow_insert_into_iceberg = 1"
check_filters

echo '-- DROP PARTITION of an identity partition'
create_metadata "${IDENTITY_TABLE_PATH}" '
    {"id": 1, "name": "id", "required": false, "type": "int"},
    {"id": 2, "name": "ts", "required": false, "type": "timestamptz"}' \
    '{"source-id": 2, "field-id": 1000, "name": "ts", "transform": "identity"}'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${IDENTITY_TABLE} ENGINE = IcebergLocal('${IDENTITY_TABLE_PATH}/') SETTINGS ${BERLIN}"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${IDENTITY_TABLE} SETTINGS allow_insert_into_iceberg = 1 VALUES (1, '2024-01-01 23:30:00'), (2, '2024-01-02 00:30:00')"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${IDENTITY_TABLE} DROP PARTITION '2024-01-02 00:30:00' SETTINGS allow_insert_into_iceberg = 1"
${CLICKHOUSE_CLIENT} --query "SELECT id, ts FROM ${IDENTITY_TABLE} ORDER BY id"
