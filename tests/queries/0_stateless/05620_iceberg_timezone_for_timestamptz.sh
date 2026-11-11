#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_timestamptz"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

mkdir -p "${TABLE_PATH}/metadata"
SCHEMA='{"type": "struct", "schema-id": 0, "fields": [
    {"id": 1, "name": "id", "required": false, "type": "int"},
    {"id": 2, "name": "ts", "required": false, "type": "timestamptz"},
    {"id": 3, "name": "local", "required": false, "type": "timestamp"},
    {"id": 4, "name": "arr", "required": false, "type": {"type": "list", "element-id": 5, "element": "timestamptz", "element-required": false}}]}'
cat > "${TABLE_PATH}/metadata/v1.metadata.json" << EOF
{
  "format-version": 2,
  "table-uuid": "8f2c1a6e-3b0d-4c55-9a51-2d7f0e4b6c11",
  "location": "${TABLE_PATH}",
  "last-sequence-number": 0,
  "last-updated-ms": 1700000000000,
  "last-column-id": 5,
  "current-schema-id": 0,
  "schemas": [${SCHEMA}],
  "default-spec-id": 0,
  "partition-specs": [{"spec-id": 0, "fields": [{"source-id": 2, "field-id": 1000, "name": "ts_day", "transform": "day"}]}],
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

BERLIN="iceberg_timezone_for_timestamptz = 'Europe/Berlin'"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} ENGINE = IcebergLocal('${TABLE_PATH}/')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} SETTINGS allow_insert_into_iceberg = 1 VALUES (1, '2024-01-01 23:30:00', '2024-01-01 23:30:00', ['2024-01-01 23:30:00'])"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} SETTINGS allow_insert_into_iceberg = 1, ${BERLIN} VALUES (2, '2024-01-02 00:30:00', '2024-01-02 00:30:00', ['2024-01-02 00:30:00'])"

echo '-- default'
${CLICKHOUSE_CLIENT} --query "SELECT id, ts, local, arr FROM ${TABLE} ORDER BY id"
${CLICKHOUSE_CLIENT} --query "DESCRIBE TABLE ${TABLE}" | cut -f1,2

echo '-- Europe/Berlin'
${CLICKHOUSE_CLIENT} --query "SELECT id, ts, local, arr FROM ${TABLE} ORDER BY id SETTINGS ${BERLIN}"
${CLICKHOUSE_CLIENT} --query "DESCRIBE TABLE ${TABLE} SETTINGS ${BERLIN}" | cut -f1,2
${CLICKHOUSE_CLIENT} --query "SELECT toTypeName(ts) FROM icebergLocal('${TABLE_PATH}/') LIMIT 1 SETTINGS ${BERLIN}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE} WHERE ts = '2024-01-02 00:30:00' SETTINGS ${BERLIN}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE} WHERE ts >= '2024-01-02 00:00:00' SETTINGS ${BERLIN}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE} WHERE ts >= '2024-01-02 00:00:00'"

echo '-- session time zone'
${CLICKHOUSE_CLIENT} --query "SELECT toTypeName(ts), ts FROM ${TABLE} ORDER BY id SETTINGS iceberg_timezone_for_timestamptz = '', session_timezone = 'Asia/Tokyo'"

