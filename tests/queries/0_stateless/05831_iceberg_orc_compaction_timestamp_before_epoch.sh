#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# ORC stores the timestamps between -1 and 0 seconds as the epoch. Compacting Parquet data files into ORC keeps
# the rows that a full read returns.
# The values are parsed and shown in the session time zone, so it is UTC.

CLIENT="$CLICKHOUSE_CLIENT --session_timezone UTC --allow_insert_into_iceberg=1"

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH:?}"' EXIT

$CLIENT -q "CREATE TABLE ${TABLE} (id Int64, ts DateTime64(6, 'UTC')) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')"
$CLIENT -q "INSERT INTO ${TABLE} VALUES (1, '1969-12-31 23:59:59.5'), (2, '1969-12-31 23:59:59.75')"
$CLIENT --mutations_sync=2 -q "ALTER TABLE ${TABLE} DELETE WHERE id = 2"
$CLIENT -q "DROP TABLE ${TABLE}"
$CLIENT -q "CREATE TABLE ${TABLE} ENGINE = IcebergLocal('${TABLE_PATH}', 'ORC')"
$CLIENT --allow_experimental_iceberg_compaction=1 -q "OPTIMIZE TABLE ${TABLE}"

$CLIENT -q "SELECT id, ts FROM ${TABLE}"
for pruning in 1 0
do
    $CLIENT -q "
        SELECT ${pruning}, groupArray(id) FROM ${TABLE} WHERE ts >= '1970-01-01 00:00:00'
        SETTINGS use_iceberg_partition_pruning = ${pruning}"
done

$CLIENT -q "DROP TABLE ${TABLE}"
