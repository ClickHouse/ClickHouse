#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# ORC stores the timestamps between -1 and 0 seconds as the epoch. Iceberg pruning must keep the rows
# of such a data file that a full read returns. The values are parsed and shown in the session time zone, so it is UTC.

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH:?}"' EXIT

for partition_by in "" "PARTITION BY icebergDay(ts)"
do
    $CLICKHOUSE_CLIENT --session_timezone UTC -q "
        CREATE TABLE ${TABLE} (id Int64, ts DateTime64(6, 'UTC')) ENGINE = IcebergLocal('${TABLE_PATH}', 'ORC') ${partition_by}"
    $CLICKHOUSE_CLIENT --session_timezone UTC --allow_insert_into_iceberg=1 -q "
        INSERT INTO ${TABLE} VALUES (1, '1969-12-31 23:59:58.5'), (2, '1969-12-31 23:59:59.25'), (3, '1969-12-31 23:59:59.5')"

    $CLICKHOUSE_CLIENT --session_timezone UTC -q "SELECT id, ts FROM ${TABLE} ORDER BY id"
    for pruning in 1 0
    do
        $CLICKHOUSE_CLIENT --session_timezone UTC -q "
            SELECT ${pruning}, groupArray(id) FROM (SELECT id FROM ${TABLE} WHERE ts >= '1970-01-01 00:00:00' ORDER BY id)
            SETTINGS use_iceberg_partition_pruning = ${pruning}"
        $CLICKHOUSE_CLIENT --session_timezone UTC -q "
            SELECT ${pruning}, groupArray(id) FROM (SELECT id FROM ${TABLE} WHERE icebergDay(ts) = 0 ORDER BY id)
            SETTINGS use_iceberg_partition_pruning = ${pruning}"
    done

    $CLICKHOUSE_CLIENT -q "DROP TABLE ${TABLE}"
    rm -rf "${TABLE_PATH:?}"
done
