#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The Iceberg timestamps in ORC data files are UTC instants: the ORC time zone settings do not shift them,
# and pruning keeps the rows that a full read returns.
# The values are parsed and shown in the session time zone, so it is UTC.

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH:?}"' EXIT

CLIENT="$CLICKHOUSE_CLIENT --session_timezone UTC"
ZONES="--output_format_orc_writer_time_zone_name=Europe/Berlin --input_format_orc_reader_time_zone_name=America/Los_Angeles"

$CLIENT -q "
    CREATE TABLE ${TABLE} (id Int64, ts DateTime64(6, 'UTC')) ENGINE = IcebergLocal('${TABLE_PATH}', 'ORC')
    SETTINGS output_format_orc_writer_time_zone_name = 'Europe/Berlin'"
$CLIENT $ZONES --allow_insert_into_iceberg=1 -q "INSERT INTO ${TABLE} VALUES (1, '2024-01-01 00:00:00')"

for client_zones in "" "$ZONES"
do
    $CLIENT $client_zones -q "SELECT id, ts FROM ${TABLE}"
    for pruning in 1 0
    do
        $CLIENT $client_zones -q "
            SELECT ${pruning}, groupArray(id) FROM ${TABLE} WHERE ts = '2024-01-01 00:00:00'
            SETTINGS use_iceberg_partition_pruning = ${pruning}"
    done
done

$CLIENT -q "DROP TABLE ${TABLE}"
