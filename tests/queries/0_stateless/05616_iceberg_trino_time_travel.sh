#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg and Avro are not in fast test

# Time travel by timestamp in an Iceberg table written by Trino with one insert per day
# (`CREATE TABLE`, then `INSERT` 1, 2, 3 on 2023-07-01, 2023-07-02, 2023-07-03). The table and the
# expected results come from the Trino test `TestIcebergReadVersionedTableByTemporal`, see
# `data_iceberg/trino/README.md`. Trino's `FOR TIMESTAMP AS OF` in a session time zone is
# translated into `iceberg_timestamp_ms`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_iceberg/trino"

# `icebergLocal` refuses a path outside the working directory of `clickhouse local`.
cd "$DATA" || exit 1

# The table is created in a dedicated `Memory` database, so it cannot clash with a table of the default database.
$CLICKHOUSE_LOCAL --session_timezone UTC -q "
SELECT * FROM icebergLocal('timetravel') ORDER BY ALL;
CREATE DATABASE trino ENGINE = Memory;
CREATE TABLE trino.timetravel ENGINE = IcebergLocal('timetravel');
SELECT made_current_at, snapshot_id, parent_id, is_current_ancestor, summary['added-records'] FROM system.iceberg_history WHERE database = 'trino' AND table = 'timetravel' ORDER BY made_current_at;
" || exit 1

# The time zone and the timestamp of every `FOR TIMESTAMP AS OF` of the Trino test, a date means its midnight.
while read -r tz ts
do
    ms=$($CLICKHOUSE_LOCAL -q "SELECT toUnixTimestamp64Milli(toDateTime64('$ts', 3, '$tz'))")
    echo "$tz $ts: $($CLICKHOUSE_LOCAL -q "SELECT groupArray(data) FROM (SELECT data FROM icebergLocal('timetravel') ORDER BY data) SETTINGS iceberg_timestamp_ms = $ms")"
done <<'LIST'
Europe/Vienna 2023-07-01
America/Los_Angeles 2023-07-01
Europe/Vienna 2023-07-02
America/Los_Angeles 2023-07-02
Europe/Vienna 2023-07-03
America/Los_Angeles 2023-07-03
Europe/Vienna 2023-07-04
America/Los_Angeles 2023-07-04
Europe/Vienna 2023-07-01 00:00:00
UTC 2023-07-01 05:02:43.953
UTC 2023-07-01 05:02:43.954
Europe/Vienna 2023-07-01 07:02:43.954
America/Los_Angeles 2023-07-01 00:00:00.1
Europe/Vienna 2023-07-02 01:00:00.12
America/Los_Angeles 2023-07-02 01:00:00.123
Europe/Vienna 2023-07-03 02:00:00.123
America/Los_Angeles 2023-07-03 02:00:00.123456
Europe/Vienna 2023-07-04 03:00:00.123456789
America/Los_Angeles 2023-07-04 03:00:00.123456789012
LIST
