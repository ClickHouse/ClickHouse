#!/usr/bin/env bash
# A backup of a pre-versioned `TimeSeries` materialized view must restore its version-0
# inner table, retain the view definition, and preserve all time-series samples.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e
CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 --ignore_drop_queries_probability=0"

mv_uuid=$($CLIENT -q "SELECT generateUUIDv4()")
inner_uuid=$($CLIENT -q "SELECT generateUUIDv4()")
inner_table=".inner_id.$mv_uuid"

# Simulate an older stored definition: the view has no version setting, but its
# hidden target is explicitly version 0.
$CLIENT -q "CREATE TABLE \`$inner_table\` UUID '$inner_uuid' ENGINE = TimeSeries SETTINGS version = 0"
$CLIENT -m -q "SET send_logs_level = 'error'; ATTACH MATERIALIZED VIEW mv_ts_legacy UUID '$mv_uuid' TO INNER UUID '$inner_uuid' (a UInt8) ENGINE = TimeSeries AS SELECT 1 AS a"
$CLIENT -q "INSERT INTO \`$inner_table\` (metric_name, tags, time_series) SELECT 'cpu', map('host', 'a'), [(toDateTime64('2026-01-01 00:00:00', 3), 1.5)] UNION ALL SELECT 'mem', map('host', 'b'), [(toDateTime64('2026-01-01 00:00:10', 3), 100)]"

check_legacy_definition_and_rows()
{
    # The view must not gain a `version` setting during normalization.
    $CLIENT -q "SELECT positionCaseInsensitive(create_table_query, 'version =') = 0 FROM system.tables WHERE database = currentDatabase() AND name = 'mv_ts_legacy'"
    # The hidden `TimeSeries` target must stay at version 0, not the current version.
    $CLIENT -q "SELECT position(engine_full, 'version = 0') > 0 FROM system.tables WHERE database = currentDatabase() AND name = '$inner_table'"
    $CLIENT -q "SELECT metric_name, tags['host'], length(time_series) FROM \`$inner_table\` ORDER BY metric_name"
}

check_legacy_definition_and_rows
backup_name="${CLICKHOUSE_TEST_UNIQUE_NAME}"
$CLIENT -q "BACKUP TABLE mv_ts_legacy TO Disk('backups', '$backup_name') FORMAT Null"
$CLIENT -q "DROP TABLE mv_ts_legacy SYNC"
$CLIENT -q "RESTORE TABLE mv_ts_legacy FROM Disk('backups', '$backup_name') FORMAT Null"
check_legacy_definition_and_rows
$CLIENT -q "DROP TABLE mv_ts_legacy SYNC"
