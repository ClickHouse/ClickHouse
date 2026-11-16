#!/usr/bin/env bash
# A materialized view with `ENGINE = TimeSeries` must survive BACKUP and RESTORE. The view stores only the
# `ENGINE = TimeSeries` clause, so the definitions of the inner tables of its hidden `TimeSeries` table must be
# generated again on restore. Before the fix the restore failed with a `LOGICAL_ERROR`: "has no inner columns".

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1 --ignore_drop_queries_probability=0"

function count_metric_families()
{
    local table
    table=$($CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.metricfamilies.%'")
    $CLIENT -q "SELECT count(), countIf(metric_family = 'm42') FROM \`$table\`"
}

$CLIENT -q "CREATE MATERIALIZED VIEW mv_ts ENGINE = TimeSeries AS SELECT 1 AS a"
$CLIENT -q "INSERT INTO \`$($CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.metricfamilies.%'")\` (metric_family) SELECT 'm' || toString(number) FROM numbers(100)"
count_metric_families

BACKUP_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}"
$CLIENT -q "BACKUP TABLE mv_ts TO Disk('backups', '$BACKUP_NAME') FORMAT Null"
$CLIENT -q "DROP TABLE mv_ts SYNC"
$CLIENT -q "RESTORE TABLE mv_ts FROM Disk('backups', '$BACKUP_NAME') FORMAT Null"

$CLIENT -q "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'mv_ts'"
count_metric_families
