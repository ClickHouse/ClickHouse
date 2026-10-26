#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

# The time ranges table has a row for a time series in each part written before the parts are merged.
# The selector joins it with ANY, so a time series is not repeated once per such part, and every sample is still selected.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --session_timezone UTC"

$CLIENT -q "CREATE TABLE ts ENGINE = TimeSeries"
time_ranges_table=$($CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.timeranges.%'")
$CLIENT -q "SYSTEM STOP MERGES \`$time_ranges_table\`"

for i in 1 2 3; do
    $CLIENT -q "INSERT INTO ts (metric_name, tags, samples) VALUES
        ('foo', map('env', 'prod'), [(toDateTime64($i * 100, 3), $i)]),
        ('foo', map('env', 'dev'), [(toDateTime64($i * 100, 3), $i * 10)])"
done

echo '-- rows of the time ranges table'
$CLIENT -q "SELECT count() FROM \`$time_ranges_table\`"

echo '-- join of the time ranges table'
$CLIENT --send_logs_level debug -q "SELECT 1 FROM timeSeriesSelector(ts, 'foo{env=\"prod\"}', 0, 1000) FORMAT Null" 2>&1 \
    | grep -o -E '(ANY|ALL) LEFT JOIN \(SELECT id, 1 AS matched' | sort -u

echo '-- selected samples'
$CLIENT -q "SELECT timestamp, value FROM timeSeriesSelector(ts, 'foo{env=\"prod\"}', 0, 1000) ORDER BY timestamp"
$CLIENT -q "SELECT count(), sum(value) FROM timeSeriesSelector(ts, 'foo', 0, 1000)"

$CLIENT -q "DROP TABLE ts"
