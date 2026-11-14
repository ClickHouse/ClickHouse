#!/usr/bin/env bash
# BACKUP and RESTORE of a TimeSeries table storing native histograms: the `histograms` column of the samples table
# (a `SimpleAggregateFunction(timeSeriesGroupArray, ...)` of histogram tuples) and the histogram samples must survive the round trip
# along with the float samples. 05214_timeseries_backup_restore covers the definition and the other inner tables.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --enable_time_series_table=1 --session_timezone=UTC --ignore_drop_queries_probability=0"

SAMPLES_QUERY="SELECT metric_name, samples, histograms.timestamp, histograms.flags, histograms.counter_reset_hint, histograms.schema, histograms.sum,
    histograms.count_int, histograms.positive_int, histograms.count_float, histograms.positive_float, histograms.custom_values"

$CLIENT -q "CREATE TABLE ts ENGINE = TimeSeries SETTINGS samples_bucket_step_seconds = 60, recent_samples_ttl_seconds = 0"
# A series with float samples and integer histograms in one bucket, a float-only series, and a gauge float histogram with custom buckets.
$CLIENT -q "INSERT INTO ts (metric_name, tags, samples, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_int) VALUES
    ('mixed', {'job': 'test'}, [(toDateTime64(130, 3), 1.5)], [toDateTime64(125, 3), toDateTime64(145, 3)], [1, 2], [0.5, 1.5], [[(0, 1)], [(0, 2)]])"
$CLIENT -q "INSERT INTO ts (metric_name, tags, samples) VALUES ('floats_only', {'job': 'test'}, [(toDateTime64(100, 3), 1.)])"
$CLIENT -q "INSERT INTO ts (metric_name, tags, histograms.timestamp, histograms.flags, histograms.counter_reset_hint, histograms.schema, histograms.sum,
    histograms.count_float, histograms.positive_float, histograms.custom_values) VALUES
    ('float_custom', {'job': 'test'}, [toDateTime64(100, 3)], [1], [3], [-53], [3.25], [2.5], [[(0, 1.5), (1, 1.)]], [[1.]])"

BACKUP_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}"
$CLIENT -q "BACKUP TABLE ts TO Disk('backups', '$BACKUP_NAME') FORMAT Null"
$CLIENT -q "RESTORE TABLE ts AS ts_restored FROM Disk('backups', '$BACKUP_NAME') FORMAT Null"

echo '--- both samples tables have the histograms column ---'
$CLIENT -q "SELECT count() FROM system.columns WHERE database = currentDatabase() AND table LIKE '.inner\_id.samples.%' AND name = 'histograms'
    AND type LIKE 'SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp DateTime64(3), flags UInt8, %'"

echo '--- the restored table has the same samples of both kinds ---'
$CLIENT -q "$SAMPLES_QUERY FROM ts_restored ORDER BY ALL"
$CLIENT -q "SELECT count() FROM (SELECT * FROM ts EXCEPT SELECT * FROM ts_restored)"
$CLIENT -q "SELECT count() FROM (SELECT * FROM ts_restored EXCEPT SELECT * FROM ts)"

$CLIENT -q "DROP TABLE ts_restored"
$CLIENT -q "DROP TABLE ts"
