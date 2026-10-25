#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

# The whole-metric probe of a selector with no matcher besides the one on `__name__` reads the tags table only.
# With other matchers it still joins the time ranges table, and a series with a foreign id is found whatever its time range is.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --session_timezone UTC"

$CLIENT -m -q "
CREATE TABLE ts ENGINE = TimeSeries TAGS INNER COLUMNS (id Tuple(UInt64, UUID));
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('foo', map('env', 'prod'), [(toDateTime64(100, 3), 1.), (toDateTime64(200, 3), 2.)]),
    ('foo', map('env', 'dev'), [(toDateTime64(150, 3), 10.)]),
    ('bar', map('env', 'dev'), [(toDateTime64(100, 3), 100.)]);
"

# Prints whether the probe of the selector joins the time ranges table, then the selected samples.
function probe()
{
    local table=$1 selector=$2 max_time=$3
    $CLIENT --send_logs_level debug -q "SELECT 1 FROM timeSeriesSelector($table, '$selector', 0, $max_time) FORMAT Null" 2>&1 \
        | grep -F 'Probing whether selector matches the whole metric' | grep -c -F 'time_ranges_subquery'
    $CLIENT -q "SELECT timestamp, value FROM timeSeriesSelector($table, '$selector', 0, $max_time) ORDER BY value, timestamp"
}

# Prints whether the samples table is read by the id range of the whole metric.
function has_id_range()
{
    local table=$1 selector=$2 max_time=$3
    $CLIENT -q "SELECT countIf(explain ILIKE '%Condition:%' AND explain ILIKE '%ffffffff-ffff-ffff-ffff-ffffffffffff%') > 0
        FROM (EXPLAIN indexes = 1 SELECT sum(value) FROM timeSeriesSelector($table, '$selector', 0, $max_time))"
}

echo '-- whole metric: the probe reads the tags table only, the id range is emitted'
probe ts 'foo' 1000
has_id_range ts 'foo' 1000

echo '-- other matchers: the probe joins the time ranges table, no id range'
probe ts 'foo{env="prod"}' 1000
has_id_range ts 'foo{env="prod"}' 1000

echo '-- a series with a foreign id and a time range outside the window is still found by the probe'
$CLIENT -m -q "
CREATE TABLE ts_foreign ENGINE = TimeSeries TAGS INNER COLUMNS (id Tuple(UInt64, UUID));
INSERT INTO FUNCTION timeSeriesTags(${CLICKHOUSE_DATABASE}.ts_foreign) (id, metric_name, tags) VALUES
    (tuple(1, toUUID('00000000-0000-0000-0000-000000000001')), 'foo', map('__name__', 'foo', 'env', 'old'));
INSERT INTO FUNCTION timeSeriesTimeRanges(${CLICKHOUSE_DATABASE}.ts_foreign) (id, min_time, max_time) VALUES
    (tuple(1, toUUID('00000000-0000-0000-0000-000000000001')), toDateTime64(5000, 3), toDateTime64(5000, 3));
INSERT INTO FUNCTION timeSeriesSamples(${CLICKHOUSE_DATABASE}.ts_foreign) (id, timestamp, value) VALUES
    (tuple(1, toUUID('00000000-0000-0000-0000-000000000001')), toDateTime64(5000, 3), 5.);
INSERT INTO ts_foreign (metric_name, tags, samples) VALUES ('foo', map('env', 'new'), [(toDateTime64(200, 3), 2.)]);
"
probe ts_foreign 'foo' 1000
has_id_range ts_foreign 'foo' 1000
probe ts_foreign 'foo' 10000
has_id_range ts_foreign 'foo' 10000

$CLIENT -q "DROP TABLE ts_foreign"
$CLIENT -q "DROP TABLE ts"
