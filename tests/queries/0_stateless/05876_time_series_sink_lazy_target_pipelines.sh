#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --enable_time_series_table 1 --session_timezone UTC --enable_parallel_replicas 0"
ASYNC_OPTS="--async_insert 1 --wait_for_async_insert 1 --async_insert_max_data_size 10485760 --async_insert_busy_timeout_max_ms 50 --wait_for_async_insert_timeout 120 --implicit_transaction 0 --log_queries 1"
SYNC_OPTS="--async_insert 0 --log_queries 1"

$CLIENT -q "DROP TABLE IF EXISTS ts_async, ts_sync, ts_empty, ts_partial"

$CLIENT -q "CREATE TABLE ts_async ENGINE = TimeSeries"
recent_async=$($CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND (name LIKE '.inner_id.recentsamples.%' OR name LIKE '.inner.recentsamples.%')")

$CLIENT -q "CREATE TABLE ts_sync ENGINE = TimeSeries"
recent_sync=$($CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND (name LIKE '.inner_id.recentsamples.%' OR name LIKE '.inner.recentsamples.%') AND name != '${recent_async}'")

$CLIENT -q "CREATE TABLE ts_empty ENGINE = TimeSeries"
$CLIENT -q "CREATE TABLE ts_partial ENGINE = TimeSeries"

# Fixed timestamps inside the recent_samples TTL window so all five inner tables receive rows.
ts1=$($CLIENT -q "SELECT toUnixTimestamp(toStartOfHour(now('UTC'))) - 120")
ts2=$((ts1 + 15))

rows="('http_requests', {'job': 'api', 'instance': 'h1'}, [(toDateTime64(${ts1}, 3), 1.5), (toDateTime64(${ts2}, 3), 2.5)], 'http_requests', 'counter', 'requests', 'Total requests'),
('up', {'job': 'api', 'instance': 'h1'}, [(toDateTime64(${ts1}, 3), 1.0)], 'up', 'gauge', '', 'Target up')"

qid_prefix="${CLICKHOUSE_DATABASE}_lazy_sink"

printf '%s\n' "$rows" | $CLIENT $ASYNC_OPTS --query_id "${qid_prefix}_stream_async" \
    -q "INSERT INTO ts_async (metric_name, tags, samples, metric_family, type, unit, help) FORMAT Values"

printf '%s\n' "$rows" | $CLIENT $SYNC_OPTS --query_id "${qid_prefix}_stream_sync" \
    -q "INSERT INTO ts_sync (metric_name, tags, samples, metric_family, type, unit, help) FORMAT Values"

$CLIENT $SYNC_OPTS --query_id "${qid_prefix}_empty_select_sync" \
    -q "INSERT INTO ts_empty (metric_name, tags, samples, metric_family, type, unit, help) SELECT '', map(), [], '', '', '', '' FROM numbers(0)"

$CLIENT $ASYNC_OPTS --query_id "${qid_prefix}_empty_select_async" \
    -q "INSERT INTO ts_empty (metric_name, tags, samples, metric_family, type, unit, help) SELECT '', map(), [], '', '', '', '' FROM numbers(0)"

${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&query_id=${qid_prefix}_empty_http_sync&async_insert=0&log_queries=1&throw_if_no_data_to_insert=0" \
    --data-binary "INSERT INTO ts_empty (metric_name, tags, samples) FORMAT TabSeparated"

${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&query_id=${qid_prefix}_empty_http_async&async_insert=1&wait_for_async_insert=1&async_insert_busy_timeout_max_ms=50&log_queries=1&throw_if_no_data_to_insert=0" \
    --data-binary "INSERT INTO ts_empty (metric_name, tags, samples) FORMAT TabSeparated"

$CLIENT $SYNC_OPTS --query_id "${qid_prefix}_partial_samples_sync" \
    -q "INSERT INTO ts_partial (metric_name, tags, samples) VALUES ('cpu_usage', {'job': 'node'}, [(toDateTime64(${ts1}, 3), 0.5)])"

$CLIENT $SYNC_OPTS --query_id "${qid_prefix}_partial_metadata_sync" \
    -q "INSERT INTO ts_partial (metric_family, type, unit, help) VALUES ('cpu_usage', 'gauge', 'ratio', 'CPU usage')"

$CLIENT -q "SYSTEM FLUSH LOGS query_log"

echo "-- inner target tables opened by each query"
$CLIENT -q "
SELECT
    replaceOne(query_id, '${qid_prefix}_', '') AS label,
    ProfileEvents['AsyncInsertQuery'] > 0 AS used_async_queue,
    arrayCount(t -> t LIKE '%.inner_id.%' OR t LIKE '%.inner.%', tables) AS inner_tables
FROM system.query_log
WHERE event_date >= yesterday()
  AND type = 'QueryFinish'
  AND query_id LIKE '${qid_prefix}_%'
ORDER BY label
"

echo "-- inner table parity between async and sync"
$CLIENT -q "
SELECT * FROM (
SELECT 'tags' AS target,
    (SELECT (count(), groupBitXor(cityHash64(*))) FROM timeSeriesTags(ts_async) FINAL)
    = (SELECT (count(), groupBitXor(cityHash64(*))) FROM timeSeriesTags(ts_sync) FINAL),
    (SELECT count() FROM timeSeriesTags(ts_async) FINAL)
UNION ALL
SELECT 'time_ranges',
    (SELECT (count(), groupBitXor(cityHash64(*))) FROM timeSeriesTimeRanges(ts_async) FINAL)
    = (SELECT (count(), groupBitXor(cityHash64(*))) FROM timeSeriesTimeRanges(ts_sync) FINAL),
    (SELECT count() FROM timeSeriesTimeRanges(ts_async) FINAL)
UNION ALL
SELECT 'samples',
    (SELECT (count(), groupBitXor(cityHash64(*))) FROM timeSeriesSamples(ts_async) FINAL)
    = (SELECT (count(), groupBitXor(cityHash64(*))) FROM timeSeriesSamples(ts_sync) FINAL),
    (SELECT count() FROM timeSeriesSamples(ts_async) FINAL)
UNION ALL
SELECT 'recent_samples',
    (SELECT (count(), groupBitXor(cityHash64(*))) FROM \`${recent_async}\` FINAL)
    = (SELECT (count(), groupBitXor(cityHash64(*))) FROM \`${recent_sync}\` FINAL),
    (SELECT count() FROM \`${recent_async}\` FINAL)
UNION ALL
SELECT 'metric_families',
    (SELECT (count(), groupBitXor(cityHash64(*))) FROM timeSeriesMetricFamilies(ts_async) FINAL)
    = (SELECT (count(), groupBitXor(cityHash64(*))) FROM timeSeriesMetricFamilies(ts_sync) FINAL),
    (SELECT count() FROM timeSeriesMetricFamilies(ts_async) FINAL)
) ORDER BY target
"

echo "-- empty table and partial table counts"
$CLIENT -q "
SELECT
    (SELECT count() FROM timeSeriesTags(ts_empty)),
    (SELECT count() FROM timeSeriesSamples(ts_empty)),
    (SELECT count() FROM timeSeriesMetricFamilies(ts_empty)),
    (SELECT count() FROM timeSeriesTags(ts_partial)),
    (SELECT count() FROM timeSeriesSamples(ts_partial)),
    (SELECT count() FROM timeSeriesMetricFamilies(ts_partial))
"

$CLIENT -q "DROP TABLE ts_async, ts_sync, ts_empty, ts_partial"
