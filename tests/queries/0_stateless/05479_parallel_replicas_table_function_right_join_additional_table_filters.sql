-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- A `RIGHT JOIN` with a read through a `TimeSeries` table function on its right side must not be
-- distributed when the query sets `additional_table_filters`, as for the left side of a `LEFT JOIN`.
-- Otherwise the replicas coordinate the left table instead, and the query fails with `unknown stream`
-- or returns every right row once per replica.

SET allow_experimental_time_series_table = 1;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas', parallel_replicas_for_non_replicated_merge_tree = 1;
SET automatic_parallel_replicas_mode = 0, parallel_replicas_plan_based = 0, parallel_replicas_prefer_local_join = 1;
SET parallel_replicas_mark_segment_size = 1, max_threads = 1, max_block_size = 8;

DROP TABLE IF EXISTS mt;
DROP TABLE IF EXISTS ts;

CREATE TABLE mt (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8;
INSERT INTO mt SELECT number FROM numbers(3000);

CREATE TABLE ts ENGINE = TimeSeries
    SAMPLES INNER ENGINE = AggregatingMergeTree SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1, index_granularity = 8;
INSERT INTO ts (metric_name, tags, samples) SELECT 'm', map('i', toString(number)), [(toDateTime64(number, 3), toFloat64(number % 7))] FROM numbers(0, 2000);
INSERT INTO ts (metric_name, tags, samples) SELECT 'm', map('i', toString(number)), [(toDateTime64(number, 3), toFloat64(number % 7))] FROM numbers(2000, 2000);
INSERT INTO ts (metric_name, tags, samples) SELECT 'm', map('i', toString(number)), [(toDateTime64(number, 3), toFloat64(number % 7))] FROM numbers(4000, 2000);

SELECT count(), sum(r.samples[1].2) FROM mt RIGHT JOIN timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS r ON mt.x = toUInt64(r.samples[1].2)
    SETTINGS enable_parallel_replicas = 0;

SELECT count(), sum(r.samples[1].2) FROM mt RIGHT JOIN timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS r ON mt.x = toUInt64(r.samples[1].2)
    SETTINGS serialize_query_plan = 1, additional_table_filters = {'mt': 'x < 5'}, parallel_replicas_local_plan = 0;

SELECT count(), sum(r.samples[1].2) FROM mt RIGHT JOIN timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS r ON mt.x = toUInt64(r.samples[1].2)
    SETTINGS serialize_query_plan = 1, additional_table_filters = {'mt': 'x < 5'}, parallel_replicas_local_plan = 1;

SELECT count(), sum(r.samples[1].2) FROM mt RIGHT JOIN timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS r ON mt.x = toUInt64(r.samples[1].2)
    WHERE r.id NOT IN (SELECT id FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) WHERE metric_name = 'none' SETTINGS additional_table_filters = {'mt': 'x < 5'})
    SETTINGS parallel_replicas_allow_in_with_subquery = 1, parallel_replicas_local_plan = 0;

SELECT count(), sum(r.samples[1].2) FROM mt RIGHT JOIN timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS r ON mt.x = toUInt64(r.samples[1].2)
    WHERE r.id NOT IN (SELECT id FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) WHERE metric_name = 'none' SETTINGS additional_table_filters = {'mt': 'x < 5'})
    SETTINGS parallel_replicas_allow_in_with_subquery = 1, parallel_replicas_local_plan = 1;

DROP TABLE ts;
DROP TABLE mt;
