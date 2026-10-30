-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- A `RightAny` join picks one left row for each right row out of the whole left side, so it cannot be
-- distributed by splitting the left side. The plan-based mode admits it, because the plan optimizer
-- keeps such a join local, but a read through a `TimeSeries` table function is planned the query-tree
-- way even in that mode, so the join must be refused as it is there. Otherwise every replica returns
-- a row for each right row.

SET allow_experimental_time_series_table = 1;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas', parallel_replicas_for_non_replicated_merge_tree = 1;
SET automatic_parallel_replicas_mode = 0;
SET any_join_distinct_right_table_keys = 1;

-- Several parts, so the replicas get a share each. Merges are disabled on the samples table itself:
-- `SYSTEM STOP MERGES ts` does not reach the inner tables of a `TimeSeries` table.
DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries
    SAMPLES INNER ENGINE = AggregatingMergeTree SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO ts (metric_name, tags, samples) SELECT 'm', map('i', toString(number)), [(toDateTime64(number, 3), toFloat64(number % 5))] FROM numbers(0, 10);
INSERT INTO ts (metric_name, tags, samples) SELECT 'm', map('i', toString(number)), [(toDateTime64(number, 3), toFloat64(number % 5))] FROM numbers(10, 10);
INSERT INTO ts (metric_name, tags, samples) SELECT 'm', map('i', toString(number)), [(toDateTime64(number, 3), toFloat64(number % 5))] FROM numbers(20, 10);
INSERT INTO ts (metric_name, tags, samples) SELECT 'm', map('i', toString(number)), [(toDateTime64(number, 3), toFloat64(number % 5))] FROM numbers(30, 10);
INSERT INTO ts (metric_name, tags, samples) SELECT 'm', map('i', toString(number)), [(toDateTime64(number, 3), toFloat64(number % 5))] FROM numbers(40, 10);
INSERT INTO ts (metric_name, tags, samples) SELECT 'm', map('i', toString(number)), [(toDateTime64(number, 3), toFloat64(number % 5))] FROM numbers(50, 10);

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table LIKE '.inner_id.samples.%' AND active;

SELECT count() FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS l
    ANY RIGHT JOIN timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS r ON l.samples[1].2 = r.samples[1].2
    SETTINGS enable_parallel_replicas = 0;

SELECT count() FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS l
    ANY RIGHT JOIN timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS r ON l.samples[1].2 = r.samples[1].2
    SETTINGS parallel_replicas_plan_based = 1, parallel_replicas_local_plan = 0;

SELECT count() FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS l
    ANY RIGHT JOIN timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS r ON l.samples[1].2 = r.samples[1].2
    SETTINGS parallel_replicas_plan_based = 1, parallel_replicas_local_plan = 1;

DROP TABLE ts;
