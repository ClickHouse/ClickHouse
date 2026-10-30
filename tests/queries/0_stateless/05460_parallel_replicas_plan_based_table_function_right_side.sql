-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- In a `RIGHT JOIN` the plan-based mode coordinates the right side, which here is a read through a
-- `TimeSeries` table function. A plan fragment can only name the storage the call resolved to on the
-- initiator - a hidden inner table named after a UUID that server assigned - so such a read must not be
-- shipped. It stays on the initiator, and the result is the same as without parallel replicas. All
-- replicas here are one server, so a shipped read would find the table anyway: what is checked is that
-- nothing is shipped.

SET allow_experimental_time_series_table = 1;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas', parallel_replicas_for_non_replicated_merge_tree = 1;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_plan_based = 1;

DROP TABLE IF EXISTS mt;
DROP TABLE IF EXISTS ts;
CREATE TABLE mt (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO mt SELECT number FROM numbers(5);

-- Several parts, so the replicas would get a share each.
CREATE TABLE ts ENGINE = TimeSeries
    SAMPLES INNER ENGINE = AggregatingMergeTree SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO ts (metric_name, tags, samples) SELECT 'm', map('i', toString(number)), [(toDateTime64(number, 3), toFloat64(number % 5))] FROM numbers(0, 10);
INSERT INTO ts (metric_name, tags, samples) SELECT 'm', map('i', toString(number)), [(toDateTime64(number, 3), toFloat64(number % 5))] FROM numbers(10, 10);
INSERT INTO ts (metric_name, tags, samples) SELECT 'm', map('i', toString(number)), [(toDateTime64(number, 3), toFloat64(number % 5))] FROM numbers(20, 10);
INSERT INTO ts (metric_name, tags, samples) SELECT 'm', map('i', toString(number)), [(toDateTime64(number, 3), toFloat64(number % 5))] FROM numbers(30, 10);
INSERT INTO ts (metric_name, tags, samples) SELECT 'm', map('i', toString(number)), [(toDateTime64(number, 3), toFloat64(number % 5))] FROM numbers(40, 10);
INSERT INTO ts (metric_name, tags, samples) SELECT 'm', map('i', toString(number)), [(toDateTime64(number, 3), toFloat64(number % 5))] FROM numbers(50, 10);

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table LIKE '.inner_id.samples.%' AND active;

SELECT count(), sum(r.samples[1].2) FROM mt RIGHT JOIN timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS r ON mt.x = toUInt64(r.samples[1].2)
    SETTINGS enable_parallel_replicas = 0;
SELECT count(), sum(r.samples[1].2) FROM mt RIGHT JOIN timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS r ON mt.x = toUInt64(r.samples[1].2)
    SETTINGS parallel_replicas_local_plan = 0;
SELECT count(), sum(r.samples[1].2) FROM mt RIGHT JOIN timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS r ON mt.x = toUInt64(r.samples[1].2)
    SETTINGS parallel_replicas_local_plan = 1;

SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') FROM (
    EXPLAIN distributed = 1
    SELECT count() FROM mt RIGHT JOIN timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS r ON mt.x = toUInt64(r.samples[1].2)
    SETTINGS parallel_replicas_local_plan = 0);

DROP TABLE ts;
DROP TABLE mt;
