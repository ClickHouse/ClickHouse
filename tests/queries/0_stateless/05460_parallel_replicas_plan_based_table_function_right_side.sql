-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- In a `RIGHT JOIN` the plan-based mode coordinates the right side, which here is a read through a
-- `TimeSeries` table function. The plan fragment names the inner table the call resolved to, so the
-- replicas announce the read under that table's name, and the initiator's own copy of the read must do the
-- same: otherwise the replicas ask the coordinator for a read it does not know.

SET allow_experimental_time_series_table = 1;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas', parallel_replicas_for_non_replicated_merge_tree = 1;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_plan_based = 1;
SET parallel_replicas_mark_segment_size = 1, max_threads = 1, max_block_size = 8;

DROP TABLE IF EXISTS mt;
DROP TABLE IF EXISTS ts;
CREATE TABLE mt (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO mt SELECT number FROM numbers(5);

-- Several parts of many small granules, so the replicas get a share each and announce the read before the
-- initiator has taken all of it.
CREATE TABLE ts ENGINE = TimeSeries
    SAMPLES INNER ENGINE = AggregatingMergeTree SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1, index_granularity = 8;
INSERT INTO ts (metric_name, tags, samples) SELECT 'm', map('i', toString(number)), [(toDateTime64(number, 3), toFloat64(number % 5))] FROM numbers(0, 2000);
INSERT INTO ts (metric_name, tags, samples) SELECT 'm', map('i', toString(number)), [(toDateTime64(number, 3), toFloat64(number % 5))] FROM numbers(2000, 2000);
INSERT INTO ts (metric_name, tags, samples) SELECT 'm', map('i', toString(number)), [(toDateTime64(number, 3), toFloat64(number % 5))] FROM numbers(4000, 2000);
INSERT INTO ts (metric_name, tags, samples) SELECT 'm', map('i', toString(number)), [(toDateTime64(number, 3), toFloat64(number % 5))] FROM numbers(6000, 2000);
INSERT INTO ts (metric_name, tags, samples) SELECT 'm', map('i', toString(number)), [(toDateTime64(number, 3), toFloat64(number % 5))] FROM numbers(8000, 2000);
INSERT INTO ts (metric_name, tags, samples) SELECT 'm', map('i', toString(number)), [(toDateTime64(number, 3), toFloat64(number % 5))] FROM numbers(10000, 2000);

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
