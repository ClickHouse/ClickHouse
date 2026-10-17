-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: the pipeline checks below expect a single local read of the samples table.

-- After `ALTER TABLE ... MODIFY SETTING id_generator` the samples inserted earlier stay under the old id of the series
-- and the samples inserted later get a new id. A PromQL query evaluates the samples of both ids as one series.

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;
SET max_threads = 4;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('host', 'h1'), [(toDateTime64(100, 3), 10)]);
ALTER TABLE ts MODIFY SETTING id_generator = 'tuple(sipHash64(metric_name), reinterpretAsUUID(sipHash128(metric_name, tags)))';
INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('host', 'h1'), [(toDateTime64(110, 3), 20)]);

SELECT '-- one set of tags is stored under two ids';
SELECT uniqExact(id), uniqExact(tags) FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts');

SELECT '-- rate(m[20s]) over a window with samples of both ids, range';
SELECT * FROM prometheusQueryRange('ts', 'rate(m[20s])', 100, 120, 10) ORDER BY tags;

SELECT '-- m, range';
SELECT * FROM prometheusQueryRange('ts', 'm', 100, 120, 10) ORDER BY tags;

SELECT '-- last_over_time(m[20s]), range';
SELECT * FROM prometheusQueryRange('ts', 'last_over_time(m[20s])', 100, 120, 10) ORDER BY tags;

SELECT '-- rate(m[20s]), instant';
SELECT * FROM prometheusQuery('ts', 'rate(m[20s])', 110) ORDER BY tags;

SELECT '-- m, instant';
SELECT * FROM prometheusQuery('ts', 'm', 110) ORDER BY tags;

SELECT '-- m[20s], instant';
SELECT * FROM prometheusQuery('ts', 'm[20s]', 110) ORDER BY tags;

-- A range query aggregates the samples by id in the order of the samples table, then merges the ids of one set of tags.
SELECT '-- range: the samples are aggregated by id, then merged by the set of tags';
SELECT countIf(explain LIKE '%Keys: id%') AS by_id, countIf(explain LIKE '%Keys: timeSeriesIdToGroup(id)%') AS by_tags
FROM (EXPLAIN actions = 1 SELECT * FROM prometheusQueryRange('ts', 'rate(m[20s])', 100, 120, 10));

-- A query at one point aggregates the samples by the set of tags in one level.
SELECT '-- one point: the samples are aggregated by the set of tags';
SELECT countIf(explain LIKE '%Keys: id%') AS by_id, countIf(explain LIKE '%Keys: timeSeriesIdToGroup(id)%') AS by_tags
FROM (EXPLAIN actions = 1 SELECT * FROM prometheusQuery('ts', 'rate(m[20s])', 110));

SELECT '-- the results do not depend on the aggregation settings';
SET optimize_aggregation_in_order = 0, aggregation_in_order_split_by_primary_key_ranges = 0;
SELECT * FROM prometheusQueryRange('ts', 'rate(m[20s])', 100, 120, 10) ORDER BY tags;
SELECT * FROM prometheusQueryRange('ts', 'm', 100, 120, 10) ORDER BY tags;
SELECT * FROM prometheusQuery('ts', 'm[20s]', 110) ORDER BY tags;
SET optimize_aggregation_in_order = 1, aggregation_in_order_split_by_primary_key_ranges = 0;
SELECT * FROM prometheusQueryRange('ts', 'rate(m[20s])', 100, 120, 10) ORDER BY tags;
SELECT * FROM prometheusQueryRange('ts', 'm', 100, 120, 10) ORDER BY tags;
SELECT * FROM prometheusQuery('ts', 'm[20s]', 110) ORDER BY tags;
SET optimize_aggregation_in_order = 0, aggregation_in_order_split_by_primary_key_ranges = 1;
SELECT * FROM prometheusQueryRange('ts', 'rate(m[20s])', 100, 120, 10) ORDER BY tags;
SELECT * FROM prometheusQueryRange('ts', 'm', 100, 120, 10) ORDER BY tags;
SELECT * FROM prometheusQuery('ts', 'm[20s]', 110) ORDER BY tags;
SET optimize_aggregation_in_order = DEFAULT, aggregation_in_order_split_by_primary_key_ranges = DEFAULT;

DROP TABLE ts;
