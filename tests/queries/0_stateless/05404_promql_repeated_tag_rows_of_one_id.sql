-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: the counts below expect a single local read of the samples table.

-- The tags table returns one id several times before its parts are merged, and the time ranges table holds one row
-- per insert until its parts are merged. Repeated rows of one id are not a set of tags stored under several ids.

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts;

-- The tags table uses MergeTree without a deduplication cache, so every insert adds a row for the series.
CREATE TABLE ts ENGINE = TimeSeries SETTINGS tags_deduplication_cache_size_bytes = 0 TAGS INNER ENGINE = MergeTree ORDER BY (metric_name, id);

INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('host', 'h1'), [(toDateTime64(100, 3), 10)]);
INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('host', 'h1'), [(toDateTime64(110, 3), 20)]);
INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('host', 'h1'), [(toDateTime64(120, 3), 30)]);

SELECT '-- one id, three rows in the tags table';
SELECT uniqExact(id), count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts');

SELECT '-- no id shares its tags, the shared-tags branch reads no samples';
SELECT count() FROM timeSeriesSelector({CLICKHOUSE_DATABASE:String}, 'ts', 'm', 100, 120, 'ids_sharing_tags');
SELECT count() FROM timeSeriesSelector({CLICKHOUSE_DATABASE:String}, 'ts', 'm', 100, 120, 'samples_of_ids_sharing_tags');
SELECT countIf(explain LIKE '%ReadFromMergeTree%') AS samples_table_reads
FROM (EXPLAIN SELECT * FROM timeSeriesSelector({CLICKHOUSE_DATABASE:String}, 'ts', 'm', 100, 120, 'samples_of_ids_sharing_tags'));

SELECT '-- rate(m[20s]), range';
SELECT * FROM prometheusQueryRange('ts', 'rate(m[20s])', 100, 120, 10) ORDER BY tags;

-- A second id for the same set of tags is found once, and the shared-tags branch reads the samples of both ids once.
ALTER TABLE ts MODIFY SETTING id_generator = 'tuple(sipHash64(metric_name), reinterpretAsUUID(sipHash128(metric_name, tags)))';
INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('host', 'h1'), [(toDateTime64(130, 3), 40)]);
INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('host', 'h1'), [(toDateTime64(140, 3), 50)]);

SELECT '-- two ids, five rows in the tags table';
SELECT uniqExact(id), count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts');

SELECT '-- both ids share their tags, the shared-tags branch reads every sample once';
SELECT count() FROM timeSeriesSelector({CLICKHOUSE_DATABASE:String}, 'ts', 'm', 100, 140, 'ids_sharing_tags');
SELECT count(), uniqExact(id) FROM timeSeriesSelector({CLICKHOUSE_DATABASE:String}, 'ts', 'm', 100, 140, 'samples_of_ids_sharing_tags');
SELECT countIf(explain LIKE '%ReadFromMergeTree%') AS samples_table_reads
FROM (EXPLAIN SELECT * FROM timeSeriesSelector({CLICKHOUSE_DATABASE:String}, 'ts', 'm', 100, 140, 'samples_of_ids_sharing_tags'));

SELECT '-- rate(m[20s]), range';
SELECT * FROM prometheusQueryRange('ts', 'rate(m[20s])', 100, 140, 10) ORDER BY tags;

DROP TABLE ts;
