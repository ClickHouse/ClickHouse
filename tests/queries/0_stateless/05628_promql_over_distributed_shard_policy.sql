-- Tags: no-parallel, no-fasttest, no-flaky-check, shard
-- no-parallel, no-flaky-check: uses the global shard_0 / shard_1 databases of test_cluster_two_shards_different_databases.
-- no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- A row policy or a table filter on a shard-local TimeSeries table must be refused, not bypassed.

SET allow_experimental_time_series_table = 1;
SET distributed_foreground_insert = 1;

DROP TABLE IF EXISTS ts_dist;
DROP ROW POLICY IF EXISTS p_05628_hidden ON shard_1.ts_local;
DROP DATABASE IF EXISTS shard_0;
DROP DATABASE IF EXISTS shard_1;

CREATE DATABASE shard_0;
CREATE DATABASE shard_1;
CREATE TABLE shard_0.ts_local ENGINE = TimeSeries;
CREATE TABLE shard_1.ts_local ENGINE = TimeSeries;
INSERT INTO shard_1.ts_local (metric_name, tags, samples) VALUES ('m', map('job', 'a'), [(toDateTime64(140, 3), 7)]);

CREATE TABLE ts_dist AS shard_1.ts_local ENGINE = Distributed(test_cluster_two_shards_different_databases, '', ts_local);

SELECT '--- no restriction';
SELECT count() FROM prometheusQuery(ts_dist, 'm', 140);
SELECT count() FROM prometheusQuery(shard_1.ts_local, 'm', 140);

SELECT '--- a row policy on a shard-local table';
CREATE ROW POLICY p_05628_hidden ON shard_1.ts_local USING 0 TO ALL;
SELECT count() FROM prometheusQuery(ts_dist, 'm', 140); -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM prometheusQueryRange(ts_dist, 'm', 130, 140, 10); -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM prometheusQuery(shard_1.ts_local, 'm', 140); -- { serverError NOT_IMPLEMENTED }
DROP ROW POLICY p_05628_hidden ON shard_1.ts_local;

SELECT '--- a table filter on a shard-local table';
SELECT count() FROM prometheusQuery(ts_dist, 'm', 140) SETTINGS additional_table_filters = {'shard_1.ts_local': '0'}; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM prometheusQuery(shard_1.ts_local, 'm', 140) SETTINGS additional_table_filters = {'shard_1.ts_local': '0'}; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM prometheusQuery(shard_1.ts_local, 'm', 140) SETTINGS additional_table_filters = {'shard_1.ts_local': '1'};

DROP TABLE ts_dist;
DROP DATABASE shard_0;
DROP DATABASE shard_1;
