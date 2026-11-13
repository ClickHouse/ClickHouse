-- Tags: no-parallel, no-fasttest, no-flaky-check, shard
-- no-parallel, no-flaky-check: uses the global shard_0 / shard_1 databases of test_cluster_two_shards_different_databases.
-- no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- A row policy or table filter on a shard-local table is refused, and declared shard-skip settings apply.

SET allow_experimental_time_series_table = 1;
SET distributed_foreground_insert = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_dist;
DROP TABLE IF EXISTS ts_dist_skip;
DROP ROW POLICY IF EXISTS p_05628_hidden ON shard_1.ts_local;
DROP DATABASE IF EXISTS shard_0;
DROP DATABASE IF EXISTS shard_1;

CREATE DATABASE shard_0;
CREATE DATABASE shard_1;

-- Only the second shard holds a TimeSeries table: the first one fails to describe it.
CREATE TABLE shard_1.ts_local ENGINE = TimeSeries;
INSERT INTO shard_1.ts_local (metric_name, tags, samples) VALUES ('m', map('job', 'a'), [(toDateTime64(140, 3), 7)]);

CREATE TABLE ts_dist AS shard_1.ts_local
    ENGINE = Distributed(test_cluster_two_shards_different_databases, '', ts_local);
CREATE TABLE ts_dist_skip AS shard_1.ts_local
    ENGINE = Distributed(test_cluster_two_shards_different_databases, '', ts_local) SETTINGS skip_unavailable_shards = 1;

SELECT '--- the table lacks on a shard: refused unless the table declares skip_unavailable_shards ---';
SELECT count() FROM prometheusQuery(ts_dist, 'm', 140); -- { serverError UNKNOWN_TABLE }
SELECT count() FROM prometheusQuery(ts_dist_skip, 'm', 140);
SELECT count() FROM prometheusQueryRange(ts_dist_skip, 'm', 130, 140, 10);
SELECT '--- an explicit setting of the query wins over the declared one ---';
SELECT count() FROM prometheusQuery(ts_dist_skip, 'm', 140) SETTINGS skip_unavailable_shards = 0; -- { serverError UNKNOWN_TABLE }

-- Both shards have the table from here on.
CREATE TABLE shard_0.ts_local ENGINE = TimeSeries;

SELECT '--- a row policy on a shard-local table is refused, not bypassed ---';
CREATE ROW POLICY p_05628_hidden ON shard_1.ts_local USING 0 TO ALL;
SELECT metric_name FROM shard_1.ts_local;
SELECT count() FROM prometheusQuery(ts_dist, 'm', 140); -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM prometheusQueryRange(ts_dist, 'm', 130, 140, 10); -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM prometheusQuery(shard_1.ts_local, 'm', 140); -- { serverError NOT_IMPLEMENTED }
DROP ROW POLICY p_05628_hidden ON shard_1.ts_local;
SELECT count() FROM prometheusQuery(shard_1.ts_local, 'm', 140);

SELECT '--- a table filter on a shard-local table is refused, not bypassed ---';
SELECT count() FROM prometheusQuery(ts_dist, 'm', 140) SETTINGS additional_table_filters = {'shard_1.ts_local': '0'}; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM prometheusQuery(shard_1.ts_local, 'm', 140) SETTINGS additional_table_filters = {'shard_1.ts_local': '0'}; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM prometheusQuery(shard_1.ts_local, 'm', 140) SETTINGS additional_table_filters = {'shard_1.ts_local': '1'};

DROP TABLE ts_dist;
DROP TABLE ts_dist_skip;
DROP DATABASE shard_0;
DROP DATABASE shard_1;
