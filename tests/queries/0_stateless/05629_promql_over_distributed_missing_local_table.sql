-- Tags: no-parallel, no-fasttest, no-flaky-check, shard
-- no-parallel, no-flaky-check: uses the global shard_1 database of test_cluster_two_shards_different_databases_with_local.
-- no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- The first shard is this server and has no table in the current database, the second one has it in shard_1.

SET allow_experimental_time_series_table = 1;
SET distributed_foreground_insert = 1;

DROP TABLE IF EXISTS ts_dist;
DROP TABLE IF EXISTS ts_dist_skip;
DROP DATABASE IF EXISTS shard_1;

CREATE DATABASE shard_1;
CREATE TABLE shard_1.ts_local ENGINE = TimeSeries;
INSERT INTO shard_1.ts_local (metric_name, tags, samples) VALUES ('m', map('job', 'a'), [(toDateTime64(140, 3), 7)]);

CREATE TABLE ts_dist AS shard_1.ts_local ENGINE = Distributed(test_cluster_two_shards_different_databases_with_local, '', ts_local);
CREATE TABLE ts_dist_skip AS shard_1.ts_local ENGINE = Distributed(test_cluster_two_shards_different_databases_with_local, '', ts_local) SETTINGS skip_unavailable_shards = 1;

SELECT '--- skip_unavailable_shards = 0';
SELECT count() FROM prometheusQuery(ts_dist, 'm', 140) SETTINGS prefer_localhost_replica = 0; -- { serverError UNKNOWN_TABLE }
SELECT count() FROM prometheusQuery(ts_dist, 'm', 140) SETTINGS prefer_localhost_replica = 1; -- { serverError UNKNOWN_TABLE }

SELECT '--- skip_unavailable_shards = 1 in the query';
SELECT count() FROM prometheusQuery(ts_dist, 'm', 140) SETTINGS skip_unavailable_shards = 1, prefer_localhost_replica = 0;
SELECT count() FROM prometheusQuery(ts_dist, 'm', 140) SETTINGS skip_unavailable_shards = 1, prefer_localhost_replica = 1;
SELECT count() FROM prometheusQueryRange(ts_dist, 'm', 130, 140, 10) SETTINGS skip_unavailable_shards = 1;

SELECT '--- skip_unavailable_shards = 1 on the table';
SELECT count() FROM prometheusQuery(ts_dist_skip, 'm', 140) SETTINGS prefer_localhost_replica = 0;
SELECT count() FROM prometheusQuery(ts_dist_skip, 'm', 140) SETTINGS prefer_localhost_replica = 1;
SELECT count() FROM prometheusQuery(ts_dist_skip, 'm', 140) SETTINGS skip_unavailable_shards = 0; -- { serverError UNKNOWN_TABLE }

DROP TABLE ts_dist;
DROP TABLE ts_dist_skip;
DROP DATABASE shard_1;
