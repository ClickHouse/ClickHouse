-- Tags: no-parallel, no-fasttest, no-flaky-check, shard
-- no-parallel, no-flaky-check: uses the global shard_0 / shard_1 databases of test_cluster_two_shards_different_databases.
-- no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- A shard-local table missing on a local shard is skipped with skip_unavailable_shards, and fails the query without it.

SET allow_experimental_time_series_table = 1;
SET distributed_foreground_insert = 1;

DROP TABLE IF EXISTS ts_dist;
DROP TABLE IF EXISTS ts_dist_skip;
DROP DATABASE IF EXISTS shard_0;
DROP DATABASE IF EXISTS shard_1;

CREATE DATABASE shard_0;
CREATE DATABASE shard_1;
CREATE TABLE shard_1.ts_local ENGINE = TimeSeries;
INSERT INTO shard_1.ts_local (metric_name, tags, samples) VALUES ('m', map('job', 'a'), [(toDateTime64(140, 3), 7)]);

CREATE TABLE ts_dist AS shard_1.ts_local ENGINE = Distributed(test_cluster_two_shards_different_databases, '', ts_local);
CREATE TABLE ts_dist_skip AS shard_1.ts_local ENGINE = Distributed(test_cluster_two_shards_different_databases, '', ts_local) SETTINGS skip_unavailable_shards = 1;

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
DROP DATABASE shard_0;
DROP DATABASE shard_1;
