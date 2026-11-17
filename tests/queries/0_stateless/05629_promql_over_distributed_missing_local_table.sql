-- Tags: no-parallel, no-fasttest, no-flaky-check, shard
-- no-parallel, no-flaky-check: uses the global shard_1 database of test_cluster_two_shards_different_databases_with_local.
-- no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- The first shard is this server and has no table in the current database, the second one has it in shard_1.

SET allow_experimental_time_series_table = 1;
SET distributed_foreground_insert = 1;

DROP TABLE IF EXISTS ts_dist;
DROP TABLE IF EXISTS ts_dist_skip;
DROP TABLE IF EXISTS ts_dist_strict;
DROP TABLE IF EXISTS t_saved;
DROP TABLE IF EXISTS t_saved_replicas;
DROP TABLE IF EXISTS t_plain;
DROP DATABASE IF EXISTS shard_1;

CREATE DATABASE shard_1;
CREATE TABLE shard_1.ts_local ENGINE = TimeSeries;
INSERT INTO shard_1.ts_local (metric_name, tags, samples) VALUES ('m', map('job', 'a'), [(toDateTime64(140, 3), 7)]);

CREATE TABLE ts_dist AS shard_1.ts_local ENGINE = Distributed(test_cluster_two_shards_different_databases_with_local, '', ts_local);
CREATE TABLE ts_dist_skip AS shard_1.ts_local ENGINE = Distributed(test_cluster_two_shards_different_databases_with_local, '', ts_local) SETTINGS skip_unavailable_shards = 1;
CREATE TABLE ts_dist_strict AS shard_1.ts_local ENGINE = Distributed(test_cluster_two_shards_different_databases_with_local, '', ts_local) SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable';

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

SELECT '--- skip_unavailable_shards_mode = unavailable skips no shard without the table';
SELECT count() FROM prometheusQuery(ts_dist, 'm', 140) SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable', prefer_localhost_replica = 0; -- { serverError UNKNOWN_TABLE }
SELECT count() FROM prometheusQuery(ts_dist, 'm', 140) SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable', prefer_localhost_replica = 1; -- { serverError UNKNOWN_TABLE }
SELECT count() FROM prometheusQuery(ts_dist_skip, 'm', 140) SETTINGS skip_unavailable_shards_mode = 'unavailable'; -- { serverError UNKNOWN_TABLE }
SELECT count() FROM prometheusQuery(ts_dist_strict, 'm', 140); -- { serverError UNKNOWN_TABLE }
SELECT count() FROM prometheusQuery(ts_dist_strict, 'm', 140) SETTINGS skip_unavailable_shards_mode = 'unavailable_or_table_missing';

SELECT '--- a skipped local shard counts against the limits';
SELECT count() FROM prometheusQuery(ts_dist, 'm', 140) SETTINGS skip_unavailable_shards = 1, max_skip_unavailable_shards_ratio = 0.4; -- { serverError TOO_MANY_UNAVAILABLE_SHARDS }
SELECT count() FROM prometheusQuery(ts_dist, 'm', 140) SETTINGS skip_unavailable_shards = 1, max_skip_unavailable_shards_ratio = 0.5;
SELECT count() FROM prometheusQuery(ts_dist, 'm', 140) SETTINGS skip_unavailable_shards = 1, max_skip_unavailable_shards_num = 1;

SELECT '--- a table created from cluster() decides on every read';
CREATE TABLE shard_1.t_plain (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO shard_1.t_plain VALUES (1);
CREATE TABLE t_saved AS cluster('test_cluster_two_shards_different_databases_with_local', view(SELECT x FROM t_plain), SETTINGS skip_unavailable_shards = 1);
SELECT count() FROM t_saved;
SELECT count() FROM t_saved SETTINGS skip_unavailable_shards = 0; -- { serverError UNKNOWN_TABLE }
SELECT count() FROM t_saved SETTINGS skip_unavailable_shards_mode = 'unavailable'; -- { serverError UNKNOWN_TABLE }

SELECT '--- the remote replica of the shard of the missing local table answers, whatever the shard skip settings are';
-- The first replica of the only shard is this server, the second one has the table in shard_1.
DESCRIBE cluster('test_cluster_one_shard_local_and_remote_replica', view(SELECT x FROM t_plain));
DESCRIBE cluster('test_cluster_one_shard_local_and_remote_replica', view(SELECT x FROM t_plain)) SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable';
DESCRIBE cluster('test_cluster_one_shard_local_and_remote_replica', view(SELECT x FROM t_plain)) SETTINGS skip_unavailable_shards = 1;
SELECT count() FROM cluster('test_cluster_one_shard_local_and_remote_replica', view(SELECT x FROM t_plain));
SELECT count() FROM cluster('test_cluster_one_shard_local_and_remote_replica', view(SELECT x FROM t_plain)) SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable';
CREATE TABLE t_saved_replicas AS cluster('test_cluster_one_shard_local_and_remote_replica', view(SELECT x FROM t_plain));
SELECT count() FROM t_saved_replicas;
SELECT count() FROM t_saved_replicas SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable';

SELECT '--- custom-key parallel replicas ask the local replica as a remote one, so its missing table is an error';
SELECT x FROM cluster('test_cluster_one_shard_local_and_remote_replica', view(SELECT x FROM t_plain))
SETTINGS prefer_localhost_replica = 1, enable_parallel_replicas = 1, max_parallel_replicas = 2, parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'x', serialize_query_plan = 0; -- { serverError UNKNOWN_TABLE }
SELECT x FROM cluster('test_cluster_one_shard_local_and_remote_replica', view(SELECT x FROM t_plain))
SETTINGS prefer_localhost_replica = 1, enable_parallel_replicas = 1, max_parallel_replicas = 2, parallel_replicas_mode = 'custom_key_range', parallel_replicas_custom_key = 'x', serialize_query_plan = 0; -- { serverError UNKNOWN_TABLE }

CREATE TABLE t_plain (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_plain VALUES (2), (3);
SELECT count() FROM t_saved;

DROP TABLE t_saved;
DROP TABLE t_saved_replicas;
DROP TABLE t_plain;
DROP TABLE ts_dist;
DROP TABLE ts_dist_skip;
DROP TABLE ts_dist_strict;
DROP DATABASE shard_1;
