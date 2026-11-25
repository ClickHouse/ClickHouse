-- Tags: shard

-- The set of an `IN` subquery in a `CHECK` constraint gets the storage filters, so `Distributed` can skip shards.

DROP TABLE IF EXISTS t_check_skip;
DROP TABLE IF EXISTS t_check_noskip;
DROP TABLE IF EXISTS dist_check_skip;
DROP TABLE IF EXISTS data_check_skip;

-- Both shards of the cluster read this table of the same server, so either shard returns the same rows.
CREATE TABLE data_check_skip (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO data_check_skip VALUES (1), (2), (3);

CREATE TABLE dist_check_skip AS data_check_skip ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), data_check_skip, k);

CREATE TABLE t_check_skip (x UInt64, CONSTRAINT c CHECK x IN (SELECT k FROM dist_check_skip WHERE k = 1)) ENGINE = MergeTree ORDER BY x;

INSERT INTO t_check_skip SETTINGS optimize_skip_unused_shards = 1, force_optimize_skip_unused_shards = 1, async_insert = 0 VALUES (1);
INSERT INTO t_check_skip SETTINGS optimize_skip_unused_shards = 1, force_optimize_skip_unused_shards = 1, async_insert = 0 VALUES (2); -- { serverError VIOLATED_CONSTRAINT }

-- Without a predicate on the sharding key no shard can be skipped.
CREATE TABLE t_check_noskip (x UInt64, CONSTRAINT c CHECK x IN (SELECT k FROM dist_check_skip)) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_check_noskip SETTINGS optimize_skip_unused_shards = 1, force_optimize_skip_unused_shards = 1, async_insert = 0 VALUES (1); -- { serverError UNABLE_TO_SKIP_UNUSED_SHARDS }

SELECT count() FROM t_check_skip;

DROP TABLE t_check_skip;
DROP TABLE t_check_noskip;
DROP TABLE dist_check_skip;
DROP TABLE data_check_skip;
