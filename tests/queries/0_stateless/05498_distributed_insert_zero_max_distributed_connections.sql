-- Tags: distributed

DROP TABLE IF EXISTS t_zero_mdc_dist;
DROP TABLE IF EXISTS t_zero_mdc;

CREATE TABLE t_zero_mdc (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_zero_mdc_dist (x UInt64) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_zero_mdc, x);

-- Two writing jobs (one per shard): both remote, then one local and one remote.
INSERT INTO t_zero_mdc_dist SETTINGS max_distributed_connections = 0, distributed_foreground_insert = 1, prefer_localhost_replica = 0 VALUES (1), (2);
INSERT INTO t_zero_mdc_dist SETTINGS max_distributed_connections = 0, distributed_foreground_insert = 1, prefer_localhost_replica = 1 VALUES (3), (4);
-- The shape from the fuzzer: an asynchronous insert into remote(), whose flush is what server shutdown waits for.
INSERT INTO FUNCTION remote('127.0.0.2', currentDatabase(), t_zero_mdc) SETTINGS max_distributed_connections = 0, async_insert = 1, wait_for_async_insert = 1 VALUES (5);

SELECT x FROM t_zero_mdc ORDER BY x;

DROP TABLE t_zero_mdc_dist;
DROP TABLE t_zero_mdc;
