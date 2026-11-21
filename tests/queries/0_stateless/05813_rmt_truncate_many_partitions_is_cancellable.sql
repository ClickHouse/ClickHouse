-- Tags: zookeeper, no-shared-merge-tree, no-replicated-database
-- Tag no-shared-merge-tree: checks the per-partition loop of `ReplicatedMergeTree`.
-- Tag no-replicated-database: the other replica runs the replicated TRUNCATE on its own and may still hold its block number locks when they are counted.

DROP TABLE IF EXISTS t SYNC;
CREATE TABLE t (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r1') PARTITION BY k ORDER BY k;
INSERT INTO t SELECT number FROM numbers(100) SETTINGS max_partitions_per_insert_block = 0;

-- The loop over the partitions stops on max_execution_time and drops nothing.
TRUNCATE TABLE t SETTINGS max_execution_time = 0.05; -- { serverError TIMEOUT_EXCEEDED }
SYSTEM SYNC REPLICA t;
SELECT count() FROM t;

-- DROP PARTITION ALL takes the same loop, and stops with timeout_overflow_mode = 'break' too: there is no partial result.
ALTER TABLE t DROP PARTITION ALL SETTINGS max_execution_time = 0.05, timeout_overflow_mode = 'break'; -- { serverError TIMEOUT_EXCEEDED }
SYSTEM SYNC REPLICA t;
SELECT count() FROM t;

-- No block number lock is left behind.
SELECT count() FROM system.zookeeper
WHERE path IN (
    SELECT concat((SELECT zookeeper_path FROM system.replicas WHERE database = currentDatabase() AND table = 't'), '/block_numbers/', name)
    FROM system.zookeeper
    WHERE path = (SELECT concat(zookeeper_path, '/block_numbers') FROM system.replicas WHERE database = currentDatabase() AND table = 't'))
  AND name LIKE 'block-%';

DROP TABLE t SYNC;
