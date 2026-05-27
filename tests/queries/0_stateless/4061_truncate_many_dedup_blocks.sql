-- Tags: long, replica, no-shared-merge-tree
-- Verify that TRUNCATE works correctly when there are many deduplication blocks
-- in ZooKeeper, which previously could exceed the max ZooKeeper message size
-- because all block removals were sent in a single unbatched multi-request.

DROP TABLE IF EXISTS truncate_many_dedup1;
DROP TABLE IF EXISTS truncate_many_dedup2;

CREATE TABLE truncate_many_dedup1 (k UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04061/truncate_many_dedup', 'r1')
    ORDER BY k
    SETTINGS replicated_deduplication_window = 10000;

CREATE TABLE truncate_many_dedup2 (k UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04061/truncate_many_dedup', 'r2')
    ORDER BY k
    SETTINGS replicated_deduplication_window = 10000;

-- Insert many individual blocks to create many deduplication entries in ZooKeeper.
-- Each INSERT creates a separate dedup block entry.
INSERT INTO truncate_many_dedup1 SELECT number FROM numbers(200) SETTINGS max_insert_block_size = 1, min_insert_block_size_rows = 1;

SYSTEM SYNC REPLICA truncate_many_dedup2;

SELECT count() FROM truncate_many_dedup1;
SELECT count() FROM truncate_many_dedup2;

-- This TRUNCATE must batch its ZooKeeper multi-requests to avoid exceeding message size limits.
TRUNCATE TABLE truncate_many_dedup1 SETTINGS replication_alter_partitions_sync = 2;

SELECT count() FROM truncate_many_dedup1;
SELECT count() FROM truncate_many_dedup2;

DROP TABLE truncate_many_dedup1;
DROP TABLE truncate_many_dedup2;
