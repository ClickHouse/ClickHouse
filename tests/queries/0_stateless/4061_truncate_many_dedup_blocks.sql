-- Tags: long, replica, no-shared-merge-tree
-- Verify that TRUNCATE batches the ZooKeeper removal requests for deduplication
-- blocks instead of sending them all in a single multi-request. An unbatched
-- multi-request could exceed the maximum ZooKeeper message size (the default
-- jute.maxbuffer of 1MB) and fail for tables with large deduplication windows.

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
-- Each row becomes a separate inserted block, hence a separate dedup block entry.
-- 200 entries comfortably exceeds zkutil::MULTI_BATCH_SIZE (100), so a correct
-- implementation must split the removals into more than one multi-request.
INSERT INTO truncate_many_dedup1 SELECT number FROM numbers(200) SETTINGS max_insert_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0;

SYSTEM SYNC REPLICA truncate_many_dedup2;

SELECT count() FROM truncate_many_dedup1;
SELECT count() FROM truncate_many_dedup2;

-- This TRUNCATE removes all deduplication block entries from ZooKeeper. The
-- removals must be split into batches of at most MULTI_BATCH_SIZE operations.
TRUNCATE TABLE truncate_many_dedup1 SETTINGS replication_alter_partitions_sync = 2;

SELECT count() FROM truncate_many_dedup1;
SELECT count() FROM truncate_many_dedup2;

-- Assert that the removals were actually batched. Each multi-request shares a
-- single xid, so grouping the Remove sub-requests for this table's nodes by
-- (session_id, xid) gives the number of operations per multi-request.
--   * The pre-fix code sends one multi-request with all 200+ removals, so the
--     largest group exceeds 100 and there is only a single group -> prints "0 0".
--   * The fixed code splits them into batches of at most 100, so the largest
--     group is <= 100 and there is more than one group -> prints "1 1".
SYSTEM FLUSH LOGS zookeeper_log;

SELECT
    max(removals_per_multi) <= 100 AS every_batch_within_limit,
    count() > 1 AS removals_were_batched
FROM
(
    SELECT count() AS removals_per_multi
    FROM system.zookeeper_log
    WHERE type = 'Request'
      AND op_num = 'Remove'
      AND match(path, '^/clickhouse/tables/' || currentDatabase() || '/test_04061/truncate_many_dedup/(blocks|async_blocks|deduplication_hashes)/')
    GROUP BY session_id, xid
);

DROP TABLE truncate_many_dedup1;
DROP TABLE truncate_many_dedup2;
