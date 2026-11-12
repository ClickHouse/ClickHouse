-- Two async inserts with the same `insert_deduplication_token` but different data are coalesced
-- into one batch. In the materialized view target, the rows of the two tokens land in different
-- partitions. For the partition of the second (duplicate) token, self-deduplication removes all
-- rows, and the recalculated block for that partition is empty. The empty block used to reach
-- `MergeTreeDataWriter::mergeBlock` and trigger the "Required source after the first merge is
-- not 0" logical error with `optimize_on_insert` enabled and a `ReplacingMergeTree` target.

DROP TABLE IF EXISTS mv_dedup_empty;
DROP TABLE IF EXISTS tgt_dedup_empty;
DROP TABLE IF EXISTS src_dedup_empty;

CREATE TABLE src_dedup_empty (p UInt64, v UInt64) ENGINE = MergeTree ORDER BY v;
CREATE TABLE tgt_dedup_empty (p UInt64, v UInt64) ENGINE = ReplacingMergeTree PARTITION BY p ORDER BY v
    SETTINGS non_replicated_deduplication_window = 100;
CREATE MATERIALIZED VIEW mv_dedup_empty TO tgt_dedup_empty AS SELECT p, v FROM src_dedup_empty;

SET async_insert = 1;
SET wait_for_async_insert = 0;
SET async_insert_deduplicate = 1;
SET deduplicate_blocks_in_dependent_materialized_views = 1;
SET optimize_on_insert = 1;
-- Prevent an automatic flush between the two inserts; the flush below is explicit.
SET async_insert_busy_timeout_min_ms = 600000;
SET async_insert_busy_timeout_max_ms = 600000;
SET async_insert_use_adaptive_busy_timeout = 0;

INSERT INTO src_dedup_empty SETTINGS insert_deduplication_token = 'tok' VALUES (0, 1);
INSERT INTO src_dedup_empty SETTINGS insert_deduplication_token = 'tok' VALUES (1, 2);

SYSTEM FLUSH ASYNC INSERT QUEUE src_dedup_empty;

-- The source table has no deduplication window, so it keeps both rows. In the target table the
-- second insert is a duplicate of the first by token, so only the first row survives.
SELECT 'src', p, v FROM src_dedup_empty ORDER BY v;
SELECT 'tgt', p, v FROM tgt_dedup_empty ORDER BY v;

DROP TABLE mv_dedup_empty;
DROP TABLE tgt_dedup_empty;
DROP TABLE src_dedup_empty;
