-- The final merge of a spilling `LIMIT BY` ends each output block once its rows reach the byte target
-- `prefer_external_sort_block_bytes`, so wide rows do not pile up into a block of `max_block_size` rows.
-- `LIMIT 1 BY` and `LIMIT 2 BY` take the two loops of the merge. The threshold is compared with the memory
-- tracked by the query, so allocations must not linger in the thread-local untracked buffers.
SET max_threads = 1;
SET max_untracked_memory = 0;

-- 256 rows of 4 KiB in blocks of 64 rows. Each block shares 4 keys with the next one.
CREATE TABLE wide_rows (k UInt64, v String) ENGINE = Memory;
INSERT INTO wide_rows SELECT number - 4 * intDiv(number, 64), repeat('x', 4096) FROM numbers(256)
SETTINGS max_block_size = 64, min_insert_block_size_rows = 64;

-- A one-byte threshold spills from the first block on, so every block becomes its own run. The merge takes
-- the keys that only one run holds in batches of up to 60 rows, and the byte target of 64 KiB ends an output
-- block after 16 rows of such a batch. With `max_block_size` alone, all merged rows would form one block. The
-- outer `length(v)` keeps the wide column in the merge.
SET max_block_size = 65536;
SET max_bytes_before_external_limit_by = 1;
SET prefer_external_sort_block_bytes = 65536;

SELECT max(blockSize()) <= 64, sum(length(v)) FROM (SELECT k, v FROM wide_rows LIMIT 1 BY k)
SETTINGS log_comment = '05878_external_limit_by_merge_block_bytes/limit_1_by';

SELECT max(blockSize()) <= 64, sum(length(v)) FROM (SELECT k, v FROM wide_rows LIMIT 2 BY k)
SETTINGS log_comment = '05878_external_limit_by_merge_block_bytes/limit_2_by';

-- `system.query_log` also holds the queries of every other test running in parallel, so read it with the
-- default settings.
SET max_block_size = DEFAULT;
SET max_untracked_memory = DEFAULT;

-- Both queries spilled and merged their runs once.
SYSTEM FLUSH LOGS query_log;
SELECT
    substring(log_comment, length('05878_external_limit_by_merge_block_bytes/') + 1) AS test_case,
    ProfileEvents['ExternalLimitByWritePart'] > 0 AS spilled,
    ProfileEvents['ExternalLimitByMerge'] AS merges
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND startsWith(log_comment, '05878_external_limit_by_merge_block_bytes/')
ORDER BY test_case;

DROP TABLE wide_rows;
