-- A patch part whose ranges cannot follow those of the read is read in full by every replica, whatever share of the
-- read it takes: a `MergeOnKey` patch of a table without a sorting key, or with a sorting key whose value is the same in
-- every row, or of the run of equal keys a query reads inside a larger part, also when the patch is a single granule or the read only three, and a `Join` patch of a part merged from its
-- source parts. Its bytes are duplicated reading, so when the patch is most of what a query reads, the duplicated-read
-- gate declines parallel replicas, which the cost model alone would take.

DROP TABLE IF EXISTS t_patch_merge_on_key;
DROP TABLE IF EXISTS t_patch_join;
DROP TABLE IF EXISTS t_patch_equal_keys;
DROP TABLE IF EXISTS t_patch_equal_run;
DROP TABLE IF EXISTS t_patch_one_granule;
DROP TABLE IF EXISTS t_patch_small_read;

-- Payloads that hardly compress; `sipHash128` because builds without SSL have no `SHA256`.
-- Fixed granules: a part of fewer than 4 marks is taken to follow the read whatever its patch.
CREATE TABLE t_patch_merge_on_key (key UInt64, a String, b String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2', apply_patches_on_merge = 0;
SYSTEM STOP MERGES t_patch_merge_on_key;
INSERT INTO t_patch_merge_on_key SELECT number, hex(sipHash128(number, 0)), '' FROM numbers(100000);

-- Every granule of the patch holds the key of every range of the part.
CREATE TABLE t_patch_equal_keys (c UInt64 DEFAULT 0, key UInt64, a String, b String) ENGINE = MergeTree ORDER BY c
SETTINGS index_granularity = 8192, enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2', apply_patches_on_merge = 0;
SYSTEM STOP MERGES t_patch_equal_keys;
INSERT INTO t_patch_equal_keys (key, a, b) SELECT number, hex(sipHash128(number, 0)), '' FROM numbers(100000);

-- The patch of 8192 rows is one granule, which every piece of the read takes.
CREATE TABLE t_patch_one_granule (key UInt64, a String, b String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2', apply_patches_on_merge = 0;
SYSTEM STOP MERGES t_patch_one_granule;
INSERT INTO t_patch_one_granule SELECT number, hex(sipHash128(number, 0)), '' FROM numbers(100000);

-- A part of 3 granules.
CREATE TABLE t_patch_small_read (key UInt64, a String, b String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2', apply_patches_on_merge = 0;
SYSTEM STOP MERGES t_patch_small_read;
INSERT INTO t_patch_small_read SELECT number, hex(sipHash128(number, 0)), '' FROM numbers(24576);

-- 12 granules of equal keys inside the first quarter of a part of 64: the pieces of the part would not tell, those of
-- the read do.
CREATE TABLE t_patch_equal_run (c UInt64, key UInt64, a String, b String) ENGINE = MergeTree ORDER BY c
SETTINGS index_granularity = 8192, enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2', apply_patches_on_merge = 0;
SYSTEM STOP MERGES t_patch_equal_run;
INSERT INTO t_patch_equal_run SELECT if(number >= 16384 AND number < 114688, 32768, number), number, hex(sipHash128(number, 0)), ''
FROM numbers(524288);

-- The patch is written for the two parts, which are then merged without applying it.
CREATE TABLE t_patch_join (key UInt64, a String, b String) ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 8192, enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v1', apply_patches_on_merge = 0;
INSERT INTO t_patch_join SELECT number, hex(sipHash128(number, 0)), '' FROM numbers(50000);
INSERT INTO t_patch_join SELECT number + 50000, hex(sipHash128(number + 50000, 0)), '' FROM numbers(50000);

SET enable_lightweight_update = 1;
UPDATE t_patch_merge_on_key SET b = concat(hex(sipHash128(key, 1)), hex(sipHash128(key, 2))) WHERE 1;
UPDATE t_patch_join SET b = concat(hex(sipHash128(key, 1)), hex(sipHash128(key, 2))) WHERE 1;
UPDATE t_patch_equal_keys SET b = concat(hex(sipHash128(key, 1)), hex(sipHash128(key, 2))) WHERE 1;
UPDATE t_patch_one_granule SET b = arrayStringConcat(arrayMap(i -> hex(sipHash128(key, i)), range(1, 33))) WHERE key < 8192;
UPDATE t_patch_small_read SET b = arrayStringConcat(arrayMap(i -> hex(sipHash128(key, i)), range(1, 33))) WHERE key < 8192;
UPDATE t_patch_equal_run SET b = concat(hex(sipHash128(key, 1)), hex(sipHash128(key, 2))) WHERE c = 32768;
OPTIMIZE TABLE t_patch_join FINAL;
SYSTEM STOP MERGES t_patch_join;

SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1,
    automatic_parallel_replicas_mode = 1;
-- See `05293_autopr_duplicated_read_ratio_gate`: the ratio alone has to decide.
SET serialize_query_plan = 0, merge_tree_min_bytes_per_task_for_remote_reading = 4096,
    automatic_parallel_replicas_min_bytes_per_replica = 0, max_rows_to_read = 0;
-- The runs must select the same granules, see `05332_autopr_min_bytes_per_replica_events`.
SET use_query_condition_cache = 0, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

-- The first run of each query collects the statistics; the second is declined, as the patch is read by every replica
-- and takes more than half of the read; the third, with the gate disabled, shows that the ratio alone declined it.
SELECT key % 10 AS k, max(a), max(b) FROM t_patch_merge_on_key GROUP BY k FORMAT Null
SETTINGS log_comment = 'patch_dup_merge_on_key_1_collect';
SELECT key % 10 AS k, max(a), max(b) FROM t_patch_merge_on_key GROUP BY k FORMAT Null
SETTINGS log_comment = 'patch_dup_merge_on_key_2_at_default';
SELECT key % 10 AS k, max(a), max(b) FROM t_patch_merge_on_key GROUP BY k FORMAT Null
SETTINGS log_comment = 'patch_dup_merge_on_key_3_disabled', automatic_parallel_replicas_max_duplicated_read_ratio = 1;

SELECT key % 10 AS k, max(a), max(b) FROM t_patch_join GROUP BY k FORMAT Null
SETTINGS log_comment = 'patch_dup_join_1_collect';
SELECT key % 10 AS k, max(a), max(b) FROM t_patch_join GROUP BY k FORMAT Null
SETTINGS log_comment = 'patch_dup_join_2_at_default';
SELECT key % 10 AS k, max(a), max(b) FROM t_patch_join GROUP BY k FORMAT Null
SETTINGS log_comment = 'patch_dup_join_3_disabled', automatic_parallel_replicas_max_duplicated_read_ratio = 1;

SELECT key % 10 AS k, max(a), max(b) FROM t_patch_equal_keys GROUP BY k FORMAT Null
SETTINGS log_comment = 'patch_dup_equal_keys_1_collect';
SELECT key % 10 AS k, max(a), max(b) FROM t_patch_equal_keys GROUP BY k FORMAT Null
SETTINGS log_comment = 'patch_dup_equal_keys_2_at_default';
SELECT key % 10 AS k, max(a), max(b) FROM t_patch_equal_keys GROUP BY k FORMAT Null
SETTINGS log_comment = 'patch_dup_equal_keys_3_disabled', automatic_parallel_replicas_max_duplicated_read_ratio = 1;

SELECT key % 10 AS k, max(a), max(b) FROM t_patch_one_granule GROUP BY k FORMAT Null
SETTINGS log_comment = 'patch_dup_one_granule_1_collect';
SELECT key % 10 AS k, max(a), max(b) FROM t_patch_one_granule GROUP BY k FORMAT Null
SETTINGS log_comment = 'patch_dup_one_granule_2_at_default';
SELECT key % 10 AS k, max(a), max(b) FROM t_patch_one_granule GROUP BY k FORMAT Null
SETTINGS log_comment = 'patch_dup_one_granule_3_disabled', automatic_parallel_replicas_max_duplicated_read_ratio = 1;

-- Fewer threads than the three replicas give, so that the cost model takes them for a small read.
SELECT key % 10 AS k, max(a), max(b) FROM t_patch_small_read GROUP BY k FORMAT Null
SETTINGS log_comment = 'patch_dup_small_read_1_collect', max_threads = 4, merge_tree_min_rows_for_concurrent_read = 8192;
SELECT key % 10 AS k, max(a), max(b) FROM t_patch_small_read GROUP BY k FORMAT Null
SETTINGS log_comment = 'patch_dup_small_read_2_at_default', max_threads = 4, merge_tree_min_rows_for_concurrent_read = 8192;
SELECT key % 10 AS k, max(a), max(b) FROM t_patch_small_read GROUP BY k FORMAT Null
SETTINGS log_comment = 'patch_dup_small_read_3_disabled', max_threads = 4, merge_tree_min_rows_for_concurrent_read = 8192,
    automatic_parallel_replicas_max_duplicated_read_ratio = 1;

-- Fewer threads than the three replicas give, so that the cost model takes them for a read of 12 granules.
SELECT key % 10 AS k, max(a), max(b) FROM t_patch_equal_run WHERE c = 32768 GROUP BY k FORMAT Null
SETTINGS log_comment = 'patch_dup_equal_run_1_collect', max_threads = 4, merge_tree_min_rows_for_concurrent_read = 8192;
SELECT key % 10 AS k, max(a), max(b) FROM t_patch_equal_run WHERE c = 32768 GROUP BY k FORMAT Null
SETTINGS log_comment = 'patch_dup_equal_run_2_at_default', max_threads = 4, merge_tree_min_rows_for_concurrent_read = 8192;
SELECT key % 10 AS k, max(a), max(b) FROM t_patch_equal_run WHERE c = 32768 GROUP BY k FORMAT Null
SETTINGS log_comment = 'patch_dup_equal_run_3_disabled', max_threads = 4, merge_tree_min_rows_for_concurrent_read = 8192,
    automatic_parallel_replicas_max_duplicated_read_ratio = 1;

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment,
       ProfileEvents['AutoParallelReplicasRejectedByDuplicatedReads'] AS rejected_by_duplicated_reads,
       ProfileEvents['ParallelReplicasUsedCount'] > 0 AS parallel_replicas_used
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15)))
    AND (current_database = currentDatabase())
    AND startsWith(log_comment, 'patch_dup_')
    AND (type = 'QueryFinish') AND (query_id = initial_query_id)
ORDER BY log_comment;

DROP TABLE t_patch_merge_on_key;
DROP TABLE t_patch_join;
DROP TABLE t_patch_equal_keys;
DROP TABLE t_patch_equal_run;
DROP TABLE t_patch_one_granule;
DROP TABLE t_patch_small_read;
