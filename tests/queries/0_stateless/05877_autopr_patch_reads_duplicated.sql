-- A patch part whose ranges cannot follow those of the read is read in full by every replica, whatever share of the
-- read it takes: a `MergeOnKey` patch of a table without a sorting key, or with a sorting key whose value is the same in
-- every row, and a `Join` patch of a part merged from its source parts. Its bytes are duplicated reading, so when the patch is most of what a query reads, the duplicated-read
-- gate declines parallel replicas, which the cost model alone would take.

DROP TABLE IF EXISTS t_patch_merge_on_key;
DROP TABLE IF EXISTS t_patch_join;
DROP TABLE IF EXISTS t_patch_equal_keys;

-- Payloads that hardly compress; `sipHash128` because builds without SSL have no `SHA256`.
CREATE TABLE t_patch_merge_on_key (key UInt64, a String, b String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2', apply_patches_on_merge = 0;
SYSTEM STOP MERGES t_patch_merge_on_key;
INSERT INTO t_patch_merge_on_key SELECT number, hex(sipHash128(number, 0)), '' FROM numbers(500000);

-- Every granule of the patch holds the key of every range of the part.
CREATE TABLE t_patch_equal_keys (c UInt64 DEFAULT 0, key UInt64, a String, b String) ENGINE = MergeTree ORDER BY c
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2', apply_patches_on_merge = 0;
SYSTEM STOP MERGES t_patch_equal_keys;
INSERT INTO t_patch_equal_keys (key, a, b) SELECT number, hex(sipHash128(number, 0)), '' FROM numbers(500000);

-- The patch is written for the two parts, which are then merged without applying it.
CREATE TABLE t_patch_join (key UInt64, a String, b String) ENGINE = MergeTree ORDER BY key
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v1', apply_patches_on_merge = 0;
INSERT INTO t_patch_join SELECT number, hex(sipHash128(number, 0)), '' FROM numbers(250000);
INSERT INTO t_patch_join SELECT number + 250000, hex(sipHash128(number + 250000, 0)), '' FROM numbers(250000);

SET enable_lightweight_update = 1;
UPDATE t_patch_merge_on_key SET b = concat(hex(sipHash128(key, 1)), hex(sipHash128(key, 2))) WHERE 1;
UPDATE t_patch_join SET b = concat(hex(sipHash128(key, 1)), hex(sipHash128(key, 2))) WHERE 1;
UPDATE t_patch_equal_keys SET b = concat(hex(sipHash128(key, 1)), hex(sipHash128(key, 2))) WHERE 1;
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
