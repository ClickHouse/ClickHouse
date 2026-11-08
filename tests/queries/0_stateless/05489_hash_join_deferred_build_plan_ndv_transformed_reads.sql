-- Tags: no-shared-catalog
-- no-shared-catalog: SYSTEM STOP MERGES stops them only on the current replica, and another one applies the mutation.

-- The `uniq` statistic of a column counts the values stored in the parts of the table. Some reads drop or change rows
-- inside `ReadFromMergeTree`: `FINAL`, `SAMPLE`, a row policy, deleted rows, a patch part, a mutation applied on the
-- fly and a lightweight update of the key. Then the statistic does not count the keys of the build, and the build
-- defers, as for a table without statistics. A plain read keeps the reserve for the whole table.

SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET join_algorithm = 'parallel_hash';
SET hash_join_layout = 'parallel';
SET collect_hash_table_stats_during_joins = 0;
-- With a spill threshold the stored blocks keep the keys, and only then does an `INNER` or `LEFT` build defer. This one
-- is far above what these builds take.
SET max_bytes_before_external_join = 10737418240;
SET max_bytes_ratio_before_external_join = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';
SET max_threads = 4;
SET enable_parallel_replicas = 0;
SET enable_hash_join_deferred_build = 1;

CREATE TABLE t_probe (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_probe SELECT number FROM numbers(100000);

-- Every table below stores the keys 0 to 99999. Its read returns 1000 distinct keys, or 10039 with `SAMPLE`.
CREATE TABLE t_final (k UInt64 STATISTICS(uniq), ver UInt64, is_deleted UInt8)
ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY k;
INSERT INTO t_final SELECT number, 1, 0 FROM numbers(100000);
INSERT INTO t_final SELECT number, 2, 1 FROM numbers(1000, 99000);

CREATE TABLE t_sample (k UInt64 STATISTICS(uniq)) ENGINE = MergeTree ORDER BY intHash32(k) SAMPLE BY intHash32(k);
INSERT INTO t_sample SELECT number FROM numbers(100000);

CREATE TABLE t_policy (k UInt64 STATISTICS(uniq)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_policy SELECT number FROM numbers(100000);
CREATE ROW POLICY p_05489 ON t_policy USING k < 1000 TO ALL;

-- The merges are stopped so that none of them drops the deleted rows or applies the patches and the mutation.
CREATE TABLE t_deleted (k UInt64 STATISTICS(uniq)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_deleted SELECT number FROM numbers(100000);
DELETE FROM t_deleted WHERE k >= 1000 SETTINGS lightweight_delete_mode = 'alter_update';
SYSTEM STOP MERGES t_deleted;

CREATE TABLE t_patch_delete (k UInt64 STATISTICS(uniq)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES t_patch_delete;
INSERT INTO t_patch_delete SELECT number FROM numbers(100000);
DELETE FROM t_patch_delete WHERE k >= 1000 SETTINGS lightweight_delete_mode = 'lightweight_update_force';

CREATE TABLE t_update (k UInt64 STATISTICS(uniq)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES t_update;
INSERT INTO t_update SELECT number FROM numbers(100000);
UPDATE t_update SET k = k % 1000 WHERE k >= 1000;

-- A replicated table, so that `SYNC REPLICA` can load the pending mutation, which a replica reads in the background.
CREATE TABLE t_on_fly (k UInt64 STATISTICS(uniq))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_on_fly', '1') ORDER BY tuple();
SYSTEM STOP MERGES t_on_fly;
INSERT INTO t_on_fly SELECT number FROM numbers(100000);
ALTER TABLE t_on_fly DELETE WHERE k >= 1000 SETTINGS mutations_sync = 0;
SYSTEM SYNC REPLICA t_on_fly PULL;

SELECT 'final', count() FROM t_probe l INNER JOIN t_final AS r FINAL ON l.k = r.k SETTINGS log_comment = '05489 final';
SELECT 'sample', count() FROM t_probe l INNER JOIN t_sample AS r SAMPLE 1 / 10 ON l.k = r.k
SETTINGS log_comment = '05489 sample';
SELECT 'policy', count() FROM t_probe l INNER JOIN t_policy AS r ON l.k = r.k SETTINGS log_comment = '05489 policy';
SELECT 'deleted', count() FROM t_probe l INNER JOIN t_deleted AS r ON l.k = r.k SETTINGS log_comment = '05489 deleted';
SELECT 'patch_delete', count() FROM t_probe l INNER JOIN t_patch_delete AS r ON l.k = r.k
SETTINGS log_comment = '05489 patch_delete';
SELECT 'update', count() FROM t_probe l INNER JOIN t_update AS r ON l.k = r.k SETTINGS log_comment = '05489 update';
SELECT 'on_fly', count() FROM t_probe l INNER JOIN t_on_fly AS r ON l.k = r.k
SETTINGS apply_mutations_on_fly = 1, log_comment = '05489 on_fly';
SELECT 'plain', count() FROM t_probe l INNER JOIN t_sample AS r ON l.k = r.k SETTINGS log_comment = '05489 plain';

DROP ROW POLICY p_05489 ON t_policy;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['HashJoinPreallocatedElementsInHashTables'] >= 90000,
    ProfileEvents['HashJoinDeferredBuilds']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05489 %'
ORDER BY log_comment;
