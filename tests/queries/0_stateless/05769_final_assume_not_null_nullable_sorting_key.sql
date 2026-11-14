-- Every NULL of a Nullable sorting key column is one key for FINAL, but assumeNotNull returns the value
-- stored under each NULL, so a condition with it must not drop rows before the FINAL merge
SET explain_query_plan_default = 'legacy', enable_parallel_replicas = 0;
SET optimize_move_to_prewhere = 1, optimize_move_to_prewhere_if_final = 1, apply_prewhere_after_final = 0;
SET apply_row_policy_after_final = 1, query_plan_optimize_lazy_final = 0;
SET use_skip_indexes = 1, use_skip_indexes_if_final = 1, use_skip_indexes_if_final_exact_mode = 1;
SET use_partition_pruning = 1, defer_partition_pruning_after_final = 1;
SET do_not_merge_across_partitions_select_final = 0, enable_automatic_decision_for_merging_across_partitions_for_final = 0;

DROP TABLE IF EXISTS t_final_nullable_key;
CREATE TABLE t_final_nullable_key (n UInt64, k Nullable(UInt64), ver UInt64)
ENGINE = ReplacingMergeTree(ver) ORDER BY (n, k)
SETTINGS allow_nullable_key = 1, index_granularity = 100, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES t_final_nullable_key;
INSERT INTO t_final_nullable_key SELECT number, nullIf(toUInt64(1), toUInt64(1)), 1 FROM numbers(1000);
INSERT INTO t_final_nullable_key SELECT number, nullIf(toUInt64(2), toUInt64(2)), 2 FROM numbers(1000);

SELECT 'stored', arraySort(groupArray(assumeNotNull(k))) FROM t_final_nullable_key WHERE n = 0;
SELECT 'final', groupArray(ver) FROM t_final_nullable_key FINAL WHERE n = 0;
SELECT 'prewhere', groupArray(ver) FROM t_final_nullable_key FINAL WHERE n = 0 AND assumeNotNull(k) = 1;
SELECT 'lambda', groupArray(ver) FROM t_final_nullable_key FINAL WHERE n = 0 AND has(arrayMap(x -> assumeNotNull(x), [k]), 1);
SELECT 'prewhere kept', count() > 0 FROM (EXPLAIN actions = 1 SELECT ver FROM t_final_nullable_key FINAL WHERE n = 0 AND k = 1)
WHERE explain ILIKE '%Prewhere filter column%k%';

CREATE ROW POLICY p_final_nullable_key ON t_final_nullable_key USING assumeNotNull(k) = 1 TO ALL;
SELECT 'row policy', groupArray(ver) FROM t_final_nullable_key FINAL WHERE n = 0;
DROP ROW POLICY p_final_nullable_key ON t_final_nullable_key;
DROP TABLE t_final_nullable_key;

DROP TABLE IF EXISTS t_final_nullable_key_index;
CREATE TABLE t_final_nullable_key_index (n UInt64, k Nullable(UInt64), ver UInt64, INDEX ik k TYPE set(0) GRANULARITY 1)
ENGINE = ReplacingMergeTree(ver) ORDER BY (n, k)
SETTINGS allow_nullable_key = 1, index_granularity = 100, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES t_final_nullable_key_index;
INSERT INTO t_final_nullable_key_index SELECT number, nullIf(toUInt64(1), toUInt64(1)), 1 FROM numbers(1000);
INSERT INTO t_final_nullable_key_index SELECT number, nullIf(toUInt64(2), toUInt64(2)), 2 FROM numbers(1000);
SELECT 'skip index', groupArray(ver) FROM t_final_nullable_key_index FINAL WHERE n = 0 AND assumeNotNull(k) = 1
SETTINGS optimize_move_to_prewhere = 0;
DROP TABLE t_final_nullable_key_index;

DROP TABLE IF EXISTS t_final_nullable_key_partition;
CREATE TABLE t_final_nullable_key_partition (n UInt64, k Nullable(UInt64), ver UInt64)
ENGINE = ReplacingMergeTree(ver) PARTITION BY assumeNotNull(k) ORDER BY (n, k)
SETTINGS allow_nullable_key = 1, index_granularity = 100, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES t_final_nullable_key_partition;
INSERT INTO t_final_nullable_key_partition SELECT number, nullIf(toUInt64(1), toUInt64(1)), 1 FROM numbers(1000);
INSERT INTO t_final_nullable_key_partition SELECT number, nullIf(toUInt64(2), toUInt64(2)), 2 FROM numbers(1000);
SELECT 'partitions', arraySort(groupUniqArray(partition)) FROM system.parts
WHERE database = currentDatabase() AND table = 't_final_nullable_key_partition' AND active;
SELECT 'partition pruning', groupArray(ver) FROM t_final_nullable_key_partition FINAL WHERE n = 0 AND assumeNotNull(k) = 1
SETTINGS optimize_move_to_prewhere = 0;
DROP TABLE t_final_nullable_key_partition;

-- without assumeNotNull every NULL is in one partition, so pruning stays
DROP TABLE IF EXISTS t_final_nullable_key_partition_plain;
CREATE TABLE t_final_nullable_key_partition_plain (n UInt64, k Nullable(UInt64), ver UInt64)
ENGINE = ReplacingMergeTree(ver) PARTITION BY k ORDER BY (n, k) SETTINGS allow_nullable_key = 1;
INSERT INTO t_final_nullable_key_partition_plain VALUES (0, 1, 1);
INSERT INTO t_final_nullable_key_partition_plain VALUES (0, 2, 1);
SELECT 'partition pruning kept', count() > 0 FROM (EXPLAIN indexes = 1 SELECT ver FROM t_final_nullable_key_partition_plain FINAL WHERE k = 1)
WHERE explain ILIKE '%Parts: 1/2%';
DROP TABLE t_final_nullable_key_partition_plain;
