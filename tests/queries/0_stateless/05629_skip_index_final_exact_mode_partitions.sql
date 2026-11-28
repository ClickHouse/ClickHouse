-- `use_skip_indexes_if_final_exact_mode` adds back the granules that can hold another version of a row
-- selected by the skip index. When the partition key is computed from primary key columns, all versions
-- of a row are in one partition, so only the parts of that partition can hold one.

SET use_skip_indexes = 1;
SET use_skip_indexes_if_final = 1;
SET use_skip_indexes_if_final_exact_mode = 1;
SET query_plan_optimize_lazy_final = 0;
SET enable_parallel_replicas = 0;
SET use_query_condition_cache = 0;
SET optimize_on_insert = 0;

DROP TABLE IF EXISTS t_partitions;

-- The partition key is not in the primary key, so by default FINAL merges across partitions.
CREATE TABLE t_partitions (k UInt64, p UInt8, v UInt8, ver UInt64, INDEX idx_v v TYPE minmax GRANULARITY 1)
ENGINE = ReplacingMergeTree(ver) PARTITION BY p ORDER BY k
SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0;

SYSTEM STOP MERGES t_partitions;

-- Key 1: the matching version is older than a version in another partition.
INSERT INTO t_partitions VALUES (1, 2, 1, 1);
INSERT INTO t_partitions VALUES (1, 1, 0, 2);
-- Key 2: the matching version is older than a version in the same partition.
INSERT INTO t_partitions VALUES (2, 3, 1, 1);
INSERT INTO t_partitions VALUES (2, 3, 0, 2);
-- Key 3: the matching version is the only one.
INSERT INTO t_partitions VALUES (3, 4, 1, 1);

SELECT 'merge across partitions';
SELECT groupArray(k) FROM (SELECT k FROM t_partitions FINAL WHERE v = 1 ORDER BY k SETTINGS do_not_merge_across_partitions_select_final = 0, enable_automatic_decision_for_merging_across_partitions_for_final = 1);
SELECT groupArray(k) FROM (SELECT k FROM t_partitions FINAL WHERE v = 1 ORDER BY k SETTINGS do_not_merge_across_partitions_select_final = 0, enable_automatic_decision_for_merging_across_partitions_for_final = 1, use_skip_indexes = 0);
SELECT trimBoth(explain) FROM
(
    EXPLAIN indexes = 1 SELECT k FROM t_partitions FINAL WHERE v = 1 SETTINGS do_not_merge_across_partitions_select_final = 0, enable_automatic_decision_for_merging_across_partitions_for_final = 1
)
WHERE explain ILIKE '%Granules:%'
ORDER BY rowNumberInAllBlocks() DESC
LIMIT 1;

SELECT 'do_not_merge_across_partitions_select_final alone';
SELECT groupArray(k) FROM (SELECT k FROM t_partitions FINAL WHERE v = 1 ORDER BY k SETTINGS do_not_merge_across_partitions_select_final = 1);
SELECT groupArray(k) FROM (SELECT k FROM t_partitions FINAL WHERE v = 1 ORDER BY k SETTINGS do_not_merge_across_partitions_select_final = 1, use_skip_indexes = 0);
SELECT trimBoth(explain) FROM
(
    EXPLAIN indexes = 1 SELECT k FROM t_partitions FINAL WHERE v = 1 SETTINGS do_not_merge_across_partitions_select_final = 1
)
WHERE explain ILIKE '%Granules:%'
ORDER BY rowNumberInAllBlocks() DESC
LIMIT 1;

DROP TABLE t_partitions;

DROP TABLE IF EXISTS t_partition_in_key;

-- The partition key is in the primary key, so no key has versions in two partitions.
CREATE TABLE t_partition_in_key (k UInt64, p UInt8, v UInt8, INDEX idx_v v TYPE minmax GRANULARITY 1)
ENGINE = ReplacingMergeTree PARTITION BY p ORDER BY (k, p)
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0;

SYSTEM STOP MERGES t_partition_in_key;

-- Four parts, one per partition, each covering the whole key range.
INSERT INTO t_partition_in_key SELECT number, number % 4, number = 100 FROM numbers(1000);

SELECT 'partition key in the primary key';
SELECT count() FROM t_partition_in_key FINAL WHERE v = 1 SETTINGS do_not_merge_across_partitions_select_final = 0, enable_automatic_decision_for_merging_across_partitions_for_final = 1;
SELECT trimBoth(explain) FROM
(
    EXPLAIN indexes = 1 SELECT k FROM t_partition_in_key FINAL WHERE v = 1 SETTINGS do_not_merge_across_partitions_select_final = 0, enable_automatic_decision_for_merging_across_partitions_for_final = 1
)
WHERE explain ILIKE '%Granules:%'
ORDER BY rowNumberInAllBlocks() DESC
LIMIT 1;
SELECT count() FROM t_partition_in_key FINAL WHERE v = 1 SETTINGS do_not_merge_across_partitions_select_final = 0, enable_automatic_decision_for_merging_across_partitions_for_final = 0;
SELECT trimBoth(explain) FROM
(
    EXPLAIN indexes = 1 SELECT k FROM t_partition_in_key FINAL WHERE v = 1 SETTINGS do_not_merge_across_partitions_select_final = 0, enable_automatic_decision_for_merging_across_partitions_for_final = 0
)
WHERE explain ILIKE '%Granules:%'
ORDER BY rowNumberInAllBlocks() DESC
LIMIT 1;

DROP TABLE t_partition_in_key;

-- A cancelling row that the skip index rejects must still hide the matching row of key 100.
DROP TABLE IF EXISTS t_collapsing;
DROP TABLE IF EXISTS t_versioned_collapsing;

CREATE TABLE t_collapsing (k UInt64, p UInt8, v UInt8, s Int8, INDEX idx_v v TYPE minmax GRANULARITY 1)
ENGINE = CollapsingMergeTree(s) PARTITION BY p ORDER BY (k, p)
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0;
CREATE TABLE t_versioned_collapsing (k UInt64, p UInt8, v UInt8, s Int8, ver UInt64, INDEX idx_v v TYPE minmax GRANULARITY 1)
ENGINE = VersionedCollapsingMergeTree(s, ver) PARTITION BY p ORDER BY (k, p)
SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0;

SYSTEM STOP MERGES t_collapsing;
SYSTEM STOP MERGES t_versioned_collapsing;

INSERT INTO t_collapsing SELECT number, number % 4, number IN (100, 201), 1 FROM numbers(1000);
INSERT INTO t_collapsing VALUES (100, 0, 0, -1);
INSERT INTO t_versioned_collapsing SELECT number, number % 4, number IN (100, 201), 1, 1 FROM numbers(1000);
INSERT INTO t_versioned_collapsing VALUES (100, 0, 0, -1, 1);

SELECT 'collapsing engines';
SELECT groupArray(k) FROM (SELECT k FROM t_collapsing FINAL WHERE v = 1 ORDER BY k);
SELECT groupArray(k) FROM (SELECT k FROM t_collapsing FINAL WHERE v = 1 ORDER BY k SETTINGS use_skip_indexes = 0);
SELECT trimBoth(explain) FROM (EXPLAIN indexes = 1 SELECT k FROM t_collapsing FINAL WHERE v = 1) WHERE explain ILIKE '%Granules:%' ORDER BY rowNumberInAllBlocks() DESC LIMIT 1;
SELECT groupArray(k) FROM (SELECT k FROM t_versioned_collapsing FINAL WHERE v = 1 ORDER BY k);
SELECT groupArray(k) FROM (SELECT k FROM t_versioned_collapsing FINAL WHERE v = 1 ORDER BY k SETTINGS use_skip_indexes = 0);
SELECT trimBoth(explain) FROM (EXPLAIN indexes = 1 SELECT k FROM t_versioned_collapsing FINAL WHERE v = 1) WHERE explain ILIKE '%Granules:%' ORDER BY rowNumberInAllBlocks() DESC LIMIT 1;

DROP TABLE t_collapsing;
DROP TABLE t_versioned_collapsing;

-- Lazy FINAL deduplicates across partitions even with do_not_merge_across_partitions_select_final, so here it must
-- see the granules of partition 3 and stay disabled.
DROP TABLE IF EXISTS t_lazy;

CREATE TABLE t_lazy (k UInt64, p UInt8, v UInt8, ver UInt64, INDEX idx_v v TYPE minmax GRANULARITY 1)
ENGINE = ReplacingMergeTree(ver) PARTITION BY p ORDER BY k
SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0;

SYSTEM STOP MERGES t_lazy;

INSERT INTO t_lazy SELECT number, 1, number = 0, 1 FROM numbers(1000);
INSERT INTO t_lazy SELECT number, 2, number = 0, 2 FROM numbers(1000);
INSERT INTO t_lazy SELECT 0, 3, 0, 3 FROM numbers(10000);

SELECT 'lazy FINAL';
SELECT arraySort(groupArray((p, ver))) FROM t_lazy FINAL WHERE v = 1 SETTINGS do_not_merge_across_partitions_select_final = 1, query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0.5, optimize_move_to_prewhere = 0;
SELECT arraySort(groupArray((p, ver))) FROM t_lazy FINAL WHERE v = 1 SETTINGS do_not_merge_across_partitions_select_final = 1, optimize_move_to_prewhere = 0;

DROP TABLE t_lazy;

-- FINAL of these engines combines rows, so partition 2 returns a matching row although none of its rows matches.
DROP TABLE IF EXISTS t_summing;
DROP TABLE IF EXISTS t_aggregating;
DROP TABLE IF EXISTS t_coalescing;

CREATE TABLE t_summing (k UInt64, p UInt8, v UInt64, INDEX idx_v v TYPE minmax GRANULARITY 1)
ENGINE = SummingMergeTree(v) PARTITION BY p ORDER BY k
SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;
CREATE TABLE t_aggregating (k UInt64, p UInt8, v SimpleAggregateFunction(sum, UInt64), INDEX idx_v v TYPE minmax GRANULARITY 1)
ENGINE = AggregatingMergeTree PARTITION BY p ORDER BY k
SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;
CREATE TABLE t_coalescing (k UInt64, p UInt8, a Nullable(UInt64), b Nullable(UInt64), INDEX idx_a a TYPE minmax GRANULARITY 1, INDEX idx_b b TYPE minmax GRANULARITY 1)
ENGINE = CoalescingMergeTree PARTITION BY p ORDER BY k
SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_summing;
SYSTEM STOP MERGES t_aggregating;
SYSTEM STOP MERGES t_coalescing;

INSERT INTO t_summing VALUES (1, 1, 2);
INSERT INTO t_summing VALUES (1, 2, 1);
INSERT INTO t_summing VALUES (1, 2, 1);
INSERT INTO t_aggregating VALUES (1, 1, 2);
INSERT INTO t_aggregating VALUES (1, 2, 1);
INSERT INTO t_aggregating VALUES (1, 2, 1);
INSERT INTO t_coalescing VALUES (1, 1, 5, 7);
INSERT INTO t_coalescing VALUES (1, 2, 5, NULL);
INSERT INTO t_coalescing VALUES (1, 2, NULL, 7);

SELECT 'engines that combine rows';
SELECT groupArray(p) FROM (SELECT p FROM t_summing FINAL WHERE v = 2 ORDER BY p SETTINGS do_not_merge_across_partitions_select_final = 1);
SELECT groupArray(p) FROM (SELECT p FROM t_summing FINAL WHERE v = 2 ORDER BY p SETTINGS do_not_merge_across_partitions_select_final = 1, use_skip_indexes = 0);
SELECT groupArray(p) FROM (SELECT p FROM t_aggregating FINAL WHERE v = 2 ORDER BY p SETTINGS do_not_merge_across_partitions_select_final = 1);
SELECT groupArray(p) FROM (SELECT p FROM t_aggregating FINAL WHERE v = 2 ORDER BY p SETTINGS do_not_merge_across_partitions_select_final = 1, use_skip_indexes = 0);
SELECT groupArray(p) FROM (SELECT p FROM t_coalescing FINAL WHERE a = 5 AND b = 7 ORDER BY p SETTINGS do_not_merge_across_partitions_select_final = 1);
SELECT groupArray(p) FROM (SELECT p FROM t_coalescing FINAL WHERE a = 5 AND b = 7 ORDER BY p SETTINGS do_not_merge_across_partitions_select_final = 1, use_skip_indexes = 0);

DROP TABLE t_summing;
DROP TABLE t_aggregating;
DROP TABLE t_coalescing;
