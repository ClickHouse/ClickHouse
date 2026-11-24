-- `optimize_move_to_prewhere_if_final_non_intersecting` applies the whole WHERE as PREWHERE to the key ranges that FINAL
-- reads without merging. Each query runs with the setting off and on, and both must return the same rows.
-- Each table has a merged part with newer versions of a few of its keys in another part, so FINAL reads some ranges
-- with merging and the others without. The merged part has rows that FINAL hides: `is_deleted = 1` and `sign = -1`.

SET optimize_move_to_prewhere = 1, optimize_move_to_prewhere_if_final = 1, optimize_on_insert = 1;

DROP TABLE IF EXISTS t_replacing;
CREATE TABLE t_replacing (key UInt64, v UInt64, ver UInt64, is_deleted UInt8)
ENGINE = ReplacingMergeTree(ver, is_deleted) PARTITION BY key % 2 ORDER BY key SETTINGS index_granularity = 128;
INSERT INTO t_replacing SELECT number, number * 7 % 1000, 1, number % 97 = 0 FROM numbers(20000);
OPTIMIZE TABLE t_replacing FINAL;
SYSTEM STOP MERGES t_replacing;
INSERT INTO t_replacing SELECT number, number * 11 % 1000, 2, number % 5 = 0 FROM numbers(5000, 300);

SELECT 'replacing';
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE v % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE v % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;

SELECT 'replacing, do_not_merge_across_partitions_select_final';
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE v % 10 = 3 SETTINGS do_not_merge_across_partitions_select_final = 1, optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE v % 10 = 3 SETTINGS do_not_merge_across_partitions_select_final = 1, optimize_move_to_prewhere_if_final_non_intersecting = 1;

SELECT 'replacing, lazy FINAL';
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE v % 10 = 3 SETTINGS query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0, optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE v % 10 = 3 SETTINGS query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0, optimize_move_to_prewhere_if_final_non_intersecting = 1;

DROP TABLE t_replacing;

DROP TABLE IF EXISTS t_collapsing;
CREATE TABLE t_collapsing (key UInt64, v UInt64, sign Int8)
ENGINE = CollapsingMergeTree(sign) ORDER BY key SETTINGS index_granularity = 128;
INSERT INTO t_collapsing SELECT number, number * 7 % 1000, 1 FROM numbers(20000);
-- Negative rows without a pair: a merge keeps them, and FINAL hides them.
INSERT INTO t_collapsing SELECT number, number * 7 % 1000, -1 FROM numbers(20000, 500);
OPTIMIZE TABLE t_collapsing FINAL;
SYSTEM STOP MERGES t_collapsing;
INSERT INTO t_collapsing SELECT number, number * 7 % 1000, -1 FROM numbers(5000, 100);

SELECT 'collapsing';
SELECT count(), sum(cityHash64(*)) FROM t_collapsing FINAL WHERE v % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_collapsing FINAL WHERE v % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;

DROP TABLE t_collapsing;

DROP TABLE IF EXISTS t_summing;
CREATE TABLE t_summing (key UInt64, v UInt64)
ENGINE = SummingMergeTree(v) ORDER BY key SETTINGS index_granularity = 128;
INSERT INTO t_summing SELECT number % 20000, number * 7 % 1000 + 1 FROM numbers(40000);
OPTIMIZE TABLE t_summing FINAL;
SYSTEM STOP MERGES t_summing;
INSERT INTO t_summing SELECT number, number * 11 % 1000 + 1 FROM numbers(5000, 300);

SELECT 'summing';
SELECT count(), sum(cityHash64(*)) FROM t_summing FINAL WHERE v % 10 < 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_summing FINAL WHERE v % 10 < 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;

DROP TABLE t_summing;
