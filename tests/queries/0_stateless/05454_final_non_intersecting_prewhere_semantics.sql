-- What `optimize_move_to_prewhere_if_final_non_intersecting` applies to the key ranges that FINAL reads without merging.

SET optimize_move_to_prewhere = 1, optimize_move_to_prewhere_if_final = 1, optimize_move_to_prewhere_if_final_non_intersecting = 1;
SET enable_analyzer = 1, optimize_on_insert = 1;

DROP TABLE IF EXISTS t_final_prewhere;
CREATE TABLE t_final_prewhere (key UInt64, v UInt64, s String, ver UInt64, is_deleted UInt8)
ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY key SETTINGS index_granularity = 128;
INSERT INTO t_final_prewhere SELECT number, number * 7 % 1000, toString(number), 1, 0 FROM numbers(20000);
-- A deleted row: a merge keeps it, and FINAL hides it.
INSERT INTO t_final_prewhere VALUES (10000, 1, 'not a number', 2, 1);
OPTIMIZE TABLE t_final_prewhere FINAL;
SYSTEM STOP MERGES t_final_prewhere;
INSERT INTO t_final_prewhere SELECT number, number * 11 % 1000, toString(number), 3, 0 FROM numbers(5000, 300);

SELECT 'Only deterministic conditions are applied';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT key FROM t_final_prewhere FINAL WHERE v = 5 AND rand() % 2 = 0)
WHERE explain LIKE '%non-intersecting FINAL ranges%';

SELECT 'Conditions do not see the rows that FINAL hides';
-- `toUInt64` cannot parse the value of the deleted row.
SELECT count() FROM t_final_prewhere FINAL WHERE toUInt64(s) % 10 = 3;

DROP TABLE IF EXISTS t_final_prewhere_collapsing;
CREATE TABLE t_final_prewhere_collapsing (key UInt64, s String, sign Int8)
ENGINE = CollapsingMergeTree(sign) ORDER BY key SETTINGS index_granularity = 128;
INSERT INTO t_final_prewhere_collapsing SELECT number, toString(number), 1 FROM numbers(20000);
-- A negative row without a pair: a merge keeps it, and FINAL hides it.
INSERT INTO t_final_prewhere_collapsing VALUES (30000, 'not a number', -1);
OPTIMIZE TABLE t_final_prewhere_collapsing FINAL;
SYSTEM STOP MERGES t_final_prewhere_collapsing;
INSERT INTO t_final_prewhere_collapsing SELECT number, toString(number), -1 FROM numbers(5000, 100);
SELECT count() FROM t_final_prewhere_collapsing FINAL WHERE toUInt64(s) % 10 = 3;
DROP TABLE t_final_prewhere_collapsing;

SELECT 'A row policy applied after FINAL disables it';
DROP ROW POLICY IF EXISTS final_prewhere_value_policy ON t_final_prewhere;
CREATE ROW POLICY final_prewhere_value_policy ON t_final_prewhere AS RESTRICTIVE FOR SELECT USING v < 500 TO ALL;
SELECT count() FROM (EXPLAIN actions = 1 SELECT key FROM t_final_prewhere FINAL WHERE v = 5)
WHERE explain LIKE '%non-intersecting FINAL ranges%';
SELECT count(), sum(cityHash64(*)) FROM t_final_prewhere FINAL WHERE v % 10 = 3;
SELECT count(), sum(cityHash64(*)) FROM t_final_prewhere FINAL WHERE v % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
DROP ROW POLICY final_prewhere_value_policy ON t_final_prewhere;

DROP TABLE t_final_prewhere;

SELECT 'Conditions see the values of lightweight updates';
DROP TABLE IF EXISTS t_final_prewhere_update;
CREATE TABLE t_final_prewhere_update (key UInt64, v UInt64, ver UInt64)
ENGINE = ReplacingMergeTree(ver) ORDER BY key
SETTINGS index_granularity = 128, enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_final_prewhere_update SELECT number, number % 100, 1 FROM numbers(20000);
OPTIMIZE TABLE t_final_prewhere_update FINAL;
SYSTEM STOP MERGES t_final_prewhere_update;
INSERT INTO t_final_prewhere_update SELECT number, number % 100, 2 FROM numbers(5000, 300);
UPDATE t_final_prewhere_update SET v = 1000 WHERE key % 1000 = 7 SETTINGS enable_lightweight_update = 1;
SELECT count(), sum(key) FROM t_final_prewhere_update FINAL WHERE v = 1000 SETTINGS apply_patch_parts = 1;
SELECT count(), sum(key) FROM t_final_prewhere_update FINAL WHERE v = 1000 SETTINGS apply_patch_parts = 1, optimize_move_to_prewhere_if_final_non_intersecting = 0;
DROP TABLE t_final_prewhere_update;

SELECT 'The WHERE does not evaluate the conditions again, but keeps their values';
DROP TABLE IF EXISTS t_final_prewhere_shared;
CREATE TABLE t_final_prewhere_shared (key UInt64, v UInt64, ver UInt64)
ENGINE = ReplacingMergeTree(ver) ORDER BY key SETTINGS index_granularity = 128;
INSERT INTO t_final_prewhere_shared SELECT number, 2, 1 FROM numbers(20000);
OPTIMIZE TABLE t_final_prewhere_shared FINAL;
SYSTEM STOP MERGES t_final_prewhere_shared;
INSERT INTO t_final_prewhere_shared SELECT number, 3, 2 FROM numbers(5000, 300);
-- `toUInt8(v)` passes with the value 2, which the other condition and the result still use.
SELECT count(), sum(key) FROM t_final_prewhere_shared FINAL WHERE toUInt8(v) AND (toUInt8(v) = 2 OR rowNumberInAllBlocks() > 1000000000);
SELECT count(), sum(key) FROM t_final_prewhere_shared FINAL WHERE toUInt8(v) AND (toUInt8(v) = 2 OR rowNumberInAllBlocks() > 1000000000) SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT sum(toUInt8(v)) FROM t_final_prewhere_shared FINAL WHERE toUInt8(v);
SELECT sum(toUInt8(v)) FROM t_final_prewhere_shared FINAL WHERE toUInt8(v) SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
-- The filter column is a result column: a condition with the value 3 is true, but the conjunction is 1.
SELECT sum(f), sum(key) FROM (SELECT key, (v = 2 AND toUInt8(key % 2 + 2)) AS f FROM t_final_prewhere_shared FINAL WHERE f);
SELECT sum(f), sum(key) FROM (SELECT key, (v = 2 AND toUInt8(key % 2 + 2)) AS f FROM t_final_prewhere_shared FINAL WHERE f)
SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
-- The conditions left in the WHERE keep their order: the guard, which is never true, comes before the division by zero.
SELECT count() FROM t_final_prewhere_shared FINAL WHERE v = 2 AND rand() < 0 AND intDiv(1, rand() % 1) > 0
SETTINGS short_circuit_function_evaluation = 'force_enable';
DROP TABLE t_final_prewhere_shared;

SELECT 'Nullable and LowCardinality conditions, with the AND chain of the WHERE split into steps and not';
DROP TABLE IF EXISTS t_final_prewhere_nullable;
CREATE TABLE t_final_prewhere_nullable (key UInt64, v Nullable(UInt64), lc LowCardinality(String), s String)
ENGINE = ReplacingMergeTree ORDER BY key SETTINGS index_granularity = 128;
INSERT INTO t_final_prewhere_nullable SELECT number, if(number % 3, 7, NULL), toString(number % 2), 'x' FROM numbers(20000);
OPTIMIZE TABLE t_final_prewhere_nullable FINAL;
SYSTEM STOP MERGES t_final_prewhere_nullable;
INSERT INTO t_final_prewhere_nullable SELECT number, 7, '1', 'y' FROM numbers(5000, 300);
SELECT count() FROM t_final_prewhere_nullable FINAL WHERE v = 7 AND s = 'x' AND lc = '1' SETTINGS query_plan_merge_filters = 0;
SELECT count() FROM t_final_prewhere_nullable FINAL WHERE v = 7 AND s = 'x' AND lc = '1' SETTINGS query_plan_merge_filters = 1;
SELECT count() FROM t_final_prewhere_nullable FINAL WHERE v = 7 AND s = 'x' AND lc = '1' SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
DROP TABLE t_final_prewhere_nullable;

SELECT 'A condition after one that stays in the WHERE stays there too';
DROP TABLE IF EXISTS t_final_prewhere_guard;
CREATE TABLE t_final_prewhere_guard (key UInt64, s String, ver UInt64)
ENGINE = ReplacingMergeTree(ver) ORDER BY key SETTINGS index_granularity = 128;
INSERT INTO t_final_prewhere_guard SELECT number, if(number % 100 = 0, 'bad', toString(number)), 1 FROM numbers(20000);
OPTIMIZE TABLE t_final_prewhere_guard FINAL;
SYSTEM STOP MERGES t_final_prewhere_guard;
INSERT INTO t_final_prewhere_guard SELECT number, toString(number), 2 FROM numbers(5000, 300);
-- The guard reads all queried columns, so it is not applied early, and `toUInt64` must not see 'bad'.
SELECT count(), sum(key) FROM t_final_prewhere_guard FINAL WHERE (s != 'bad' OR key = 4294967295) AND toUInt64(s) > 0
SETTINGS query_plan_optimize_lazy_final = 0;
DROP TABLE t_final_prewhere_guard;

SELECT 'The conditions applied early keep the order of the WHERE';
DROP TABLE IF EXISTS t_final_prewhere_order;
CREATE TABLE t_final_prewhere_order (key UInt64, guard UInt64, s String, payload String)
ENGINE = ReplacingMergeTree ORDER BY key SETTINGS index_granularity = 128;
INSERT INTO t_final_prewhere_order
SELECT number, if(number % 100 = 0, 0, cityHash64(number)), if(number % 100 = 0, 'bad', '1'), toString(number) FROM numbers(20000);
OPTIMIZE TABLE t_final_prewhere_order FINAL;
-- The column `s` is smaller than `guard`, but `toUInt64(s)` must not see 'bad', which only the rows with `guard = 0` have.
SELECT count(), sum(length(payload)) FROM t_final_prewhere_order FINAL WHERE guard != 0 AND toUInt64(s) > 0
SETTINGS query_plan_optimize_lazy_final = 0;
SELECT count(), sum(length(payload)) FROM t_final_prewhere_order FINAL WHERE guard != 0 AND toUInt64(s) > 0
SETTINGS query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0;
DROP TABLE t_final_prewhere_order;

SELECT 'Not applied with SAMPLE, which drops rows after reading';
DROP TABLE IF EXISTS t_final_prewhere_sample;
CREATE TABLE t_final_prewhere_sample (key UInt64, s String, h UInt64 MATERIALIZED cityHash64(key))
ENGINE = ReplacingMergeTree ORDER BY (key, h) SAMPLE BY h SETTINGS index_granularity = 128;
-- Only the rows outside of `SAMPLE 1/2` have 'bad'.
INSERT INTO t_final_prewhere_sample SELECT number, if(cityHash64(number) < 9223372036854775808, toString(number), 'bad') FROM numbers(20000);
OPTIMIZE TABLE t_final_prewhere_sample FINAL;
SELECT count(), sum(key) FROM t_final_prewhere_sample FINAL SAMPLE 1/2 WHERE toUInt64(s) % 3 = 1 SETTINGS query_plan_optimize_lazy_final = 0;
SELECT count(), sum(key) FROM t_final_prewhere_sample FINAL SAMPLE 1/2 WHERE toUInt64(s) % 3 = 1
SETTINGS query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0;
DROP TABLE t_final_prewhere_sample;

SELECT 'Not applied with a stateful function in the WHERE, which depends on the rows it sees';
DROP TABLE IF EXISTS t_final_prewhere_stateful;
CREATE TABLE t_final_prewhere_stateful (key UInt64, v UInt64) ENGINE = ReplacingMergeTree ORDER BY key SETTINGS index_granularity = 128;
INSERT INTO t_final_prewhere_stateful SELECT number, number % 2 FROM numbers(20000);
OPTIMIZE TABLE t_final_prewhere_stateful FINAL;
SELECT count() FROM t_final_prewhere_stateful FINAL WHERE v = 1 AND rowNumberInAllBlocks() < 10
SETTINGS max_threads = 1, query_plan_optimize_lazy_final = 0;
SELECT count() FROM t_final_prewhere_stateful FINAL WHERE v = 1 AND rowNumberInAllBlocks() < 10
SETTINGS max_threads = 1, query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0;
DROP TABLE t_final_prewhere_stateful;

SELECT 'A condition that can throw is not applied early after another condition';
DROP TABLE IF EXISTS t_final_prewhere_throw;
CREATE TABLE t_final_prewhere_throw (key UInt64, s String) ENGINE = ReplacingMergeTree ORDER BY key;
INSERT INTO t_final_prewhere_throw SELECT number, if(number % 2 = 0, '1.2.3.4', 'bad') FROM numbers(10);
OPTIMIZE TABLE t_final_prewhere_throw FINAL;
-- Both conditions read `s`, so PREWHERE would evaluate them at once.
SELECT count(), sum(key) FROM t_final_prewhere_throw FINAL WHERE s != 'bad' AND IPv4StringToNum(s) > 0 SETTINGS query_plan_optimize_lazy_final = 0;
SELECT count(), sum(key) FROM t_final_prewhere_throw FINAL WHERE s != 'bad' AND IPv4StringToNum(s) > 0
SETTINGS query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0;
DROP TABLE t_final_prewhere_throw;

SELECT 'Not applied with a stateful function in PREWHERE, which would see fewer rows after the filter of the engine';
DROP TABLE IF EXISTS t_final_prewhere_stateful_prewhere;
CREATE TABLE t_final_prewhere_stateful_prewhere (key UInt64, v UInt8, ver UInt64, is_deleted UInt8)
ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY key;
INSERT INTO t_final_prewhere_stateful_prewhere SELECT number, 2, 1, number % 2 = 0 FROM numbers(10);
OPTIMIZE TABLE t_final_prewhere_stateful_prewhere FINAL;
SELECT count(), sum(key) FROM t_final_prewhere_stateful_prewhere FINAL PREWHERE rowNumberInAllBlocks() % 2 = 0 WHERE v = 2
SETTINGS max_threads = 1, apply_prewhere_after_final = 0, query_plan_optimize_lazy_final = 0;
SELECT count(), sum(key) FROM t_final_prewhere_stateful_prewhere FINAL PREWHERE rowNumberInAllBlocks() % 2 = 0 WHERE v = 2
SETTINGS max_threads = 1, apply_prewhere_after_final = 0, query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0;
DROP TABLE t_final_prewhere_stateful_prewhere;

SELECT 'A condition that can throw is not applied early before another condition, which the WHERE may evaluate first';
DROP TABLE IF EXISTS t_final_prewhere_reorder;
CREATE TABLE t_final_prewhere_reorder (key UInt64, s String, guard UInt8) ENGINE = ReplacingMergeTree ORDER BY key;
INSERT INTO t_final_prewhere_reorder SELECT number, if(number % 2 = 0, '1', 'bad'), number % 2 = 0 FROM numbers(10);
OPTIMIZE TABLE t_final_prewhere_reorder FINAL;
SELECT count(), sum(key) FROM t_final_prewhere_reorder FINAL WHERE toUInt64(s) > 0 AND guard != 0 SETTINGS query_plan_optimize_lazy_final = 0;
SELECT count(), sum(key) FROM t_final_prewhere_reorder FINAL WHERE toUInt64(s) > 0 AND guard != 0
SETTINGS query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0;
DROP TABLE t_final_prewhere_reorder;

DROP TABLE IF EXISTS t_final_prewhere_reorder_merged;
CREATE TABLE t_final_prewhere_reorder_merged (key UInt64, s String) ENGINE = ReplacingMergeTree ORDER BY key;
-- Only the row with `key = length(s)` has 'bad'.
INSERT INTO t_final_prewhere_reorder_merged SELECT number, if(number = 3, 'bad', '1') FROM numbers(10);
OPTIMIZE TABLE t_final_prewhere_reorder_merged FINAL;
-- All ranges are merged, so the WHERE evaluates the conditions applied early too.
SELECT count(), sum(key) FROM t_final_prewhere_reorder_merged FINAL WHERE toUInt64(s) > 0 AND key != length(s)
SETTINGS query_plan_merge_filters = 0, split_parts_ranges_into_intersecting_and_non_intersecting_final = 0, query_plan_optimize_lazy_final = 0;
DROP TABLE t_final_prewhere_reorder_merged;

SELECT 'A column with the name of the filter of the conditions applied early';
DROP TABLE IF EXISTS t_final_prewhere_column_name;
CREATE TABLE t_final_prewhere_column_name (key UInt64, v UInt8, __prefiltered_conditions UInt64) ENGINE = ReplacingMergeTree ORDER BY key;
INSERT INTO t_final_prewhere_column_name SELECT number, number % 4, number FROM numbers(100);
OPTIMIZE TABLE t_final_prewhere_column_name FINAL;
SELECT sum(__prefiltered_conditions) FROM t_final_prewhere_column_name FINAL WHERE v = 2 SETTINGS query_plan_optimize_lazy_final = 0;
DROP TABLE t_final_prewhere_column_name;

SELECT 'Not applied to a column that a part lacks, whose default the reader computes before the other filters';
DROP TABLE IF EXISTS t_final_prewhere_default;
CREATE TABLE t_final_prewhere_default (key UInt64, payload String) ENGINE = ReplacingMergeTree ORDER BY key;
INSERT INTO t_final_prewhere_default SELECT number, toString(number) FROM numbers(10);
OPTIMIZE TABLE t_final_prewhere_default FINAL;
ALTER TABLE t_final_prewhere_default ADD COLUMN b UInt64 DEFAULT intDiv(1, key);
SELECT sum(key), sum(length(payload)) FROM t_final_prewhere_default FINAL WHERE key != 0 AND b > 0
SETTINGS apply_prewhere_after_final = 0, enable_multiple_prewhere_read_steps = 0, query_plan_optimize_lazy_final = 0;
SELECT sum(key), sum(length(payload)) FROM t_final_prewhere_default FINAL WHERE key != 0 AND b > 0
SETTINGS apply_prewhere_after_final = 0, enable_multiple_prewhere_read_steps = 0, query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0;
DROP TABLE t_final_prewhere_default;
