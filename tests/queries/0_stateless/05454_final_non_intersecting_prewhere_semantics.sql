-- Semantics of `optimize_move_to_prewhere_if_final_non_intersecting`, which applies the whole WHERE as PREWHERE to the key
-- ranges that FINAL reads without merging.

SET optimize_move_to_prewhere = 1, optimize_move_to_prewhere_if_final = 1, optimize_move_to_prewhere_if_final_non_intersecting = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_final_prewhere;
CREATE TABLE t_final_prewhere (key UInt64, v UInt64, s String, ver UInt64, is_deleted UInt8)
ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY key SETTINGS index_granularity = 128;
SYSTEM STOP MERGES t_final_prewhere;
INSERT INTO t_final_prewhere SELECT number, number * 7 % 1000, toString(number), 1, 0 FROM numbers(20000);
-- A deleted row: a merge keeps it, and FINAL hides it.
INSERT INTO t_final_prewhere VALUES (10000, 1, 'not a number', 2, 1);
SYSTEM START MERGES t_final_prewhere;
OPTIMIZE TABLE t_final_prewhere FINAL;
SYSTEM STOP MERGES t_final_prewhere;
INSERT INTO t_final_prewhere SELECT number, number * 11 % 1000, toString(number), 3, 0 FROM numbers(5000, 300);

SELECT 'Only deterministic conditions are applied to the non-intersecting ranges';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT key FROM t_final_prewhere FINAL WHERE v = 5 AND s != '')
WHERE explain LIKE '%non-intersecting FINAL ranges%';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT key FROM t_final_prewhere FINAL WHERE v = 5 AND rand() % 2 = 0)
WHERE explain LIKE '%non-intersecting FINAL ranges%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT key FROM t_final_prewhere FINAL WHERE rand() % 2 = 0)
WHERE explain LIKE '%non-intersecting FINAL ranges%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT key FROM t_final_prewhere FINAL WHERE v = 5 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0)
WHERE explain LIKE '%non-intersecting FINAL ranges%';

SELECT 'Conditions do not see the rows FINAL hides';
-- The deleted row has a value that `toUInt64` cannot parse.
SELECT count() FROM t_final_prewhere FINAL WHERE toUInt64(s) % 10 = 3;
SELECT count() FROM t_final_prewhere FINAL WHERE toUInt64(s) % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;

DROP TABLE IF EXISTS t_final_prewhere_collapsing;
CREATE TABLE t_final_prewhere_collapsing (key UInt64, s String, sign Int8)
ENGINE = CollapsingMergeTree(sign) ORDER BY key SETTINGS index_granularity = 128;
SYSTEM STOP MERGES t_final_prewhere_collapsing;
INSERT INTO t_final_prewhere_collapsing SELECT number, toString(number), 1 FROM numbers(20000);
-- A negative row without a pair: a merge keeps it, and FINAL hides it.
INSERT INTO t_final_prewhere_collapsing VALUES (30000, 'not a number', -1);
SYSTEM START MERGES t_final_prewhere_collapsing;
OPTIMIZE TABLE t_final_prewhere_collapsing FINAL;
SYSTEM STOP MERGES t_final_prewhere_collapsing;
INSERT INTO t_final_prewhere_collapsing SELECT number, toString(number), -1 FROM numbers(5000, 100);

SELECT count() FROM t_final_prewhere_collapsing FINAL WHERE toUInt64(s) % 10 = 3;
SELECT count() FROM t_final_prewhere_collapsing FINAL WHERE toUInt64(s) % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
DROP TABLE t_final_prewhere_collapsing;

SELECT 'Row policies';
DROP ROW POLICY IF EXISTS final_prewhere_key_policy ON t_final_prewhere;
CREATE ROW POLICY final_prewhere_key_policy ON t_final_prewhere AS RESTRICTIVE FOR SELECT USING key % 3 = 0 TO ALL;
SELECT count(), sum(cityHash64(*)) FROM t_final_prewhere FINAL WHERE v % 10 = 3;
SELECT count(), sum(cityHash64(*)) FROM t_final_prewhere FINAL WHERE v % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
DROP ROW POLICY final_prewhere_key_policy ON t_final_prewhere;

-- A policy on a column outside the sorting key is applied after FINAL, so the conditions of the query are applied after it too.
DROP ROW POLICY IF EXISTS final_prewhere_value_policy ON t_final_prewhere;
CREATE ROW POLICY final_prewhere_value_policy ON t_final_prewhere AS RESTRICTIVE FOR SELECT USING v < 500 TO ALL;
SELECT count(), sum(cityHash64(*)) FROM t_final_prewhere FINAL WHERE v % 10 = 3;
SELECT count(), sum(cityHash64(*)) FROM t_final_prewhere FINAL WHERE v % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count() FROM (EXPLAIN actions = 1 SELECT key FROM t_final_prewhere FINAL WHERE v = 5)
WHERE explain LIKE '%non-intersecting FINAL ranges%';
DROP ROW POLICY final_prewhere_value_policy ON t_final_prewhere;

SELECT 'The query condition cache';
SELECT count(), sum(cityHash64(*)) FROM t_final_prewhere FINAL WHERE v = 7 SETTINGS use_query_condition_cache = 1;
SELECT count(), sum(cityHash64(*)) FROM t_final_prewhere WHERE v = 7 SETTINGS use_query_condition_cache = 1;
SELECT count(), sum(cityHash64(*)) FROM t_final_prewhere WHERE v = 7 SETTINGS use_query_condition_cache = 0;
SELECT count(), sum(cityHash64(*)) FROM t_final_prewhere FINAL WHERE v = 7 SETTINGS use_query_condition_cache = 1;
SELECT count(), sum(cityHash64(*)) FROM t_final_prewhere FINAL WHERE v = 7 SETTINGS use_query_condition_cache = 0;

DROP TABLE t_final_prewhere;

SELECT 'Lightweight updates';
DROP TABLE IF EXISTS t_final_prewhere_update;
CREATE TABLE t_final_prewhere_update (key UInt64, v UInt64, ver UInt64)
ENGINE = ReplacingMergeTree(ver) ORDER BY key
SETTINGS index_granularity = 128, enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES t_final_prewhere_update;
INSERT INTO t_final_prewhere_update SELECT number, number % 100, 1 FROM numbers(20000);
SYSTEM START MERGES t_final_prewhere_update;
OPTIMIZE TABLE t_final_prewhere_update FINAL;
SYSTEM STOP MERGES t_final_prewhere_update;
INSERT INTO t_final_prewhere_update SELECT number, number % 100, 2 FROM numbers(5000, 300);
UPDATE t_final_prewhere_update SET v = 1000 WHERE key % 1000 = 7 SETTINGS enable_lightweight_update = 1;
SELECT count(), sum(key) FROM t_final_prewhere_update FINAL WHERE v = 1000 SETTINGS apply_patch_parts = 1;
SELECT count(), sum(key) FROM t_final_prewhere_update FINAL WHERE v = 1000 SETTINGS apply_patch_parts = 1, optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(key) FROM t_final_prewhere_update FINAL WHERE v = 7 SETTINGS apply_patch_parts = 1;
SELECT count(), sum(key) FROM t_final_prewhere_update FINAL WHERE v = 7 SETTINGS apply_patch_parts = 1, optimize_move_to_prewhere_if_final_non_intersecting = 0;

SELECT 'Mutations on the fly';
ALTER TABLE t_final_prewhere_update UPDATE v = 2000 WHERE key % 1000 = 9 SETTINGS mutations_sync = 0;
SELECT count(), sum(key) FROM t_final_prewhere_update FINAL WHERE v = 2000 SETTINGS apply_mutations_on_fly = 1;
SELECT count(), sum(key) FROM t_final_prewhere_update FINAL WHERE v = 2000 SETTINGS apply_mutations_on_fly = 1, optimize_move_to_prewhere_if_final_non_intersecting = 0;
DROP TABLE t_final_prewhere_update;
