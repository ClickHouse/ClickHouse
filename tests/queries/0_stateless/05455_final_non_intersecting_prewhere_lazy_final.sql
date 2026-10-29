-- Lazy FINAL reads the parts that no other part intersects without FINAL, and with
-- `optimize_move_to_prewhere_if_final_non_intersecting` it applies the whole WHERE to them as PREWHERE.
-- Each query runs with the setting off and on, and both must return the same rows.

SET optimize_move_to_prewhere = 1, optimize_move_to_prewhere_if_final = 1;
SET query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0;

DROP TABLE IF EXISTS t_lazy_final_prewhere;
CREATE TABLE t_lazy_final_prewhere (key UInt64, v UInt64, s String, ver UInt64, is_deleted UInt8)
ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY key SETTINGS index_granularity = 128;
SYSTEM STOP MERGES t_lazy_final_prewhere;
INSERT INTO t_lazy_final_prewhere SELECT number, number * 7 % 1000, toString(number), 1, number % 97 = 0 FROM numbers(20000);
INSERT INTO t_lazy_final_prewhere VALUES (10000, 1, 'not a number', 2, 1);
SYSTEM START MERGES t_lazy_final_prewhere;
OPTIMIZE TABLE t_lazy_final_prewhere FINAL;
SYSTEM STOP MERGES t_lazy_final_prewhere;

SELECT 'All parts are non-intersecting';
SELECT count(), sum(cityHash64(*)) FROM t_lazy_final_prewhere FINAL WHERE v % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_lazy_final_prewhere FINAL WHERE v % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;
SELECT count(), sum(cityHash64(key, v)) FROM t_lazy_final_prewhere FINAL WHERE toUInt64(s) % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(key, v)) FROM t_lazy_final_prewhere FINAL WHERE toUInt64(s) % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;

-- A part with newer versions of the keys of another part, and a part that intersects no other part.
INSERT INTO t_lazy_final_prewhere SELECT number, number * 11 % 1000, toString(number), 3, number % 5 = 0 FROM numbers(5000, 300);
INSERT INTO t_lazy_final_prewhere SELECT number, number * 13 % 1000, toString(number), 1, 0 FROM numbers(30000, 1000);

SELECT 'Some parts are intersecting';
SELECT count(), sum(cityHash64(*)) FROM t_lazy_final_prewhere FINAL WHERE v % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_lazy_final_prewhere FINAL WHERE v % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;
SELECT count(), sum(cityHash64(key, v)) FROM t_lazy_final_prewhere FINAL WHERE v % 10 = 3 AND key > 4000 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(key, v)) FROM t_lazy_final_prewhere FINAL WHERE v % 10 = 3 AND key > 4000 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;
SELECT count(), sum(cityHash64(key, v)) FROM t_lazy_final_prewhere FINAL WHERE toUInt64(s) % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(key, v)) FROM t_lazy_final_prewhere FINAL WHERE toUInt64(s) % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;
SELECT count(), sum(cityHash64(key)) FROM t_lazy_final_prewhere FINAL WHERE is_deleted = 0 AND v < 100 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(key)) FROM t_lazy_final_prewhere FINAL WHERE is_deleted = 0 AND v < 100 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;

DROP TABLE t_lazy_final_prewhere;
