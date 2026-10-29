-- `optimize_move_to_prewhere_if_final_non_intersecting` applies the whole WHERE as PREWHERE to the key ranges that FINAL
-- reads without merging. Each query runs with the setting off and on, and both must return the same rows.
-- Each table has one merged part, which also has rows FINAL hides, plus small parts with newer versions of some keys,
-- so that FINAL reads some ranges with merging and the others without.

SET optimize_move_to_prewhere = 1, optimize_move_to_prewhere_if_final = 1;

DROP TABLE IF EXISTS t_replacing;
CREATE TABLE t_replacing (key UInt64, v UInt64, s String, ver UInt64, is_deleted UInt8)
ENGINE = ReplacingMergeTree(ver, is_deleted) PARTITION BY key % 2 ORDER BY key SETTINGS index_granularity = 128;
SYSTEM STOP MERGES t_replacing;
INSERT INTO t_replacing SELECT number, number * 7 % 1000, toString(number * 13 % 100), 1, number % 97 = 0 FROM numbers(20000);
SYSTEM START MERGES t_replacing;
OPTIMIZE TABLE t_replacing FINAL;
SYSTEM STOP MERGES t_replacing;
INSERT INTO t_replacing SELECT number, number * 11 % 1000, toString(number * 17 % 100), 2, number % 5 = 0 FROM numbers(5000, 300);
INSERT INTO t_replacing SELECT number, number * 3 % 1000, toString(number % 100), 3, 0 FROM numbers(15000, 100);
INSERT INTO t_replacing SELECT number, number * 5 % 1000, toString(number % 100), 0, 0 FROM numbers(15050, 100);

SELECT 'replacing';
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE v % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE v % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE v % 10 = 3 AND key > 4000 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE v % 10 = 3 AND key > 4000 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE s LIKE '%7%' SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE s LIKE '%7%' SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE v IN (SELECT number * 3 FROM numbers(100)) SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE v IN (SELECT number * 3 FROM numbers(100)) SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE ver >= 1 AND v < 500 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE ver >= 1 AND v < 500 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE is_deleted = 0 AND v < 100 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE is_deleted = 0 AND v < 100 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;
SELECT count(), sum(cityHash64(key, v)) FROM t_replacing FINAL WHERE v % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(key, v)) FROM t_replacing FINAL WHERE v % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;

SELECT 'replacing, do_not_merge_across_partitions_select_final';
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE v % 10 = 3 SETTINGS do_not_merge_across_partitions_select_final = 1, optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE v % 10 = 3 SETTINGS do_not_merge_across_partitions_select_final = 1, optimize_move_to_prewhere_if_final_non_intersecting = 1;

SELECT 'replacing, without split into layers';
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE v % 10 = 3 SETTINGS split_intersecting_parts_ranges_into_layers_final = 0, optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE v % 10 = 3 SETTINGS split_intersecting_parts_ranges_into_layers_final = 0, optimize_move_to_prewhere_if_final_non_intersecting = 1;

SELECT 'replacing, vertical FINAL';
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE v % 10 = 3 SETTINGS enable_vertical_final = 1, optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE v % 10 = 3 SETTINGS enable_vertical_final = 1, optimize_move_to_prewhere_if_final_non_intersecting = 1;

SELECT 'replacing, one thread';
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE v % 10 = 3 SETTINGS max_final_threads = 1, max_threads = 1, optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_replacing FINAL WHERE v % 10 = 3 SETTINGS max_final_threads = 1, max_threads = 1, optimize_move_to_prewhere_if_final_non_intersecting = 1;

SELECT 'replacing, in order';
SELECT key, v, s FROM t_replacing FINAL WHERE v % 10 = 3 ORDER BY key LIMIT 5 SETTINGS optimize_read_in_order = 1, optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT key, v, s FROM t_replacing FINAL WHERE v % 10 = 3 ORDER BY key LIMIT 5 SETTINGS optimize_read_in_order = 1, optimize_move_to_prewhere_if_final_non_intersecting = 1;
SELECT key, v, s FROM t_replacing FINAL WHERE v % 10 = 3 ORDER BY key DESC LIMIT 5 SETTINGS optimize_read_in_order = 1, optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT key, v, s FROM t_replacing FINAL WHERE v % 10 = 3 ORDER BY key DESC LIMIT 5 SETTINGS optimize_read_in_order = 1, optimize_move_to_prewhere_if_final_non_intersecting = 1;

DROP TABLE t_replacing;

DROP TABLE IF EXISTS t_replacing_no_version;
CREATE TABLE t_replacing_no_version (key UInt64, v UInt64, s String)
ENGINE = ReplacingMergeTree ORDER BY key SETTINGS index_granularity = 128;
SYSTEM STOP MERGES t_replacing_no_version;
INSERT INTO t_replacing_no_version SELECT number, number * 7 % 1000, toString(number) FROM numbers(20000);
SYSTEM START MERGES t_replacing_no_version;
OPTIMIZE TABLE t_replacing_no_version FINAL;
SYSTEM STOP MERGES t_replacing_no_version;
INSERT INTO t_replacing_no_version SELECT number, number * 11 % 1000, 'new' FROM numbers(5000, 300);

SELECT 'replacing without version';
SELECT count(), sum(cityHash64(*)) FROM t_replacing_no_version FINAL WHERE v % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_replacing_no_version FINAL WHERE v % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;
SELECT count(), sum(cityHash64(*)) FROM t_replacing_no_version FINAL WHERE s = 'new' SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_replacing_no_version FINAL WHERE s = 'new' SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;

DROP TABLE t_replacing_no_version;

DROP TABLE IF EXISTS t_collapsing;
CREATE TABLE t_collapsing (key UInt64, v UInt64, sign Int8)
ENGINE = CollapsingMergeTree(sign) ORDER BY key SETTINGS index_granularity = 128;
SYSTEM STOP MERGES t_collapsing;
INSERT INTO t_collapsing SELECT number, number * 7 % 1000, 1 FROM numbers(20000);
-- Cancel some rows, and leave negative rows without a pair: a merge keeps them, and FINAL hides them.
INSERT INTO t_collapsing SELECT number, number * 7 % 1000, -1 FROM numbers(20000) WHERE number % 31 = 0;
INSERT INTO t_collapsing SELECT number, number * 7 % 1000, -1 FROM numbers(20000, 500);
SYSTEM START MERGES t_collapsing;
OPTIMIZE TABLE t_collapsing FINAL;
SYSTEM STOP MERGES t_collapsing;
INSERT INTO t_collapsing SELECT number, number * 7 % 1000, -1 FROM numbers(5000, 100);
INSERT INTO t_collapsing SELECT number, number * 11 % 1000, 1 FROM numbers(5000, 100);

SELECT 'collapsing';
SELECT count(), sum(cityHash64(*)) FROM t_collapsing FINAL WHERE v % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_collapsing FINAL WHERE v % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;
SELECT count(), sum(cityHash64(*)) FROM t_collapsing FINAL WHERE sign = -1 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_collapsing FINAL WHERE sign = -1 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;

DROP TABLE t_collapsing;

DROP TABLE IF EXISTS t_versioned_collapsing;
CREATE TABLE t_versioned_collapsing (key UInt64, v UInt64, sign Int8, ver UInt64)
ENGINE = VersionedCollapsingMergeTree(sign, ver) ORDER BY key SETTINGS index_granularity = 128;
SYSTEM STOP MERGES t_versioned_collapsing;
INSERT INTO t_versioned_collapsing SELECT number, number * 7 % 1000, 1, 1 FROM numbers(20000);
INSERT INTO t_versioned_collapsing SELECT number, number * 7 % 1000, -1, 1 FROM numbers(20000) WHERE number % 31 = 0;
SYSTEM START MERGES t_versioned_collapsing;
OPTIMIZE TABLE t_versioned_collapsing FINAL;
SYSTEM STOP MERGES t_versioned_collapsing;
INSERT INTO t_versioned_collapsing SELECT number, number * 7 % 1000, -1, 1 FROM numbers(5000, 100);
INSERT INTO t_versioned_collapsing SELECT number, number * 11 % 1000, 1, 2 FROM numbers(5000, 100);

SELECT 'versioned collapsing';
SELECT count(), sum(cityHash64(*)) FROM t_versioned_collapsing FINAL WHERE v % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_versioned_collapsing FINAL WHERE v % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;

DROP TABLE t_versioned_collapsing;

DROP TABLE IF EXISTS t_summing;
CREATE TABLE t_summing (key UInt64, v UInt64, s String)
ENGINE = SummingMergeTree(v) ORDER BY key SETTINGS index_granularity = 128;
SYSTEM STOP MERGES t_summing;
INSERT INTO t_summing SELECT number % 20000, number * 7 % 1000, toString(number % 20000) FROM numbers(40000);
SYSTEM START MERGES t_summing;
OPTIMIZE TABLE t_summing FINAL;
SYSTEM STOP MERGES t_summing;
INSERT INTO t_summing SELECT number, number * 11 % 1000, toString(number) FROM numbers(5000, 300);

SELECT 'summing';
SELECT count(), sum(cityHash64(*)) FROM t_summing FINAL WHERE v % 10 < 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_summing FINAL WHERE v % 10 < 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;
SELECT count(), sum(cityHash64(*)) FROM t_summing FINAL WHERE s LIKE '%3' SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_summing FINAL WHERE s LIKE '%3' SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;

DROP TABLE t_summing;

DROP TABLE IF EXISTS t_aggregating;
CREATE TABLE t_aggregating (key UInt64, v SimpleAggregateFunction(max, UInt64), total SimpleAggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree ORDER BY key SETTINGS index_granularity = 128;
SYSTEM STOP MERGES t_aggregating;
INSERT INTO t_aggregating SELECT number % 20000, number * 7 % 1000, number FROM numbers(40000);
SYSTEM START MERGES t_aggregating;
OPTIMIZE TABLE t_aggregating FINAL;
SYSTEM STOP MERGES t_aggregating;
INSERT INTO t_aggregating SELECT number, number * 11 % 1000, number FROM numbers(5000, 300);

SELECT 'aggregating';
SELECT count(), sum(cityHash64(*)) FROM t_aggregating FINAL WHERE v % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_aggregating FINAL WHERE v % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;

DROP TABLE t_aggregating;

DROP TABLE IF EXISTS t_coalescing;
CREATE TABLE t_coalescing (key UInt64, v Nullable(UInt64), s Nullable(String))
ENGINE = CoalescingMergeTree ORDER BY key SETTINGS index_granularity = 128;
SYSTEM STOP MERGES t_coalescing;
INSERT INTO t_coalescing SELECT number, number * 7 % 1000, NULL FROM numbers(20000);
INSERT INTO t_coalescing SELECT number, NULL, toString(number) FROM numbers(20000) WHERE number % 3 = 0;
SYSTEM START MERGES t_coalescing;
OPTIMIZE TABLE t_coalescing FINAL;
SYSTEM STOP MERGES t_coalescing;
INSERT INTO t_coalescing SELECT number, number * 11 % 1000, NULL FROM numbers(5000, 300);

SELECT 'coalescing';
SELECT count(), sum(cityHash64(*)) FROM t_coalescing FINAL WHERE v % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT count(), sum(cityHash64(*)) FROM t_coalescing FINAL WHERE v % 10 = 3 SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;

DROP TABLE t_coalescing;
