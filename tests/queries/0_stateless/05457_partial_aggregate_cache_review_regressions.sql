-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-parallel: Messes with the global partial aggregate cache.
-- no-random-* / no-parallel-replicas: settings randomization changes whether the cache is used at all.

-- Each case warms the partial aggregate cache with one query and then runs a query that must not reuse the warmed
-- states, because the states it would read differ although the query hash and the part identity look the same.

SYSTEM DROP AGGREGATE CACHE;

SET use_partial_aggregate_cache = 1;
SET optimize_aggregation_in_order = 0;
SET max_rows_to_group_by = 0;
SET group_by_overflow_mode = 'throw';

SELECT '--- string boundaries in the query hash';
DROP TABLE IF EXISTS t_pac_hash;
CREATE TABLE t_pac_hash (k UInt8, a UInt64, acount UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pac_hash VALUES (1, 10, 99);
SELECT k, sum(a), count() FROM t_pac_hash GROUP BY k;
SELECT k, sum(acount) FROM t_pac_hash GROUP BY k;
DROP TABLE t_pac_hash;

SELECT '--- settings affecting expressions';
DROP TABLE IF EXISTS t_pac_tz;
CREATE TABLE t_pac_tz (k UInt8, ts UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pac_tz VALUES (1, 0);
SELECT k, sum(toHour(toDateTime(ts))) FROM t_pac_tz GROUP BY k SETTINGS session_timezone = 'UTC';
SELECT k, sum(toHour(toDateTime(ts))) FROM t_pac_tz GROUP BY k SETTINGS session_timezone = 'Asia/Tokyo';
DROP TABLE t_pac_tz;

SELECT '--- metadata-only change of a default';
DROP TABLE IF EXISTS t_pac_default;
CREATE TABLE t_pac_default (k UInt8) ENGINE = MergeTree ORDER BY k;
SYSTEM STOP MERGES t_pac_default;
INSERT INTO t_pac_default VALUES (1), (1), (2);
ALTER TABLE t_pac_default ADD COLUMN v UInt64 DEFAULT 1;
SELECT k, sum(v) FROM t_pac_default GROUP BY k ORDER BY k;
ALTER TABLE t_pac_default MODIFY COLUMN v UInt64 DEFAULT 2;
SELECT k, sum(v) FROM t_pac_default GROUP BY k ORDER BY k;
DROP TABLE t_pac_default;

SELECT '--- switch to in-order aggregation';
DROP TABLE IF EXISTS t_pac_in_order;
CREATE TABLE t_pac_in_order (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
SYSTEM STOP MERGES t_pac_in_order;
INSERT INTO t_pac_in_order SELECT number % 3, 1 FROM numbers(30);
INSERT INTO t_pac_in_order SELECT number % 3, 1 FROM numbers(30);
SELECT k, sum(v) FROM t_pac_in_order GROUP BY k ORDER BY k SETTINGS optimize_aggregation_in_order = 0;
SELECT k, sum(v) FROM t_pac_in_order GROUP BY k ORDER BY k SETTINGS optimize_aggregation_in_order = 1;
DROP TABLE t_pac_in_order;

SELECT '--- read stopped early with read_overflow_mode = break';
DROP TABLE IF EXISTS t_pac_break;
CREATE TABLE t_pac_break (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 100;
INSERT INTO t_pac_break SELECT number % 2, 1 FROM numbers(100000);
SELECT k, sum(v) FROM t_pac_break GROUP BY k FORMAT Null SETTINGS max_rows_to_read = 1000, read_overflow_mode = 'break', max_block_size = 100, max_threads = 1;
SELECT k, sum(v) FROM t_pac_break GROUP BY k ORDER BY k;
DROP TABLE t_pac_break;

SELECT '--- nondeterministic virtual column';
DROP TABLE IF EXISTS t_pac_part_index;
CREATE TABLE t_pac_part_index (p UInt8, v UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY tuple();
INSERT INTO t_pac_part_index VALUES (1, 1);
INSERT INTO t_pac_part_index VALUES (2, 2);
SELECT _part_index, sum(v) FROM t_pac_part_index GROUP BY _part_index ORDER BY _part_index;
ALTER TABLE t_pac_part_index DROP PARTITION 1;
SELECT _part_index, sum(v) FROM t_pac_part_index GROUP BY _part_index ORDER BY _part_index;
DROP TABLE t_pac_part_index;

SELECT '--- external aggregation of a part';
DROP TABLE IF EXISTS t_pac_spill;
CREATE TABLE t_pac_spill (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_pac_spill SELECT number FROM numbers(100000);
SELECT count() FROM (SELECT k FROM t_pac_spill GROUP BY k SETTINGS max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1, max_threads = 1);
SELECT count() FROM (SELECT k FROM t_pac_spill GROUP BY k);
DROP TABLE t_pac_spill;

SELECT '--- absolute SAMPLE';
DROP TABLE IF EXISTS t_pac_sample;
CREATE TABLE t_pac_sample (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY intHash32(k) SAMPLE BY intHash32(k);
SYSTEM STOP MERGES t_pac_sample;
INSERT INTO t_pac_sample SELECT number, 1 FROM numbers(20000);
SELECT count() FROM t_pac_sample SAMPLE 10000 GROUP BY k % 1 FORMAT Null;
INSERT INTO t_pac_sample SELECT number + 20000, 1 FROM numbers(20000);
SELECT
    (SELECT count() FROM t_pac_sample SAMPLE 10000 GROUP BY k % 1)
    = (SELECT count() FROM t_pac_sample SAMPLE 10000 GROUP BY k % 1 SETTINGS use_partial_aggregate_cache = 0);
DROP TABLE t_pac_sample;
