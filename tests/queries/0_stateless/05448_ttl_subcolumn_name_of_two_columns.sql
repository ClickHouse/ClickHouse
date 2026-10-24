-- A TTL expression reads a subcolumn name that two columns can supply (`j.m.keys`: keys of the Map `j.m`, or the path `m.keys`
-- of the JSON `j`) from the same column queries read it from: the Map. Rows matching only through the JSON path must stay.

DROP TABLE IF EXISTS t_a;
DROP TABLE IF EXISTS t_b;
DROP TABLE IF EXISTS t_c;
DROP TABLE IF EXISTS t_d;
DROP TABLE IF EXISTS t_e;
DROP TABLE IF EXISTS t_f;
DROP TABLE IF EXISTS t_g;
DROP TABLE IF EXISTS t_h;
DROP TABLE IF EXISTS t_i;

SELECT 'A: DELETE WHERE, horizontal merge';
CREATE TABLE t_a (id UInt64, `j.m` Map(String, String), j JSON(`m.keys` Array(String)), ts DateTime('UTC') DEFAULT '2000-01-01', v String)
ENGINE = MergeTree ORDER BY id TTL ts + INTERVAL 1 DAY DELETE WHERE has(j.m.keys, 'a')
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 10, ttl_only_drop_parts = 0, enable_vertical_merge_algorithm = 0;
SYSTEM STOP MERGES t_a;
INSERT INTO t_a (id, `j.m`, j, v) SELECT number, if(number % 2, map('a', ''), map('b', '')), '{"m":{"keys":["a"]}}', 'v' FROM numbers(100);
INSERT INTO t_a (id, `j.m`, j, v) SELECT number, if(number % 2, map('a', ''), map('b', '')), '{"m":{"keys":["a"]}}', 'v' FROM numbers(100);
SELECT count(), countIf(has(j.m.keys, 'a')) FROM t_a;
SYSTEM START MERGES t_a;
OPTIMIZE TABLE t_a FINAL;
SELECT count(), countIf(has(j.m.keys, 'a')) FROM t_a;

SELECT 'B: DELETE WHERE, vertical merge';
CREATE TABLE t_b (id UInt64, `j.m` Map(String, String), j JSON(`m.keys` Array(String)), ts DateTime('UTC') DEFAULT '2000-01-01', v String)
ENGINE = MergeTree ORDER BY id TTL ts + INTERVAL 1 DAY DELETE WHERE has(j.m.keys, 'a')
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 10, ttl_only_drop_parts = 0,
    enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1, vertical_merge_optimize_ttl_delete = 1;
SYSTEM STOP MERGES t_b;
INSERT INTO t_b (id, `j.m`, j, v) SELECT number, if(number % 2, map('a', ''), map('b', '')), '{"m":{"keys":["a"]}}', 'v' FROM numbers(100);
INSERT INTO t_b (id, `j.m`, j, v) SELECT number, if(number % 2, map('a', ''), map('b', '')), '{"m":{"keys":["a"]}}', 'v' FROM numbers(100);
SELECT count(), countIf(has(j.m.keys, 'a')) FROM t_b;
SYSTEM START MERGES t_b;
OPTIMIZE TABLE t_b FINAL;
SELECT count(), countIf(has(j.m.keys, 'a')) FROM t_b;

SELECT 'C: DELETE WHERE, part TTL info written at insert';
CREATE TABLE t_c (id UInt64, `j.m` Map(String, String), j JSON(`m.keys` Array(String)), ts DateTime('UTC') DEFAULT '2000-01-01', v String)
ENGINE = MergeTree ORDER BY id TTL ts + INTERVAL 1 DAY DELETE WHERE has(j.m.keys, 'a')
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 10, ttl_only_drop_parts = 0, enable_vertical_merge_algorithm = 0;
SYSTEM STOP MERGES t_c;
INSERT INTO t_c (id, `j.m`, j, v) SELECT number, if(number % 2, map('a', ''), map('b', '')), '{"m":{"keys":["zzz"]}}', 'v' FROM numbers(100);
INSERT INTO t_c (id, `j.m`, j, v) SELECT number, if(number % 2, map('a', ''), map('b', '')), '{"m":{"keys":["zzz"]}}', 'v' FROM numbers(100);
SELECT count(), countIf(has(j.m.keys, 'a')) FROM t_c;
SYSTEM START MERGES t_c;
OPTIMIZE TABLE t_c FINAL;
SELECT count(), countIf(has(j.m.keys, 'a')) FROM t_c;

SELECT 'D: DELETE WHERE, untyped JSON';
CREATE TABLE t_d (id UInt64, `j.m` Map(String, String), j JSON, ts DateTime('UTC') DEFAULT '2000-01-01', v String)
ENGINE = MergeTree ORDER BY id TTL ts + INTERVAL 1 DAY DELETE WHERE has(j.m.keys, 'a')
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 10, ttl_only_drop_parts = 0, enable_vertical_merge_algorithm = 0;
SYSTEM STOP MERGES t_d;
INSERT INTO t_d (id, `j.m`, j, v) SELECT number, if(number % 2, map('a', ''), map('b', '')), '{"m":{"keys":["a"]}}', 'v' FROM numbers(100);
INSERT INTO t_d (id, `j.m`, j, v) SELECT number, if(number % 2, map('a', ''), map('b', '')), '{"m":{"keys":["a"]}}', 'v' FROM numbers(100);
SELECT count(), countIf(has(j.m.keys, 'a')) FROM t_d;
SYSTEM START MERGES t_d;
OPTIMIZE TABLE t_d FINAL;
SELECT count(), countIf(has(j.m.keys, 'a')) FROM t_d;

SELECT 'E: rows TTL and RECOMPRESS TTL';
CREATE TABLE t_e (id UInt64, `j.m` Map(String, String), j JSON(`m.keys` Array(String)), ts DateTime('UTC') DEFAULT '2000-01-01', v String)
ENGINE = MergeTree ORDER BY id TTL addYears(ts, 100 * length(j.m.keys)), addYears(ts, 100 * length(j.m.keys) + 1) RECOMPRESS CODEC(ZSTD(1))
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 10, ttl_only_drop_parts = 0, enable_vertical_merge_algorithm = 0;
SYSTEM STOP MERGES t_e;
INSERT INTO t_e (id, `j.m`, j, v) SELECT number, if(number % 2, map('b', ''), map()), '{"m":{"keys":[]}}', 'v' FROM numbers(100);
INSERT INTO t_e (id, `j.m`, j, v) SELECT number, if(number % 2, map('b', ''), map()), '{"m":{"keys":[]}}', 'v' FROM numbers(100);
SELECT count() FROM t_e;
SELECT toTimeZone(min(recompression_ttl_info.min[1]), 'UTC'), toTimeZone(max(recompression_ttl_info.max[1]), 'UTC') FROM system.parts WHERE database = currentDatabase() AND table = 't_e' AND active;
SYSTEM START MERGES t_e;
OPTIMIZE TABLE t_e FINAL;
SELECT count() FROM t_e;
SELECT toTimeZone(min(recompression_ttl_info.min[1]), 'UTC'), toTimeZone(max(recompression_ttl_info.max[1]), 'UTC') FROM system.parts WHERE database = currentDatabase() AND table = 't_e' AND active;

SELECT 'F: JSON path starting with the name of another column, vertical merge';
CREATE TABLE t_f (id UInt64, j UInt8, `j.m` JSON, ts DateTime('UTC') DEFAULT '2000-01-01', v String)
ENGINE = MergeTree ORDER BY id TTL ts + INTERVAL 1 DAY DELETE WHERE j.m.p.:Int64 = 1
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 10, ttl_only_drop_parts = 0,
    enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1, vertical_merge_optimize_ttl_delete = 1;
SYSTEM STOP MERGES t_f;
INSERT INTO t_f (id, j, `j.m`, v) SELECT number, 0, toJSONString(map('p', number % 2)), 'v' FROM numbers(100);
INSERT INTO t_f (id, j, `j.m`, v) SELECT number, 0, toJSONString(map('p', number % 2)), 'v' FROM numbers(100);
SELECT count(), countIf(j.m.p.:Int64 = 1) FROM t_f;
SYSTEM START MERGES t_f;
OPTIMIZE TABLE t_f FINAL;
SELECT count(), countIf(j.m.p.:Int64 = 1) FROM t_f;

SELECT 'G: GROUP BY TTL';
CREATE TABLE t_g (id UInt64, `j.m` Map(String, String), j JSON(`m.keys` Array(String)), ts DateTime('UTC') DEFAULT '2000-01-01', v UInt64)
ENGINE = MergeTree ORDER BY id TTL addYears(ts, 100 * length(j.m.keys)) GROUP BY id SET v = sum(v)
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 10, ttl_only_drop_parts = 0, enable_vertical_merge_algorithm = 0;
SYSTEM STOP MERGES t_g;
INSERT INTO t_g (id, `j.m`, j, v) SELECT number, if(number % 2, map('b', ''), map()), '{"m":{"keys":[]}}', 1 FROM numbers(100);
INSERT INTO t_g (id, `j.m`, j, v) SELECT number, if(number % 2, map('b', ''), map()), '{"m":{"keys":[]}}', 1 FROM numbers(100);
SELECT count(), countIf(v = 2) FROM t_g;
SYSTEM START MERGES t_g;
OPTIMIZE TABLE t_g FINAL;
SELECT count(), countIf(v = 2) FROM t_g;

SELECT 'H: column TTL';
CREATE TABLE t_h (id UInt64, `j.m` Map(String, String), j JSON(`m.keys` Array(String)), ts DateTime('UTC') DEFAULT '2000-01-01', v String TTL addYears(ts, 100 * length(j.m.keys)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 10, ttl_only_drop_parts = 0, enable_vertical_merge_algorithm = 0;
SYSTEM STOP MERGES t_h;
INSERT INTO t_h (id, `j.m`, j, v) SELECT number, if(number % 2, map('b', ''), map()), '{"m":{"keys":[]}}', 'v' FROM numbers(100);
INSERT INTO t_h (id, `j.m`, j, v) SELECT number, if(number % 2, map('b', ''), map()), '{"m":{"keys":[]}}', 'v' FROM numbers(100);
SELECT count(), countIf(v = 'v') FROM t_h;
SYSTEM START MERGES t_h;
OPTIMIZE TABLE t_h FINAL;
SELECT count(), countIf(v = 'v') FROM t_h;

SELECT 'I: DELETE WHERE, vertical merge, both columns merged';
CREATE TABLE t_i (id UInt64, `j.m` Map(String, String), j JSON(`m.keys` Array(String), x UInt8), ts DateTime('UTC') DEFAULT '2000-01-01', v String)
ENGINE = MergeTree ORDER BY id TTL ts + INTERVAL 1 DAY DELETE WHERE has(j.m.keys, 'a') AND j.x = 0
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 10, ttl_only_drop_parts = 0,
    enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1, vertical_merge_optimize_ttl_delete = 1;
SYSTEM STOP MERGES t_i;
INSERT INTO t_i (id, `j.m`, j, v) SELECT number, if(number % 2, map('a', ''), map('b', '')), '{"m":{"keys":["a"]}}', 'v' FROM numbers(100);
INSERT INTO t_i (id, `j.m`, j, v) SELECT number, if(number % 2, map('a', ''), map('b', '')), '{"m":{"keys":["a"]}}', 'v' FROM numbers(100);
SELECT count(), countIf(has(j.m.keys, 'a')) FROM t_i;
SYSTEM START MERGES t_i;
OPTIMIZE TABLE t_i FINAL;
SELECT count(), countIf(has(j.m.keys, 'a')) FROM t_i;

SELECT 'merge algorithms';
SYSTEM FLUSH LOGS part_log;
SELECT DISTINCT table, merge_algorithm FROM system.part_log
WHERE database = currentDatabase() AND event_type = 'MergeParts' AND table IN ('t_a', 't_b', 't_f', 't_i')
ORDER BY ALL;

DROP TABLE t_a;
DROP TABLE t_b;
DROP TABLE t_c;
DROP TABLE t_d;
DROP TABLE t_e;
DROP TABLE t_f;
DROP TABLE t_g;
DROP TABLE t_h;
DROP TABLE t_i;
