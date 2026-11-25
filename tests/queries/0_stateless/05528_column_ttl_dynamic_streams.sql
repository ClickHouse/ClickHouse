-- https://github.com/ClickHouse/ClickHouse/issues/129673
-- A column TTL that expires a `Dynamic` or `JSON` column must remove all of its streams from a Wide part,
-- as it does for a `String` column: the part must not keep files of a column it no longer has.
-- `auto_statistics_types` is pinned: statistics files of an expired column are not removed, and the
-- statistics types built for `String` and `Dynamic` differ.

DROP TABLE IF EXISTS t_ttl_string;
DROP TABLE IF EXISTS t_ttl_dynamic;
DROP TABLE IF EXISTS t_ttl_json;
DROP TABLE IF EXISTS t_ttl_dynamic_future;
DROP TABLE IF EXISTS t_ttl_merge_string;
DROP TABLE IF EXISTS t_ttl_merge_dynamic;
DROP TABLE IF EXISTS t_ttl_merge_json;

-- `MATERIALIZE TTL` rewrites only the expired column.

CREATE TABLE t_ttl_string (id UInt64, ts DateTime, c String TTL ts + INTERVAL 1 DAY)
ENGINE = MergeTree ORDER BY id SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, auto_statistics_types = '';

CREATE TABLE t_ttl_dynamic (id UInt64, ts DateTime, c Dynamic TTL ts + INTERVAL 1 DAY)
ENGINE = MergeTree ORDER BY id SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, auto_statistics_types = '';

CREATE TABLE t_ttl_json (id UInt64, ts DateTime, c JSON TTL ts + INTERVAL 1 DAY)
ENGINE = MergeTree ORDER BY id SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, auto_statistics_types = '';

CREATE TABLE t_ttl_dynamic_future (id UInt64, ts DateTime, c Dynamic TTL ts + INTERVAL 100 YEAR)
ENGINE = MergeTree ORDER BY id SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, auto_statistics_types = '';

SYSTEM STOP TTL MERGES t_ttl_string;
SYSTEM STOP TTL MERGES t_ttl_dynamic;
SYSTEM STOP TTL MERGES t_ttl_json;
SYSTEM STOP TTL MERGES t_ttl_dynamic_future;

INSERT INTO t_ttl_string SELECT 1, toDateTime('2020-01-01 00:00:00'), 'hello';
INSERT INTO t_ttl_dynamic SELECT 1, toDateTime('2020-01-01 00:00:00'), CAST('hello', 'Dynamic');
INSERT INTO t_ttl_json SELECT 1, toDateTime('2020-01-01 00:00:00'), '{"a" : 1, "b" : "x", "c" : [1, 2]}';
INSERT INTO t_ttl_dynamic_future SELECT 1, toDateTime('2020-01-01 00:00:00'), CAST('hello', 'Dynamic');

ALTER TABLE t_ttl_string MATERIALIZE TTL SETTINGS mutations_sync = 2;
ALTER TABLE t_ttl_dynamic MATERIALIZE TTL SETTINGS mutations_sync = 2;
ALTER TABLE t_ttl_json MATERIALIZE TTL SETTINGS mutations_sync = 2;
ALTER TABLE t_ttl_dynamic_future MATERIALIZE TTL SETTINGS mutations_sync = 2;

-- A TTL merge writes the whole part. Background merges are disabled, so each table gets exactly one merge,
-- by `OPTIMIZE`: a later merge would clean up the expired column on its own, and the part level changes
-- the serialization of `_block_offset`, so the parts of different tables would not be comparable.

CREATE TABLE t_ttl_merge_string (id UInt64, ts DateTime, c String TTL ts + INTERVAL 1 DAY)
ENGINE = MergeTree ORDER BY id SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, auto_statistics_types = '',
    max_number_of_merges_with_ttl_in_pool = 0, min_parts_to_merge_at_once = 3;

CREATE TABLE t_ttl_merge_dynamic (id UInt64, ts DateTime, c Dynamic TTL ts + INTERVAL 1 DAY)
ENGINE = MergeTree ORDER BY id SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, auto_statistics_types = '',
    max_number_of_merges_with_ttl_in_pool = 0, min_parts_to_merge_at_once = 3;

CREATE TABLE t_ttl_merge_json (id UInt64, ts DateTime, c JSON TTL ts + INTERVAL 1 DAY)
ENGINE = MergeTree ORDER BY id SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, auto_statistics_types = '',
    max_number_of_merges_with_ttl_in_pool = 0, min_parts_to_merge_at_once = 3;

INSERT INTO t_ttl_merge_string SELECT 1, toDateTime('2020-01-01 00:00:00'), 'hello';
INSERT INTO t_ttl_merge_string SELECT 2, toDateTime('2020-01-01 00:00:00'), 'world';
INSERT INTO t_ttl_merge_dynamic SELECT 1, toDateTime('2020-01-01 00:00:00'), CAST('hello', 'Dynamic');
INSERT INTO t_ttl_merge_dynamic SELECT 2, toDateTime('2020-01-01 00:00:00'), CAST(42, 'Dynamic');
INSERT INTO t_ttl_merge_json SELECT 1, toDateTime('2020-01-01 00:00:00'), '{"a" : 1, "c" : [1, 2]}';
INSERT INTO t_ttl_merge_json SELECT 2, toDateTime('2020-01-01 00:00:00'), '{"b" : "x"}';

OPTIMIZE TABLE t_ttl_merge_string FINAL;
OPTIMIZE TABLE t_ttl_merge_dynamic FINAL;
OPTIMIZE TABLE t_ttl_merge_json FINAL;

-- Persisted virtual columns such as `_block_number` depend on randomized settings.
SELECT table, groupArray(column) FROM
(
    SELECT table, column FROM system.parts_columns
    WHERE database = currentDatabase() AND table LIKE 't_ttl_%' AND active AND NOT startsWith(column, '_')
    ORDER BY table, column_position
)
GROUP BY table ORDER BY table;

-- The expired `String` column leaves only the files of `id` and `ts`; the same number is expected for the others.
SELECT table, files = (SELECT files FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_string' AND active)
FROM system.parts
WHERE database = currentDatabase() AND table IN ('t_ttl_dynamic', 't_ttl_json') AND active
ORDER BY table;

SELECT table, files = (SELECT files FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_merge_string' AND active)
FROM system.parts
WHERE database = currentDatabase() AND table IN ('t_ttl_merge_dynamic', 't_ttl_merge_json') AND active
ORDER BY table;

SELECT * FROM t_ttl_dynamic;
SELECT * FROM t_ttl_json;
SELECT * FROM t_ttl_dynamic_future;
SELECT * FROM t_ttl_merge_dynamic ORDER BY id;
SELECT * FROM t_ttl_merge_json ORDER BY id;

CHECK TABLE t_ttl_dynamic SETTINGS check_query_single_value_result = 1;
CHECK TABLE t_ttl_json SETTINGS check_query_single_value_result = 1;
CHECK TABLE t_ttl_merge_dynamic SETTINGS check_query_single_value_result = 1;
CHECK TABLE t_ttl_merge_json SETTINGS check_query_single_value_result = 1;

DROP TABLE t_ttl_string;
DROP TABLE t_ttl_dynamic;
DROP TABLE t_ttl_json;
DROP TABLE t_ttl_dynamic_future;
DROP TABLE t_ttl_merge_string;
DROP TABLE t_ttl_merge_dynamic;
DROP TABLE t_ttl_merge_json;
