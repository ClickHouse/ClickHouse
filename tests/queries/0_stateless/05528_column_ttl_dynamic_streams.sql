-- https://github.com/ClickHouse/ClickHouse/issues/129673
-- A column TTL that expires a `Dynamic` or `JSON` column must remove all of its streams from a Wide part,
-- as it does for a `String` column: the part must not keep files of a column it no longer has.

DROP TABLE IF EXISTS t_ttl_string;
DROP TABLE IF EXISTS t_ttl_dynamic;
DROP TABLE IF EXISTS t_ttl_json;
DROP TABLE IF EXISTS t_ttl_dynamic_future;

CREATE TABLE t_ttl_string (id UInt64, ts DateTime, c String TTL ts + INTERVAL 1 DAY)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, materialize_ttl_recalculate_only = 0,
    add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_string_columns = 0, add_minmax_index_for_temporal_columns = 0;

CREATE TABLE t_ttl_dynamic (id UInt64, ts DateTime, c Dynamic TTL ts + INTERVAL 1 DAY)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, materialize_ttl_recalculate_only = 0,
    add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_string_columns = 0, add_minmax_index_for_temporal_columns = 0;

CREATE TABLE t_ttl_json (id UInt64, ts DateTime, c JSON TTL ts + INTERVAL 1 DAY)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, materialize_ttl_recalculate_only = 0,
    add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_string_columns = 0, add_minmax_index_for_temporal_columns = 0;

CREATE TABLE t_ttl_dynamic_future (id UInt64, ts DateTime, c Dynamic TTL ts + INTERVAL 100 YEAR)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, materialize_ttl_recalculate_only = 0,
    add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_string_columns = 0, add_minmax_index_for_temporal_columns = 0;

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

SELECT table, groupArray(column) FROM
(
    SELECT table, column FROM system.parts_columns
    WHERE database = currentDatabase() AND table LIKE 't_ttl_%' AND active
    ORDER BY table, column_position
)
GROUP BY table ORDER BY table;

-- The expired `String` column leaves only the files of `id` and `ts`; the same number is expected for the others.
SELECT table, files = (SELECT files FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_string' AND active)
FROM system.parts
WHERE database = currentDatabase() AND table IN ('t_ttl_dynamic', 't_ttl_json') AND active
ORDER BY table;

SELECT * FROM t_ttl_dynamic;
SELECT * FROM t_ttl_json;
SELECT * FROM t_ttl_dynamic_future;

CHECK TABLE t_ttl_dynamic SETTINGS check_query_single_value_result = 1;
CHECK TABLE t_ttl_json SETTINGS check_query_single_value_result = 1;

DROP TABLE t_ttl_string;
DROP TABLE t_ttl_dynamic;
DROP TABLE t_ttl_json;
DROP TABLE t_ttl_dynamic_future;
