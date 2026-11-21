-- A session setting that changes the result type of a key function (here it adds `Nullable`) changes the value
-- the same expression has for some rows: the key is built under the server settings, where a NULL row is stored
-- as a default value, while the query sees NULL. Matching the two by name pruned such rows away.
-- https://github.com/ClickHouse/ClickHouse/pull/109196

SET optimize_use_implicit_projections = 1;

DROP TABLE IF EXISTS t_null_plain;
DROP TABLE IF EXISTS t_null_pk;
DROP TABLE IF EXISTS t_null_minmax;
DROP TABLE IF EXISTS t_null_set;
DROP TABLE IF EXISTS t_null_json;
DROP TABLE IF EXISTS t_null_sample;
DROP TABLE IF EXISTS t_null_greatest;

CREATE TABLE t_null_plain (x Nullable(UInt32), v UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_null_plain VALUES (5, 1);
INSERT INTO t_null_plain VALUES (NULL, 2);

CREATE TABLE t_null_pk (x Nullable(UInt32), v UInt32) ENGINE = MergeTree ORDER BY toUInt32OrDefault(x) SETTINGS index_granularity = 1;
INSERT INTO t_null_pk VALUES (5, 1);
INSERT INTO t_null_pk VALUES (NULL, 2);

CREATE TABLE t_null_minmax (x Nullable(UInt32), v UInt32, INDEX i_x toUInt32OrDefault(x) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY v SETTINGS index_granularity = 1;
INSERT INTO t_null_minmax VALUES (5, 1);
INSERT INTO t_null_minmax VALUES (NULL, 2);

CREATE TABLE t_null_set (x Nullable(UInt32), v UInt32, INDEX i_x toUInt32OrDefault(x) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY v SETTINGS index_granularity = 1;
INSERT INTO t_null_set VALUES (5, 1);
INSERT INTO t_null_set VALUES (NULL, 2);

CREATE TABLE t_null_json (s String, v UInt32) ENGINE = MergeTree ORDER BY JSON_VALUE(s, '$.a') SETTINGS index_granularity = 1;
INSERT INTO t_null_json VALUES ('{"a":"x"}', 1);
INSERT INTO t_null_json VALUES ('{"b":"y"}', 2);

CREATE TABLE t_null_sample (x Nullable(UInt32), v UInt32) ENGINE = MergeTree ORDER BY toUInt32OrDefault(x) SAMPLE BY toUInt32OrDefault(x);
INSERT INTO t_null_sample VALUES (1, 1), (2, 2), (3, 3), (NULL, 4);

CREATE TABLE t_null_greatest (x Nullable(UInt32), y Nullable(UInt32), a UInt32, v UInt32, INDEX i_a greatest(a, v) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY greatest(x, y) SETTINGS allow_nullable_key = 1, index_granularity = 1;
INSERT INTO t_null_greatest VALUES (NULL, 1, 7, 2), (5, 1, 8, 1);

SELECT '-- baseline session: the key and the skip indexes are used';
SELECT v FROM t_null_pk WHERE toUInt32OrDefault(x) = 5 SETTINGS force_primary_key = 1;
SELECT v FROM t_null_pk WHERE toUInt32OrDefault(x) IS NULL;
SELECT v FROM t_null_minmax WHERE toUInt32OrDefault(x) = 5 SETTINGS force_data_skipping_indices = 'i_x', use_query_condition_cache = 0;
SELECT v FROM t_null_set WHERE toUInt32OrDefault(x) = 5 SETTINGS force_data_skipping_indices = 'i_x', use_query_condition_cache = 0;
SELECT v FROM t_null_json WHERE JSON_VALUE(s, '$.a') = 'x' SETTINGS force_primary_key = 1;
SELECT min(toUInt32OrDefault(x)), max(toUInt32OrDefault(x)) FROM t_null_pk;
SELECT count() FROM t_null_sample SAMPLE 1/2 OFFSET 0;
SELECT count() FROM t_null_sample SAMPLE 1/2 OFFSET 1/2;

SELECT '-- cast_keep_nullable = 1: the NULL row is found, the same as without a key';
SET cast_keep_nullable = 1;
SELECT v FROM t_null_plain WHERE toUInt32OrDefault(x) IS NULL;
SELECT v FROM t_null_pk WHERE toUInt32OrDefault(x) IS NULL;
SELECT v FROM t_null_pk WHERE toUInt32OrDefault(x) IS NULL SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT v FROM t_null_minmax WHERE toUInt32OrDefault(x) IS NULL SETTINGS use_query_condition_cache = 0;
SELECT v FROM t_null_minmax WHERE toUInt32OrDefault(x) IS NULL SETTINGS force_data_skipping_indices = 'i_x'; -- { serverError INDEX_NOT_USED }
SELECT v FROM t_null_set WHERE toUInt32OrDefault(x) IS NULL SETTINGS use_query_condition_cache = 0;
SELECT v FROM t_null_set WHERE toUInt32OrDefault(x) IS NULL SETTINGS force_data_skipping_indices = 'i_x'; -- { serverError INDEX_NOT_USED }
SELECT v FROM t_null_pk WHERE toUInt32OrDefault(x) = 5;
SELECT min(toUInt32OrDefault(x)), max(toUInt32OrDefault(x)) FROM t_null_plain;
SELECT min(toUInt32OrDefault(x)), max(toUInt32OrDefault(x)) FROM t_null_pk;
SELECT '-- the two halves of a sample still cover every row';
SELECT count() FROM t_null_sample SAMPLE 1/2 OFFSET 0;
SELECT count() FROM t_null_sample SAMPLE 1/2 OFFSET 1/2;
SELECT '-- a key that does not depend on the setting stays usable';
SELECT v FROM t_null_json WHERE JSON_VALUE(s, '$.a') = 'x' SETTINGS force_primary_key = 1;
SET cast_keep_nullable = 0;

SELECT '-- function_json_value_return_type_allow_nullable = 1: the row without the path is found';
SET function_json_value_return_type_allow_nullable = 1;
SELECT v FROM t_null_json WHERE JSON_VALUE(s, '$.a') IS NULL;
SELECT v FROM t_null_json WHERE JSON_VALUE(s, '$.a') IS NULL SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT v FROM t_null_pk WHERE toUInt32OrDefault(x) = 5 SETTINGS force_primary_key = 1;
SET function_json_value_return_type_allow_nullable = 0;

SELECT '-- least_greatest_legacy_null_behavior = 1: the same type, but a NULL argument makes the result NULL';
SELECT v FROM t_null_greatest WHERE greatest(x, y) = 1 SETTINGS force_primary_key = 1;
SELECT count() FROM (EXPLAIN SELECT min(greatest(x, y)), max(greatest(x, y)) FROM t_null_greatest SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1, enable_parallel_replicas = 0) WHERE explain LIKE '%_minmax_count_projection%';
SET least_greatest_legacy_null_behavior = 1;
SELECT v FROM t_null_greatest WHERE greatest(x, y) IS NULL SETTINGS use_query_condition_cache = 0;
SELECT v FROM t_null_greatest WHERE greatest(x, y) IS NULL SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT count() FROM (EXPLAIN SELECT min(greatest(x, y)), max(greatest(x, y)) FROM t_null_greatest SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1, enable_parallel_replicas = 0) WHERE explain LIKE '%_minmax_count_projection%';
SELECT min(greatest(x, y)), max(greatest(x, y)) FROM t_null_greatest SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SELECT ifNull(toString(greatest(x, y)), 'NULL') FROM t_null_greatest ORDER BY greatest(x, y) SETTINGS optimize_read_in_order = 1;
SELECT '-- arguments that cannot be NULL keep the index usable';
SELECT v FROM t_null_greatest WHERE greatest(a, v) = 7 SETTINGS force_data_skipping_indices = 'i_a', use_query_condition_cache = 0;
SET least_greatest_legacy_null_behavior = 0;

DROP TABLE t_null_plain;
DROP TABLE t_null_pk;
DROP TABLE t_null_minmax;
DROP TABLE t_null_set;
DROP TABLE t_null_json;
DROP TABLE t_null_sample;
DROP TABLE t_null_greatest;
