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

DROP TABLE t_null_plain;
DROP TABLE t_null_pk;
DROP TABLE t_null_minmax;
DROP TABLE t_null_set;
DROP TABLE t_null_json;
DROP TABLE t_null_sample;
