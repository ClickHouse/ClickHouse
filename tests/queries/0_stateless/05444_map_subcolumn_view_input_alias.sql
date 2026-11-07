DROP VIEW IF EXISTS v_alias;
DROP VIEW IF EXISTS v_nested_alias;
DROP VIEW IF EXISTS v_where_alias;
DROP VIEW IF EXISTS v_subcolumn_alias;
DROP VIEW IF EXISTS v_null_subcolumn_alias;
DROP VIEW IF EXISTS v_direct;
DROP TABLE IF EXISTS t;

CREATE TABLE t (x Map(String, String), y Map(String, String)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t VALUES ({'k':'physical'}, {'k':'expected'});
CREATE VIEW v_alias SQL SECURITY INVOKER AS SELECT y AS x, x AS m FROM t;
CREATE VIEW v_nested_alias SQL SECURITY INVOKER AS SELECT toTypeName(y AS x) AS type, x AS m FROM t;
CREATE VIEW v_where_alias SQL SECURITY INVOKER AS SELECT x AS m FROM t WHERE notEmpty(y AS x);
CREATE VIEW v_subcolumn_alias SQL SECURITY INVOKER AS SELECT x AS m, 'wrong' AS `x.key_k` FROM t;
CREATE VIEW v_null_subcolumn_alias SQL SECURITY INVOKER AS SELECT x AS m, NULL AS `x.key_k` FROM t;
CREATE VIEW v_direct SQL SECURITY INVOKER AS SELECT y AS m FROM t;

SELECT m['k'], toTypeName(m['k']) FROM v_alias
SETTINGS optimize_functions_to_subcolumns = 0, prefer_column_name_to_alias = 0;
SELECT m['k'], toTypeName(m['k']) FROM v_alias
SETTINGS optimize_functions_to_subcolumns = 1, prefer_column_name_to_alias = 0;
SELECT m['k'], toTypeName(m['k']) FROM v_alias
SETTINGS optimize_functions_to_subcolumns = 0, prefer_column_name_to_alias = 1;
SELECT m['k'], toTypeName(m['k']) FROM v_alias
SETTINGS optimize_functions_to_subcolumns = 1, prefer_column_name_to_alias = 1;
SELECT m['k'], toTypeName(m['k']) FROM v_nested_alias
SETTINGS optimize_functions_to_subcolumns = 0, prefer_column_name_to_alias = 0;
SELECT m['k'], toTypeName(m['k']) FROM v_nested_alias
SETTINGS optimize_functions_to_subcolumns = 1, prefer_column_name_to_alias = 0;

SELECT m['k'], toTypeName(m['k']) FROM v_where_alias
SETTINGS optimize_functions_to_subcolumns = 0, prefer_column_name_to_alias = 0;
SELECT m['k'], toTypeName(m['k']) FROM v_where_alias
SETTINGS optimize_functions_to_subcolumns = 1, prefer_column_name_to_alias = 0;
SELECT m['k'], toTypeName(m['k']) FROM v_subcolumn_alias
SETTINGS optimize_functions_to_subcolumns = 0, prefer_column_name_to_alias = 0;
SELECT m['k'], toTypeName(m['k']) FROM v_subcolumn_alias
SETTINGS optimize_functions_to_subcolumns = 1, prefer_column_name_to_alias = 0;
SELECT m['k'], toTypeName(m['k']) FROM v_null_subcolumn_alias
SETTINGS optimize_functions_to_subcolumns = 0, prefer_column_name_to_alias = 0;
SELECT m['k'], toTypeName(m['k']) FROM v_null_subcolumn_alias
SETTINGS optimize_functions_to_subcolumns = 1, prefer_column_name_to_alias = 0;

SELECT m['k'], toTypeName(m['k']) FROM v_direct SETTINGS optimize_functions_to_subcolumns = 0;
SELECT m['k'], toTypeName(m['k']) FROM v_direct SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() > 0
FROM (
    EXPLAIN PLAN actions = 1
    SELECT m['k'] FROM v_direct SETTINGS optimize_functions_to_subcolumns = 1
)
WHERE explain ILIKE '%Output: y.key_k%';

ALTER TABLE t MODIFY SETTING map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic';
SELECT count() = 0
FROM (
    EXPLAIN PLAN actions = 1
    SELECT m['k'] FROM v_direct SETTINGS optimize_functions_to_subcolumns = 1
)
WHERE explain ILIKE '%y.key_k%';
SELECT m.key_k, toTypeName(m.key_k) FROM v_direct SETTINGS optimize_functions_to_subcolumns = 0;

DROP VIEW v_alias;
DROP VIEW v_nested_alias;
DROP VIEW v_where_alias;
DROP VIEW v_subcolumn_alias;
DROP VIEW v_null_subcolumn_alias;
DROP VIEW v_direct;
DROP TABLE t;
