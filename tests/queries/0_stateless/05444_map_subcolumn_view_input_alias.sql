DROP VIEW IF EXISTS v_alias;
DROP VIEW IF EXISTS v_nested_alias;
DROP VIEW IF EXISTS v_direct;
DROP TABLE IF EXISTS t;

CREATE TABLE t (x Map(String, String), y Map(String, String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t VALUES ({'k':'physical'}, {'k':'expected'});
CREATE VIEW v_alias SQL SECURITY INVOKER AS SELECT y AS x, x AS m FROM t;
CREATE VIEW v_nested_alias SQL SECURITY INVOKER AS SELECT toTypeName(y AS x) AS type, x AS m FROM t;
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

SELECT m['k'], toTypeName(m['k']) FROM v_direct SETTINGS optimize_functions_to_subcolumns = 0;
SELECT m['k'], toTypeName(m['k']) FROM v_direct SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() > 0
FROM (
    EXPLAIN PLAN actions = 1
    SELECT m['k'] FROM v_direct SETTINGS optimize_functions_to_subcolumns = 1
)
WHERE explain ILIKE '%Output: y.key_k%';

DROP VIEW v_alias;
DROP VIEW v_nested_alias;
DROP VIEW v_direct;
DROP TABLE t;