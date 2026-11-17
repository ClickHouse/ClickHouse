DROP VIEW IF EXISTS v_alias;
DROP VIEW IF EXISTS v_nested_alias;
DROP VIEW IF EXISTS v_where_alias;
DROP VIEW IF EXISTS v_subcolumn_alias;
DROP VIEW IF EXISTS v_null_subcolumn_alias;
DROP VIEW IF EXISTS v_direct;
DROP VIEW IF EXISTS v_dotted;
DROP VIEW IF EXISTS v_dotted_quoted;
DROP VIEW IF EXISTS v_dotted_alias_qualified;
DROP VIEW IF EXISTS v_dotted_table_qualified;
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_dotted;

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

CREATE TABLE t_dotted (m Map(String, String), `x.m` Map(String, String), `t_dotted.m` Map(String, String))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t_dotted VALUES ({'k':'ordinary'}, {'k':'dotted'}, {'k':'table_dotted'});
CREATE VIEW v_dotted SQL SECURITY INVOKER AS SELECT x.m AS attrs FROM t_dotted;
CREATE VIEW v_dotted_quoted SQL SECURITY INVOKER AS SELECT `x.m` AS attrs FROM t_dotted;
CREATE VIEW v_dotted_alias_qualified SQL SECURITY INVOKER AS SELECT x.m AS attrs FROM t_dotted AS x;
CREATE VIEW v_dotted_table_qualified SQL SECURITY INVOKER AS SELECT t_dotted.m AS attrs FROM t_dotted;

SELECT attrs['k'], toTypeName(attrs['k']) FROM v_dotted SETTINGS optimize_functions_to_subcolumns = 0;
SELECT attrs['k'], toTypeName(attrs['k']) FROM v_dotted SETTINGS optimize_functions_to_subcolumns = 1;
SELECT attrs['k'], toTypeName(attrs['k']) FROM v_dotted_quoted SETTINGS optimize_functions_to_subcolumns = 0;
SELECT attrs['k'], toTypeName(attrs['k']) FROM v_dotted_quoted SETTINGS optimize_functions_to_subcolumns = 1;
SELECT attrs.key_k, toTypeName(attrs.key_k) FROM v_dotted SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() > 0
FROM (
    EXPLAIN PLAN actions = 1
    SELECT attrs['k'] FROM v_dotted SETTINGS optimize_functions_to_subcolumns = 1
)
WHERE explain ILIKE '%Output: x.m.key_k%';

-- Qualified names can also match a stored dotted name; preserve the analyzer's binding.
SELECT
    (SELECT attrs['k'] FROM v_dotted_alias_qualified SETTINGS optimize_functions_to_subcolumns = 0)
        = (SELECT attrs['k'] FROM v_dotted_alias_qualified SETTINGS optimize_functions_to_subcolumns = 1),
    toTypeName((SELECT attrs['k'] FROM v_dotted_alias_qualified SETTINGS optimize_functions_to_subcolumns = 1));
SELECT
    (SELECT attrs['k'] FROM v_dotted_table_qualified SETTINGS optimize_functions_to_subcolumns = 0)
        = (SELECT attrs['k'] FROM v_dotted_table_qualified SETTINGS optimize_functions_to_subcolumns = 1),
    toTypeName((SELECT attrs['k'] FROM v_dotted_table_qualified SETTINGS optimize_functions_to_subcolumns = 1));

DROP VIEW v_dotted;
DROP VIEW v_dotted_quoted;
DROP VIEW v_dotted_alias_qualified;
DROP VIEW v_dotted_table_qualified;
DROP TABLE t_dotted;
