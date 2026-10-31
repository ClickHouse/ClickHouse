-- Regression tests for `optimize_push_subcolumns_into_subqueries`.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_alias_merge_join;
DROP TABLE IF EXISTS t_push_subcolumns_json_array_sibling;
DROP TABLE IF EXISTS t_push_subcolumns_index;

-- `StorageMerge` replaces the projection of a subquery with a join by the required columns,
-- so the alias list of the subquery does not apply to it.
SELECT 'alias list with merge and join key';
CREATE TABLE t_push_subcolumns_alias_merge_join (id UInt64, tup Tuple(a UInt32, b UInt32)) ENGINE = Memory;
INSERT INTO t_push_subcolumns_alias_merge_join VALUES (0, (1, 2));
SELECT x.a FROM (SELECT t.id, t.tup FROM merge(currentDatabase(), '^t_push_subcolumns_alias_merge_join$') AS t INNER JOIN numbers(1) AS n ON t.id = n.number) AS s(i, x);
SELECT x.a FROM (SELECT t.id, t.tup FROM merge(currentDatabase(), '^t_push_subcolumns_alias_merge_join$') AS t INNER JOIN numbers(1) AS n ON t.id = n.number) AS s(i, x) SETTINGS optimize_push_subcolumns_into_subqueries = 0;

-- The flat name of the nested JSON-array subcolumn of a path can be claimed by a sibling tuple element.
SELECT 'json array sibling';
CREATE TABLE t_push_subcolumns_json_array_sibling (c Tuple(j JSON, `j.a.:\`Array(JSON)\`.@\`b\`` Array(Dynamic))) ENGINE = Memory;
INSERT INTO t_push_subcolumns_json_array_sibling VALUES (('{"a":[{"b":7}]}', [99]));
SELECT x[1].b FROM (SELECT getSubcolumn(c, 'j.a') AS x FROM (SELECT c FROM t_push_subcolumns_json_array_sibling));
SELECT x[1].b FROM (SELECT getSubcolumn(c, 'j.a') AS x FROM (SELECT c FROM t_push_subcolumns_json_array_sibling)) SETTINGS optimize_push_subcolumns_into_subqueries = 0;

-- A function over a column of the primary key is not replaced with a subcolumn, which the index analysis does not recognize.
SELECT 'primary key';
CREATE TABLE t_push_subcolumns_index (s String) ENGINE = MergeTree ORDER BY length(s) SETTINGS index_granularity = 128;
INSERT INTO t_push_subcolumns_index SELECT repeat('a', number % 100) FROM numbers(100000);
SELECT length(s) AS l, count() FROM (SELECT s FROM t_push_subcolumns_index) WHERE length(s) = 5 GROUP BY l SETTINGS force_primary_key = 1;

DROP TABLE t_push_subcolumns_alias_merge_join;
DROP TABLE t_push_subcolumns_json_array_sibling;
DROP TABLE t_push_subcolumns_index;
