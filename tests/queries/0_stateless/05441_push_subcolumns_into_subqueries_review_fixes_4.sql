-- Regression tests for `optimize_push_subcolumns_into_subqueries`.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_json_array_sibling_outer;
DROP TABLE IF EXISTS t_push_subcolumns_json_array_sibling_table;
DROP TABLE IF EXISTS t_push_subcolumns_partial;
DROP TABLE IF EXISTS t_push_subcolumns_computed_export;

-- The flat name of the nested JSON-array subcolumn of a path can be claimed by a sibling tuple element,
-- also when the `getSubcolumn` chain is in the outer expression.
SELECT 'json array sibling in the outer expression';
CREATE TABLE t_push_subcolumns_json_array_sibling_outer (c Tuple(j JSON, `j.a.:\`Array(JSON)\`.@\`b\`` Array(Dynamic))) ENGINE = Memory;
INSERT INTO t_push_subcolumns_json_array_sibling_outer VALUES (('{"a":[{"b":7}]}', [99]));
SELECT tupleElement(arrayElement(getSubcolumn(getSubcolumn(c, 'j'), 'a'), 1), 'b') FROM (SELECT c FROM t_push_subcolumns_json_array_sibling_outer);
SELECT tupleElement(arrayElement(getSubcolumn(getSubcolumn(c, 'j'), 'a'), 1), 'b') FROM (SELECT c FROM t_push_subcolumns_json_array_sibling_outer) SETTINGS optimize_push_subcolumns_into_subqueries = 0;

-- The same for an exported subcolumn read of a table column.
SELECT 'json array sibling in the exported table subcolumn';
CREATE TABLE t_push_subcolumns_json_array_sibling_table (c Tuple(j JSON, `j.a.:\`Array(JSON)\`.@\`b\`` Array(Dynamic))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_push_subcolumns_json_array_sibling_table VALUES (('{"a":[{"b":7}]}', [99]));
SELECT x[1].b FROM (SELECT c.j.a AS x FROM t_push_subcolumns_json_array_sibling_table);
SELECT x[1].b FROM (SELECT c.j.a AS x FROM t_push_subcolumns_json_array_sibling_table) SETTINGS optimize_push_subcolumns_into_subqueries = 0;

-- `tup.a.b` is not pushed because the subquery reads `tup.a` in its filter; its occurrence keeps the whole `tup`
-- alive, so the sibling `tup.c` must not be pushed either.
SELECT 'partially pushable siblings';
CREATE TABLE t_push_subcolumns_partial (tup Tuple(a Tuple(b String, d UInt8), c String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_push_subcolumns_partial SELECT ((toString(number), number % 255), toString(number * 2)) FROM numbers(10);
SELECT count() FROM
(
    EXPLAIN actions = 1
    SELECT sum(cityHash64(tup.a.b)), sum(cityHash64(tup.c)) FROM (SELECT tup FROM t_push_subcolumns_partial WHERE cityHash64(tup.a) > 0)
    SETTINGS enable_parallel_replicas = 0
)
WHERE explain LIKE '%tup.c%' OR explain LIKE '%tup.a.b%';
SELECT sum(cityHash64(tup.a.b)), sum(cityHash64(tup.c)) FROM (SELECT tup FROM t_push_subcolumns_partial WHERE cityHash64(tup.a) > 0);
SELECT sum(cityHash64(tup.a.b)), sum(cityHash64(tup.c)) FROM (SELECT tup FROM t_push_subcolumns_partial WHERE cityHash64(tup.a) > 0)
SETTINGS optimize_push_subcolumns_into_subqueries = 0;

-- A live computed export of the subquery reads the whole column, so the subcolumn is extracted from it.
SELECT 'whole column used in a live computed export';
CREATE TABLE t_push_subcolumns_computed_export (tup Tuple(a String, b UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_push_subcolumns_computed_export SELECT (toString(number), number % 255) FROM numbers(10);
SELECT count() FROM
(
    EXPLAIN actions = 1
    SELECT sum(cityHash64(tup.a)) FROM (SELECT tup, cityHash64(tup) AS h FROM t_push_subcolumns_computed_export) WHERE h > 0
    SETTINGS enable_parallel_replicas = 0
)
WHERE explain LIKE '%tup.a%';
SELECT count() FROM
(
    EXPLAIN actions = 1
    SELECT sum(cityHash64(tup.a)), any(h) > 0 FROM (SELECT tup, cityHash64(tup) AS h FROM t_push_subcolumns_computed_export)
    SETTINGS enable_parallel_replicas = 0
)
WHERE explain LIKE '%tup.a%';
SELECT sum(cityHash64(tup.a)) = (SELECT sum(cityHash64(toString(number))) FROM numbers(10))
FROM (SELECT tup, cityHash64(tup) AS h FROM t_push_subcolumns_computed_export) WHERE h > 0;

-- A dead computed export does not block the pushdown.
SELECT 'dead computed export';
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT sum(cityHash64(tup.a)) FROM (SELECT tup, cityHash64(tup) AS h FROM t_push_subcolumns_computed_export)
    SETTINGS enable_parallel_replicas = 0
)
WHERE explain LIKE '%tup.a%';

DROP TABLE t_push_subcolumns_json_array_sibling_outer;
DROP TABLE t_push_subcolumns_json_array_sibling_table;
DROP TABLE t_push_subcolumns_partial;
DROP TABLE t_push_subcolumns_computed_export;
