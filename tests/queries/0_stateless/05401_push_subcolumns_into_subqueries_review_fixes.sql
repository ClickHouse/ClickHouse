-- Regression tests for `optimize_push_subcolumns_into_subqueries`.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_outer_join;
DROP TABLE IF EXISTS t_push_subcolumns_name_collision;
DROP TABLE IF EXISTS t_push_subcolumns_limit_range;
DROP TABLE IF EXISTS t_push_subcolumns_variant;

-- A column of a table on the outer side of a JOIN inside the subquery is filled with NULL for
-- non-matched rows, while its `null` subcolumn read directly from the table would be filled with 0.
SELECT 'outer join';
CREATE TABLE t_push_subcolumns_outer_join (id UInt64, n Nullable(UInt32)) ENGINE = Memory;
SELECT isNull(x) FROM (SELECT r.n AS x FROM numbers(1) AS l LEFT JOIN t_push_subcolumns_outer_join AS r ON l.number = r.id) SETTINGS join_use_nulls = 0;
SELECT getSubcolumn(x, 'null') FROM (SELECT r.n AS x FROM numbers(1) AS l LEFT JOIN t_push_subcolumns_outer_join AS r ON l.number = r.id) SETTINGS join_use_nulls = 0;
SELECT isNull(x) FROM (SELECT r.n AS x FROM t_push_subcolumns_outer_join AS r RIGHT JOIN numbers(1) AS l ON l.number = r.id) SETTINGS join_use_nulls = 0;
SELECT isNull(x) FROM (SELECT r.n AS x FROM numbers(1) AS l FULL JOIN t_push_subcolumns_outer_join AS r ON l.number = r.id) SETTINGS join_use_nulls = 0;
SELECT isNull(x) FROM (SELECT r.n AS x FROM numbers(1) AS l LEFT JOIN t_push_subcolumns_outer_join AS r ON l.number = r.id) SETTINGS join_use_nulls = 1;

-- A synthesized subcolumn name must not resolve to an unrelated sibling element with the same type.
SELECT 'name collision';
CREATE TABLE t_push_subcolumns_name_collision (c Tuple(`a.size` UInt64, a String)) ENGINE = Memory;
INSERT INTO t_push_subcolumns_name_collision VALUES ((99, 'abc'));
SELECT length(x) FROM (SELECT c.a AS x FROM t_push_subcolumns_name_collision);
SELECT empty(x), notEmpty(x) FROM (SELECT c.a AS x FROM t_push_subcolumns_name_collision);
SELECT getSubcolumn(x, 'size') FROM (SELECT c.a AS x FROM t_push_subcolumns_name_collision);

-- LIMIT AFTER and LIMIT UNTIL are evaluated after aggregation over the aggregation keys.
SELECT 'limit range';
CREATE TABLE t_push_subcolumns_limit_range (tup Tuple(a UInt32, b UInt32)) ENGINE = Memory;
INSERT INTO t_push_subcolumns_limit_range VALUES ((1, 2)), ((3, 4)), ((5, 6));
SELECT tup.a FROM (SELECT tup FROM t_push_subcolumns_limit_range) GROUP BY tup.a ORDER BY tup.a LIMIT UNTIL tup.a > 0;
SELECT tup.a FROM (SELECT tup FROM t_push_subcolumns_limit_range) GROUP BY tup.a ORDER BY tup.a LIMIT AFTER tup.a > 1;
SELECT tup.a FROM (SELECT tup FROM t_push_subcolumns_limit_range) ORDER BY tup.a LIMIT AFTER tup.b > 2 UNTIL tup.b > 4;

-- `variantElement` pushes the alternative subcolumn, whose type is the nullable-safe alternative.
SELECT 'variantElement';
CREATE TABLE t_push_subcolumns_variant (id UInt64, v Variant(UInt32, String, Array(UInt32))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_push_subcolumns_variant VALUES (1, 42), (2, 'abc'), (3, [1, 2]), (4, NULL);
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1
    SELECT variantElement(v, 'UInt32') FROM (SELECT v FROM t_push_subcolumns_variant)
    SETTINGS enable_parallel_replicas = 0
)
WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Output%';
SELECT variantElement(v, 'UInt32'), variantElement(v, 'Array(UInt32)') FROM (SELECT id, v FROM t_push_subcolumns_variant) ORDER BY id;

DROP TABLE t_push_subcolumns_outer_join;
DROP TABLE t_push_subcolumns_name_collision;
DROP TABLE t_push_subcolumns_limit_range;
DROP TABLE t_push_subcolumns_variant;
