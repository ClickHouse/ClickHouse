-- A subcolumn is not pushed into a subquery when the whole column is still read by it:
-- reading the subcolumn in addition to the whole column prunes nothing.

DROP TABLE IF EXISTS t_parent;

CREATE TABLE t_parent (k UInt64, tup Tuple(a String, b UInt64)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_parent SELECT number, (toString(number), number) FROM numbers(10);

SET optimize_push_subcolumns_into_subqueries = 1;

SELECT 'only the subcolumn';
SELECT countIf(explain LIKE '%column_name: tup.a,%'), countIf(explain LIKE '%column_name: tup,%')
FROM (EXPLAIN QUERY TREE SELECT sum(length(tup.a)) FROM (SELECT tup FROM t_parent));
SELECT sum(length(tup.a)) FROM (SELECT tup FROM t_parent);

SELECT 'the whole column is used by the outer query';
SELECT countIf(explain LIKE '%column_name: tup.a,%'), countIf(explain LIKE '%column_name: tup,%') > 0
FROM (EXPLAIN QUERY TREE SELECT sum(cityHash64(tup)), sum(length(tup.a)) FROM (SELECT tup FROM t_parent));
SELECT sum(cityHash64(tup)), sum(length(tup.a)) FROM (SELECT tup FROM t_parent);
SELECT sum(cityHash64(tup)), sum(length(tup.a)) FROM (SELECT tup FROM t_parent) SETTINGS optimize_push_subcolumns_into_subqueries = 0;

SELECT 'the whole column is exposed under another name';
SELECT countIf(explain LIKE '%column_name: tup.a,%'), countIf(explain LIKE '%column_name: tup,%') > 0
FROM (EXPLAIN QUERY TREE SELECT sum(cityHash64(u)), sum(length(tup.a)) FROM (SELECT tup, tup AS u FROM t_parent));
SELECT sum(cityHash64(u)), sum(length(tup.a)) FROM (SELECT tup, tup AS u FROM t_parent);

SELECT 'the whole column is used by the subquery';
SELECT countIf(explain LIKE '%column_name: tup.a,%'), countIf(explain LIKE '%column_name: tup,%') > 0
FROM (EXPLAIN QUERY TREE SELECT sum(length(tup.a)) FROM (SELECT tup FROM t_parent WHERE cityHash64(tup) > 0));
SELECT sum(length(tup.a)) FROM (SELECT tup FROM t_parent WHERE cityHash64(tup) > 0);

DROP TABLE t_parent;
