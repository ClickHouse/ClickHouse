-- `LIMIT AFTER` and `LIMIT UNTIL` are evaluated after the aggregation: a subcolumn access there must not be
-- pushed into the subquery when the query groups by the whole column.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_subcolumn_pushdown_limit_after_until;
CREATE TABLE t_subcolumn_pushdown_limit_after_until (tup Tuple(a UInt64, b String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_subcolumn_pushdown_limit_after_until VALUES ((1, 'x')), ((2, 'y'));

SELECT 'after, off';
SELECT tup, count() FROM (SELECT tup FROM t_subcolumn_pushdown_limit_after_until) GROUP BY tup ORDER BY tup LIMIT AFTER tup.a = 1
SETTINGS optimize_push_subcolumns_into_subqueries = 0;
SELECT 'after, on';
SELECT tup, count() FROM (SELECT tup FROM t_subcolumn_pushdown_limit_after_until) GROUP BY tup ORDER BY tup LIMIT AFTER tup.a = 1
SETTINGS optimize_push_subcolumns_into_subqueries = 1;

SELECT 'until, off';
SELECT tup, count() FROM (SELECT tup FROM t_subcolumn_pushdown_limit_after_until) GROUP BY tup ORDER BY tup LIMIT UNTIL tup.a = 2
SETTINGS optimize_push_subcolumns_into_subqueries = 0;
SELECT 'until, on';
SELECT tup, count() FROM (SELECT tup FROM t_subcolumn_pushdown_limit_after_until) GROUP BY tup ORDER BY tup LIMIT UNTIL tup.a = 2
SETTINGS optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE t_subcolumn_pushdown_limit_after_until;
