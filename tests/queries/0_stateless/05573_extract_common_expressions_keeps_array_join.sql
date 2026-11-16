-- Random settings limits: optimize_extract_common_expressions=(1, None)

-- Common-expression extraction must not discard an `OR` operand that holds `arrayJoin`: in `WHERE` and `JOIN ON`
-- `arrayJoin` multiplies the rows.

CREATE TABLE t (a UInt64, arr Array(UInt8)) ENGINE = Memory;
INSERT INTO t SELECT number, [1, 2] FROM numbers(3);

SELECT count() FROM (SELECT a FROM t WHERE (a > 0 AND arrayJoin(arr) > 0) OR a > 0);
SELECT count() FROM (SELECT a FROM t WHERE (a > 0 AND a < 10) OR (a < 10 AND arrayJoin(arr) > 0 AND a > 0));
SELECT count() FROM (SELECT a FROM t WHERE a < 10 AND ((a > 0 AND arrayJoin(arr) > 0) OR a > 0));
SELECT count() FROM (SELECT a FROM t WHERE arrayJoin(arr) = 1 OR (arrayJoin(arr) = 1 AND arrayJoin([5, 6, 7]) > 0));
SELECT count() FROM (SELECT a FROM t WHERE (arrayJoin(arr) = 1 AND a > 0) OR (arrayJoin(arr) = 1 AND a > 0 AND arrayJoin([5, 6, 7]) > 0));
SELECT count() FROM t AS l INNER JOIN t AS r ON l.a = r.a AND ((l.a > 0 AND arrayJoin(l.arr) > 0) OR l.a > 0) SETTINGS join_algorithm = 'hash';
SELECT count() FROM t AS l INNER JOIN t AS r ON (l.a = r.a AND l.a > 0) OR (l.a = r.a AND l.a > 0 AND arrayJoin(l.arr) > 0) SETTINGS join_algorithm = 'hash';

-- An operand whose `arrayJoin` is also in the kept part is still discarded.
SELECT count() FROM (EXPLAIN QUERY TREE SELECT a FROM t WHERE arrayJoin(arr) = 1 OR (arrayJoin(arr) = 1 AND a > 0)) WHERE explain ILIKE '%function_name: greater%';
SELECT count() FROM t AS l INNER JOIN t AS r ON (l.a = r.a AND arrayJoin(l.arr) = 1) OR (l.a = r.a AND arrayJoin(l.arr) = 1 AND l.a > 0) SETTINGS join_algorithm = 'partial_merge';
