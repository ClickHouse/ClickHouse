-- Rows with a NULL sort key are ordered according to NULLS FIRST/LAST and not skipped
-- when `OrderBy` is not the outermost combinator in the direct form.

DROP TABLE IF EXISTS t_order_by_inner;
CREATE TABLE t_order_by_inner (v String, nv Nullable(String), k Nullable(Int32), c UInt8) ENGINE = Memory;
INSERT INTO t_order_by_inner VALUES ('a', 'a', 2, 1), ('b', NULL, NULL, 1), ('c', 'c', 1, 1), ('d', 'd', 0, 0), ('e', 'e', 5, 0);

SELECT '-- If';
SELECT groupArrayOrderByIf('0 ASC NULLS FIRST')(v, k, c) FROM t_order_by_inner;
SELECT groupArrayOrderByIf('0 ASC NULLS LAST')(v, k, c) FROM t_order_by_inner;
SELECT groupArrayOrderByIf('0 DESC NULLS FIRST LIMIT 2')(v, k, c) FROM t_order_by_inner;

SELECT '-- The same as the outermost OrderBy';
SELECT groupArrayIfOrderBy('0 ASC NULLS FIRST')(v, c, k) FROM t_order_by_inner;
SELECT groupArrayIf(v, c ORDER BY k ASC NULLS FIRST) FROM t_order_by_inner;

SELECT '-- State and Merge';
SELECT finalizeAggregation(groupArrayOrderByState('0 ASC NULLS FIRST')(v, k)), toTypeName(groupArrayOrderByState('0 ASC NULLS FIRST')(v, k)) FROM t_order_by_inner;
SELECT groupArrayOrderByMerge('0 ASC NULLS FIRST')(s) FROM (SELECT groupArrayOrderByState('0 ASC NULLS FIRST')(v, k) AS s FROM t_order_by_inner GROUP BY c);
SELECT groupArrayOrderByMerge('0 ASC NULLS LAST')(s) FROM (SELECT groupArrayOrderByState('0 ASC NULLS LAST')(v, k) AS s FROM t_order_by_inner GROUP BY c);

SELECT '-- A Nullable argument of the nested function is still skipped, a NULL sort key is not';
SELECT groupArrayOrderByIf('0 ASC NULLS FIRST')(nv, k, c) FROM t_order_by_inner;
SELECT finalizeAggregation(groupArrayOrderByState('0 ASC NULLS LAST')(nv, k)) FROM t_order_by_inner;
SELECT countOrderByIf('0 ASC')(nv, k, c), countOrderByIf('0 ASC')(v, k, c) FROM t_order_by_inner;
SELECT sumOrderByIf('0 ASC')(x, k, c), toTypeName(sumOrderByIf('0 ASC')(x, k, c))
FROM values('x Nullable(Int32), k Nullable(Int32), c UInt8', (1, NULL, 1), (NULL, 2, 1), (10, 3, 1), (100, NULL, 0));

SELECT '-- Other outer combinators';
SELECT groupArrayOrderByOrNull('0 ASC NULLS FIRST')(v, k), groupArrayOrderByOrDefault('0 ASC NULLS LAST')(v, k) FROM t_order_by_inner;
SELECT groupArrayOrderByArray('0 ASC NULLS FIRST')(vs, ks) FROM values('vs Array(String), ks Array(Nullable(Int32))', (['a', 'b'], [2, NULL]), (['c'], [1]));

DROP TABLE t_order_by_inner;
