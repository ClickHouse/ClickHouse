-- A correlated subquery on the right side of IN / NOT IN is rewritten into EXISTS and returns what the regular IN
-- returns for the same values.

SET allow_correlated_subqueries = 1;

DROP TABLE IF EXISTS o;
DROP TABLE IF EXISTS i;
DROP TABLE IF EXISTS j;
CREATE TABLE o (k UInt8, x Int32, tags String, xn Nullable(Int32), kn Nullable(UInt8), arr Array(Int32), xa Int32 ALIAS coalesce(xn, 0), xz Nullable(Int32) ALIAS xn) ENGINE = Memory;
CREATE TABLE i (k UInt8, e Int32, f Float64) ENGINE = Memory;
CREATE TABLE j (e Int32, a Int32) ENGINE = Memory;
INSERT INTO o VALUES (1, 1, 'a,b', 1, 1, [1]), (1, 2, 'c', NULL, NULL, [2]), (2, 2, 'b', 2, 2, [3]), (2, 3, '', 3, 2, []), (3, 1, 'b', NULL, 3, [1]);
INSERT INTO i VALUES (1, 1, 0.5), (1, 1, 0.5), (1, 5, 1.5), (2, 2, 2.5), (2, 3, 3.5);
INSERT INTO j VALUES (1, 10), (1, 10), (2, 20), (5, 50), (7, 70);

SELECT 'placements';
SELECT k, x FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k) ORDER BY k, x;
SELECT k, x FROM o WHERE x NOT IN (SELECT e FROM i WHERE i.k = o.k) ORDER BY k, x;
SELECT k, sum(x) FROM o GROUP BY k HAVING k IN (SELECT i.k FROM i WHERE i.k = o.k AND i.e > 1) ORDER BY k;
SELECT k, x, CASE WHEN x IN (SELECT e FROM i WHERE i.k = o.k) THEN 'in' ELSE 'out' END FROM o ORDER BY k, x;
SELECT k, x FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k) OR k = 3 ORDER BY k, x;
SELECT k, x FROM o WHERE (o.x AS a) IN (SELECT e FROM i WHERE i.k = o.k) AND a > 1 ORDER BY k, x;
SELECT k, x * 10 AS y FROM o WHERE y IN (SELECT e * 10 FROM i WHERE i.k = o.k) ORDER BY k, y;
WITH v -> v + 1 AS abs SELECT k, x FROM o WHERE abs(x) IN (SELECT e FROM i WHERE i.k = o.k) ORDER BY k, x;
SELECT k, x, 1 IN (SELECT e FROM i WHERE i.k = o.k) FROM o ORDER BY k, x;
SELECT k, x, (x, k) IN (SELECT e, i.k FROM i WHERE i.k = o.k) FROM o ORDER BY k, x;
-- Inside a correlated EXISTS, correlated with the subquery of EXISTS.
SELECT k, x FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.k = o.k AND i.e IN (SELECT j.e FROM j WHERE j.a = i.e * 10)) ORDER BY k, x;

SELECT 'shapes';
SELECT k, x, x IN (SELECT DISTINCT e FROM i WHERE i.k = o.k) FROM o ORDER BY k, x;
SELECT k, x, x IN (SELECT e FROM i WHERE i.k = o.k GROUP BY e HAVING count() > 1) FROM o ORDER BY k, x;
SELECT k, x,
    x IN (SELECT toInt32(count()) FROM i WHERE i.k = o.k GROUP BY tuple()) AS got,
    isNotDistinctFrom(got, multiIf(
        k = 1, x IN (SELECT toInt32(count()) FROM i WHERE i.k = 1 GROUP BY tuple()),
        k = 2, x IN (SELECT toInt32(count()) FROM i WHERE i.k = 2 GROUP BY tuple()),
        x IN (SELECT toInt32(count()) FROM i WHERE i.k = 3 GROUP BY tuple())))
FROM o ORDER BY k, x;
SELECT k, x, x IN (SELECT i.e FROM i JOIN j ON i.e = j.e WHERE i.k = o.k) FROM o ORDER BY k, x;
SELECT k, x,
    x IN (SELECT e FROM i WHERE i.k = o.k UNION ALL SELECT j.e FROM j WHERE j.a = o.k * 10) AS got,
    isNotDistinctFrom(got, multiIf(
        k = 1, x IN (SELECT e FROM i WHERE i.k = 1 UNION ALL SELECT j.e FROM j WHERE j.a = 10),
        k = 2, x IN (SELECT e FROM i WHERE i.k = 2 UNION ALL SELECT j.e FROM j WHERE j.a = 20),
        x IN (SELECT e FROM i WHERE i.k = 3 UNION ALL SELECT j.e FROM j WHERE j.a = 30)))
FROM o ORDER BY k, x;
SELECT k, x,
    x IN (SELECT s.e FROM (SELECT k, e FROM i ORDER BY e LIMIT 3) AS s WHERE s.k = o.k) AS got,
    isNotDistinctFrom(got, multiIf(
        k = 1, x IN (SELECT s.e FROM (SELECT k, e FROM i ORDER BY e LIMIT 3) AS s WHERE s.k = 1),
        k = 2, x IN (SELECT s.e FROM (SELECT k, e FROM i ORDER BY e LIMIT 3) AS s WHERE s.k = 2),
        x IN (SELECT s.e FROM (SELECT k, e FROM i ORDER BY e LIMIT 3) AS s WHERE s.k = 3)))
FROM o ORDER BY k, x;

-- `WITH TOTALS` of the subquery is ignored, as by the set of the regular IN.
SELECT 'totals';
SELECT number FROM numbers(4) AS t WHERE number IN (SELECT number FROM numbers(10) WHERE number % 2 = t.number % 2 AND number < 3 GROUP BY number WITH TOTALS) ORDER BY number;
SELECT number FROM numbers(10) AS t GROUP BY number WITH TOTALS HAVING number IN (SELECT number FROM numbers(5) WHERE number = t.number GROUP BY number WITH TOTALS) ORDER BY number;
SELECT number FROM numbers(10) AS t GROUP BY number WITH TOTALS HAVING number NOT IN (SELECT number FROM numbers(5) WHERE number = t.number GROUP BY number WITH TOTALS) ORDER BY number;
SELECT number FROM numbers(4) AS t WHERE number IN (SELECT l.n FROM (SELECT number AS n FROM numbers(3)) AS l JOIN (SELECT number AS n FROM numbers(10) GROUP BY n WITH TOTALS) AS r ON l.n = r.n WHERE l.n = t.number) ORDER BY number;
SELECT number FROM numbers(4) AS t WHERE number IN (SELECT n FROM (SELECT number AS n FROM numbers(2) GROUP BY n WITH TOTALS UNION ALL SELECT 3) WHERE n = t.number) ORDER BY number;
WITH c AS (SELECT number AS n FROM numbers(3) GROUP BY n WITH TOTALS)
SELECT n, n IN (SELECT c2.n FROM c AS c2 WHERE c2.n = c.n) FROM c ORDER BY n;

-- DISTINCT is dropped only where it cannot change the set.
SELECT 'distinct';
SELECT k, x, x IN (SELECT DISTINCT e FROM i WHERE i.k = o.k) FROM o ORDER BY k, x;
SELECT k, x, x IN (SELECT DISTINCT e FROM i WHERE i.k = o.k) FROM o ORDER BY k, x SETTINGS max_rows_in_distinct = 1, distinct_overflow_mode = 'break'; -- { serverError NOT_IMPLEMENTED }
SELECT k, x,
    x IN (SELECT e FROM i WHERE i.k = o.k UNION DISTINCT SELECT j.e FROM j WHERE j.a = o.k * 10) AS got,
    isNotDistinctFrom(got, multiIf(
        k = 1, x IN (SELECT e FROM i WHERE i.k = 1 UNION DISTINCT SELECT j.e FROM j WHERE j.a = 10),
        k = 2, x IN (SELECT e FROM i WHERE i.k = 2 UNION DISTINCT SELECT j.e FROM j WHERE j.a = 20),
        x IN (SELECT e FROM i WHERE i.k = 3 UNION DISTINCT SELECT j.e FROM j WHERE j.a = 30)))
FROM o ORDER BY k, x;
SELECT k, x, x IN (SELECT e FROM i WHERE i.k = o.k UNION DISTINCT SELECT j.e FROM j WHERE j.a = o.k * 10)
FROM o ORDER BY k, x SETTINGS max_rows_in_distinct = 1, distinct_overflow_mode = 'break'; -- { serverError NOT_IMPLEMENTED }

-- A lambda named after a function that the rewrite builds does not replace it.
SELECT 'shadowing';
WITH a -> a AS not, a -> 1 AS exists, (a, b) -> 1 AS accurateCastOrNull, a -> 1 AS reinterpretAsUInt64, a -> 1 AS isNull,
    (a, b, c) -> 7 AS if, (a, b) -> 1 AS and, (a, b) -> 1 AS tupleElement, a -> 1 AS arrayJoin
SELECT k, x,
    x NOT IN (SELECT e FROM i WHERE i.k = o.k),
    toFloat64(x) IN (SELECT e FROM i WHERE i.k = o.k),
    toFloat64(x) IN (SELECT toFloat64(e) FROM i WHERE i.k = o.k),
    xn IN (SELECT e FROM i WHERE i.k = o.k),
    (x, k) IN (SELECT e, i.k FROM i WHERE i.k = o.k),
    CAST((x, k), 'Tuple(Int32, UInt8)') IN (SELECT e, i.k FROM i WHERE i.k = o.k),
    x IN (SELECT groupArray(e) FROM i WHERE i.k = o.k GROUP BY i.k)
FROM o ORDER BY k, x;
-- `<=>`, because a lambda named `equals` replaces the `=` of the query as well.
WITH (a, b) -> 1 AS equals SELECT k, x FROM o WHERE x IN (SELECT e FROM i WHERE i.k <=> o.k) ORDER BY k, x;
-- A lambda named `tuple` gives the left argument its value, here the swapped pair, as in the regular IN.
WITH (a, b) -> arrayZip([b], [a])[1] AS tuple
SELECT k, x,
    (x, k) IN (SELECT e, i.k FROM i WHERE i.k = o.k) AS got,
    (x, k) NOT IN (SELECT e, i.k FROM i WHERE i.k = o.k) AS got_not,
    got = multiIf(k = 1, (x, k) IN (SELECT e, k FROM i WHERE k = 1), k = 2, (x, k) IN (SELECT e, k FROM i WHERE k = 2), (x, k) IN (SELECT e, k FROM i WHERE k = 3)),
    got_not = multiIf(k = 1, (x, k) NOT IN (SELECT e, k FROM i WHERE k = 1), k = 2, (x, k) NOT IN (SELECT e, k FROM i WHERE k = 2), (x, k) NOT IN (SELECT e, k FROM i WHERE k = 3))
FROM o ORDER BY k, x;

-- An ALIAS column key is a column of `o` as well.
SELECT 'alias columns';
SELECT k, x, xz IN (SELECT e FROM i WHERE i.k = o.k) AS got,
    isNotDistinctFrom(got, multiIf(k = 1, xz IN (SELECT e FROM i WHERE i.k = 1), k = 2, xz IN (SELECT e FROM i WHERE i.k = 2), xz IN (SELECT e FROM i WHERE i.k = 3)))
FROM o ORDER BY k, x;

SELECT 'refusals';
SELECT k FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.kn); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE coalesce(o.xn, 0) IN (SELECT e FROM i WHERE i.k = o.k); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE xa IN (SELECT e FROM i WHERE i.k = o.k); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE (xa, k) NOT IN (SELECT e, i.k FROM i WHERE i.k = o.k); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE rand() % 3 IN (SELECT e FROM i WHERE i.k = o.k); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE toInt32(rowNumberInAllBlocks()) IN (SELECT e FROM i WHERE i.k = o.k); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k AND rand() % 2 = 0); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE 'b' IN (SELECT arrayJoin(splitByChar(',', o.tags))); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE arrayJoin([1, 2]) IN (SELECT e FROM i WHERE i.k = o.k); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE unnest([1, 2]) IN (SELECT e FROM i WHERE i.k = o.k) SETTINGS normalize_function_names = 0; -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o GROUP BY k HAVING toInt32(count()) IN (SELECT e FROM i WHERE i.k = o.k); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE NULL IN (SELECT e FROM i WHERE i.k = o.k); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE tuple(x) IN (SELECT tuple(e) FROM i WHERE i.k = o.k); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE arr IN (SELECT [e] FROM i WHERE i.k = o.k); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE toInt8(x) IN (SELECT e FROM values('k UInt8, e Enum8(\'a\' = 1, \'b\' = 2)', (1, 'a'), (2, 'b')) AS ie WHERE ie.k = o.k); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE CAST(x, 'Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3)') IN (SELECT e FROM i WHERE i.k = o.k); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k LIMIT 1); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k OFFSET 1); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k LIMIT 1 BY e); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT DISTINCT ON (f) e FROM i WHERE i.k = o.k); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k) SETTINGS transform_null_in = 1; -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE nullIn(x, (SELECT e FROM i WHERE i.k = o.k)); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k) SETTINGS max_rows_in_set = 1000, set_overflow_mode = 'break'; -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k) SETTINGS max_bytes_in_set = 1000000, set_overflow_mode = 'break'; -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT toInt32(count()) FROM i WHERE i.k = o.k); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT toInt32(count()) FROM i WHERE i.k = o.k GROUP BY tuple()) SETTINGS empty_result_for_aggregation_by_constant_keys_on_empty_set = 0; -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT toInt32(count()) FROM i WHERE i.k = o.k GROUP BY GROUPING SETS (())); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x NOT IN (SELECT toInt32(count()) FROM i WHERE i.k = o.k GROUP BY GROUPING SETS (()) HAVING count() = 0); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k GROUP BY e) SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'break'; -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k SETTINGS additional_table_filters = {'i': 'rand() % 2 = 0'}); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT DISTINCT e FROM i WHERE i.k = o.k SETTINGS max_rows_in_distinct = 1); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k) SETTINGS additional_table_filters = {'i': 'rand() % 2 = 0'}; -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE arrayExists(v -> v IN (SELECT e FROM i WHERE i.k = o.k), arr); -- { serverError NOT_IMPLEMENTED }
SELECT (o.x + 1 AS z) IN (SELECT e FROM values('e Int32, k Int32', (1, 1)) AS i WHERE i.k = z)
FROM values('x Int32, z Int32', (0, 2)) AS o SETTINGS prefer_column_name_to_alias = 1; -- { serverError NOT_IMPLEMENTED }
SELECT k, x * 10 AS y FROM o WHERE y IN (SELECT e * 10 FROM i WHERE i.k = o.k) SETTINGS enable_global_with_statement = 0; -- { serverError NOT_IMPLEMENTED }
WITH v -> v + 1 AS abs SELECT k, x FROM o WHERE abs(x) IN (SELECT e FROM i WHERE i.k = o.k) ORDER BY k, x SETTINGS enable_global_with_statement = 0; -- { serverError NOT_IMPLEMENTED }
SELECT k FROM remote('127.0.0.1', currentDatabase(), o) AS o WHERE x IN (SELECT e FROM i WHERE i.k = o.k); -- { serverError NOT_IMPLEMENTED }

DROP VIEW IF EXISTS v;
CREATE VIEW v AS SELECT k, e FROM i;
SELECT k FROM o WHERE x IN (SELECT e FROM v WHERE v.k = o.k) SETTINGS analyzer_inline_views = 0; -- { serverError NOT_IMPLEMENTED }
DROP VIEW v;

DROP TABLE IF EXISTS o_mt;
CREATE TABLE o_mt (k UInt8, x Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO o_mt VALUES (1, 1), (2, 2);
SELECT k FROM o_mt PREWHERE x IN (SELECT e FROM i WHERE i.k = o_mt.k); -- { serverError NOT_IMPLEMENTED }
DROP TABLE o_mt;

-- A size limit that throws cannot change a result.
SELECT k, x FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k GROUP BY e) ORDER BY k, x SETTINGS max_rows_to_group_by = 100, group_by_overflow_mode = 'throw';
SELECT k, x FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k) ORDER BY k, x SETTINGS max_rows_in_set = 1000, set_overflow_mode = 'throw';
SELECT k, x, x IN (SELECT DISTINCT e FROM i WHERE i.k = o.k) FROM o ORDER BY k, x SETTINGS max_rows_in_distinct = 1000, distinct_overflow_mode = 'throw';
SELECT k, x, x IN (SELECT e FROM i WHERE i.k = o.k UNION DISTINCT SELECT j.e FROM j WHERE j.a = o.k * 10)
FROM o ORDER BY k, x SETTINGS max_rows_in_distinct = 1000, distinct_overflow_mode = 'throw';

DROP TABLE o;
DROP TABLE i;
DROP TABLE j;
