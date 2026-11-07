-- A correlated subquery on the right side of IN / NOT IN is rewritten into EXISTS and returns what the regular IN
-- returns for the same values.

SET allow_correlated_subqueries = 1;

SELECT 'issue';
SELECT r.id
FROM values('id Int32, owner Int32', (1, 10), (2, 20)) r
WHERE r.id IN (SELECT p.id FROM values('id Int32, owner Int32', (1, 10), (2, 20)) p WHERE p.owner = r.owner)
ORDER BY 1;
SELECT r.id
FROM values('id Int32, owner Int32', (1, 10), (2, 20)) r
WHERE r.id IN (SELECT p.id FROM values('id Int32, owner Int32', (1, 10), (2, 20)) p WHERE p.owner = r.owner)
ORDER BY 1
SETTINGS join_use_nulls = 1, group_by_use_nulls = 1, prefer_column_name_to_alias = 1, union_default_mode = 'DISTINCT', cast_keep_nullable = 1;
SELECT r.id
FROM values('id Int32, owner Int32', (1, 10), (2, 20)) r
WHERE r.id IN (SELECT p.id FROM values('id Int32, owner Int32', (1, 10), (2, 20)) p WHERE p.owner = r.owner)
ORDER BY 1
SETTINGS rewrite_in_to_join = 1;

DROP TABLE IF EXISTS r_mt;
DROP TABLE IF EXISTS p_mt;
CREATE TABLE r_mt (id Int32, owner Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE p_mt (id Int32, owner Int32) ENGINE = MergeTree ORDER BY owner;
INSERT INTO r_mt VALUES (1, 10), (2, 20), (3, 30), (4, 10);
INSERT INTO p_mt VALUES (1, 10), (2, 20), (3, 31), (4, 11);
SELECT r.id FROM r_mt r WHERE r.id IN (SELECT p.id FROM p_mt p WHERE p.owner = r.owner) ORDER BY 1;
SELECT r.id FROM r_mt r WHERE r.id NOT IN (SELECT p.id FROM p_mt p WHERE p.owner = r.owner) ORDER BY 1;
DROP TABLE r_mt;
DROP TABLE p_mt;

-- The result and its type must be the ones of the regular IN, which builds a set for each key separately.
SELECT 'parity';
DROP TABLE IF EXISTS o;
DROP TABLE IF EXISTS i;

CREATE TABLE o (k UInt8, x Int32) ENGINE = Memory;
CREATE TABLE i (k UInt8, e Int32) ENGINE = Memory;
INSERT INTO o VALUES (1, 1), (1, 2), (2, 2), (2, 3), (3, 1);
INSERT INTO i VALUES (1, 1), (2, 2), (2, 3);
SELECT 'Int32 / Int32', k, x,
    x IN (SELECT e FROM i WHERE i.k = o.k) AS got,
    x NOT IN (SELECT e FROM i WHERE i.k = o.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, x IN (SELECT e FROM i WHERE k = 1), k = 2, x IN (SELECT e FROM i WHERE k = 2), x IN (SELECT e FROM i WHERE k = 3))),
    isNotDistinctFrom(got_not, multiIf(k = 1, x NOT IN (SELECT e FROM i WHERE k = 1), k = 2, x NOT IN (SELECT e FROM i WHERE k = 2), x NOT IN (SELECT e FROM i WHERE k = 3))),
    toTypeName(got) = toTypeName(x IN (SELECT e FROM i WHERE k = 1)) AND toTypeName(got_not) = toTypeName(x NOT IN (SELECT e FROM i WHERE k = 1))
FROM o ORDER BY k, x;
DROP TABLE o;
DROP TABLE i;

CREATE TABLE o (k UInt8, x Nullable(Int32)) ENGINE = Memory;
CREATE TABLE i (k UInt8, e Int64) ENGINE = Memory;
INSERT INTO o VALUES (1, 1), (1, NULL), (2, 2), (3, NULL);
INSERT INTO i VALUES (1, 1), (2, 3);
SELECT 'Nullable(Int32) / Int64', k, x,
    x IN (SELECT e FROM i WHERE i.k = o.k) AS got,
    x NOT IN (SELECT e FROM i WHERE i.k = o.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, x IN (SELECT e FROM i WHERE k = 1), k = 2, x IN (SELECT e FROM i WHERE k = 2), x IN (SELECT e FROM i WHERE k = 3))),
    isNotDistinctFrom(got_not, multiIf(k = 1, x NOT IN (SELECT e FROM i WHERE k = 1), k = 2, x NOT IN (SELECT e FROM i WHERE k = 2), x NOT IN (SELECT e FROM i WHERE k = 3))),
    toTypeName(got) = toTypeName(x IN (SELECT e FROM i WHERE k = 1)) AND toTypeName(got_not) = toTypeName(x NOT IN (SELECT e FROM i WHERE k = 1))
FROM o ORDER BY k, x;
DROP TABLE o;
DROP TABLE i;

CREATE TABLE o (k UInt8, x Int16) ENGINE = Memory;
CREATE TABLE i (k UInt8, e Nullable(Int32)) ENGINE = Memory;
INSERT INTO o VALUES (1, 1), (1, 3), (2, 3), (3, 1);
INSERT INTO i VALUES (1, 1), (1, NULL), (2, NULL);
SELECT 'Int16 / Nullable(Int32)', k, x,
    x IN (SELECT e FROM i WHERE i.k = o.k) AS got,
    x NOT IN (SELECT e FROM i WHERE i.k = o.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, x IN (SELECT e FROM i WHERE k = 1), k = 2, x IN (SELECT e FROM i WHERE k = 2), x IN (SELECT e FROM i WHERE k = 3))),
    isNotDistinctFrom(got_not, multiIf(k = 1, x NOT IN (SELECT e FROM i WHERE k = 1), k = 2, x NOT IN (SELECT e FROM i WHERE k = 2), x NOT IN (SELECT e FROM i WHERE k = 3))),
    toTypeName(got) = toTypeName(x IN (SELECT e FROM i WHERE k = 1)) AND toTypeName(got_not) = toTypeName(x NOT IN (SELECT e FROM i WHERE k = 1))
FROM o ORDER BY k, x;
DROP TABLE o;
DROP TABLE i;

CREATE TABLE o (k UInt8, x String) ENGINE = Memory;
CREATE TABLE i (k UInt8, e UInt8) ENGINE = Memory;
INSERT INTO o VALUES (1, '1'), (1, 'x'), (1, '256'), (2, '2'), (3, '1');
INSERT INTO i VALUES (1, 1), (1, 0), (2, 2);
SELECT 'String / UInt8', k, x,
    x IN (SELECT e FROM i WHERE i.k = o.k) AS got,
    x NOT IN (SELECT e FROM i WHERE i.k = o.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, x IN (SELECT e FROM i WHERE k = 1), k = 2, x IN (SELECT e FROM i WHERE k = 2), x IN (SELECT e FROM i WHERE k = 3))),
    isNotDistinctFrom(got_not, multiIf(k = 1, x NOT IN (SELECT e FROM i WHERE k = 1), k = 2, x NOT IN (SELECT e FROM i WHERE k = 2), x NOT IN (SELECT e FROM i WHERE k = 3))),
    toTypeName(got) = toTypeName(x IN (SELECT e FROM i WHERE k = 1)) AND toTypeName(got_not) = toTypeName(x NOT IN (SELECT e FROM i WHERE k = 1))
FROM o ORDER BY k, x;
DROP TABLE o;
DROP TABLE i;

CREATE TABLE o (k UInt8, x UInt8) ENGINE = Memory;
CREATE TABLE i (k UInt8, e String) ENGINE = Memory;
INSERT INTO o VALUES (1, 1), (1, 2), (2, 7), (3, 1);
INSERT INTO i VALUES (1, '1'), (1, 'x'), (2, '07');
SELECT 'UInt8 / String', k, x,
    x IN (SELECT e FROM i WHERE i.k = o.k) AS got,
    x NOT IN (SELECT e FROM i WHERE i.k = o.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, x IN (SELECT e FROM i WHERE k = 1), k = 2, x IN (SELECT e FROM i WHERE k = 2), x IN (SELECT e FROM i WHERE k = 3))),
    isNotDistinctFrom(got_not, multiIf(k = 1, x NOT IN (SELECT e FROM i WHERE k = 1), k = 2, x NOT IN (SELECT e FROM i WHERE k = 2), x NOT IN (SELECT e FROM i WHERE k = 3))),
    toTypeName(got) = toTypeName(x IN (SELECT e FROM i WHERE k = 1)) AND toTypeName(got_not) = toTypeName(x NOT IN (SELECT e FROM i WHERE k = 1))
FROM o ORDER BY k, x;
DROP TABLE o;
DROP TABLE i;

CREATE TABLE o (k UInt8, x Decimal32(2)) ENGINE = Memory;
CREATE TABLE i (k UInt8, e Int32) ENGINE = Memory;
INSERT INTO o VALUES (1, 1.50), (1, 2), (2, 1), (3, 1);
INSERT INTO i VALUES (1, 1), (2, 1);
SELECT 'Decimal32(2) / Int32', k, x,
    x IN (SELECT e FROM i WHERE i.k = o.k) AS got,
    x NOT IN (SELECT e FROM i WHERE i.k = o.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, x IN (SELECT e FROM i WHERE k = 1), k = 2, x IN (SELECT e FROM i WHERE k = 2), x IN (SELECT e FROM i WHERE k = 3))),
    isNotDistinctFrom(got_not, multiIf(k = 1, x NOT IN (SELECT e FROM i WHERE k = 1), k = 2, x NOT IN (SELECT e FROM i WHERE k = 2), x NOT IN (SELECT e FROM i WHERE k = 3))),
    toTypeName(got) = toTypeName(x IN (SELECT e FROM i WHERE k = 1)) AND toTypeName(got_not) = toTypeName(x NOT IN (SELECT e FROM i WHERE k = 1))
FROM o ORDER BY k, x;
DROP TABLE o;
DROP TABLE i;

CREATE TABLE o (k UInt8, x UInt8) ENGINE = Memory;
CREATE TABLE i (k UInt8, e Bool) ENGINE = Memory;
INSERT INTO o VALUES (1, 2), (1, 1), (1, 0), (2, 0), (3, 1);
INSERT INTO i VALUES (1, true), (2, false);
SELECT 'UInt8 / Bool', k, x,
    x IN (SELECT e FROM i WHERE i.k = o.k) AS got,
    x NOT IN (SELECT e FROM i WHERE i.k = o.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, x IN (SELECT e FROM i WHERE k = 1), k = 2, x IN (SELECT e FROM i WHERE k = 2), x IN (SELECT e FROM i WHERE k = 3))),
    isNotDistinctFrom(got_not, multiIf(k = 1, x NOT IN (SELECT e FROM i WHERE k = 1), k = 2, x NOT IN (SELECT e FROM i WHERE k = 2), x NOT IN (SELECT e FROM i WHERE k = 3))),
    toTypeName(got) = toTypeName(x IN (SELECT e FROM i WHERE k = 1)) AND toTypeName(got_not) = toTypeName(x NOT IN (SELECT e FROM i WHERE k = 1))
FROM o ORDER BY k, x;
DROP TABLE o;
DROP TABLE i;

CREATE TABLE o (k UInt8, x LowCardinality(Nullable(String))) ENGINE = Memory;
CREATE TABLE i (k UInt8, e String) ENGINE = Memory;
INSERT INTO o VALUES (1, 'a'), (1, NULL), (1, 'b'), (2, 'b'), (3, 'a');
INSERT INTO i VALUES (1, 'a'), (2, 'b');
SELECT 'LowCardinality(Nullable(String)) / String', k, x,
    x IN (SELECT e FROM i WHERE i.k = o.k) AS got,
    x NOT IN (SELECT e FROM i WHERE i.k = o.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, x IN (SELECT e FROM i WHERE k = 1), k = 2, x IN (SELECT e FROM i WHERE k = 2), x IN (SELECT e FROM i WHERE k = 3))),
    isNotDistinctFrom(got_not, multiIf(k = 1, x NOT IN (SELECT e FROM i WHERE k = 1), k = 2, x NOT IN (SELECT e FROM i WHERE k = 2), x NOT IN (SELECT e FROM i WHERE k = 3))),
    toTypeName(got) = toTypeName(x IN (SELECT e FROM i WHERE k = 1)) AND toTypeName(got_not) = toTypeName(x NOT IN (SELECT e FROM i WHERE k = 1))
FROM o ORDER BY k, x;
DROP TABLE o;
DROP TABLE i;

CREATE TABLE o (k UInt8, x DateTime64(3, 'UTC')) ENGINE = Memory;
CREATE TABLE i (k UInt8, e DateTime('UTC')) ENGINE = Memory;
INSERT INTO o VALUES (1, '2020-01-01 10:00:00.000'), (1, '2020-01-01 10:00:00.500'), (2, '2020-01-01 10:00:01.000'), (3, '2020-01-01 10:00:00.000');
INSERT INTO i VALUES (1, '2020-01-01 10:00:00'), (2, '2020-01-01 10:00:01');
SELECT 'DateTime64(3) / DateTime', k, x,
    x IN (SELECT e FROM i WHERE i.k = o.k) AS got,
    x NOT IN (SELECT e FROM i WHERE i.k = o.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, x IN (SELECT e FROM i WHERE k = 1), k = 2, x IN (SELECT e FROM i WHERE k = 2), x IN (SELECT e FROM i WHERE k = 3))),
    isNotDistinctFrom(got_not, multiIf(k = 1, x NOT IN (SELECT e FROM i WHERE k = 1), k = 2, x NOT IN (SELECT e FROM i WHERE k = 2), x NOT IN (SELECT e FROM i WHERE k = 3))),
    toTypeName(got) = toTypeName(x IN (SELECT e FROM i WHERE k = 1)) AND toTypeName(got_not) = toTypeName(x NOT IN (SELECT e FROM i WHERE k = 1))
FROM o ORDER BY k, x;
DROP TABLE o;
DROP TABLE i;

CREATE TABLE o (k UInt8, x Date) ENGINE = Memory;
CREATE TABLE i (k UInt8, e String) ENGINE = Memory;
INSERT INTO o VALUES (1, '2020-01-01'), (1, '2020-01-02'), (2, '2020-01-02'), (3, '2020-01-01');
INSERT INTO i VALUES (1, '2020-01-01'), (1, 'not a date'), (2, '2020-01-02');
SELECT 'Date / String', k, x,
    x IN (SELECT e FROM i WHERE i.k = o.k) AS got,
    x NOT IN (SELECT e FROM i WHERE i.k = o.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, x IN (SELECT e FROM i WHERE k = 1), k = 2, x IN (SELECT e FROM i WHERE k = 2), x IN (SELECT e FROM i WHERE k = 3))),
    isNotDistinctFrom(got_not, multiIf(k = 1, x NOT IN (SELECT e FROM i WHERE k = 1), k = 2, x NOT IN (SELECT e FROM i WHERE k = 2), x NOT IN (SELECT e FROM i WHERE k = 3))),
    toTypeName(got) = toTypeName(x IN (SELECT e FROM i WHERE k = 1)) AND toTypeName(got_not) = toTypeName(x NOT IN (SELECT e FROM i WHERE k = 1))
FROM o ORDER BY k, x;
DROP TABLE o;
DROP TABLE i;

CREATE TABLE o (k UInt8, x Float64) ENGINE = Memory;
CREATE TABLE i (k UInt8, e Float64) ENGINE = Memory;
INSERT INTO o VALUES (1, nan), (1, -0.), (1, 0.), (1, 0.1), (2, 0.), (2, -0.), (3, 0.1);
INSERT INTO i VALUES (1, nan), (1, 0.1), (1, 0.), (2, -0.);
SELECT 'Float64 / Float64', k, x,
    x IN (SELECT e FROM i WHERE i.k = o.k) AS got,
    x NOT IN (SELECT e FROM i WHERE i.k = o.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, x IN (SELECT e FROM i WHERE k = 1), k = 2, x IN (SELECT e FROM i WHERE k = 2), x IN (SELECT e FROM i WHERE k = 3))),
    isNotDistinctFrom(got_not, multiIf(k = 1, x NOT IN (SELECT e FROM i WHERE k = 1), k = 2, x NOT IN (SELECT e FROM i WHERE k = 2), x NOT IN (SELECT e FROM i WHERE k = 3))),
    toTypeName(got) = toTypeName(x IN (SELECT e FROM i WHERE k = 1)) AND toTypeName(got_not) = toTypeName(x NOT IN (SELECT e FROM i WHERE k = 1))
FROM o ORDER BY k, toString(x);
DROP TABLE o;
DROP TABLE i;

CREATE TABLE o (k UInt8, x Float32) ENGINE = Memory;
CREATE TABLE i (k UInt8, e Float64) ENGINE = Memory;
INSERT INTO o VALUES (1, nan), (1, -0.), (1, 0.), (1, 0.1), (1, 0.5), (2, 0.), (3, 0.5);
INSERT INTO i VALUES (1, nan), (1, 0.1), (1, 0.5), (2, -0.);
SELECT 'Float32 / Float64', k, x,
    x IN (SELECT e FROM i WHERE i.k = o.k) AS got,
    x NOT IN (SELECT e FROM i WHERE i.k = o.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, x IN (SELECT e FROM i WHERE k = 1), k = 2, x IN (SELECT e FROM i WHERE k = 2), x IN (SELECT e FROM i WHERE k = 3))),
    isNotDistinctFrom(got_not, multiIf(k = 1, x NOT IN (SELECT e FROM i WHERE k = 1), k = 2, x NOT IN (SELECT e FROM i WHERE k = 2), x NOT IN (SELECT e FROM i WHERE k = 3))),
    toTypeName(got) = toTypeName(x IN (SELECT e FROM i WHERE k = 1)) AND toTypeName(got_not) = toTypeName(x NOT IN (SELECT e FROM i WHERE k = 1))
FROM o ORDER BY k, toString(x);
DROP TABLE o;
DROP TABLE i;

CREATE TABLE o (k UInt8, x Float64) ENGINE = Memory;
CREATE TABLE i (k UInt8, e Int32) ENGINE = Memory;
INSERT INTO o VALUES (1, nan), (1, -0.), (1, 1.5), (1, 1.), (2, 0.), (3, 1.);
INSERT INTO i VALUES (1, 0), (1, 1), (2, 0);
SELECT 'Float64 / Int32', k, x,
    x IN (SELECT e FROM i WHERE i.k = o.k) AS got,
    x NOT IN (SELECT e FROM i WHERE i.k = o.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, x IN (SELECT e FROM i WHERE k = 1), k = 2, x IN (SELECT e FROM i WHERE k = 2), x IN (SELECT e FROM i WHERE k = 3))),
    isNotDistinctFrom(got_not, multiIf(k = 1, x NOT IN (SELECT e FROM i WHERE k = 1), k = 2, x NOT IN (SELECT e FROM i WHERE k = 2), x NOT IN (SELECT e FROM i WHERE k = 3))),
    toTypeName(got) = toTypeName(x IN (SELECT e FROM i WHERE k = 1)) AND toTypeName(got_not) = toTypeName(x NOT IN (SELECT e FROM i WHERE k = 1))
FROM o ORDER BY k, toString(x);
DROP TABLE o;
DROP TABLE i;

CREATE TABLE o (k UInt8, a Nullable(Int32), b String) ENGINE = Memory;
CREATE TABLE i (k UInt8, c Int64, d String) ENGINE = Memory;
INSERT INTO o VALUES (1, 1, 'a'), (1, 1, 'b'), (1, NULL, 'a'), (2, 2, 'b'), (3, 1, 'a');
INSERT INTO i VALUES (1, 1, 'a'), (2, 2, 'b');
SELECT 'Tuple(Nullable(Int32), String) / Int64, String', k, a, b,
    (a, b) IN (SELECT c, d FROM i WHERE i.k = o.k) AS got,
    (a, b) NOT IN (SELECT c, d FROM i WHERE i.k = o.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, (a, b) IN (SELECT c, d FROM i WHERE k = 1), k = 2, (a, b) IN (SELECT c, d FROM i WHERE k = 2), (a, b) IN (SELECT c, d FROM i WHERE k = 3))),
    isNotDistinctFrom(got_not, multiIf(k = 1, (a, b) NOT IN (SELECT c, d FROM i WHERE k = 1), k = 2, (a, b) NOT IN (SELECT c, d FROM i WHERE k = 2), (a, b) NOT IN (SELECT c, d FROM i WHERE k = 3))),
    toTypeName(got) = toTypeName((a, b) IN (SELECT c, d FROM i WHERE k = 1)) AND toTypeName(got_not) = toTypeName((a, b) NOT IN (SELECT c, d FROM i WHERE k = 1))
FROM o ORDER BY k, a, b;
DROP TABLE o;
DROP TABLE i;

-- An array subquery on the right of IN is the set of its elements.
CREATE TABLE o (k UInt8, x Int32) ENGINE = Memory;
CREATE TABLE i (k UInt8, e Int32) ENGINE = Memory;
INSERT INTO o VALUES (1, 1), (1, 2), (2, 3), (3, 1);
INSERT INTO i VALUES (1, 1), (1, 5), (2, 3);
SELECT 'Int32 / Array(Int32)', k, x,
    x IN (SELECT groupArray(e) FROM i WHERE i.k = o.k GROUP BY i.k) AS got,
    x NOT IN (SELECT groupArray(e) FROM i WHERE i.k = o.k GROUP BY i.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, x IN (SELECT groupArray(e) FROM i WHERE k = 1 GROUP BY k), k = 2, x IN (SELECT groupArray(e) FROM i WHERE k = 2 GROUP BY k), x IN (SELECT groupArray(e) FROM i WHERE k = 3 GROUP BY k))),
    isNotDistinctFrom(got_not, multiIf(k = 1, x NOT IN (SELECT groupArray(e) FROM i WHERE k = 1 GROUP BY k), k = 2, x NOT IN (SELECT groupArray(e) FROM i WHERE k = 2 GROUP BY k), x NOT IN (SELECT groupArray(e) FROM i WHERE k = 3 GROUP BY k)))
FROM o ORDER BY k, x;
DROP TABLE o;
DROP TABLE i;

DROP TABLE IF EXISTS o;
DROP TABLE IF EXISTS i;
DROP TABLE IF EXISTS j;
CREATE TABLE o (k UInt8, x Int32, tags String, xn Nullable(Int32), kn Nullable(UInt8), arr Array(Int32)) ENGINE = Memory;
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
SELECT k, x, 1 IN (SELECT e FROM i WHERE i.k = o.k) FROM o ORDER BY k, x;
SELECT k, x, (x, k) IN (SELECT e, i.k FROM i WHERE i.k = o.k) FROM o ORDER BY k, x;
-- Inside a correlated EXISTS, correlated with the subquery of EXISTS.
SELECT k, x FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.k = o.k AND i.e IN (SELECT j.e FROM j WHERE j.a = i.e * 10)) ORDER BY k, x;

SELECT 'shapes';
SELECT k, x, x IN (SELECT DISTINCT e FROM i WHERE i.k = o.k) FROM o ORDER BY k, x;
SELECT k, x, x IN (SELECT e FROM i WHERE i.k = o.k GROUP BY e HAVING count() > 1) FROM o ORDER BY k, x;
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

SELECT 'refusals';
SELECT k FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.kn); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE coalesce(o.xn, 0) IN (SELECT e FROM i WHERE i.k = o.k); -- { serverError NOT_IMPLEMENTED }
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
SELECT k FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k LIMIT 1); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k OFFSET 1); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k LIMIT 1 BY e); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT DISTINCT ON (f) e FROM i WHERE i.k = o.k); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k) SETTINGS transform_null_in = 1; -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE nullIn(x, (SELECT e FROM i WHERE i.k = o.k)); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k) SETTINGS max_rows_in_set = 1000; -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k) SETTINGS max_bytes_in_set = 1000000; -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT toInt32(count()) FROM i WHERE i.k = o.k); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k GROUP BY e) SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'break'; -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k SETTINGS additional_table_filters = {'i': 'rand() % 2 = 0'}); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT DISTINCT e FROM i WHERE i.k = o.k SETTINGS max_rows_in_distinct = 1); -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k) SETTINGS additional_table_filters = {'i': 'rand() % 2 = 0'}; -- { serverError NOT_IMPLEMENTED }
SELECT k FROM o WHERE arrayExists(v -> v IN (SELECT e FROM i WHERE i.k = o.k), arr); -- { serverError NOT_IMPLEMENTED }
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

-- A keyed GROUP BY limit that throws cannot change a result.
SELECT k, x FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k GROUP BY e) ORDER BY k, x SETTINGS max_rows_to_group_by = 100, group_by_overflow_mode = 'throw';

DROP TABLE o;
DROP TABLE i;
DROP TABLE j;
