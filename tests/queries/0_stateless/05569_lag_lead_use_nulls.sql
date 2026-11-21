-- lag_lead_use_nulls: lag and lead without a default return NULL for a row outside the partition.

SELECT '-- Disabled: the default value of the type';
SELECT i, lag(i) OVER w AS l, toTypeName(l), lead(i) OVER w AS n, toTypeName(n)
FROM VALUES('i Int64', 1, 2, 3) WINDOW w AS (ORDER BY i) ORDER BY i
SETTINGS lag_lead_use_nulls = 0;

SET lag_lead_use_nulls = 1;

SELECT '-- Enabled: NULL, and the result is Nullable';
SELECT i, lag(i) OVER w AS l, toTypeName(l), lead(i) OVER w AS n, toTypeName(n)
FROM VALUES('i Int64', 1, 2, 3) WINDOW w AS (ORDER BY i) ORDER BY i;

SELECT '-- With an offset, in any letter case, per partition';
SELECT number, lag(number, 2) OVER w AS l, LEAD(number, 2) OVER w AS n
FROM numbers(6) WINDOW w AS (PARTITION BY number % 2 ORDER BY number) ORDER BY number;

SELECT '-- An explicit default, lagInFrame and leadInFrame are not affected';
SELECT number,
    lag(number, 1, 100) OVER w AS l, toTypeName(l),
    lagInFrame(number) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lf, toTypeName(lf),
    leadInFrame(number) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS nf, toTypeName(nf)
FROM numbers(3) WINDOW w AS (ORDER BY number) ORDER BY number;

SELECT '-- Argument types: a type that cannot be inside Nullable keeps the default value of the type';
SELECT number,
    lag(toString(number)) OVER w AS s, toTypeName(s),
    lag(toDate('2024-01-01') + number) OVER w AS d, toTypeName(d),
    lag(toLowCardinality(toString(number))) OVER w AS lc, toTypeName(lc),
    lag(toNullable(number)) OVER w AS nul, toTypeName(nul),
    lag(42) OVER w AS c, toTypeName(c),
    lag((number, toString(number))) OVER w AS tup, toTypeName(tup),
    lag([number]) OVER w AS arr, toTypeName(arr),
    lag(map(number, number)) OVER w AS m, toTypeName(m)
FROM numbers(3) WINDOW w AS (ORDER BY number) ORDER BY number;

SELECT '-- The same as lag and lead over a Nullable argument, across blocks and partitions';
SELECT countIf(NOT isNotDistinctFrom(a, b)), countIf(a IS NULL), countIf(NOT isNotDistinctFrom(c, d)), countIf(c IS NULL), count()
FROM
(
    SELECT
        lag(number, 3) OVER w AS a, lag(toNullable(number), 3) OVER w AS b,
        lead(number, 3) OVER w AS c, lead(toNullable(number), 3) OVER w AS d
    FROM numbers(1000)
    WINDOW w AS (PARTITION BY number % 7 ORDER BY number)
    SETTINGS max_block_size = 64
);

SELECT '-- Over a grouping key and an aggregate';
SELECT number % 3 AS k, lag(k) OVER w AS lk, lag(sum(number)) OVER w AS ls, toTypeName(ls)
FROM numbers(10) GROUP BY k WINDOW w AS (ORDER BY k) ORDER BY k;

SELECT '-- With group_by_use_nulls: the same result as lag and lead over toNullable with the setting disabled';
WITH 'x' AS v SELECT lag(v) OVER (ORDER BY v) AS l, lead(v) OVER (ORDER BY v) AS n, toTypeName(l)
GROUP BY ROLLUP(v) ORDER BY ALL NULLS FIRST SETTINGS group_by_use_nulls = 1;
WITH 'x' AS v SELECT lag(toNullable(v)) OVER (ORDER BY v) AS l, lead(toNullable(v)) OVER (ORDER BY v) AS n, toTypeName(l)
GROUP BY ROLLUP(v) ORDER BY ALL NULLS FIRST SETTINGS group_by_use_nulls = 1, lag_lead_use_nulls = 0;
SELECT number AS k, lag(k) OVER (ORDER BY k NULLS FIRST) AS l, toTypeName(l)
FROM numbers(2) GROUP BY ROLLUP(k) ORDER BY ALL NULLS FIRST SETTINGS group_by_use_nulls = 1;
SELECT number AS k, lag(toNullable(k)) OVER (ORDER BY k NULLS FIRST) AS l, toTypeName(l)
FROM numbers(2) GROUP BY ROLLUP(k) ORDER BY ALL NULLS FIRST SETTINGS group_by_use_nulls = 1, lag_lead_use_nulls = 0;

SELECT '-- With GROUP BY ALL';
SELECT number AS k, lag(k) OVER (ORDER BY k) AS l, toTypeName(l)
FROM numbers(2) GROUP BY ALL ORDER BY k;

SELECT '-- Evaluated on the shards';
SELECT l FROM remote('127.0.0.{1,2}', view(SELECT lag(number) OVER (ORDER BY number) AS l FROM numbers(3)))
ORDER BY l NULLS FIRST SETTINGS serialize_query_plan = 0;
