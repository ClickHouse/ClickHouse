-- With `group_by_use_nulls`, the matchers inside the expressions of transformers that are never executed
-- (a `REPLACE` of a column removed by a preceding `EXCEPT`, an `APPLY` of a matcher that produces no columns)
-- do not rewrite `GROUP BY`, so they do not prevent deferring the transformers of the other matchers:
-- `t.* APPLY (x -> getSubcolumn(x, 'null'))` is valid only for the `Nullable` key.

SET enable_analyzer = 1;
SET group_by_use_nulls = 1;

SELECT 'replace after except';
SELECT t.c, t.* APPLY (x -> getSubcolumn(x, 'null')),
       * EXCEPT(c) REPLACE (identity(COLUMNS('c') REPLACE (1 AS c)) AS c)
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY c WITH ROLLUP
ORDER BY t.c NULLS LAST;

SELECT 'unmatched matcher';
SELECT t.c, t.* APPLY (x -> getSubcolumn(x, 'null')),
       COLUMNS('^z$') REPLACE (identity(COLUMNS('c') REPLACE (1 AS c)) AS z)
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY c WITH ROLLUP
ORDER BY t.c NULLS LAST;

SELECT 'apply after except';
SELECT t.c, t.* APPLY (x -> getSubcolumn(x, 'null')),
       * EXCEPT(c) APPLY (x -> identity(COLUMNS('c') REPLACE (1 AS c)))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY c WITH ROLLUP
ORDER BY t.c NULLS LAST;

SELECT 'qualified matcher';
SELECT t.c, t.* APPLY (x -> getSubcolumn(x, 'null')),
       t.* EXCEPT(c) REPLACE (identity(COLUMNS('c') REPLACE (1 AS c)) AS c)
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY c WITH ROLLUP
ORDER BY t.c NULLS LAST;

-- An executed replacement still rewrites `GROUP BY`.
SELECT 'executed replacement';
SELECT * REPLACE (identity(COLUMNS('c') REPLACE (1 AS c)) AS c), count()
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY c WITH ROLLUP
ORDER BY ALL;
