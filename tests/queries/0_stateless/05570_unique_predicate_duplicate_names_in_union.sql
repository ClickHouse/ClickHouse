-- The `UNIQUE` predicate must read the columns of its subquery by position.
-- A `UNION` whose first query has several columns with the same name must not make
-- the NULL filter or the duplicate detection read the first of these columns several times.

SELECT 'Only the first row has no NULL';
SELECT UNIQUE(SELECT toNullable(1) AS x, toNullable(1) AS x UNION ALL SELECT NULL, 2 UNION ALL SELECT NULL, 2);

SELECT 'The rows differ only in the second column';
SELECT UNIQUE(SELECT 1 AS x, 1 AS x UNION ALL SELECT 1, 2);

SELECT 'Duplicate rows';
SELECT UNIQUE(SELECT 1 AS x, 1 AS x UNION ALL SELECT 1, 1);

SELECT 'Nested union';
SELECT UNIQUE(SELECT 1 AS x, 1 AS x UNION ALL (SELECT 1, 2 UNION ALL SELECT 1, 3));
SELECT UNIQUE(SELECT 1 AS x, 1 AS x UNION ALL (SELECT 1, 2 UNION ALL SELECT 1, 2));
