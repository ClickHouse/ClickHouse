-- A bare `INTERPOLATE` (without a column list) inside a subquery or CTE was dropped,
-- so the rows added by `WITH FILL` got default values instead of the interpolated ones.
-- https://github.com/ClickHouse/ClickHouse/issues/86151

SET enable_analyzer = 1;

SELECT 'top level';
SELECT * FROM (SELECT toDate('2025-01-01') AS d, 99 AS v)
ORDER BY d WITH FILL TO toDate('2025-01-04') INTERPOLATE;

SELECT 'subquery';
SELECT * FROM
(
    SELECT * FROM (SELECT toDate('2025-01-01') AS d, 99 AS v)
    ORDER BY d WITH FILL TO toDate('2025-01-04') INTERPOLATE
);

SELECT 'CTE';
WITH t AS
(
    SELECT * FROM (SELECT toDate('2025-01-01') AS d, 99 AS v)
    ORDER BY d WITH FILL TO toDate('2025-01-04') INTERPOLATE
)
SELECT * FROM t;

SELECT 'outer query reads one of two interpolated columns';
SELECT d, close FROM
(
    SELECT * FROM (SELECT toDate('2025-01-01') AS d, 99 AS close, 7 AS volume)
    ORDER BY d WITH FILL TO toDate('2025-01-04') INTERPOLATE
);

SELECT 'outer query reads only the sort key';
SELECT d FROM
(
    SELECT * FROM (SELECT toDate('2025-01-01') AS d, 99 AS v)
    ORDER BY d WITH FILL TO toDate('2025-01-04') INTERPOLATE
);
