-- The first row of every partition has no previous row, so its derivative is 0 wherever the partition starts inside a block,
-- and the other rows see only their own partition.

SELECT p, n, nonNegativeDerivative(m, ts) OVER (PARTITION BY p ORDER BY n) AS d
FROM
(
    SELECT intDiv(number, 4) AS p, number % 4 AS n, toFloat64(p * 100 + n * 10) AS m, toDateTime('2024-01-01 00:00:00', 'UTC') + number AS ts
    FROM numbers(12)
)
ORDER BY p, n
SETTINGS max_block_size = 1;

SELECT p, n, nonNegativeDerivative(m, ts) OVER (PARTITION BY p ORDER BY n) AS d
FROM
(
    SELECT intDiv(number, 4) AS p, number % 4 AS n, toFloat64(p * 100 + n * 10) AS m, toDateTime('2024-01-01 00:00:00', 'UTC') + number AS ts
    FROM numbers(12)
)
ORDER BY p, n
SETTINGS max_block_size = 3;

SELECT p, n, nonNegativeDerivative(m, ts) OVER (PARTITION BY p ORDER BY n) AS d
FROM
(
    SELECT intDiv(number, 4) AS p, number % 4 AS n, toFloat64(p * 100 + n * 10) AS m, toDateTime('2024-01-01 00:00:00', 'UTC') + number AS ts
    FROM numbers(12)
)
ORDER BY p, n;
