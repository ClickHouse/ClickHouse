-- Random settings limits: optimize_distinct_in_order=(1, 1)
-- The row filled from `FROM` of the first key is in the group of the next original row:
-- a later key without `FROM` takes the value of that row, not the column default.

SELECT * FROM values('a Int64, b Int64, c Int64', (0, 5, 5), (1, -3, 1))
ORDER BY a WITH FILL FROM 1, b WITH FILL, c WITH FILL FROM 0;

SELECT * FROM values('a Int64, b Int64, c Int64', (0, 5, 5), (1, 3, 1))
ORDER BY a WITH FILL FROM 1, b DESC WITH FILL, c WITH FILL FROM 0;

SELECT * FROM values('a Int64, b Int64, c Int64', (0, 5, 5), (1, -3, 3))
ORDER BY a WITH FILL FROM 1, b WITH FILL, c WITH FILL FROM 0;

SELECT * FROM values('a Int64, b Int64, c Int64', (0, 5, 5), (1, 0, -1), (1, 1, -1), (1, 2, 2))
ORDER BY a WITH FILL FROM 1, b WITH FILL, c WITH FILL FROM 0;

SELECT DISTINCT a, b FROM
(
    SELECT * FROM values('a Int64, b Int64, c Int64', (0, 5, 5), (1, 0, -1), (1, 1, -1), (1, 2, 2))
    ORDER BY a WITH FILL FROM 1, b WITH FILL, c WITH FILL FROM 0
);

SELECT * FROM values('a Int64, b Int64, c Int64', (0, 5, 5), (1, -3, -1))
ORDER BY a WITH FILL FROM 1 TO 3, b DESC WITH FILL, c WITH FILL FROM 0;

SELECT * FROM values('a Int64, b Int64, c Int64, d Int64', (0, 5, 5, 5), (1, -3, -4, 2))
ORDER BY a WITH FILL FROM 1, b WITH FILL, c WITH FILL, d WITH FILL FROM 0;

SELECT * FROM values('a Int64, b Int64, c Int64, d Int64, e Int64', (0, 5, 5, 5, 5), (1, -3, 2, -4, 2))
ORDER BY a WITH FILL FROM 1, b WITH FILL, c WITH FILL FROM 0, d WITH FILL, e WITH FILL FROM 0;

SELECT * FROM values('a Int64, b Int64, c Int64', (0, 5, 5), (1, -3, -1), (1, -2, 2))
ORDER BY a WITH FILL FROM 1, b WITH FILL STALENESS 3, c WITH FILL FROM 0;

SELECT * FROM values('p Int64, a Int64, b Int64, c Int64', (8, 0, 5, 5), (8, 1, -3, 1), (9, 0, 7, 7), (9, 1, -6, 2))
ORDER BY p, a WITH FILL FROM 1, b WITH FILL, c WITH FILL FROM 0
SETTINGS use_with_fill_by_sorting_prefix = 1;

SELECT a, b, c FROM
(
    SELECT [0, 1, 1, 1][number + 1] AS a, [5, 0, 1, 2][number + 1] AS b, [5, -1, -1, 2][number + 1] AS c FROM numbers(4)
)
ORDER BY a WITH FILL FROM 1, b WITH FILL, c WITH FILL FROM 0
SETTINGS max_block_size = 1;

SELECT * FROM values('a Int64, b Nullable(Int64), c Int64', (0, 5, 5), (1, -3, 2))
ORDER BY a WITH FILL FROM 1, b WITH FILL, c WITH FILL FROM 0;

SELECT * FROM values('a Int64, b Date, c Int64', (0, '2020-01-05', 5), (1, '2020-01-02', 2))
ORDER BY a WITH FILL FROM 1, b WITH FILL, c WITH FILL FROM 0;

SELECT * FROM values('a Int64, b Int64, c Int64', (0, 5, 5), (1, 3, 1))
ORDER BY a WITH FILL FROM 1, b WITH FILL TO 0, c WITH FILL FROM 0;

SELECT * FROM values('a Int64, b Int64, c Int64', (0, 0, 5), (1, 10, 1))
ORDER BY a WITH FILL FROM 1, b WITH FILL STALENESS 3, c WITH FILL FROM 0;

SELECT * FROM values('a Int64, b Int64', (0, 5), (1, -3), (1, 2))
ORDER BY a WITH FILL FROM 1, b WITH FILL;

SELECT * FROM values('a Int64, b Int64, c Int64, d Int64', (1, -3, 7, 1), (1, 10, -2, 2))
ORDER BY a WITH FILL, b WITH FILL FROM 10, c WITH FILL, d WITH FILL FROM 0;

-- After a later FROM that differs from the original row, a key without FROM takes the values of its own group's first row.
SELECT * FROM values('a Int64, b Int64, c Int64, d Int64', (0, 10, 5, 5), (1, -3, 7, 1), (1, 10, 3, 8))
ORDER BY a WITH FILL, b WITH FILL FROM 10, c WITH FILL, d WITH FILL FROM 5;

SELECT * FROM values('a Int64, b Int64, c Int64, d Int64', (0, 10, 5, 5), (1, -3, 7, 1), (1, 10, 9, 8))
ORDER BY a WITH FILL, b WITH FILL FROM 10, c WITH FILL, d WITH FILL FROM 5;

SELECT * FROM values('a Int64, b Int64, c Int64, d Int64', (0, 10, 5, 5), (1, -3, 7, 1), (1, 10, 3, 8))
ORDER BY a WITH FILL TO 3, b WITH FILL FROM 10, c WITH FILL, d WITH FILL FROM 5;

SELECT * FROM values('a Int64, b Int64, c Int64, d Int64', (0, 10, 5, 5), (1, -3, 7, 1), (1, 10, 3, 8))
ORDER BY a WITH FILL STALENESS 2, b WITH FILL FROM 10, c WITH FILL, d WITH FILL FROM 5;

SELECT * FROM values('a Int64, b Int64, c Int64, d Int64', (0, -10, 5, 5), (1, 3, 7, 1), (1, -10, 3, 8))
ORDER BY a WITH FILL, b DESC WITH FILL FROM -10, c WITH FILL, d WITH FILL FROM 5;

SELECT * FROM values('a Int64, b Int64, c Int64, d Int64, e Int64', (0, 0, 10, 0, 5), (1, 2, -3, 7, 1), (1, 2, 10, 3, 8))
ORDER BY a WITH FILL FROM 1, b WITH FILL, c WITH FILL FROM 10, d WITH FILL, e WITH FILL FROM 5;

SELECT * FROM values('a Int64, b Int64, c Int64, d Int64', (0, -5, 5, 5), (1, -3, 7, 1))
ORDER BY a WITH FILL, b WITH FILL FROM -5, c WITH FILL, d WITH FILL FROM 5;

SELECT * FROM values('a Int64, b Int64, c Int64, d Int64', (0, 10, 5, 5), (5, -3, 7, 1), (5, 10, 3, 8))
ORDER BY a WITH FILL STALENESS 1, b WITH FILL FROM 10, c WITH FILL, d WITH FILL FROM 5;

SELECT a, b, c, d FROM
(
    SELECT [0, 1, 1][number + 1] AS a, [10, -3, 10][number + 1] AS b, [5, 7, 3][number + 1] AS c, [5, 1, 8][number + 1] AS d FROM numbers(3)
)
ORDER BY a WITH FILL, b WITH FILL FROM 10, c WITH FILL, d WITH FILL FROM 5
SETTINGS max_block_size = 1;

SELECT * FROM values('p Int64, a Int64, b Int64, c Int64, d Int64', (8, 0, 10, 5, 5), (8, 1, -3, 7, 1), (8, 1, 10, 3, 8), (9, 0, 10, 5, 5), (9, 1, -3, 7, 1), (9, 1, 10, 4, 7))
ORDER BY p, a WITH FILL, b WITH FILL FROM 10, c WITH FILL, d WITH FILL FROM 5
SETTINGS use_with_fill_by_sorting_prefix = 1;
