-- A filling row whose values are already in the result is not inserted again.

-- A NULL in a Nullable fill key, with several fill keys
SELECT * FROM values('i Nullable(Int64), k Int64', (1, 0), (3, 5), (NULL, -1), (NULL, 2))
ORDER BY i ASC NULLS LAST WITH FILL TO 2, k WITH FILL;

SELECT * FROM values('i Nullable(Int64), k Int64', (1, 0), (3, 5), (NULL, -1))
ORDER BY i ASC NULLS LAST WITH FILL TO 2, k WITH FILL;

SELECT * FROM values('g Int64, i Nullable(Int64), k Int64', (0, 1, 0), (0, 3, 5), (0, NULL, -1), (0, NULL, 2))
ORDER BY g, i ASC NULLS LAST WITH FILL TO 2, k WITH FILL;

SELECT * FROM values('a Nullable(Int64), b Int64', (0, 5), (1, 2), (NULL, 1), (NULL, 3))
ORDER BY a ASC NULLS LAST WITH FILL, b WITH FILL;

SELECT * FROM values('a Nullable(Int64), b Nullable(Int64)', (1, 0), (4, 5), (NULL, -1), (NULL, 1))
ORDER BY a ASC NULLS LAST WITH FILL TO 3, b ASC NULLS LAST WITH FILL FROM 0;

SELECT * FROM values('i Nullable(Int64), k Nullable(Int64)', (1, 0), (3, 5), (NULL, NULL))
ORDER BY i ASC NULLS LAST WITH FILL TO 2, k WITH FILL;

-- An ordinary sorting key before a fill key
SELECT * FROM values('a Int64, o Int64, b Int64', (1, 0, 2), (1, 5, 1), (2, 0, 0))
ORDER BY a WITH FILL, o, b WITH FILL;

SELECT * FROM values('g Int64, i Int64', (0, 1), (0, 3), (1, 2), (1, 4))
ORDER BY g, i WITH FILL
SETTINGS use_with_fill_by_sorting_prefix = 0;

-- A FROM row that is not in the result yet is still inserted
SELECT * FROM values('a Nullable(Int64), b Nullable(Int64)', (0, 1), (2, 3), (NULL, 4))
ORDER BY a ASC NULLS LAST WITH FILL FROM 1, b WITH FILL;

SELECT DISTINCT i FROM
(
    SELECT * FROM values('i Nullable(Int64), k Int64', (1, 0), (3, 5), (NULL, -1), (NULL, 2))
    ORDER BY i ASC NULLS LAST WITH FILL TO 2, k WITH FILL
)
SETTINGS optimize_distinct_in_order = 1;
