-- A filling row whose values are already in the result is not inserted again.

-- A NULL in a Nullable fill key, with several fill keys
SELECT * FROM values('i Nullable(Int64), k Int64', (1, 0), (3, 5), (NULL, -1), (NULL, 2))
ORDER BY i ASC NULLS LAST WITH FILL TO 2, k WITH FILL;

SELECT * FROM values('i Nullable(Int64), k Int64', (1, 0), (3, 5), (NULL, -1), (NULL, 2))
ORDER BY i ASC NULLS LAST WITH FILL TO 2, k WITH FILL
SETTINGS max_block_size = 1;

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

SELECT * FROM values('a Int64, o Int64, b Int64', (1, 2, 1), (1, 0, 0), (2, 3, 2))
ORDER BY a WITH FILL TO 4, o DESC, b WITH FILL TO 2;

SELECT * FROM values('a Int64, o Int64, b Int64', (1, 2, 0), (1, 0, 0), (2, 3, 2))
ORDER BY a WITH FILL TO 4, o DESC, b WITH FILL TO 2;

SELECT * FROM values('g Int64, i Int64', (0, 1), (0, 3), (1, 2), (1, 4))
ORDER BY g, i WITH FILL
SETTINGS use_with_fill_by_sorting_prefix = 0;

-- A FROM row that is not in the result yet is still inserted
SELECT * FROM values('a Nullable(Int64), b Nullable(Int64)', (0, 1), (2, 3), (NULL, 4))
ORDER BY a ASC NULLS LAST WITH FILL FROM 1, b WITH FILL;

-- A filling row moved by STALENESS is not in the result yet and is still inserted
SELECT * FROM values('a Int64, o Int64, b Int64', (1, 2, 0), (1, 2, 3), (1, 0, 1), (2, 0, 4))
ORDER BY a WITH FILL, o DESC, b WITH FILL STEP 2 STALENESS 2;

-- A STALENESS jump that does not move the filling row keeps it in the result
SELECT * FROM values('a Int64, o Int64, b Int64', (1, 2, 2), (1, 1, 3), (1, 0, 1), (2, 0, 4))
ORDER BY a WITH FILL, o DESC, b WITH FILL STEP 2 STALENESS 2;

-- A STALENESS jump that only resets a later fill key changes the filling row too
SELECT * FROM values('a Int64, o Int64, b Int64, c Nullable(Int64)', (2, 3, 4, 3), (2, 2, 5, 0), (2, 0, 1, 1))
ORDER BY a WITH FILL TO 4 STALENESS 3, o DESC, b WITH FILL STEP 3, c WITH FILL;

SELECT * FROM values('a Int64, o Int64, b Int64, c Nullable(Int64)', (2, 3, 4, 3), (2, 2, 5, 0), (2, 0, 1, 1), (3, 0, 0, 1))
ORDER BY a WITH FILL TO 4 STALENESS 3, o DESC, b WITH FILL STEP 3, c WITH FILL;

-- An unset key of the filling row is inserted, and compared, as the default of its column
SELECT * FROM values('a Int64, o Int64, b Int64, c Int64', (0, 0, 0, 0), (0, 1, 3, 0), (0, 2, 2, 0), (0, 3, 1, 0), (0, 4, 3, 1))
ORDER BY a WITH FILL, o, b WITH FILL STEP 2 STALENESS 2, c WITH FILL;

SELECT * FROM values('a Int64, o Int64, b Int64, c Int64', (0, 0, 0, 0), (0, 1, 3, 0), (0, 2, 2, 0), (0, 3, 1, 0))
ORDER BY a WITH FILL, o, b WITH FILL STEP 2 STALENESS 2, c WITH FILL;

-- A Float32 or BFloat16 fill key is compared as it is inserted, after rounding
SELECT * FROM values('a Float32, o Int64, b Int64', (0.7, 0, 2), (0.7, 5, 1), (2, 0, 0))
ORDER BY a WITH FILL FROM 0.7 TO 1, o, b WITH FILL FROM 2;

SELECT * FROM values('a Nullable(BFloat16), o Int64, b Int64', (0.7, 0, 2), (0.7, 5, 1), (2, 0, 0))
ORDER BY a WITH FILL FROM 0.7 TO 1, o, b WITH FILL FROM 2
SETTINGS max_block_size = 1;

SELECT * FROM values('a Int64, b Float32', (0, 0.7), (1, 0.5))
ORDER BY a WITH FILL, b WITH FILL FROM 0.7;

SELECT * FROM values('a Int64, o Int64, b Float32', (1, 2, 100000000), (1, 0, 100000016), (1, -1, 100000008), (2, 0, 100000032))
ORDER BY a WITH FILL, o DESC, b WITH FILL STEP 15 STALENESS 15;

-- A filling row that is not inserted again counts as inserted for the rows after it
SELECT * FROM values('a Nullable(Int64), b Nullable(Int64), c Nullable(Int64)', (1, 0, NULL), (3, 4, NULL), (0, 3, NULL), (NULL, 2, NULL))
ORDER BY a ASC NULLS FIRST WITH FILL, b DESC NULLS LAST WITH FILL STEP -3 STALENESS -3, c DESC NULLS LAST WITH FILL TO 1;

-- The rows of another sorting prefix group are not in the result of this one
SELECT * FROM values('g Int64, i Int64', (0, 1), (0, 2), (1, 1), (1, 2))
ORDER BY g, i WITH FILL FROM 5 TO 6;

SELECT DISTINCT i FROM
(
    SELECT * FROM values('i Nullable(Int64), k Int64', (1, 0), (3, 5), (NULL, -1), (NULL, 2))
    ORDER BY i ASC NULLS LAST WITH FILL TO 2, k WITH FILL
)
SETTINGS optimize_distinct_in_order = 1;

-- The rows of a call that threw are not in the result of the next one
SELECT * FROM values('a Int64', (0), (10000000000)) ORDER BY a WITH FILL TO 20000000000
UNION ALL SELECT 1
FORMAT Null SETTINGS max_memory_usage = '20Mi'; -- { serverError MEMORY_LIMIT_EXCEEDED }
