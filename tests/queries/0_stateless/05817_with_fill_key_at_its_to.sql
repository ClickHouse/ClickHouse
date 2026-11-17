-- A fill key that is not the last one has a value at or past its TO in the data:
-- every result must stay in the order of its ORDER BY.

SELECT * FROM values('i Int64, j Int64, k Int64', (0, 2, 0), (0, 3, 5), (0, 3, 8))
ORDER BY i WITH FILL, j WITH FILL TO 2, k WITH FILL;

SELECT * FROM values('i Int64, k Int64', (1, 0), (3, 5), (3, 8))
ORDER BY i WITH FILL TO 1, k WITH FILL;

SELECT * FROM values('d Date, k Int64', ('2020-01-03', 0), ('2020-01-05', 3), ('2020-01-05', 6))
ORDER BY d WITH FILL TO toDate('2020-01-03'), k WITH FILL;

SELECT * FROM values('i Int64, j Int64, k Int64', (0, 3, 0), (0, 3, 2), (0, 4, 0), (1, 0, 0))
ORDER BY i WITH FILL, j WITH FILL TO 2, k WITH FILL TO 4;

SELECT * FROM values('k1 Int64, k2 Int64, k3 Int64', (2, 4, 3), (2, 3, 0), (3, 4, 2))
ORDER BY k1 WITH FILL, k2 DESC WITH FILL TO 5, k3 DESC WITH FILL TO 0;

SELECT * FROM values('i Int64, j Int64, k Int64', (0, 2, 0), (0, 3, 5), (1, 0, 0))
ORDER BY i WITH FILL, j WITH FILL TO 2, k WITH FILL TO 3;

SELECT * FROM values('i Int64, k Int64', (1, 0), (3, 5), (3, 8))
ORDER BY i WITH FILL TO 1 STALENESS 5, k WITH FILL;

-- A NULL in a fill key that is not the last one, in the next row or in the filling row.

SELECT * FROM values('i Nullable(Int64), k Int64', (1, 0), (3, 5), (NULL, 2))
ORDER BY i WITH FILL TO 1, k WITH FILL;

SELECT DISTINCT i FROM (SELECT * FROM values('i Nullable(Int64), k Int64', (1, 0), (3, 5), (NULL, 2))
ORDER BY i WITH FILL TO 1, k WITH FILL);

SELECT * FROM values('i Nullable(Int64), k Int64', (3, 0), (1, 5), (NULL, 2))
ORDER BY i DESC NULLS LAST WITH FILL TO 3, k WITH FILL;

SELECT * FROM values('i Int64, j Nullable(Int64), k Int64', (0, 1, 0), (0, 3, 5), (0, NULL, 2))
ORDER BY i WITH FILL, j WITH FILL TO 1, k WITH FILL;

SELECT * FROM values('i Nullable(Int64), k Int64', (1, 0), (NULL, 2), (NULL, 5))
ORDER BY i WITH FILL, k WITH FILL TO 8;

SELECT * FROM values('i Int64, k Nullable(Int64)', (2, NULL), (2, 3), (2, 4))
ORDER BY i WITH FILL TO 0, k NULLS FIRST WITH FILL FROM 0 TO 2;

SELECT * FROM values('i Nullable(Int64), k Int64', (NULL, 0), (1, 5), (1, 8))
ORDER BY i NULLS FIRST WITH FILL, k WITH FILL;

SELECT * FROM values('a Nullable(Int64), b Nullable(Int64), c Nullable(Int64)', (NULL, 0, 3), (NULL, NULL, NULL), (NULL, NULL, 1))
ORDER BY a NULLS FIRST WITH FILL TO -1, b DESC WITH FILL TO 4, c NULLS FIRST WITH FILL TO 5;

SELECT * FROM values('i Nullable(Int64), k Nullable(Int64)', (0, 3), (NULL, 1), (1, 4))
ORDER BY i DESC NULLS FIRST WITH FILL TO 1 STALENESS -3, k NULLS FIRST WITH FILL;
