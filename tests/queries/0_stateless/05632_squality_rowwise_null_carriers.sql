SET allow_lossy_numeric_supertype = 0;
SET transform_null_in = 1;
SET enable_nullable_tuple_type = 1;

SELECT x IN (materialize(x)), x NOT IN (materialize(x))
FROM values('x Variant(Int64, String)', (NULL));

SELECT x IN (materialize(x)), x NOT IN (materialize(x))
FROM values('x Dynamic', (NULL));

SELECT x IN (s), x NOT IN (s), x IN (CAST(s AS Tuple(Nullable(Int32), Int32)))
FROM values('x Tuple(Nullable(Int32), Int32), s String', ((NULL, 1), '(NULL,1)'));

SELECT x IN (s), x NOT IN (s)
FROM values('x Nullable(Tuple(Nullable(Int32), Int32)), s String', ((NULL, 1), '(NULL,1)'));

SELECT x IN ([materialize(x)]), x NOT IN ([materialize(x)])
FROM values('x Array(Nullable(Int32))', ([NULL, 1]));

SELECT x IN (materialize(x)), x NOT IN (materialize(x))
FROM (SELECT map('a', CAST(NULL AS Nullable(Int32))) AS x);

SELECT x IN (materialize(x)), x NOT IN (materialize(x))
FROM (SELECT map('a', [CAST(NULL AS Nullable(Int32)), 1]) AS x);

SELECT x IN (materialize(y)), x NOT IN (materialize(y))
FROM
(
    SELECT map('a', [CAST(NULL AS Nullable(Int32)), 1]) AS x,
           map('a', [CAST(NULL AS Nullable(Int32))]) AS y
);

WITH (a, b) -> 0 AS arrayElement
SELECT x IN (materialize(x)), x NOT IN (materialize(x))
FROM values('x Nullable(Int64)', (1));

WITH (a, b) -> 0 AS arrayMap
SELECT x IN (materialize(x)), x NOT IN (materialize(x))
FROM values('x Nullable(Int64)', (1));

WITH (a, b) -> 0 AS equals
SELECT x IN (materialize(x)), x NOT IN (materialize(x))
FROM values('x Int64', (1));
