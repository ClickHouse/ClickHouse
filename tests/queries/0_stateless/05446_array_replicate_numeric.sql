-- Replicating arrays of numbers (`arrayWithConstant` over a non-constant array, `materialize` of a constant array)
-- must reproduce every element of every row: lengths 0..36, element widths 1..32 bytes, repeat counts 0..3.

SELECT 'UInt8', count(), countIf(toString(x) != s)
FROM
(
    SELECT toString(a) AS s, arrayJoin(arrayWithConstant(number % 4, a)) AS x
    FROM (SELECT CAST(arrayMap(i -> number * 7 + i, range(number % 37)) AS Array(UInt8)) AS a, number FROM numbers(1000))
);

SELECT 'BFloat16', count(), countIf(toString(x) != s)
FROM
(
    SELECT toString(a) AS s, arrayJoin(arrayWithConstant(number % 4, a)) AS x
    FROM (SELECT CAST(arrayMap(i -> number * 7 + i, range(number % 37)) AS Array(BFloat16)) AS a, number FROM numbers(1000))
);

SELECT 'Float32', count(), countIf(toString(x) != s)
FROM
(
    SELECT toString(a) AS s, arrayJoin(arrayWithConstant(number % 4, a)) AS x
    FROM (SELECT CAST(arrayMap(i -> number * 7 + i, range(number % 37)) AS Array(Float32)) AS a, number FROM numbers(1000))
);

SELECT 'Decimal64(3)', count(), countIf(toString(x) != s)
FROM
(
    SELECT toString(a) AS s, arrayJoin(arrayWithConstant(number % 4, a)) AS x
    FROM (SELECT CAST(arrayMap(i -> number * 7 + i, range(number % 37)) AS Array(Decimal64(3))) AS a, number FROM numbers(1000))
);

SELECT 'UInt128', count(), countIf(toString(x) != s)
FROM
(
    SELECT toString(a) AS s, arrayJoin(arrayWithConstant(number % 4, a)) AS x
    FROM (SELECT CAST(arrayMap(i -> number * 7 + i, range(number % 37)) AS Array(UInt128)) AS a, number FROM numbers(1000))
);

SELECT 'Int256', count(), countIf(toString(x) != s)
FROM
(
    SELECT toString(a) AS s, arrayJoin(arrayWithConstant(number % 4, a)) AS x
    FROM (SELECT CAST(arrayMap(i -> number * 7 + i, range(number % 37)) AS Array(Int256)) AS a, number FROM numbers(1000))
);

SELECT 'Nullable(UInt32)', count(), countIf(toString(x) != s)
FROM
(
    SELECT toString(a) AS s, arrayJoin(arrayWithConstant(number % 4, a)) AS x
    FROM (SELECT CAST(arrayMap(i -> if(i % 3 = 0, NULL, number * 7 + i), range(number % 37)) AS Array(Nullable(UInt32))) AS a, number FROM numbers(1000))
);

SELECT 'Tuple(UInt16, Decimal128(2))', count(), countIf(toString(x) != s)
FROM
(
    SELECT toString(a) AS s, arrayJoin(arrayWithConstant(number % 4, a)) AS x
    FROM (SELECT CAST(arrayMap(i -> (number * 7 + i, number * 7 + i), range(number % 37)) AS Array(Tuple(UInt16, Decimal128(2)))) AS a, number FROM numbers(1000))
);

SELECT groupUniqArray(toString(materialize(CAST(range(17) AS Array(UInt8))))) FROM numbers(100000);
