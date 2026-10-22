-- `hasAny` and `hasAll` with a long constant array of numbers look the elements of the rows up in a table of the constant.
-- The same calls with the constant materialized search the arrays, so the results must be equal. Rows contain all values
-- of the constant, all but one, or none, and some rows only values of the constant, so that every result occurs.

SET max_block_size = 65536;

SELECT 'UInt8',
    countIf(any_1 != any_1m), countIf(any_2 != any_2m), countIf(all_1 != all_1m), countIf(all_2 != all_2m),
    countIf(any_1) > 0 AND countIf(NOT any_1) > 0, countIf(all_1) > 0 AND countIf(NOT all_1) > 0,
    countIf(all_2) > 0 AND countIf(NOT all_2) > 0
FROM
(
    SELECT
        hasAny(a, c) AS any_1, hasAny(a, materialize(c)) AS any_1m,
        hasAny(c, a) AS any_2, hasAny(materialize(c), a) AS any_2m,
        hasAll(a, c) AS all_1, hasAll(a, materialize(c)) AS all_1m,
        hasAll(c, a) AS all_2, hasAll(materialize(c), a) AS all_2m
    FROM (SELECT r AS a, c FROM (SELECT number, arraySort(v -> cityHash64(v, number), arrayConcat(arrayFilter((v, i) -> number % 4 IN (0, 2) OR (number % 4 = 1 AND i != cityHash64(number) % 1100 + 1), c, arrayEnumerate(c)), if(number % 4 = 2, [], arrayMap(x -> toUInt8(cityHash64(number, x) % 256), range(cityHash64(number, 1) % 20))))) AS r, c FROM (SELECT number, arrayMap(x -> toUInt8(intHash64(x) % 200), range(1100)) AS c FROM numbers(5000))))
);

SELECT 'UInt16',
    countIf(any_1 != any_1m), countIf(any_2 != any_2m), countIf(all_1 != all_1m), countIf(all_2 != all_2m),
    countIf(any_1) > 0 AND countIf(NOT any_1) > 0, countIf(all_1) > 0 AND countIf(NOT all_1) > 0,
    countIf(all_2) > 0 AND countIf(NOT all_2) > 0
FROM
(
    SELECT
        hasAny(a, c) AS any_1, hasAny(a, materialize(c)) AS any_1m,
        hasAny(c, a) AS any_2, hasAny(materialize(c), a) AS any_2m,
        hasAll(a, c) AS all_1, hasAll(a, materialize(c)) AS all_1m,
        hasAll(c, a) AS all_2, hasAll(materialize(c), a) AS all_2m
    FROM (SELECT r AS a, c FROM (SELECT number, arraySort(v -> cityHash64(v, number), arrayConcat(arrayFilter((v, i) -> number % 4 IN (0, 2) OR (number % 4 = 1 AND i != cityHash64(number) % 600 + 1), c, arrayEnumerate(c)), if(number % 4 = 2, [], arrayMap(x -> toUInt16(cityHash64(number, x) % 60000), range(cityHash64(number, 1) % 20))))) AS r, c FROM (SELECT number, arrayMap(x -> toUInt16(intHash64(x) % 60000), range(600)) AS c FROM numbers(5000))))
);

SELECT 'UInt64',
    countIf(any_1 != any_1m), countIf(any_2 != any_2m), countIf(all_1 != all_1m), countIf(all_2 != all_2m),
    countIf(any_1) > 0 AND countIf(NOT any_1) > 0, countIf(all_1) > 0 AND countIf(NOT all_1) > 0,
    countIf(all_2) > 0 AND countIf(NOT all_2) > 0
FROM
(
    SELECT
        hasAny(a, c) AS any_1, hasAny(a, materialize(c)) AS any_1m,
        hasAny(c, a) AS any_2, hasAny(materialize(c), a) AS any_2m,
        hasAll(a, c) AS all_1, hasAll(a, materialize(c)) AS all_1m,
        hasAll(c, a) AS all_2, hasAll(materialize(c), a) AS all_2m
    FROM (SELECT r AS a, c FROM (SELECT number, arraySort(v -> cityHash64(v, number), arrayConcat(arrayFilter((v, i) -> number % 4 IN (0, 2) OR (number % 4 = 1 AND i != cityHash64(number) % 200 + 1), c, arrayEnumerate(c)), if(number % 4 = 2, [], arrayMap(x -> toUInt64(cityHash64(number, x) % 100000), range(cityHash64(number, 1) % 20))))) AS r, c FROM (SELECT number, arrayMap(x -> toUInt64(intHash64(x) % 100000), range(200)) AS c FROM numbers(5000))))
);

SELECT 'Float64',
    countIf(any_1 != any_1m), countIf(any_2 != any_2m), countIf(all_1 != all_1m), countIf(all_2 != all_2m),
    countIf(any_1) > 0 AND countIf(NOT any_1) > 0, countIf(all_1) > 0 AND countIf(NOT all_1) > 0,
    countIf(all_2) > 0 AND countIf(NOT all_2) > 0
FROM
(
    SELECT
        hasAny(a, c) AS any_1, hasAny(a, materialize(c)) AS any_1m,
        hasAny(c, a) AS any_2, hasAny(materialize(c), a) AS any_2m,
        hasAll(a, c) AS all_1, hasAll(a, materialize(c)) AS all_1m,
        hasAll(c, a) AS all_2, hasAll(materialize(c), a) AS all_2m
    FROM (SELECT r AS a, c FROM (SELECT number, arraySort(v -> cityHash64(v, number), arrayConcat(arrayFilter((v, i) -> number % 4 IN (0, 2) OR (number % 4 = 1 AND i != cityHash64(number) % 200 + 1), c, arrayEnumerate(c)), if(number % 4 = 2, [], arrayMap(x -> toFloat64(multiIf(cityHash64(number, x) % 13 = 0, -0., cityHash64(number, x) % 11 = 0, 0., (cityHash64(number, x) % 100000) / 4)), range(cityHash64(number, 1) % 20))))) AS r, c FROM (SELECT number, arrayMap(x -> toFloat64(multiIf(intHash64(x) % 13 = 0, -0., intHash64(x) % 11 = 0, 0., (intHash64(x) % 100000) / 4)), range(200)) AS c FROM numbers(5000))))
);

SELECT 'BFloat16',
    countIf(any_1 != any_1m), countIf(any_2 != any_2m), countIf(all_1 != all_1m), countIf(all_2 != all_2m),
    countIf(any_1) > 0 AND countIf(NOT any_1) > 0, countIf(all_1) > 0 AND countIf(NOT all_1) > 0,
    countIf(all_2) > 0 AND countIf(NOT all_2) > 0
FROM
(
    SELECT
        hasAny(a, c) AS any_1, hasAny(a, materialize(c)) AS any_1m,
        hasAny(c, a) AS any_2, hasAny(materialize(c), a) AS any_2m,
        hasAll(a, c) AS all_1, hasAll(a, materialize(c)) AS all_1m,
        hasAll(c, a) AS all_2, hasAll(materialize(c), a) AS all_2m
    FROM (SELECT r AS a, c FROM (SELECT number, arraySort(v -> cityHash64(v, number), arrayConcat(arrayFilter((v, i) -> number % 4 IN (0, 2) OR (number % 4 = 1 AND i != cityHash64(number) % 200 + 1), c, arrayEnumerate(c)), if(number % 4 = 2, [], arrayMap(x -> toBFloat16(multiIf(cityHash64(number, x) % 13 = 0, -0., cityHash64(number, x) % 11 = 0, 0., cityHash64(number, x) % 200)), range(cityHash64(number, 1) % 20))))) AS r, c FROM (SELECT number, arrayMap(x -> toBFloat16(multiIf(intHash64(x) % 13 = 0, -0., intHash64(x) % 11 = 0, 0., intHash64(x) % 200)), range(200)) AS c FROM numbers(5000))))
);

SELECT 'Decimal32',
    countIf(any_1 != any_1m), countIf(any_2 != any_2m), countIf(all_1 != all_1m), countIf(all_2 != all_2m),
    countIf(any_1) > 0 AND countIf(NOT any_1) > 0, countIf(all_1) > 0 AND countIf(NOT all_1) > 0,
    countIf(all_2) > 0 AND countIf(NOT all_2) > 0
FROM
(
    SELECT
        hasAny(a, c) AS any_1, hasAny(a, materialize(c)) AS any_1m,
        hasAny(c, a) AS any_2, hasAny(materialize(c), a) AS any_2m,
        hasAll(a, c) AS all_1, hasAll(a, materialize(c)) AS all_1m,
        hasAll(c, a) AS all_2, hasAll(materialize(c), a) AS all_2m
    FROM (SELECT r AS a, c FROM (SELECT number, arraySort(v -> cityHash64(v, number), arrayConcat(arrayFilter((v, i) -> number % 4 IN (0, 2) OR (number % 4 = 1 AND i != cityHash64(number) % 200 + 1), c, arrayEnumerate(c)), if(number % 4 = 2, [], arrayMap(x -> toDecimal32(cityHash64(number, x) % 100000, 2), range(cityHash64(number, 1) % 20))))) AS r, c FROM (SELECT number, arrayMap(x -> toDecimal32(intHash64(x) % 100000, 2), range(200)) AS c FROM numbers(5000))))
);

SELECT 'UInt256',
    countIf(any_1 != any_1m), countIf(any_2 != any_2m), countIf(all_1 != all_1m), countIf(all_2 != all_2m),
    countIf(any_1) > 0 AND countIf(NOT any_1) > 0, countIf(all_1) > 0 AND countIf(NOT all_1) > 0,
    countIf(all_2) > 0 AND countIf(NOT all_2) > 0
FROM
(
    SELECT
        hasAny(a, c) AS any_1, hasAny(a, materialize(c)) AS any_1m,
        hasAny(c, a) AS any_2, hasAny(materialize(c), a) AS any_2m,
        hasAll(a, c) AS all_1, hasAll(a, materialize(c)) AS all_1m,
        hasAll(c, a) AS all_2, hasAll(materialize(c), a) AS all_2m
    FROM (SELECT r AS a, c FROM (SELECT number, arraySort(v -> cityHash64(v, number), arrayConcat(arrayFilter((v, i) -> number % 4 IN (0, 2) OR (number % 4 = 1 AND i != cityHash64(number) % 200 + 1), c, arrayEnumerate(c)), if(number % 4 = 2, [], arrayMap(x -> toUInt256(cityHash64(number, x) % 100000), range(cityHash64(number, 1) % 20))))) AS r, c FROM (SELECT number, arrayMap(x -> toUInt256(intHash64(x) % 100000), range(200)) AS c FROM numbers(5000))))
);

SELECT 'Nullable',
    countIf(any_1 != any_1m), countIf(any_2 != any_2m), countIf(all_1 != all_1m), countIf(all_2 != all_2m),
    countIf(any_1) > 0 AND countIf(NOT any_1) > 0, countIf(all_1) > 0 AND countIf(NOT all_1) > 0,
    countIf(all_2) > 0 AND countIf(NOT all_2) > 0
FROM
(
    SELECT
        hasAny(a, c) AS any_1, hasAny(a, materialize(c)) AS any_1m,
        hasAny(c, a) AS any_2, hasAny(materialize(c), a) AS any_2m,
        hasAll(a, c) AS all_1, hasAll(a, materialize(c)) AS all_1m,
        hasAll(c, a) AS all_2, hasAll(materialize(c), a) AS all_2m
    FROM (SELECT if(number % 3 = 0, arrayPushBack(r, NULL), arrayFilter(v -> isNotNull(v), r)) AS a, c FROM (SELECT number, arraySort(v -> cityHash64(v, number), arrayConcat(arrayFilter((v, i) -> number % 4 IN (0, 2) OR (number % 4 = 1 AND i != cityHash64(number) % 200 + 1), c, arrayEnumerate(c)), if(number % 4 = 2, [], arrayMap(x -> if(cityHash64(number, x) % 9 = 0, NULL, toUInt64(cityHash64(number, x) % 100000)), range(cityHash64(number, 1) % 20))))) AS r, c FROM (SELECT number, arrayMap(x -> if(intHash64(x) % 9 = 0, NULL, toUInt64(intHash64(x) % 100000)), range(200)) AS c FROM numbers(5000))))
);

-- Explicit results. The other argument is the constant, and is long enough for the lookup.
SELECT 'hasAny(materialize([-0.]), arrayReverse(arrayMap(x -> x / 1, range(20))))', countIf(hasAny(materialize([-0.]), arrayReverse(arrayMap(x -> x / 1, range(20)))) = 1) FROM numbers(3000);
SELECT 'hasAny(materialize([nan]), arrayPushBack(arrayMap(x -> x / 1, range(20)), nan))', countIf(hasAny(materialize([nan]), arrayPushBack(arrayMap(x -> x / 1, range(20)), nan)) = 0) FROM numbers(3000);
SELECT 'hasAll(arrayPushBack(arrayMap(x -> x / 1, range(20)), nan), materialize([1., nan]))', countIf(hasAll(arrayPushBack(arrayMap(x -> x / 1, range(20)), nan), materialize([1., nan])) = 0) FROM numbers(3000);
SELECT 'hasAll(materialize(arrayMap(x -> x / 1, range(100))), arrayPushBack(arrayMap(x -> x / 1, range(70)), nan))', countIf(hasAll(materialize(arrayMap(x -> x / 1, range(100))), arrayPushBack(arrayMap(x -> x / 1, range(70)), nan)) = 0) FROM numbers(3000);
SELECT 'hasAny(arrayPushBack(range(20), NULL), materialize([1000, NULL]))', countIf(hasAny(arrayPushBack(range(20), NULL), materialize([1000, NULL])) = 1) FROM numbers(3000);
SELECT 'hasAny(materialize([1000]), arrayPushBack(range(20), NULL))', countIf(hasAny(materialize([1000]), arrayPushBack(range(20), NULL)) = 0) FROM numbers(3000);
SELECT 'hasAll(arrayPushBack(range(20), NULL), materialize([3, NULL]))', countIf(hasAll(arrayPushBack(range(20), NULL), materialize([3, NULL])) = 1) FROM numbers(3000);
SELECT 'hasAll(range(20), materialize([3, NULL]))', countIf(hasAll(range(20), materialize([3, NULL])) = 0) FROM numbers(3000);
SELECT 'hasAll(materialize(range(toUInt64(200))), arrayPushBack(range(toUInt64(150)), NULL))', countIf(hasAll(materialize(range(toUInt64(200))), arrayPushBack(range(toUInt64(150)), NULL)) = 0) FROM numbers(3000);
SELECT 'hasAll(materialize(arrayPushBack(range(toUInt64(200)), NULL)), arrayPushBack(range(toUInt64(150)), NULL))', countIf(hasAll(materialize(arrayPushBack(range(toUInt64(200)), NULL)), arrayPushBack(range(toUInt64(150)), NULL)) = 1) FROM numbers(3000);
SELECT 'hasAll(materialize(arrayMap(x -> toUInt64(x % 150), range(300))), arrayMap(x -> toUInt64(x % 150), range(600)))', countIf(hasAll(materialize(arrayMap(x -> toUInt64(x % 150), range(300))), arrayMap(x -> toUInt64(x % 150), range(600))) = 1) FROM numbers(3000);
SELECT 'hasAll(materialize(arrayMap(x -> toUInt64(x % 149), range(300))), arrayMap(x -> toUInt64(x % 150), range(600)))', countIf(hasAll(materialize(arrayMap(x -> toUInt64(x % 149), range(300))), arrayMap(x -> toUInt64(x % 150), range(600))) = 0) FROM numbers(3000);
