-- `if` whose branches are cast to a common type is filled by `IColumn::insertFromEither`.
-- Each query prints the result type and a hash of the results. The condition is random for the first half
-- of the rows and in runs of 100 rows (longer than the 64-row chunks of the generic implementation) for the
-- second half.

SET session_timezone = 'UTC';

-- ColumnVector
SELECT 'vector', any(toTypeName(r)), count(), sum(cityHash64(toString(r))) FROM (SELECT if(number < 2500, cityHash64(number) % 2, intDiv(number, 100) % 2) = 1 AS c,
    if(c, toDateTime(number * 1000), toDate(number)) AS r FROM numbers(5000));
SELECT 'vector, then const', any(toTypeName(r)), count(), sum(cityHash64(toString(r))) FROM (SELECT if(number < 2500, cityHash64(number) % 2, intDiv(number, 100) % 2) = 1 AS c,
    if(c, toDateTime('2020-01-01 00:00:00'), toDate(number)) AS r FROM numbers(5000));
SELECT 'vector, else const', any(toTypeName(r)), count(), sum(cityHash64(toString(r))) FROM (SELECT if(number < 2500, cityHash64(number) % 2, intDiv(number, 100) % 2) = 1 AS c,
    if(c, toDateTime(number * 1000), toDate('2020-01-01')) AS r FROM numbers(5000));
SELECT 'vector, both const', any(toTypeName(r)), count(), sum(cityHash64(toString(r))) FROM (SELECT if(number < 2500, cityHash64(number) % 2, intDiv(number, 100) % 2) = 1 AS c,
    if(c, toDateTime('2020-01-01 00:00:00'), toDate('2021-01-01')) AS r FROM numbers(5000));
SELECT 'vector, nullable', any(toTypeName(r)), count(), sum(cityHash64(toString(r))) FROM (SELECT if(number < 2500, cityHash64(number) % 2, intDiv(number, 100) % 2) = 1 AS c,
    if(c, toDateTime(number * 1000), if(number % 3 = 0, NULL, toDate(number))) AS r FROM numbers(5000));

-- ColumnDecimal
SELECT 'decimal', any(toTypeName(r)), count(), sum(cityHash64(toString(r))) FROM (SELECT if(number < 2500, cityHash64(number) % 2, intDiv(number, 100) % 2) = 1 AS c,
    if(c, toDateTime64(number, 3), toDateTime64(number * 2, 6)) AS r FROM numbers(5000));
SELECT 'decimal, then const', any(toTypeName(r)), count(), sum(cityHash64(toString(r))) FROM (SELECT if(number < 2500, cityHash64(number) % 2, intDiv(number, 100) % 2) = 1 AS c,
    if(c, toDateTime64('2020-01-01 00:00:00.123', 3), toDateTime64(number * 2, 6)) AS r FROM numbers(5000));

-- ColumnString
SELECT 'string', any(toTypeName(r)), count(), sum(cityHash64(toString(r))) FROM (SELECT if(number < 2500, cityHash64(number) % 2, intDiv(number, 100) % 2) = 1 AS c,
    if(c, CAST(number % 2 + 1 AS Enum8('a' = 1, 'bb' = 2)), repeat('x', number % 20)) AS r FROM numbers(5000));
SELECT 'string, then const', any(toTypeName(r)), count(), sum(cityHash64(toString(r))) FROM (SELECT if(number < 2500, cityHash64(number) % 2, intDiv(number, 100) % 2) = 1 AS c,
    if(c, CAST('bb' AS Enum8('a' = 1, 'bb' = 2)), repeat('x', number % 20)) AS r FROM numbers(5000));
SELECT 'string, else const', any(toTypeName(r)), count(), sum(cityHash64(toString(r))) FROM (SELECT if(number < 2500, cityHash64(number) % 2, intDiv(number, 100) % 2) = 1 AS c,
    if(c, CAST(number % 2 + 1 AS Enum8('a' = 1, 'bb' = 2)), 'a long constant string') AS r FROM numbers(5000));

-- ColumnArray: short arrays (with empty ones), long arrays, constant arrays, nested strings and arrays
SELECT 'array, short', any(toTypeName(r)), count(), sum(cityHash64(toString(r))) FROM (SELECT if(number < 2500, cityHash64(number) % 2, intDiv(number, 100) % 2) = 1 AS c,
    if(c, arrayMap(x -> toDateTime(x * 1000), range(number % 3)), arrayMap(x -> toDate(x), range(number % 2))) AS r FROM numbers(5000));
SELECT 'array, long', any(toTypeName(r)), count(), sum(cityHash64(toString(r))) FROM (SELECT if(number < 2500, cityHash64(number) % 2, intDiv(number, 100) % 2) = 1 AS c,
    if(c, arrayMap(x -> toDateTime(x * 1000), range(number % 50)), arrayMap(x -> toDate(x), range(number % 40))) AS r FROM numbers(5000));
SELECT 'array, then const', any(toTypeName(r)), count(), sum(cityHash64(toString(r))) FROM (SELECT if(number < 2500, cityHash64(number) % 2, intDiv(number, 100) % 2) = 1 AS c,
    if(c, [toDateTime('2020-01-01 00:00:00'), toDateTime('2020-01-02 00:00:00')], arrayMap(x -> toDate(x), range(number % 3))) AS r FROM numbers(5000));
SELECT 'array, both const', any(toTypeName(r)), count(), sum(cityHash64(toString(r))) FROM (SELECT if(number < 2500, cityHash64(number) % 2, intDiv(number, 100) % 2) = 1 AS c,
    if(c, [toDateTime('2020-01-01 00:00:00')], [toDate('2021-01-01'), toDate('2021-01-02')]) AS r FROM numbers(5000));
SELECT 'array of strings', any(toTypeName(r)), count(), sum(cityHash64(toString(r))) FROM (SELECT if(number < 2500, cityHash64(number) % 2, intDiv(number, 100) % 2) = 1 AS c,
    if(c, [CAST(number % 2 + 1 AS Enum8('a' = 1, 'bb' = 2))], [toString(number), 'x']) AS r FROM numbers(5000));
SELECT 'array of arrays', any(toTypeName(r)), count(), sum(cityHash64(toString(r))) FROM (SELECT if(number < 2500, cityHash64(number) % 2, intDiv(number, 100) % 2) = 1 AS c,
    if(c, [[toDateTime(number * 1000)]], [[toDate(number)], []]) AS r FROM numbers(5000));

-- Generic implementation in `IColumnHelper`: `Variant` result
SELECT 'variant', any(toTypeName(r)), count(), sum(cityHash64(toString(r))) FROM (SELECT if(number < 2500, cityHash64(number) % 2, intDiv(number, 100) % 2) = 1 AS c,
    if(c, number, toString(number)) AS r FROM numbers(5000))
SETTINGS use_variant_as_common_type = 1;

-- Rows of a `LEFT JOIN` without a match get the default value of the right key
DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;
CREATE TABLE t_left (s String, fs FixedString(3), a Array(String)) ENGINE = Memory;
CREATE TABLE t_right (s String, fs FixedString(3), a Array(String)) ENGINE = Memory;
INSERT INTO t_left VALUES ('a', 'aaa', ['a']), ('b', 'bbb', ['b', 'b']), ('c', 'ccc', []);
INSERT INTO t_right VALUES ('b', 'bbb', ['b', 'b']);
SELECT l.s, r.s FROM t_left AS l LEFT JOIN t_right AS r ON l.s = r.s ORDER BY l.s SETTINGS join_use_nulls = 0;
SELECT l.fs, r.fs FROM t_left AS l LEFT JOIN t_right AS r ON l.fs = r.fs ORDER BY l.fs SETTINGS join_use_nulls = 0;
SELECT l.a, r.a FROM t_left AS l LEFT JOIN t_right AS r ON l.a = r.a ORDER BY l.a SETTINGS join_use_nulls = 0;
DROP TABLE t_left;
DROP TABLE t_right;
