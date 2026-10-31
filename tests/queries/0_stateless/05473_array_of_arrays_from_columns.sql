-- Function `array` with non-constant arguments of type `Array(T)`, for fixed-width and other nested types.

SELECT [[], range(number), [number]] FROM numbers(4);
SELECT [(range(number), 'a'), ([number], 'b')] FROM numbers(3);
SELECT toTypeName([arrayMap(x -> toDecimal32(x / 4, 2), range(number % 9)), []]) FROM numbers(1);

-- Each query returns the number of rows where `[a, b, c]` differs from the concatenation of its arguments.

SELECT 'UInt8', countIf(toString([a, b, c]) != concat('[', toString(a), ',', toString(b), ',', toString(c), ']'))
FROM (SELECT arrayMap(x -> toUInt8(x), range(number % 40)) AS a, []::Array(UInt8) AS b, arrayMap(x -> toUInt8(x * 7), range(number % 3)) AS c FROM numbers(100000));

SELECT 'UInt64', countIf(toString([a, b, c]) != concat('[', toString(a), ',', toString(b), ',', toString(c), ']'))
FROM (SELECT range(number % 20) AS a, [number] AS b, range(number % 5) AS c FROM numbers(100000));

SELECT 'Decimal', countIf(toString([a, b, c]) != concat('[', toString(a), ',', toString(b), ',', toString(c), ']'))
FROM (SELECT arrayMap(x -> toDecimal32(x / 4, 2), range(number % 9)) AS a, arrayMap(x -> toDecimal32(x, 2), range(number % 2)) AS b, arrayMap(x -> toDecimal32(-x / 8, 2), range(number % 5)) AS c FROM numbers(100000));

SELECT 'FixedString', countIf(toString([a, b, c]) != concat('[', toString(a), ',', toString(b), ',', toString(c), ']'))
FROM (SELECT arrayMap(x -> toFixedString(toString(x % 1000), 3), range(number % 11)) AS a, []::Array(FixedString(3)) AS b, arrayMap(x -> toFixedString(toString(number % 100), 3), range(number % 2)) AS c FROM numbers(100000));

SELECT 'String', countIf(toString([a, b, c]) != concat('[', toString(a), ',', toString(b), ',', toString(c), ']'))
FROM (SELECT arrayMap(x -> toString(x), range(number % 6)) AS a, []::Array(String) AS b, [toString(number)] AS c FROM numbers(100000));

SELECT 'Nullable', countIf(toString([a, b, c]) != concat('[', toString(a), ',', toString(b), ',', toString(c), ']'))
FROM (SELECT arrayMap(x -> if(x % 3 = 0, NULL, toUInt8(x)), range(number % 8)) AS a, []::Array(Nullable(UInt8)) AS b, arrayMap(x -> toNullable(toUInt8(x)), range(number % 2)) AS c FROM numbers(100000));

SELECT 'Array', countIf(toString([a, b, c]) != concat('[', toString(a), ',', toString(b), ',', toString(c), ']'))
FROM (SELECT arrayMap(x -> range(x % 3)::Array(UInt8), range(number % 4)) AS a, []::Array(Array(UInt8)) AS b, arrayMap(x -> range(x)::Array(UInt8), range(number % 2)) AS c FROM numbers(100000));

SELECT 'Const', countIf(toString([[1, 2, 3], a, []]) != concat('[[1,2,3],', toString(a), ',[]]'))
FROM (SELECT range(number % 4) AS a FROM numbers(100000));
