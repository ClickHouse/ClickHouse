-- Function `array` with non-constant `Array` arguments: `[a, b]` must equal `arrayConcat([a], [b])`.
-- Fixed-width element types are copied as raw bytes (inner arrays below and above 64 bytes), other types element by element.

SELECT [[], range(number), [number]] FROM numbers(4);

SELECT 'UInt8', countIf([a, b] != arrayConcat([a], [b])) FROM (SELECT range(number % 20) AS a, [toUInt8(number)] AS b FROM numbers(10000));
SELECT 'UInt64', countIf([a, b, a] != arrayConcat([a], [b], [a])) FROM (SELECT arrayMap(x -> number * 20 + x, range(number % 20)) AS a, range(number % 3)::Array(UInt64) AS b FROM numbers(10000));
SELECT 'Decimal128', countIf([a, b] != arrayConcat([a], [b])) FROM (SELECT arrayMap(x -> toDecimal128(x / 4, 2), range(number % 20)) AS a, arrayMap(x -> toDecimal128(-x, 2), range(number % 3)) AS b FROM numbers(10000));
SELECT 'FixedString', countIf([a, b] != arrayConcat([a], [b])) FROM (SELECT arrayMap(x -> toFixedString(toString(x), 2), range(number % 20)) AS a, []::Array(FixedString(2)) AS b FROM numbers(10000));
SELECT 'String', countIf([a, b] != arrayConcat([a], [b])) FROM (SELECT arrayMap(x -> toString(x), range(number % 20)) AS a, [toString(number)] AS b FROM numbers(10000));
SELECT 'Nullable', countIf([a, b] != arrayConcat([a], [b])) FROM (SELECT arrayMap(x -> if(x % 3 = 0, NULL, x), range(number % 20)) AS a, [toNullable(toUInt8(number))] AS b FROM numbers(10000));
SELECT 'Array', countIf([a, b] != arrayConcat([a], [b])) FROM (SELECT arrayMap(x -> range(x % 3), range(number % 20)) AS a, [range(number % 4)] AS b FROM numbers(10000));
SELECT 'Tuple', countIf([(a, 'x'), (b, 'y')] != arrayConcat([(a, 'x')], [(b, 'y')])) FROM (SELECT arrayMap(x -> number * 20 + x, range(number % 20)) AS a, range(number % 3)::Array(UInt64) AS b FROM numbers(10000));
SELECT 'Const', countIf([[1, 2, 3], a] != arrayConcat([[1, 2, 3]], [a])) FROM (SELECT arrayMap(x -> number * 20 + x, range(number % 20)) AS a FROM numbers(10000));
