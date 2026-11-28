-- An integer number of seconds outside the `Time` range saturates instead of wrapping.

SELECT t, toInt64(t) FROM format(JSONEachRow, 't Time',
    '{"t":4294967297} {"t":5000000000} {"t":-4294967297} {"t":18446744073709551615} {"t":-18446744073709551615} {"t":3600000} {"t":-3599999} {"t":52225}');
SELECT t, toInt64(t) FROM format(Values, 't Time', '(4294967297), (-4294967297), (3600000), (52225)');
SELECT x, arrayMap(v -> toInt64(v), x) FROM (SELECT '[4294967297, 5000000000, 18446744073709551615, 52225]'::Array(Time) AS x);
SELECT toInt64(x.1) FROM (SELECT '(4294967297)'::Tuple(Time) AS x);
SELECT t = '999:59:59'::Time FROM format(JSONEachRow, 't Time', '{"t":5000000000}');
SELECT toInt64(t) FROM format(JSONEachRow, 't Nullable(Time)', '{"t":4294967297}');
SELECT v, variantType(v), toInt64(v.Time) FROM format(JSONEachRow, 'v Variant(Time, UUID)', '{"v":4294967297}');
SELECT v, variantType(v), toInt64(v.Time) FROM format(Values, 'v Variant(String, Time)', '(4294967297)');

-- The same for an integer number of ticks read into `Time64`.

SELECT t, toInt64(t) FROM format(JSONEachRow, 't Time64(0)',
    '{"t":18446744073709551615} {"t":5000000000} {"t":-18446744073709551615} {"t":-5000000000} {"t":3600000} {"t":52225}');
SELECT t FROM format(JSONEachRow, 't Time64(3)', '{"t":5000000000000} {"t":-18446744073709551615} {"t":3599999999} {"t":52225123}');
SELECT t, toInt64(t) FROM format(Values, 't Time64(0)', '(18446744073709551615), (-5000000000), (3600000), (52225)');
SELECT x, arrayMap(v -> toInt64(v), x) FROM (SELECT '[18446744073709551615, 5000000000, 52225]'::Array(Time64(0)) AS x);
SELECT '(5000000000000000000)'::Tuple(Time64(9));
SELECT t = '999:59:59'::Time64(0) FROM format(JSONEachRow, 't Time64(0)', '{"t":5000000000}');
SELECT t FROM format(JSONEachRow, 't Nullable(Time64(6))', '{"t":18446744073709551615}');
SELECT v, variantType(v) FROM format(JSONEachRow, 'v Variant(Time64(0), UUID)', '{"v":18446744073709551615}');
SELECT v, variantType(v), toInt64(variantElement(v, 'Time64(0)')) FROM format(Values, 'v Variant(String, Time64(0))', '(5000000000)');
