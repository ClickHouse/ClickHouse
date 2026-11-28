-- A JSON number read into Date / Date32 must follow toDate / toDate32 (as the Values format does):
-- a day number up to the type's maximum day number, otherwise a Unix timestamp. https://github.com/ClickHouse/ClickHouse/issues/123783

SELECT n, a, toDate(n), b, toDate32(n)
FROM format(JSONEachRow, 'n Int64, a Date, b Date32', '
{"n": 0, "a": 0, "b": 0}
{"n": 42, "a": 42, "b": 42}
{"n": 65535, "a": 65535, "b": 65535}
{"n": 65536, "a": 65536, "b": 65536}
{"n": 70000, "a": 70000, "b": 70000}
{"n": 2932896, "a": 2932896, "b": 2932896}
{"n": 2932897, "a": 2932897, "b": 2932897}
{"n": 1546300800, "a": 1546300800, "b": 1546300800}
{"n": 4294967295, "a": 4294967295, "b": 4294967295}
{"n": 4294967296, "a": 4294967296, "b": 4294967296}
{"n": 253402300799, "a": 253402300799, "b": 253402300799}
{"n": 253402300800, "a": 253402300800, "b": 253402300800}
{"n": 9223372036854775807, "a": 9223372036854775807, "b": 9223372036854775807}
') SETTINGS session_timezone = 'UTC';

SELECT n, a, toDate(n), b, toDate32(n)
FROM format(JSONEachRow, 'n Int64, a Date, b Date32', '
{"n": -1, "a": -1, "b": -1}
{"n": -719528, "a": -719528, "b": -719528}
{"n": -719529, "a": -719529, "b": -719529}
') SETTINGS session_timezone = 'UTC';

SELECT a FROM format(JSONEachRow, 'a Date', '{"a": 70000}') SETTINGS session_timezone = 'UTC', date_time_overflow_behavior = 'throw';
SELECT a FROM format(JSONEachRow, 'a Date', '{"a": 4294967296}') SETTINGS session_timezone = 'UTC', date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT a FROM format(JSONEachRow, 'a Date', '{"a": -1}') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT b FROM format(JSONEachRow, 'b Date32', '{"b": 253402300800}') SETTINGS session_timezone = 'UTC', date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT b FROM format(JSONEachRow, 'b Date32', '{"b": -719529}') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

-- The try path, used by Variant.
SELECT a, variantType(a) FROM format(JSONEachRow, 'a Variant(Date32, String)', '{"a": 1546300800}') SETTINGS session_timezone = 'UTC';
