-- The offset and length arguments of the substring-like functions are read with IColumn::getInt(),
-- which returns Int64. A UInt64 value of 2^63 or more was therefore read as a negative number and
-- applied with its documented negative meaning ("count from the other end"):
-- right('abcdefghij', 18446744073709551615) returned 'bcdefghij'. It must be reported as out of
-- range instead, as substringIndex does.
-- https://github.com/ClickHouse/ClickHouse/issues/125943

--- Controls: everything that fits Int64 behaves exactly as it did before.

-- Signed literals.
SELECT left('abcdefghij', 5) AS a, right('abcdefghij', 5) AS b, substring('abcdefghij', 3, 4) AS c, byteSlice('hello', 2, 3) AS d;
-- Small UInt64 literals.
SELECT left('abcdefghij', toUInt64(5)) AS a, right('abcdefghij', toUInt64(5)) AS b, substring('abcdefghij', toUInt64(3), toUInt64(4)) AS c, byteSlice('hello', toUInt64(2), toUInt64(3)) AS d;
-- The UTF8 variants.
SELECT leftUTF8('abcdefghij', toUInt64(5)) AS a, rightUTF8('abcdefghij', toUInt64(5)) AS b, substringUTF8('abcdefghij', toUInt64(3), toUInt64(4)) AS c;
-- Int64 maximum: the largest value that still fits.
SELECT left('abcdefghij', 9223372036854775807) AS a, right('abcdefghij', 9223372036854775807) AS b, substring('abcdefghij', 3, 9223372036854775807) AS c, leftUTF8('abcdefghij', 9223372036854775807) AS d;
SELECT length(bitSlice('abcdefghij', 1, 9223372036854775807));
-- Genuinely negative (signed) arguments keep their documented meaning.
SELECT left('abcdefghij', -1) AS a, right('abcdefghij', -1) AS b, substring('abcdefghij', 3, -1) AS c, substring('abcdefghij', -1) AS d, byteSlice('hello', 2, -1) AS e;
SELECT leftUTF8('abcdefghij', -1) AS a, rightUTF8('abcdefghij', -1) AS b, substringUTF8('abcdefghij', 3, -1) AS c;
SELECT length(bitSlice('abcdefghij', 1, -1)) AS a, length(bitSlice('abcdefghij', -1, 8)) AS b;
-- Int64 minimum fits Int64; the slice saturates to an empty string instead of overflowing.
SELECT left('abcdefghij', -9223372036854775808);
SELECT right('abcdefghij', -9223372036854775808);
SELECT substring('abcdefghij', 3, -9223372036854775808);

--- A UInt64 argument of 2^63 or more is out of range for Int64.

-- A materialized string next to a constant argument reaches the function with the argument still
-- wrapped in a ColumnConst.
SELECT left(materialize('abcdefghij'), 18446744073709551615); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT right(materialize('abcdefghij'), 18446744073709551615); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substring(materialize('abcdefghij'), 18446744073709551615); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT bitSlice(materialize('abcdefghij'), 18446744073709551615); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

-- Constant length: 2^63, 2^63+1 and the maximum UInt64.
SELECT left('abcdefghij', 9223372036854775808); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT right('abcdefghij', 9223372036854775808); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT right('abcdefghij', 18446744073709551615); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT left('abcdefghij', toUInt64(9223372036854775809)); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substring('abcdefghij', 3, 9223372036854775809); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substring('abcdefghij', 3, 18446744073709551615); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
-- Constant offset.
SELECT substring('abcdefghij', 18446744073709551615); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substring('abcdefghij', toUInt64(9223372036854775809), 2); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
-- The UTF8 variants, for both the length and the offset.
SELECT leftUTF8('abcdefghij', 9223372036854775809); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT rightUTF8('abcdefghij', 9223372036854775809); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substringUTF8('abcdefghij', 3, 9223372036854775809); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substringUTF8('abcdefghij', 9223372036854775809); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
-- byteSlice is an alias of substring.
SELECT byteSlice('hello', 2, 9223372036854775809); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
-- bitSlice measures its offset and length in bits.
SELECT bitSlice('abcdefghij', 1, 9223372036854775808); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT bitSlice('abcdefghij', 9223372036854775808, 2); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

--- Non-constant arguments are checked as well.

SELECT left(s, n) FROM (SELECT 'abcdefghij' AS s, arrayJoin([toUInt64(3), toUInt64(9223372036854775809)]) AS n); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT right(s, n) FROM (SELECT 'abcdefghij' AS s, arrayJoin([toUInt64(3), toUInt64(18446744073709551615)]) AS n); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substring(s, n) FROM (SELECT 'abcdefghij' AS s, arrayJoin([toUInt64(3), toUInt64(18446744073709551615)]) AS n); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substring(s, n, m) FROM (SELECT 'abcdefghij' AS s, arrayJoin([toUInt64(3)]) AS n, arrayJoin([toUInt64(2), toUInt64(9223372036854775809)]) AS m); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT leftUTF8(s, n) FROM (SELECT 'abcdefghij' AS s, arrayJoin([toUInt64(3), toUInt64(9223372036854775809)]) AS n); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substringUTF8(s, n) FROM (SELECT 'abcdefghij' AS s, arrayJoin([toUInt64(3), toUInt64(9223372036854775809)]) AS n); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT byteSlice(s, n, m) FROM (SELECT 'hello' AS s, arrayJoin([toUInt64(2)]) AS n, arrayJoin([toUInt64(3), toUInt64(18446744073709551615)]) AS m); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT bitSlice(s, n, m) FROM (SELECT 'abcdefghij' AS s, arrayJoin([toUInt64(1)]) AS n, arrayJoin([toUInt64(8), toUInt64(9223372036854775809)]) AS m); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

-- Non-constant arguments that fit Int64 are unchanged.
SELECT left(s, n) FROM (SELECT 'abcdefghij' AS s, arrayJoin([toUInt64(3), toUInt64(10), toUInt64(9223372036854775807)]) AS n);
SELECT substring(s, n, m) FROM (SELECT 'abcdefghij' AS s, arrayJoin([toUInt64(3)]) AS n, arrayJoin([toUInt64(2), toUInt64(9223372036854775807)]) AS m);

-- Nullable arguments: NULL stays NULL, and an out-of-range value is still reported.
SELECT left(s, n) FROM (SELECT 'abcdefghij' AS s, arrayJoin([toNullable(toUInt64(5)), NULL]) AS n);
SELECT left(s, n) FROM (SELECT 'abcdefghij' AS s, arrayJoin([toNullable(toUInt64(5)), toNullable(toUInt64(9223372036854775809))]) AS n); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

--- A NULL argument stays NULL even when the value under the NULL does not fit Int64.
-- `nullIf` leaves the original value in the slot behind the NULL, so an out-of-range 2^64-1 sits
-- under a NULL row. That row's result is NULL, not VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, because the
-- offset/length check must not run on the value behind a NULL.

SELECT left('abcdefghij', nullIf(n, toUInt64(18446744073709551615))) FROM (SELECT arrayJoin([toUInt64(3), toUInt64(18446744073709551615)]) AS n);
SELECT right('abcdefghij', nullIf(n, toUInt64(18446744073709551615))) FROM (SELECT arrayJoin([toUInt64(3), toUInt64(18446744073709551615)]) AS n);
SELECT substring('abcdefghij', nullIf(n, toUInt64(18446744073709551615))) FROM (SELECT arrayJoin([toUInt64(3), toUInt64(18446744073709551615)]) AS n);
SELECT substring('abcdefghij', 3, nullIf(m, toUInt64(18446744073709551615))) FROM (SELECT arrayJoin([toUInt64(2), toUInt64(18446744073709551615)]) AS m);
SELECT leftUTF8('abcdefghij', nullIf(n, toUInt64(18446744073709551615))) FROM (SELECT arrayJoin([toUInt64(3), toUInt64(18446744073709551615)]) AS n);
SELECT substringUTF8('abcdefghij', nullIf(n, toUInt64(18446744073709551615))) FROM (SELECT arrayJoin([toUInt64(3), toUInt64(18446744073709551615)]) AS n);
SELECT byteSlice('hello', 2, nullIf(m, toUInt64(18446744073709551615))) FROM (SELECT arrayJoin([toUInt64(3), toUInt64(18446744073709551615)]) AS m);
SELECT bitSlice('abcdefghij', nullIf(n, toUInt64(18446744073709551615))) FROM (SELECT arrayJoin([toUInt64(1), toUInt64(18446744073709551615)]) AS n);
SELECT bitSlice('abcdefghij', 1, nullIf(m, toUInt64(18446744073709551615))) FROM (SELECT arrayJoin([toUInt64(8), toUInt64(18446744073709551615)]) AS m);
-- substringIndex shares the same check, including for a constant and a non-constant string.
SELECT substringIndex('a.b.c', '.', nullIf(n, toUInt64(18446744073709551615))) FROM (SELECT arrayJoin([toUInt64(2), toUInt64(18446744073709551615)]) AS n);
SELECT substringIndex(materialize('a.b.c'), '.', nullIf(n, toUInt64(18446744073709551615))) FROM (SELECT arrayJoin([toUInt64(2), toUInt64(18446744073709551615)]) AS n);
SELECT substringIndexUTF8('a。b。c', '。', nullIf(n, toUInt64(18446744073709551615))) FROM (SELECT arrayJoin([toUInt64(2), toUInt64(18446744073709551615)]) AS n);

--- The result of a row is NULL when any argument is NULL there, so an out-of-range value of a non-Nullable UInt64
-- argument in that row is not checked either: here the NULL comes from the string or from another numeric argument.
SELECT left(nullIf(if(number = 0, '', 'abcdefghij'), ''), if(number = 0, toUInt64(18446744073709551615), toUInt64(3))) FROM numbers(2);
SELECT right(nullIf(if(number = 0, '', 'abcdefghij'), ''), if(number = 0, toUInt64(18446744073709551615), toUInt64(3))) FROM numbers(2);
SELECT leftUTF8(nullIf(if(number = 0, '', 'abcdefghij'), ''), if(number = 0, toUInt64(18446744073709551615), toUInt64(3))) FROM numbers(2);
SELECT substring(nullIf(if(number = 0, '', 'abcdefghij'), ''), if(number = 0, toUInt64(18446744073709551615), toUInt64(3))) FROM numbers(2);
SELECT substringIndex(nullIf(if(number = 0, '', 'a.b.c'), ''), '.', if(number = 0, toUInt64(18446744073709551615), toUInt64(2))) FROM numbers(2);
SELECT substring('abcdefghij', if(number = 0, toUInt64(18446744073709551615), toUInt64(3)), nullIf(if(number = 0, -1, 2), -1)) FROM numbers(2);
SELECT bitSlice('abcdefghij', if(number = 0, toUInt64(18446744073709551615), toUInt64(1)), nullIf(if(number = 0, -1, 8), -1)) FROM numbers(2);
SELECT left(toLowCardinality(nullIf(if(number = 0, '', 'abcdefghij'), '')), if(number = 0, toUInt64(18446744073709551615), toUInt64(3))) FROM numbers(2);

--- The dictionary of a `LowCardinality(UInt64)` argument keeps entries that no row references after a filter,
-- and only the referenced ones are checked.
SET allow_suspicious_low_cardinality_types = 1;
DROP TABLE IF EXISTS t_lc_05582;
CREATE TABLE t_lc_05582 (n LowCardinality(UInt64)) ENGINE = Memory;
INSERT INTO t_lc_05582 VALUES (3), (18446744073709551615);
SELECT left('abcdefghij', n) FROM t_lc_05582 WHERE n < 100;
SELECT right('abcdefghij', n) FROM t_lc_05582 WHERE n < 100;
SELECT leftUTF8('abcdefghij', n) FROM t_lc_05582 WHERE n < 100;
SELECT substring('abcdefghij', n) FROM t_lc_05582 WHERE n < 100;
SELECT substring('abcdefghij', 1, n) FROM t_lc_05582 WHERE n < 100;
SELECT hex(bitSlice('abcdefghij', n, 8)) FROM t_lc_05582 WHERE n < 100;
SELECT substringIndex('a.b.c.d', '.', n) FROM t_lc_05582 WHERE n < 100;
SELECT left('abcdefghij', n) FROM (SELECT toLowCardinality(toNullable(n)) AS n FROM t_lc_05582) WHERE n < 100;
-- A value that is referenced is still reported.
SELECT left('abcdefghij', n) FROM t_lc_05582; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
DROP TABLE t_lc_05582;
-- Extreme signed lengths and offsets saturate instead of throwing, also behind NULL.
SELECT substring(CAST(NULL, 'Nullable(String)'), toUInt64(9223372036854775807), toInt64(-4611686018427387904));
SELECT substring('abc', toUInt64(9223372036854775807), toInt64(-4611686018427387904));
SELECT substring('abc', toUInt64(1), toInt64('-9223372036854775808'));
SELECT substring('abc', toInt64('-9223372036854775808'), toInt64(-1));
SELECT substring(CAST(NULL, 'Nullable(String)'), toUInt64(1), toInt64('-9223372036854775808'));
SELECT substring('abc', toUInt64(1), nullIf(if(number = 0, toInt64('-9223372036854775808'), 2), toInt64('-9223372036854775808'))) FROM numbers(2);
