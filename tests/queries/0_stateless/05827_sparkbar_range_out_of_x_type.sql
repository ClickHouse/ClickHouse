-- `min_x` and `max_x` of `sparkbar` that do not fit the type of `x` are rejected instead of being wrapped.
SELECT sparkbar(4, 0, 100000)(x, 1) FROM (SELECT toUInt16(number * 10000) AS x FROM numbers(7)); -- { serverError BAD_ARGUMENTS }
SELECT sparkbar(4, 0, 5000000000)(x, 1) FROM (SELECT toUInt32(number) AS x FROM numbers(7)); -- { serverError BAD_ARGUMENTS }
SELECT sparkbar(8, 0, 263)(x, 1) FROM (SELECT toUInt8(number + 1) AS x FROM numbers(7)); -- { serverError BAD_ARGUMENTS }
SELECT sparkbar(8, 0, 4294967303)(x, 1) FROM (SELECT toUInt8(number + 1) AS x FROM numbers(7)); -- { serverError BAD_ARGUMENTS }
SELECT sparkbar(8, 256, 263)(x, 1) FROM (SELECT toUInt8(number + 1) AS x FROM numbers(7)); -- { serverError BAD_ARGUMENTS }
SELECT sparkbar(8, 0, 256)(x, 1) FROM (SELECT toUInt8(number + 1) AS x FROM numbers(7)); -- { serverError BAD_ARGUMENTS }
SELECT sparkbar(8, 0, 65540)(x, 1) FROM (SELECT toDate(number) AS x FROM numbers(7)); -- { serverError BAD_ARGUMENTS }

SELECT concat('|', sparkbar(8, 0, 255)(x, 1), '|') FROM (SELECT toUInt8(number + 1) AS x FROM numbers(7));
SELECT concat('|', sparkbar(8, 0, 7)(x, 1), '|') FROM (SELECT toUInt8(number + 1) AS x FROM numbers(7));
SELECT concat('|', sparkbar(4, 0, 65535)(x, 1), '|') FROM (SELECT toUInt16(number * 10000) AS x FROM numbers(7));
SELECT concat('|', sparkbar(4, 0, 100000)(x, 1), '|') FROM (SELECT toUInt64(number * 10000) AS x FROM numbers(7));
SELECT concat('|', sparkbar(4, 0, 4294967295)(x, 1), '|') FROM (SELECT toUInt32(number * 1000000000) AS x FROM numbers(5));
