-- The conversion from float to integer checks the range of the values for the whole column at once,
-- so a non-finite value at any position of a column must still be reported,
-- and a value outside the range of the result type must be converted the same way at any position.

SELECT sum(toInt32(number * 1.5)) FROM numbers(1000);
SELECT sum(toInt64(toFloat32(number) - 500.5)) FROM numbers(1000);
SELECT sum(toUInt8(number % 200 + 0.5)) FROM numbers(1000);

SELECT toInt32(if(number = 0, nan, number * 1.)) FROM numbers(1000) FORMAT Null; -- { serverError CANNOT_CONVERT_TYPE }
SELECT toInt32(if(number = 999, nan, number * 1.)) FROM numbers(1000) FORMAT Null; -- { serverError CANNOT_CONVERT_TYPE }
SELECT toInt64(if(number = 500, inf, number * 1.)) FROM numbers(1000) FORMAT Null; -- { serverError CANNOT_CONVERT_TYPE }
SELECT toUInt16(if(number = 997, -inf, number * 1.)) FROM numbers(1000) FORMAT Null; -- { serverError CANNOT_CONVERT_TYPE }
SELECT toInt8(if(number = 998, toFloat32(nan), toFloat32(number % 100))) FROM numbers(1000) FORMAT Null; -- { serverError CANNOT_CONVERT_TYPE }
SELECT toUInt32(if(number = 3, toFloat32(inf), toFloat32(number))) FROM numbers(1000) FORMAT Null; -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(if(number = 777, nan, number * 1.) AS Int16) FROM numbers(1000) FORMAT Null; -- { serverError CANNOT_CONVERT_TYPE }
SELECT toInt32(if(number = 999, nan, if(number = 500, 1e10, number * 1.))) FROM numbers(1000) FORMAT Null; -- { serverError CANNOT_CONVERT_TYPE }

-- The conversions that return `NULL` or check every value keep handling the non-finite values row by row.
SELECT accurateCastOrNull(if(number = 2, nan, number * 1.), 'Int32') FROM numbers(4);
SELECT accurateCast(if(number = 2, nan, number * 1.), 'Int32') FROM numbers(4) FORMAT Null; -- { serverError CANNOT_CONVERT_TYPE }

-- The result for a value outside the range of the result type is not defined, and it differs between platforms,
-- but it must be the same in every row. Each block of 127 rows has a single value, and has rows converted
-- by the vectorized code and rows converted after it, for any vector width.
SELECT 'Float64';
SELECT
    v,
    [uniqExact(toInt8(v)), uniqExact(toUInt8(v)), uniqExact(toInt16(v)), uniqExact(toUInt16(v)),
     uniqExact(toInt32(v)), uniqExact(toUInt32(v)), uniqExact(toInt64(v)), uniqExact(toUInt64(v))]
FROM
(
    SELECT [-1e30, -9223372036854777856., -3e9, -2147483649., -65537., -129., -1., 128., 256., 32768., 65536.,
            2147483648., 4294967296., 5e9, 9223372036854775808., 18446744073709551616., 1e30][intDiv(number, 127) + 1] AS v
    FROM numbers(127 * 17)
)
GROUP BY v
ORDER BY v
SETTINGS max_block_size = 127;

SELECT 'Float32';
SELECT
    v,
    [uniqExact(toInt8(x)), uniqExact(toUInt8(x)), uniqExact(toInt16(x)), uniqExact(toUInt16(x)),
     uniqExact(toInt32(x)), uniqExact(toUInt32(x)), uniqExact(toInt64(x)), uniqExact(toUInt64(x))]
FROM
(
    SELECT [-1e30, -3e9, -2147483904., -65537., -129., -1., 128., 256., 32768., 65536.,
            2147483648., 4294967296., 5e9, 9223372036854775808., 18446744073709551616., 1e30][intDiv(number, 127) + 1] AS v,
        toFloat32(v) AS x
    FROM numbers(127 * 16)
)
GROUP BY v
ORDER BY v
SETTINGS max_block_size = 127;

SELECT 'BFloat16';
SELECT
    v,
    [uniqExact(toInt8(x)), uniqExact(toUInt8(x)), uniqExact(toInt16(x)), uniqExact(toUInt16(x)),
     uniqExact(toInt32(x)), uniqExact(toUInt32(x)), uniqExact(toInt64(x)), uniqExact(toUInt64(x))]
FROM
(
    SELECT [-1e30, -3e9, -129., -1., 128., 256., 65536., 4294967296., 5e9, 9223372036854775808., 18446744073709551616., 1e30][intDiv(number, 127) + 1] AS v,
        toBFloat16(v) AS x
    FROM numbers(127 * 12)
)
GROUP BY v
ORDER BY v
SETTINGS max_block_size = 127;

-- A column with a value outside the range is converted row by row, and its other values are still converted exactly.
SELECT sumIf(toInt32(x), number != 500), sumIf(toUInt32(x), number != 500), sumIf(toUInt8(x), number != 500)
FROM (SELECT number, if(number = 500, 1e10, (number % 200) * 1.25) AS x FROM numbers(1000));

-- The values next to the bounds of the result type are converted exactly.
SELECT 'Float64 bounds';
SELECT min(toInt8(x)), max(toInt8(x)) FROM (SELECT if(number % 2, -128.9, 127.9) AS x FROM numbers(127));
SELECT min(toUInt8(x)), max(toUInt8(x)) FROM (SELECT if(number % 2, -0.9, 255.9) AS x FROM numbers(127));
SELECT min(toInt16(x)), max(toInt16(x)) FROM (SELECT if(number % 2, -32768.9, 32767.9) AS x FROM numbers(127));
SELECT min(toUInt16(x)), max(toUInt16(x)) FROM (SELECT if(number % 2, -0.9, 65535.9) AS x FROM numbers(127));
SELECT min(toInt32(x)), max(toInt32(x)) FROM (SELECT if(number % 2, -2147483648.9, 2147483647.9) AS x FROM numbers(127));
SELECT min(toUInt32(x)), max(toUInt32(x)) FROM (SELECT if(number % 2, -0.9, 4294967295.9) AS x FROM numbers(127));
SELECT min(toInt64(x)), max(toInt64(x)) FROM (SELECT if(number % 2, -9223372036854775808., 9223372036854774784.) AS x FROM numbers(127));
SELECT min(toUInt64(x)), max(toUInt64(x)) FROM (SELECT if(number % 2, -0.9, 18446744073709549568.) AS x FROM numbers(127));

SELECT 'Float32 bounds';
SELECT min(toInt32(x)), max(toInt32(x)) FROM (SELECT toFloat32(if(number % 2, -2147483648., 2147483520.)) AS x FROM numbers(127));
SELECT min(toUInt32(x)), max(toUInt32(x)) FROM (SELECT toFloat32(if(number % 2, -0.9, 4294967040.)) AS x FROM numbers(127));
SELECT min(toInt64(x)), max(toInt64(x)) FROM (SELECT toFloat32(if(number % 2, -9223372036854775808., 9223371487098961920.)) AS x FROM numbers(127));
SELECT min(toUInt64(x)), max(toUInt64(x)) FROM (SELECT toFloat32(if(number % 2, -0.9, 18446742974197923840.)) AS x FROM numbers(127));
