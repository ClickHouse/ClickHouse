-- Casts of Decimal, DateTime64 and Time64 columns to integer and floating-point types.

-- Wide decimals with values around the Int64 range of the underlying integer (in ticks), all in range of the targets.
SELECT d, toInt128(d), toInt256(d), toFloat64(d) FROM (SELECT arrayJoin(['0', '0.99', '-0.99', '1.5', '-1.5', '92233720368547758.07', '-92233720368547758.08', '92233720368547758.08', '-92233720368547758.09', '1000000000000000000000000000000', '-1000000000000000000000000000000'])::Decimal128(2) AS d);
SELECT d, toInt128(d), toInt256(d), toFloat64(d) FROM (SELECT arrayJoin(['0', '0.99', '-0.99', '1.5', '-1.5', '92233720368547758.07', '-92233720368547758.08', '92233720368547758.08', '-92233720368547758.09', '1000000000000000000000000000000', '-1000000000000000000000000000000'])::Decimal256(2) AS d);

-- To Int64: the whole part decides the range, not the number of ticks.
SELECT d, toInt64(d) FROM (SELECT arrayJoin(['-0.99', '1.5', '92233720368547758.08', '-92233720368547758.09', '9223372036854775807.99', '-9223372036854775808.99'])::Decimal128(2) AS d);
SELECT d, toInt64(d) FROM (SELECT arrayJoin(['-0.99', '1.5', '92233720368547758.08', '-92233720368547758.09', '9223372036854775807.99', '-9223372036854775808.99'])::Decimal256(2) AS d);
SELECT toInt64(d) FROM (SELECT arrayJoin(['0', '1.5', '9223372036854775808'])::Decimal128(2) AS d); -- { serverError DECIMAL_OVERFLOW }
SELECT toInt64(d) FROM (SELECT arrayJoin(['0', '1.5', '-9223372036854775809'])::Decimal256(2) AS d); -- { serverError DECIMAL_OVERFLOW }

-- Scale multipliers above the Int64 range.
SELECT d, toInt128(d), toInt8(d) FROM (SELECT arrayJoin(['0', '1.5', '-1.5', '127.9', '-128.9'])::Decimal128(19) AS d);
SELECT d, toInt128(d) FROM (SELECT arrayJoin(['1.5', '-1.5', '1000000000000000000', '-1000000000000000000'])::Decimal128(19) AS d);
SELECT d, toInt128(d), toInt8(d) FROM (SELECT arrayJoin(['0', '1.5', '-1.5', '127.9', '-128.9'])::Decimal256(40) AS d);
SELECT d, toInt128(d) FROM (SELECT arrayJoin(['1.5', '-1.5', '1000000000000000000000000000000000', '-1000000000000000000000000000000000'])::Decimal256(40) AS d);

-- Scale 0 and the limits of small targets.
SELECT d, toInt8(d) FROM (SELECT arrayJoin(['-128', '127'])::Decimal32(0) AS d);
SELECT d, toUInt8(d) FROM (SELECT arrayJoin(['0', '255'])::Decimal32(0) AS d);
SELECT toInt8(d) FROM (SELECT arrayJoin(['-128', '127', '128'])::Decimal32(0) AS d); -- { serverError DECIMAL_OVERFLOW }
SELECT d, toInt8(d), toUInt8(abs(d)) FROM (SELECT arrayJoin(['-128', '127'])::Decimal256(0) AS d);
SELECT toUInt8(d) FROM (SELECT arrayJoin(['0', '255', '256'])::Decimal256(0) AS d); -- { serverError DECIMAL_OVERFLOW }

-- Unsigned targets truncate towards zero before the sign check.
SELECT toUInt32(materialize(toDecimal64('-0.5', 1)));
SELECT toUInt32(d) FROM (SELECT arrayJoin(['0.5', '-1.5'])::Decimal64(1) AS d); -- { serverError DECIMAL_OVERFLOW }

-- DateTime64 and Time64 sources.
SELECT dt, toInt64(dt), toFloat64(dt) FROM (SELECT arrayJoin(['1969-12-31 23:59:59.500', '1970-01-01 00:00:00.999', '2020-01-01 00:00:00.999'])::DateTime64(3, 'UTC') AS dt);
SELECT toUnixTimestamp(dt) FROM (SELECT arrayJoin(['1970-01-01 00:00:00.999', '2020-01-01 00:00:00.999'])::DateTime64(3, 'UTC') AS dt);
SELECT t, toInt32(t) FROM (SELECT arrayJoin(['-01:00:00.5', '01:00:00.5'])::Time64(1) AS t);

-- Nullable source with NULLs, and BFloat16 / Float32 targets.
SELECT d, CAST(d AS Nullable(Int32)) FROM (SELECT arrayJoin(['1.99', NULL, '-2147483648.99', NULL, '2147483647'])::Nullable(Decimal128(2)) AS d);
SELECT d, toBFloat16(d), toFloat32(d) FROM (SELECT arrayJoin(['0', '1.5', '-1.5', '12345.6789', '-99999999999999999999.99999'])::Decimal256(5) AS d);
