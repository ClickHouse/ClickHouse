-- An unsigned scale above the Int16 range must be rejected, not read as a negative Int64.

-- Constant N.
SELECT round(1234.5678, 18446744073709551614); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT floor(1234.5678, 18446744073709551614); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT ceil(1234.5678, 18446744073709551614); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT trunc(1234.5678, 18446744073709551614); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT roundBankers(1234.5678, 18446744073709551614); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT round(toDecimal64(1234.5678, 4), 18446744073709551614); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT round(1234, 18446744073709551615); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT round(1234.5678, toUInt64(32768)); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- Non-constant value, constant N.
SELECT round(materialize(1234.5678), 18446744073709551614); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT round(materialize(toDecimal64(1234.5678, 4)), 18446744073709551614); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- Non-constant N.
SELECT s, round(1234.5678, s) FROM (SELECT arrayJoin([toUInt64(2), toUInt64(18446744073709551614)]) AS s); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT round(toDecimal32(12.34, 2), materialize(toUInt64(18446744073709551614))); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT round(1234, materialize(toUInt64(18446744073709551615))); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- Scales in range keep working.
SELECT round(1234.5678, toUInt64(2)), round(1234.5678, -2), round(1234.5678, toUInt64(32767)), round(1234.5678, -32768);
SELECT round(1234.5678, materialize(toUInt64(2))), round(toDecimal64(1234.5678, 4), materialize(toUInt8(2))), floor(1234.5678, materialize(toInt64(-2)));
