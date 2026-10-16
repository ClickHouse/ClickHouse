-- A scale or precision above UInt32 max is rejected instead of being reduced modulo 2^32 (issue #125942).

SELECT CAST(1 AS DateTime64(4294967299, 'UTC')); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT CAST(1 AS DateTime64(4294967296)); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT CAST(1 AS DateTime(4294967299, 'UTC')); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT CAST(1 AS DateTime(4294967296)); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT CAST(1 AS Time64(4294967299)); -- { serverError BAD_ARGUMENTS }
SELECT CAST(1 AS Time(4294967299)); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_125942 (d DateTime64(4294967299, 'UTC')) ENGINE = Memory; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT toDecimal32OrDefault('1.5', 4294967297); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT toDecimal256OrDefault('1.5', 4294967299); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT toDateTime64OrDefault('2020-01-01', 4294967297, 'UTC'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT now64(4294967299); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT now64(-4294967293); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- The scale arguments of the conversion functions.
SELECT toDecimal32(3, 4294967297); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT toDateTime64(3, 4294967297, 'UTC'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT toTime64(3, 4294967297); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT toDateTime(3, 4294967296, 'UTC'); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT toTypeName(CAST(1 AS DateTime64(9, 'UTC'))), toTypeName(CAST(1 AS Time64(9))), toTypeName(CAST(1 AS DateTime(0, 'UTC'))),
    toTypeName(toDecimal32OrDefault('1.5', 9)), toTypeName(toDateTime64OrDefault('2020-01-01', 9, 'UTC')), toTypeName(now64(9));
