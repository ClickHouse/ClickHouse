-- The scale of a `Decimal` parameter in the name of an `AggregateFunction` type is read from text,
-- without `DataTypeFactory`. A scale above the maximum precision used to overrun a buffer on the stack
-- when the parameter was written back as text.

SELECT toTypeName(CAST(1 AS AggregateFunction(quantile(CAST('1e-10', 'Decimal32(10)')), UInt64))); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT toTypeName(CAST(1 AS AggregateFunction(quantile(CAST('1e-19', 'Decimal64(19)')), UInt64))); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT toTypeName(CAST(1 AS AggregateFunction(quantile(CAST('1e-39', 'Decimal128(39)')), UInt64))); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT toTypeName(CAST(1 AS AggregateFunction(quantile(CAST('1e-77', 'Decimal256(77)')), UInt64))); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT toTypeName(CAST(1 AS AggregateFunction(quantile(CAST('1e-88', 'Decimal256(88)')), UInt64))); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT toTypeName(CAST(1 AS AggregateFunction(quantile(CAST('3.462164989639087034239942739923635180669988563696446547344599548079e-22', 'Decimal256(88)')), UInt64))); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT toTypeName(CAST(1 AS AggregateFunction(quantile(CAST('1', 'Decimal256(4294967295)')), UInt64))); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- The maximum scale is still accepted, and the parameter is written back in full.
DROP TABLE IF EXISTS t_decimal_scale_parameter;
CREATE TABLE t_decimal_scale_parameter (s AggregateFunction(quantile(CAST('1e-76', 'Decimal256(76)')), UInt64)) ENGINE = Memory;
SELECT position(type, '::Decimal256(76)') > 0, position(type, concat('0.', repeat('0', 75), '1')) > 0
FROM system.columns WHERE database = currentDatabase() AND table = 't_decimal_scale_parameter';
DROP TABLE t_decimal_scale_parameter;
