-- Boundary cases that previously wrapped around an integer (which UBSan now reports).

-- The product of the inner dimensions of an Npy shape is not bounded by the whole product when a dimension is zero.
SELECT * FROM format(Npy, 'x Array(Array(Array(UInt8)))',
    unhex('934e554d5059010054007b276465736372273a20277c7531272c2027666f727472616e5f6f72646572273a2066616c73652c20277368617065273a2028302c20343239343936373239362c20343239343936373239362c2030292c207d0a'))
SETTINGS input_format_parallel_parsing = 0; -- { serverError INCORRECT_DATA }

-- A negative NaN in the bfloat16 prefix of the `Quantized` codec.
SET enable_quantized_codec = 1;
DROP TABLE IF EXISTS t_quantized_nan;
CREATE TABLE t_quantized_nan (v Array(Float32) CODEC(Quantized('prefix', 1, 1, 'bf16'))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_quantized_nan SELECT [reinterpretAsFloat32(toUInt32(4294967295))];
SELECT count(), isNaN(any(v)[1]) FROM t_quantized_nan;
DROP TABLE t_quantized_nan;

-- Decimal exponents at the limits of `Int32`.
SELECT * FROM format(TSV, 'x Decimal64(0)', '1e-2147483648\n');
SELECT * FROM format(TSV, 'x Decimal64(0)', '1.1e-2147483648\n');
SELECT toDecimal64OrNull('1e-2147483648', 0), toDecimal64OrNull('1.1e-2147483648', 0), toDecimal64OrNull('1e2147483647', 0),
    toDecimal64OrNull('1.5e2147483647', 0);

-- A serialized `windowFunnel` state with the maximum event identifier.
SELECT finalizeAggregation(CAST(unhex('0101000000000000000100000001ffffffffffffffff')
    AS AggregateFunction(windowFunnel(1, 'strict_once'), UInt32, UInt8))); -- { serverError INCORRECT_DATA }
