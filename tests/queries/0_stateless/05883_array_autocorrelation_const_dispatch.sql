-- The constant-array path of arrayAutocorrelation (executeConstArray). The function enables
-- useDefaultImplementationForConstants(), which unwraps the arguments only when all of them are
-- constant. A constant array therefore keeps its ColumnConst wrapper only while max_lag is not
-- constant, which is why every query below passes materialize(2); with a constant max_lag the
-- non-constant path runs instead. The first column names the element type the array was cast to.
--
-- The expected values follow the definition documented for arrayAutocorrelation in
-- docs/reference/functions/regular-functions/array-functions.mdx: one value per lag in
-- [0, max_lag), each sum((x[t] - mean) * (x[t + lag] - mean)) / sum((x[t] - mean)^2).
-- For [0, 1, 0, 1] the mean is 0.5 and the variance sum is 4 * 0.25 = 1, so lag 0 is 1 (its
-- numerator equals the denominator) and lag 1 is 3 * (-0.25) / 1 = -0.75. Every intermediate
-- value is exact in binary floating point, and scale 2 maps the Decimal natives 0 and 100 back
-- to 0.0 and 1.0 exactly, so the result is the same for every element type.

SELECT 'UInt16', arrayAutocorrelation(CAST([0, 1, 0, 1], 'Array(UInt16)'), materialize(2));
SELECT 'UInt32', arrayAutocorrelation(CAST([0, 1, 0, 1], 'Array(UInt32)'), materialize(2));
SELECT 'UInt64', arrayAutocorrelation(CAST([0, 1, 0, 1], 'Array(UInt64)'), materialize(2));
SELECT 'UInt128', arrayAutocorrelation(CAST([0, 1, 0, 1], 'Array(UInt128)'), materialize(2));
SELECT 'UInt256', arrayAutocorrelation(CAST([0, 1, 0, 1], 'Array(UInt256)'), materialize(2));
SELECT 'Int8', arrayAutocorrelation(CAST([0, 1, 0, 1], 'Array(Int8)'), materialize(2));
SELECT 'Int16', arrayAutocorrelation(CAST([0, 1, 0, 1], 'Array(Int16)'), materialize(2));
SELECT 'Int32', arrayAutocorrelation(CAST([0, 1, 0, 1], 'Array(Int32)'), materialize(2));
SELECT 'Int64', arrayAutocorrelation(CAST([0, 1, 0, 1], 'Array(Int64)'), materialize(2));
SELECT 'Int128', arrayAutocorrelation(CAST([0, 1, 0, 1], 'Array(Int128)'), materialize(2));
SELECT 'Int256', arrayAutocorrelation(CAST([0, 1, 0, 1], 'Array(Int256)'), materialize(2));
SELECT 'Float32', arrayAutocorrelation(CAST([0, 1, 0, 1], 'Array(Float32)'), materialize(2));
SELECT 'Float64', arrayAutocorrelation(CAST([0, 1, 0, 1], 'Array(Float64)'), materialize(2));
SELECT 'Decimal32', arrayAutocorrelation(CAST([0, 1, 0, 1], 'Array(Decimal32(2))'), materialize(2));
SELECT 'Decimal64', arrayAutocorrelation(CAST([0, 1, 0, 1], 'Array(Decimal64(2))'), materialize(2));
SELECT 'Decimal128', arrayAutocorrelation(CAST([0, 1, 0, 1], 'Array(Decimal128(2))'), materialize(2));
SELECT 'Decimal256', arrayAutocorrelation(CAST([0, 1, 0, 1], 'Array(Decimal256(2))'), materialize(2));
