-- `exp`, `log`, `tanh` and `sigmoid` are computed for whole columns with vectorized code.
-- They must be accurate to a few ULP and give the same special values as libm.
-- The comparisons use tolerances, so that the result does not depend on the instruction set.

SELECT 'exact';
SELECT exp(0), log(1), sigmoid(0), tanh(0), exp(toFloat32(0)), log(toFloat32(1)), sigmoid(toFloat32(0));

SELECT 'special values';
SELECT isNaN(exp(nan)), exp(inf), exp(-inf), exp(710), exp(-746), exp(709.782712893384) < inf;
SELECT log(0), log(-0.), isNaN(log(-1)), isNaN(log(-inf)), log(inf), isNaN(log(nan)), log(5e-324) < -744;
SELECT tanh(inf), tanh(-inf), isNaN(tanh(nan)), tanh(-0.), tanh(1000), tanh(-1000);
SELECT sigmoid(inf), sigmoid(-inf), isNaN(sigmoid(nan)), sigmoid(1000), sigmoid(-1000);
SELECT isNaN(exp(toFloat32(nan))), exp(toFloat32(inf)), exp(toFloat32(89)), exp(toFloat32(-110)), log(toFloat32(0)), isNaN(log(toFloat32(-1))), tanh(toFloat32(inf)), sigmoid(toFloat32(-inf));

SELECT 'tiny arguments';
SELECT abs(tanh(1e-20) - 1e-20) <= 1e-35, abs(sigmoid(1e-20) - 0.5) <= 1e-16, abs(exp(1e-20) - 1) <= 1e-16;
-- The result is about exp(-709.78), a subnormal number: not 0 as with 1 / (1 + exp(-x)).
SELECT sigmoid(-709.78) > 0, abs(sigmoid(-709.78) / exp(-709.78) - 1) < 1e-10;

SELECT 'accuracy';
-- Relative errors against expressions computed with libm. `pow(e(), x)` and `log2(x) * ln(2)` have errors of a few ULP.
SELECT
    max(abs(exp(x) / pow(e(), x) - 1)) < 1e-14,
    max(abs(tanh(x) - sinh(x) / cosh(x)) / greatest(abs(tanh(x)), 1e-300)) < 1e-14,
    max(abs(sigmoid(x) * (1 + pow(e(), -x)) - 1)) < 1e-13
FROM (SELECT number / 1000 - 30 AS x FROM numbers(60001));

SELECT max(abs(log(x) - log2(x) * 0.6931471805599453) / abs(log(x))) < 1e-14
FROM (SELECT pow(10, number / 1000 - 30) AS x FROM numbers(60001)) WHERE x != 1;

SELECT
    max(abs(exp(x) / toFloat32(pow(e(), x)) - 1)) < 1e-6,
    max(abs(log(x) - toFloat32(log2(toFloat64(x)) * 0.6931471805599453)) / abs(log(x))) < 1e-6,
    max(abs(tanh(x) - toFloat32(sinh(toFloat64(x)) / cosh(toFloat64(x)))) / greatest(abs(tanh(x)), 1e-30)) < 1e-6
FROM (SELECT toFloat32(number / 1000 - 30) AS x FROM numbers(60001)) WHERE x > 0;

-- https://github.com/ClickHouse/ClickHouse/issues/62390
SELECT abs(tanh(-0.1) + 0.09966799462495582) < 3e-16, abs(tanh(0.5) - 0.46211715726000974) < 3e-16;

SELECT 'argument types';
SELECT toTypeName(exp(toFloat32(1))), toTypeName(log(1)), toTypeName(tanh(toDecimal32(1, 2))), toTypeName(sigmoid(1::Int8));
SELECT exp(toDecimal32(1, 2)) = exp(1.), log(toUInt8(10)) = log(10.), tanh(toInt64(-3)) = tanh(-3.), sigmoid(toDecimal64(2, 3)) = sigmoid(2.);
SELECT isNaN(exp(nan::Nullable(Float64))), log(NULL::Nullable(Float64)), exp(toLowCardinality(0.));
