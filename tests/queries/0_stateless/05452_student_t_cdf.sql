-- Known closed forms. With one degree of freedom Student's t is Cauchy, so the CDF at 1 is
-- exactly 0.75, and the distribution is symmetric about zero at any degrees of freedom.
SELECT round(studentTCDF(0, 1), 12), round(studentTCDF(1, 1), 12), round(studentTCDF(0, 2.5), 12);

-- Symmetry, for whole and fractional degrees of freedom alike.
SELECT max(abs(studentTCDF(-x, df) + studentTCDF(x, df) - 1)) < 1e-15
FROM (SELECT arrayJoin([0.5, 1., 2., 7.25]) AS x, arrayJoin([1., 4., 12.75, 1000.]) AS df);

-- Monotonic in x.
SELECT min(studentTCDF(x, 5) <= studentTCDF(x + 0.25, 5))
FROM (SELECT (number - 100) / 10. AS x FROM numbers(200));

-- Approaches the standard normal distribution as the degrees of freedom grow.
SELECT abs(studentTCDF(1.5, 100000000) - (1 + erf(1.5 / sqrt(2))) / 2) < 1e-7;

-- Degenerate degrees of freedom give NaN rather than throwing.
SELECT isNaN(studentTCDF(1, 0)), isNaN(studentTCDF(1, -1)), isNaN(studentTCDF(1, nan)), isNaN(studentTCDF(nan, 5));

-- Infinities.
SELECT studentTCDF(-inf, 5), studentTCDF(inf, 5), round(studentTCDF(1.5, inf), 12);

-- NULL propagates.
SELECT studentTCDF(NULL, 5), studentTCDF(1.5, NULL), toTypeName(studentTCDF(materialize(toNullable(1.5)), 5));

-- Argument types: integers, floats and decimals are accepted, the result is always Float64.
SELECT toTypeName(studentTCDF(1, 5));
SELECT round(studentTCDF(toInt32(1), toUInt8(5)), 12), round(studentTCDF(toFloat32(1), toFloat32(5)), 6), round(studentTCDF(toDecimal64(1, 2), toDecimal32(5, 1)), 12);

-- The four constant/non-constant combinations the binary math wrapper dispatches on.
SELECT round(sum(studentTCDF(1.5, 8)), 9) FROM numbers(4);
SELECT round(sum(studentTCDF(materialize(1.5), 8)), 9) FROM numbers(4);
SELECT round(sum(studentTCDF(1.5, materialize(8.))), 9) FROM numbers(4);
SELECT round(sum(studentTCDF(materialize(1.5), materialize(8.))), 9) FROM numbers(4);

-- Wrong arity and non-numeric arguments are rejected.
SELECT studentTCDF(1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT studentTCDF(1, 2, 3); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT studentTCDF('a', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT studentTCDF(1, 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
