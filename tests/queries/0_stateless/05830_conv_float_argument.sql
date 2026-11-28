-- conv converts a float by the digits of its integer part, as the same number written as a string,
-- not by its text, which uses the exponent form ('1e21', '1e-07') whose letters are digits in the higher bases.

-- 1e21 is 1000000000000000000000, which overflows like the string does; 1e-7 is 0.
SELECT conv(toFloat64(1e21), 10, 10), conv(toFloat64(1e21), 16, 10), conv(toFloat64(-1e21), 10, 10), conv(toFloat64(1e-7), 16, 10);
SELECT conv(toFloat32(1e30), 10, 10), conv(materialize(toFloat64(1e21)), 10, 10);

-- Floats without an exponent convert as before: the digits of the integer part, read in the source base.
SELECT conv(toFloat64(15.9), 16, 10), conv(toFloat64(-2.5), 10, 10), conv(toFloat64(-0.0), 10, 2), conv(toFloat32(123.75), 16, 10);

-- NaN and infinity have no digits.
SELECT conv(nan, 36, 10); -- { serverError BAD_ARGUMENTS }
SELECT conv(inf, 36, 10); -- { serverError BAD_ARGUMENTS }
SELECT conv(-inf, 10, 10); -- { serverError BAD_ARGUMENTS }

-- A non-finite value masked by NULL, or on a row with a NULL base, gives NULL.
SELECT conv(nullIf(x, inf), 10, 10) FROM (SELECT arrayJoin([1., inf]) AS x);
SELECT conv(if(isNaN(x), NULL, x), 10, 10) FROM (SELECT arrayJoin([1., nan]) AS x);
SELECT conv(x, b, 10) FROM values('x Float64, b Nullable(Int64)', (1, 10), (inf, NULL));
