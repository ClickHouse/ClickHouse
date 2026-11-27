-- A weight is a number of occurrences: a sum of weights that does not fit `UInt64` is rejected instead of wrapping
-- around, which would make the quantile forget the dominant value.

SELECT quantileInterpolatedWeighted(0.5)(x, w), quantileBFloat16Weighted(0.5)(x, w), quantileTimingWeighted(0.5)(x, w)
FROM values('x UInt64, w UInt64', (10, 9223372036854775808), (10, 1), (1000, 1))
SETTINGS max_threads = 1;

SELECT quantileInterpolatedWeighted(0.5)(x, w) FROM values('x UInt64, w UInt64', (1, 18446744073709551615), (1, 1), (2, 1)); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT quantileBFloat16Weighted(0.5)(x, w) FROM values('x UInt64, w UInt64', (1, 18446744073709551615), (1, 1), (2, 1)); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT quantileTimingWeighted(0.5)(x, w) FROM values('x UInt64, w UInt64', (10, 18446744073709551615), (10, 1), (1000, 1)); -- { serverError ARGUMENT_OUT_OF_BOUND }
