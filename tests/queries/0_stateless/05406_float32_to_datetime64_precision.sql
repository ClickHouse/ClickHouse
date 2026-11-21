-- A `Float32` source is scaled to the ticks of `DateTime64` / `Time64` in `Float64`: in the 24-bit mantissa of a
-- `Float32` the product with the scale multiplier is rounded, which used to shift the result and to treat an
-- in-range value near the end of the scale-9 window as out of range.

-- 1665519700 is exact in `Float32`; computed in `Float32`, the milliseconds were off by 30 seconds.
SELECT toDateTime64(toFloat32(1665519700), 3, 'UTC');
SELECT CAST(toFloat32(1665519700), 'DateTime64(3, \'UTC\')');
SELECT toDateTime64(toFloat32(1665519700), 3, 'UTC') = toDateTime64(toFloat64(toFloat32(1665519700)), 3, 'UTC');

-- 9223371776 is exact in `Float32` and fits the scale-9 ticks, but its `Float32` product rounds to 2^63.
SELECT CAST(9223371776::Float32, 'DateTime64(9, \'UTC\')') SETTINGS date_time_overflow_behavior = 'ignore';
SELECT CAST(9223371776::Float32, 'DateTime64(9, \'UTC\')') SETTINGS date_time_overflow_behavior = 'throw';
SELECT CAST(9223371776::Float32, 'DateTime64(9, \'UTC\')') SETTINGS date_time_overflow_behavior = 'saturate';
SELECT accurateCast(9223371776::Float32, 'DateTime64(9, \'UTC\')');
SELECT accurateCastOrNull(9223371776::Float32, 'DateTime64(9, \'UTC\')');

-- A value that is really out of range is still detected.
SELECT CAST(9223372800::Float32, 'DateTime64(9, \'UTC\')') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(9223372800::Float32, 'DateTime64(9, \'UTC\')') SETTINGS date_time_overflow_behavior = 'saturate';
SELECT accurateCastOrNull(9223372800::Float32, 'DateTime64(9, \'UTC\')');

-- The same for `Time64` and for a non-constant column.
SELECT CAST(toFloat32(3599999), 'Time64(6)') SETTINGS date_time_overflow_behavior = 'throw';
SELECT CAST(toFloat32(3600000), 'Time64(6)') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDateTime64(materialize(toFloat32(number * 1665519700)), 3, 'UTC') FROM numbers(2);
