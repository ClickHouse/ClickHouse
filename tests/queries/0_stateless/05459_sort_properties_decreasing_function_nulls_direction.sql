-- A sorted subquery seen through a decreasing function (`negate`, `/ -2`) is sorted in the opposite
-- direction, but its NaN/NULL stay at the same end of the stream. The propagated sort property flipped
-- only `direction`, so an outer ORDER BY asking for the other NaN/NULL placement was elided as satisfied.

SET optimize_sorting_by_input_stream_properties = 1;

DROP TABLE IF EXISTS t_sort_prop_nan;
CREATE TABLE t_sort_prop_nan (x Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_sort_prop_nan VALUES (1), (nan), (2);

SELECT 'negate of an ascending subquery, DESC NULLS FIRST';
SELECT -x AS n FROM (SELECT x FROM t_sort_prop_nan ORDER BY x LIMIT 10) ORDER BY n DESC NULLS FIRST;
SELECT -x AS n FROM (SELECT x FROM t_sort_prop_nan ORDER BY x LIMIT 10) ORDER BY n DESC NULLS FIRST SETTINGS optimize_sorting_by_input_stream_properties = 0;

SELECT 'negate of a descending subquery, ASC NULLS FIRST';
SELECT -x AS n FROM (SELECT x FROM t_sort_prop_nan ORDER BY x DESC LIMIT 10) ORDER BY n ASC NULLS FIRST;
SELECT -x AS n FROM (SELECT x FROM t_sort_prop_nan ORDER BY x DESC LIMIT 10) ORDER BY n ASC NULLS FIRST SETTINGS optimize_sorting_by_input_stream_properties = 0;

SELECT 'under an outer LIMIT, where the wrong order also returns wrong rows';
SELECT -x AS n FROM (SELECT x FROM t_sort_prop_nan ORDER BY x LIMIT 10) ORDER BY n DESC NULLS FIRST LIMIT 1;
SELECT -x AS n FROM (SELECT x FROM t_sort_prop_nan ORDER BY x LIMIT 10) ORDER BY n DESC NULLS FIRST LIMIT 1 SETTINGS optimize_sorting_by_input_stream_properties = 0;

SELECT 'the placement the stream already has can still be elided';
SELECT -x AS n FROM (SELECT x FROM t_sort_prop_nan ORDER BY x LIMIT 10) ORDER BY n DESC NULLS LAST;
SELECT -x AS n FROM (SELECT x FROM t_sort_prop_nan ORDER BY x LIMIT 10) ORDER BY n DESC NULLS LAST SETTINGS optimize_sorting_by_input_stream_properties = 0;

DROP TABLE t_sort_prop_nan;

DROP TABLE IF EXISTS t_sort_prop_null;
CREATE TABLE t_sort_prop_null (x Nullable(Float64)) ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_nullable_key = 1;
INSERT INTO t_sort_prop_null VALUES (1), (NULL), (2);

SELECT 'NULL through a division by a negative constant';
SELECT x / -2 AS n FROM (SELECT x FROM t_sort_prop_null ORDER BY x LIMIT 10) ORDER BY n DESC NULLS FIRST;
SELECT x / -2 AS n FROM (SELECT x FROM t_sort_prop_null ORDER BY x LIMIT 10) ORDER BY n DESC NULLS FIRST SETTINGS optimize_sorting_by_input_stream_properties = 0;

DROP TABLE t_sort_prop_null;

-- `sign` is monotonic but returns 1 for NaN, so in `sign(-x)` NaN becomes an ordinary value at the end of the stream.
DROP TABLE IF EXISTS t_sort_prop_sign;
CREATE TABLE t_sort_prop_sign (x Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_sort_prop_sign VALUES (-1), (1), (nan);

SELECT 'a decreasing chain with a function that turns NaN into an ordinary value';
SELECT sign(-x) AS n FROM (SELECT x FROM t_sort_prop_sign ORDER BY x LIMIT 10) ORDER BY n DESC;
SELECT sign(-x) AS n FROM (SELECT x FROM t_sort_prop_sign ORDER BY x LIMIT 10) ORDER BY n DESC SETTINGS optimize_sorting_by_input_stream_properties = 0;
SELECT sign(-x) AS n FROM (SELECT x FROM t_sort_prop_sign ORDER BY x LIMIT 10) ORDER BY n DESC LIMIT 2;
SELECT sign(-x) AS n FROM (SELECT x FROM t_sort_prop_sign ORDER BY x LIMIT 10) ORDER BY n DESC LIMIT 2 SETTINGS optimize_sorting_by_input_stream_properties = 0;

SELECT 'the same for an increasing function of a descending subquery';
SELECT sign(x) AS n FROM (SELECT x FROM t_sort_prop_sign ORDER BY x DESC LIMIT 10) ORDER BY n DESC LIMIT 2;
SELECT sign(x) AS n FROM (SELECT x FROM t_sort_prop_sign ORDER BY x DESC LIMIT 10) ORDER BY n DESC LIMIT 2 SETTINGS optimize_sorting_by_input_stream_properties = 0;

DROP TABLE t_sort_prop_sign;

-- `-x` is `x` for the minimum of `Int64`, so the result of `negate` is not sorted if the stream can contain it.
DROP TABLE IF EXISTS t_sort_prop_int_min;
CREATE TABLE t_sort_prop_int_min (x Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_sort_prop_int_min VALUES (-9223372036854775808), (1), (2);

SELECT 'negate of the minimum of a signed integer type';
SELECT -x AS n FROM (SELECT x FROM t_sort_prop_int_min ORDER BY x LIMIT 10) ORDER BY n DESC;
SELECT -x AS n FROM (SELECT x FROM t_sort_prop_int_min ORDER BY x LIMIT 10) ORDER BY n DESC SETTINGS optimize_sorting_by_input_stream_properties = 0;
SELECT -x AS n FROM (SELECT x FROM t_sort_prop_int_min ORDER BY x LIMIT 10) ORDER BY n DESC LIMIT 1;
SELECT -x AS n FROM (SELECT x FROM t_sort_prop_int_min ORDER BY x LIMIT 10) ORDER BY n DESC LIMIT 1 SETTINGS optimize_sorting_by_input_stream_properties = 0;

DROP TABLE t_sort_prop_int_min;

-- `Interval` and `Decimal` are stored as signed integers, so `negate` maps their minimum to itself too.
DROP TABLE IF EXISTS t_sort_prop_interval_min;
CREATE TABLE t_sort_prop_interval_min (x IntervalSecond) ENGINE = Memory;
INSERT INTO t_sort_prop_interval_min SELECT toIntervalSecond(toInt64('-9223372036854775808'));
INSERT INTO t_sort_prop_interval_min SELECT toIntervalSecond(number) FROM numbers(1, 2);

SELECT 'negate of the minimum of an interval';
SELECT -x AS n FROM (SELECT x FROM t_sort_prop_interval_min ORDER BY x LIMIT 10) ORDER BY n DESC;
SELECT -x AS n FROM (SELECT x FROM t_sort_prop_interval_min ORDER BY x LIMIT 10) ORDER BY n DESC SETTINGS optimize_sorting_by_input_stream_properties = 0;
SELECT -x AS n FROM (SELECT x FROM t_sort_prop_interval_min ORDER BY x LIMIT 10) ORDER BY n DESC LIMIT 1;
SELECT -x AS n FROM (SELECT x FROM t_sort_prop_interval_min ORDER BY x LIMIT 10) ORDER BY n DESC LIMIT 1 SETTINGS optimize_sorting_by_input_stream_properties = 0;

DROP TABLE t_sort_prop_interval_min;

DROP TABLE IF EXISTS t_sort_prop_decimal_min;
CREATE TABLE t_sort_prop_decimal_min (d32 Decimal32(0), d64 Decimal64(0), d128 Decimal128(0)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_sort_prop_decimal_min SELECT
    toDecimal32(toInt32('-2147483648'), 0),
    toDecimal64(toInt64('-9223372036854775808'), 0),
    toDecimal128(toInt128('-170141183460469231731687303715884105728'), 0);
INSERT INTO t_sort_prop_decimal_min SELECT number, number, number FROM numbers(1, 2);

SELECT 'negate of the minimum of a decimal';
SELECT -d32 AS n FROM (SELECT d32 FROM t_sort_prop_decimal_min ORDER BY d32 LIMIT 10) ORDER BY n DESC LIMIT 1;
SELECT -d32 AS n FROM (SELECT d32 FROM t_sort_prop_decimal_min ORDER BY d32 LIMIT 10) ORDER BY n DESC LIMIT 1 SETTINGS optimize_sorting_by_input_stream_properties = 0;
SELECT -d64 AS n FROM (SELECT d64 FROM t_sort_prop_decimal_min ORDER BY d64 LIMIT 10) ORDER BY n DESC;
SELECT -d64 AS n FROM (SELECT d64 FROM t_sort_prop_decimal_min ORDER BY d64 LIMIT 10) ORDER BY n DESC SETTINGS optimize_sorting_by_input_stream_properties = 0;
SELECT -d64 AS n FROM (SELECT d64 FROM t_sort_prop_decimal_min ORDER BY d64 LIMIT 10) ORDER BY n DESC LIMIT 1;
SELECT -d64 AS n FROM (SELECT d64 FROM t_sort_prop_decimal_min ORDER BY d64 LIMIT 10) ORDER BY n DESC LIMIT 1 SETTINGS optimize_sorting_by_input_stream_properties = 0;
SELECT -d128 AS n FROM (SELECT d128 FROM t_sort_prop_decimal_min ORDER BY d128 LIMIT 10) ORDER BY n DESC LIMIT 1;
SELECT -d128 AS n FROM (SELECT d128 FROM t_sort_prop_decimal_min ORDER BY d128 LIMIT 10) ORDER BY n DESC LIMIT 1 SETTINGS optimize_sorting_by_input_stream_properties = 0;

DROP TABLE t_sort_prop_decimal_min;

-- `toBFloat16` keeps only the high half of a `Float32`, so a signaling NaN, whose payload is in the low half, becomes infinity.
DROP TABLE IF EXISTS t_sort_prop_bfloat16;
CREATE TABLE t_sort_prop_bfloat16 (x Float32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_sort_prop_bfloat16 SELECT reinterpretAsFloat32(toUInt32(4286578689));
INSERT INTO t_sort_prop_bfloat16 VALUES (-1), (1);

SELECT 'a conversion to BFloat16 that turns NaN into infinity';
SELECT toBFloat16(-x) AS n FROM (SELECT x FROM t_sort_prop_bfloat16 ORDER BY x LIMIT 10) ORDER BY n DESC;
SELECT toBFloat16(-x) AS n FROM (SELECT x FROM t_sort_prop_bfloat16 ORDER BY x LIMIT 10) ORDER BY n DESC SETTINGS optimize_sorting_by_input_stream_properties = 0;
SELECT toBFloat16(-x) AS n FROM (SELECT x FROM t_sort_prop_bfloat16 ORDER BY x LIMIT 10) ORDER BY n DESC LIMIT 1;
SELECT toBFloat16(-x) AS n FROM (SELECT x FROM t_sort_prop_bfloat16 ORDER BY x LIMIT 10) ORDER BY n DESC LIMIT 1 SETTINGS optimize_sorting_by_input_stream_properties = 0;

DROP TABLE t_sort_prop_bfloat16;
