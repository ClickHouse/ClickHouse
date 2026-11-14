-- Weights and granularities near 2^64 must not hit unsigned wrap-around (checked by UBSan).

-- The total weight is exactly 2^64 and does not fit `UInt64`.
SELECT quantileExactWeightedInterpolated(x, w) FROM values('x UInt64, w UInt64', (1, 18446744073709551615), (2, 1)); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT quantilesExactWeightedInterpolated(0.5, 0.9)(x, w) FROM values('x UInt64, w UInt64', (1, 18446744073709551615), (2, 1)); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- A large weighted count in the state of `quantileTimingWeighted`: serialization and deserialization.
SELECT finalizeAggregation(CAST(unhex(hex(quantileTimingWeightedState(toUInt64(1000), toUInt64(9223372036854775808)))), 'AggregateFunction(quantileTimingWeighted, UInt64, UInt64)'));

-- Truncation of `topKWeighted` leaves a counter with `count` less than `error`.
SELECT length(topKWeighted(1, 1)(number, if(number = 64, toUInt64(18446744073709551615), toUInt64(1)))) FROM numbers(65) SETTINGS max_threads = 1;

-- Ordering skip indexes with granularities above `INT64_MAX`.
DROP TABLE IF EXISTS t_huge_index_granularity;
CREATE TABLE t_huge_index_granularity
(
    a UInt64,
    b UInt64,
    INDEX ia a TYPE minmax GRANULARITY 9223372036854775808,
    INDEX ib b TYPE minmax GRANULARITY 9223372036854775809
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_huge_index_granularity SELECT number, number * 2 FROM numbers(10);
SELECT count() FROM t_huge_index_granularity WHERE a = 3 AND b = 6;
SELECT count() FROM t_huge_index_granularity WHERE a > 3 AND b < 15;
DROP TABLE t_huge_index_granularity;
