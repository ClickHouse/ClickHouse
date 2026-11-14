-- `FirstApplicable(Wallaby, ZSTD(1))`: `Wallaby` for `Float32` and `Float64`, the next alternative for other types.
-- The `Float64` sum is rounded: its last digit depends on the summation order (e.g. with parallel replicas).

DROP TABLE IF EXISTS t_first_applicable_wallaby;

SET enable_wallaby_codec = 1;

CREATE TABLE t_first_applicable_wallaby
(
    f64 Float64 CODEC(FirstApplicable(Wallaby, ZSTD(1))),
    f32 Float32 CODEC(FirstApplicable(Wallaby, ZSTD(1))),
    i UInt64 CODEC(FirstApplicable(Wallaby, ZSTD(1))),
    s String CODEC(FirstApplicable(Wallaby, ZSTD(1)))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_first_applicable_wallaby SELECT number / 10, number / 4, number, toString(number) FROM numbers(10000);

SELECT round(sum(f64), 3), sum(f32), sum(i), sum(toUInt64(s)) FROM t_first_applicable_wallaby;

-- `Wallaby` alone is still rejected for a non-float column.
CREATE TABLE t_first_applicable_wallaby_bad (s String CODEC(Wallaby)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- None of the alternatives is applicable.
CREATE TABLE t_first_applicable_wallaby_bad (s String CODEC(FirstApplicable(Wallaby, ALP))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

DROP TABLE t_first_applicable_wallaby;
