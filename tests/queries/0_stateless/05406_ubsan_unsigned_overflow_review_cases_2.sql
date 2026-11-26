-- Unsigned arithmetic found in review of the `unsigned-integer-overflow` UBSan check.

-- Signed endpoints across zero in integer interpolation.
SELECT quantileInterpolatedWeighted(0.5)(x, 1) FROM values('x Int64', (-9007199254740994), (9007199254740994));
SELECT quantileInterpolatedWeighted(0.25)(x, 1) FROM values('x Int64', (-9223372036854775808), (9223372036854775807));

-- A sum of weights of the same value that does not fit `UInt64`.
SELECT quantileExactWeighted(x, w) FROM values('x UInt64, w UInt64', (1, 18446744073709551615), (1, 1)); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT quantileExactWeighted(x, w) FROM values('x UInt64, w UInt64', (1, 18446744073709551614), (1, 1));

-- A `hop` window longer than the range of `DateTime` must not wrap around to a short one.
SELECT hop(toDateTime('1970-01-01 00:01:40', 'UTC'), INTERVAL 1 SECOND, INTERVAL 4294967297 SECOND, 'UTC'); -- { serverError BAD_ARGUMENTS }
SELECT hop(toDateTime('2020-01-01 00:00:00', 'UTC'), INTERVAL 1 WEEK, INTERVAL 100000 WEEK); -- { serverError BAD_ARGUMENTS }
SELECT hop(toDateTime('2020-01-01 00:00:00', 'UTC'), INTERVAL 1 WEEK, INTERVAL 2 WEEK);

-- Counters of `flameGraph` above `UInt64`.
SELECT length(flameGraph(['a'], toInt64(9223372036854775807))) > 0 FROM numbers(3) SETTINGS allow_introspection_functions = 1, max_threads = 1;

-- `LIMIT` with an offset whose sum with the limit does not fit `UInt64`, with the lazy `FINAL`.
DROP TABLE IF EXISTS t_lazy_final_huge_limit;
CREATE TABLE t_lazy_final_huge_limit (id UInt64, v UInt64) ENGINE = ReplacingMergeTree ORDER BY id;
INSERT INTO t_lazy_final_huge_limit SELECT number, number FROM numbers(10);
SELECT count() FROM (SELECT id FROM t_lazy_final_huge_limit FINAL LIMIT 8, 18446744073709551615) SETTINGS query_plan_optimize_lazy_final = 1, exact_rows_before_limit = 0;
DROP TABLE t_lazy_final_huge_limit;

-- `generateRandom` of `Date32` covers the days before the epoch.
SELECT min(d) < '1970-01-01', max(d) >= '1970-01-01' FROM (SELECT d FROM generateRandom('d Date32', 1) LIMIT 1000);
