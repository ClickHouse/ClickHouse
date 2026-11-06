-- Merging states of `contingency`, `cramersV`, `cramersVBiasCorrected` and `theilsU` must throw when the total
-- number of rows does not fit into `UInt64`, instead of wrapping the counts into an inconsistent state.

-- 100 rows scaled by 2^57 still fit, and the result does not change.
SELECT cramersVMerge(st) = cramersVMerge(multiply(st, 144115188075855872)) FROM (SELECT cramersVState(toUInt8(number % 10), toUInt8(number % 6)) AS st FROM numbers(100));
SELECT contingencyMerge(st) = contingencyMerge(multiply(st, 144115188075855872)) FROM (SELECT contingencyState(toUInt8(number % 10), toUInt8(number % 6)) AS st FROM numbers(100));
SELECT theilsUMerge(st) = theilsUMerge(multiply(st, 144115188075855872)) FROM (SELECT theilsUState(toUInt8(number % 10), toUInt8(number % 6)) AS st FROM numbers(100));

-- 100 rows scaled by 2^58 do not fit.
SELECT cramersVMerge(multiply(st, 288230376151711744)) FROM (SELECT cramersVState(toUInt8(number % 10), toUInt8(number % 6)) AS st FROM numbers(100)); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT contingencyMerge(multiply(st, 288230376151711744)) FROM (SELECT contingencyState(toUInt8(number % 10), toUInt8(number % 6)) AS st FROM numbers(100)); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT cramersVBiasCorrectedMerge(multiply(st, 288230376151711744)) FROM (SELECT cramersVBiasCorrectedState(toUInt8(number % 10), toUInt8(number % 6)) AS st FROM numbers(100)); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT theilsUMerge(multiply(st, 288230376151711744)) FROM (SELECT theilsUState(toUInt8(number % 10), toUInt8(number % 6)) AS st FROM numbers(100)); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT cramersVMerge(multiply(st, 9223372036854775806)) FROM (SELECT cramersVState(toUInt8(number % 10), toUInt8(number % 6)) AS st FROM numbers(100)); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- Window states.
SELECT finalizeAggregation(multiply(st, 9223372036854775806)) FROM (SELECT cramersVState(toUInt8(number % 10), toUInt8(number % 6)) OVER () AS st FROM numbers(100) LIMIT 1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT finalizeAggregation(multiply(st, 9223372036854775806)) FROM (SELECT theilsUState(toUInt8(number % 10), toUInt8(number % 6)) OVER () AS st FROM numbers(100) LIMIT 1); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- Window states merged into aggregation states: two states of 2^62 rows fit, two states of 2^63 rows do not.
SELECT cramersVMerge(multiply(st, 4611686018427387904)) FROM (SELECT cramersVState(toUInt8(number % 2), toUInt8(number % 2)) OVER (ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS st FROM numbers(2));
SELECT theilsUMerge(multiply(st, 4611686018427387904)) FROM (SELECT theilsUState(toUInt8(number % 2), toUInt8(number % 2)) OVER (ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS st FROM numbers(2));
SELECT cramersVMerge(multiply(st, 9223372036854775808)) FROM (SELECT cramersVState(toUInt8(number % 2), toUInt8(number % 2)) OVER (ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS st FROM numbers(2)); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT theilsUMerge(multiply(st, 9223372036854775808)) FROM (SELECT theilsUState(toUInt8(number % 2), toUInt8(number % 2)) OVER (ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS st FROM numbers(2)); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- Aggregation states merged into window states.
SELECT cramersVMerge(st) OVER () FROM (SELECT multiply(cramersVState(toUInt8(number % 2), toUInt8(number % 2)), 4611686018427387904) AS st FROM numbers(2) GROUP BY number);
SELECT cramersVMerge(st) OVER () FROM (SELECT multiply(cramersVState(toUInt8(number % 2), toUInt8(number % 2)), 9223372036854775808) AS st FROM numbers(2) GROUP BY number); -- { serverError ARGUMENT_OUT_OF_BOUND }
