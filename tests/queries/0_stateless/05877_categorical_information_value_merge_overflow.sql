-- Merging `categoricalInformationValue` states must throw when a count does not fit into UInt64, instead of wrapping it.

-- Counts scaled by 2^61 or 2^62 still fit, and the result does not change.
SELECT finalizeAggregation(multiply(s, toUInt64(2305843009213693952))) = finalizeAggregation(s) FROM (SELECT categoricalInformationValueState(number % 2 = 0, number % 3 = 0) AS s FROM numbers(5));
SELECT categoricalInformationValueMerge(s) FROM (SELECT multiply(categoricalInformationValueState(number % 2 = 0, number % 3 = 0), toUInt64(4611686018427387904)) AS s FROM numbers(4) GROUP BY number % 2);

-- Counts past 2^64.
SELECT finalizeAggregation(multiply(s, toUInt64(9223372036854775808))) FROM (SELECT categoricalInformationValueState(number % 2 = 0, number % 3 = 0) AS s FROM numbers(4)); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT finalizeAggregation(multiply(s, toUInt64(9223372036854775809))) FROM (SELECT categoricalInformationValueState(number % 2 = 0, number % 3 = 0) AS s FROM numbers(5)); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT categoricalInformationValueMerge(s) FROM (SELECT multiply(categoricalInformationValueState(number % 2 = 0, number % 3 = 0), toUInt64(9223372036854775808)) AS s FROM numbers(4) GROUP BY number % 2); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- Crafted states whose per-category `no` or `yes` counter overflows while both totals fit.
SELECT finalizeAggregation(multiply(CAST(unhex('0000000000000080000000000000000001000000000000000100000000000000'), 'AggregateFunction(categoricalInformationValue, UInt8, UInt8)'), toUInt64(2))); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT finalizeAggregation(multiply(CAST(unhex('0000000000000000000000000000008001000000000000000100000000000000'), 'AggregateFunction(categoricalInformationValue, UInt8, UInt8)'), toUInt64(2))); -- { serverError ARGUMENT_OUT_OF_BOUND }
