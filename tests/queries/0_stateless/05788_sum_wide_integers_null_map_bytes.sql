-- With short-circuit evaluation disabled, `if` with a `NULL` branch reuses its condition as the null map, so a NULL may
-- be marked by any nonzero byte. The inputs below are [1, NULL], and the value hidden under the NULL must be ignored.
SET short_circuit_function_evaluation = 'disable', optimize_rewrite_aggregate_function_with_if = 0;

SELECT
    sum(if(toUInt8(number * 2), NULL, toInt128(number + 1))),
    sum(if(toUInt8(number * 2), NULL, toUInt128(number + 1))),
    sum(if(toUInt8(number * 2), NULL, toInt256(number + 1))),
    sum(if(toUInt8(number * 2), NULL, toUInt256(number + 1))),
    sum(if(toUInt8(number * 2), NULL, toDecimal128(number + 1, 0))),
    sum(if(toUInt8(number * 2), NULL, toDecimal256(number + 1, 0))),
    sumWithOverflow(if(toUInt8(number * 2), NULL, toInt256(number + 1))),
    avg(if(toUInt8(number * 2), NULL, toInt256(number + 1)))
FROM numbers(2);
