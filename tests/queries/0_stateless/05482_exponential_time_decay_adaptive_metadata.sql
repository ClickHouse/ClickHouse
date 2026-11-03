SET allow_experimental_time_decay_aggregate_functions = 1;
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 0;
SET adaptive_aggregator_disable_thaw = 1;
SET max_threads = 2;
SET max_block_size = 256;
SET optimize_aggregation_in_order = 0;

-- Direct keys must retain their source metadata even when adaptive aggregation is requested.
SELECT count() = 4, sum(s) = 4999950000,
    countIf(tupleElement(k, 'value') = 2 AND tupleElement(k, 'timestamp') IN (0, 1, 2, 3)) = 4
FROM
(
    SELECT CAST((2., toFloat64(number % 4)), 'ExponentialTimeDecaying64(10, direct)') AS k,
        sum(number) AS s
    FROM numbers_mt(100000)
    GROUP BY k
);

SELECT count() = 4, sum(s) = 4999950000,
    countIf(tupleElement(k, 'value') = 2 AND tupleElement(k, 'timestamp') IN (0, 1, 2, 3)) = 4
FROM
(
    SELECT CAST((2., toFloat64(number % 4)), 'ExponentialTimeDecaying128(10, direct)') AS k,
        sum(number) AS s
    FROM numbers_mt(100000)
    GROUP BY k
);
