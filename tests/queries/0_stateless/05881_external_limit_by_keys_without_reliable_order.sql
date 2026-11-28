-- A spilled `LIMIT BY` tells its keys apart by comparison, except for a key without a reliable order, which
-- it compares by fingerprint. Aggregate states compare as equal to each other, also inside `Dynamic` or a
-- `Variant`, and `JSON` can hold values of any type. Each query keeps the rows at ranks 1 and 2 of three
-- keys, as processing in memory does. A one-byte threshold spills from the first block on, and the threshold
-- is compared with the memory tracked by the query, so allocations must not linger in the thread-local
-- untracked buffers.
SET max_threads = 1, max_block_size = 2, max_untracked_memory = 0;
SET max_bytes_ratio_before_external_limit_by = 0, max_bytes_before_external_limit_by = 1;

SELECT count(), arraySort(groupArray(number)) FROM
(
    SELECT CAST(initializeAggregation('sumState', toUInt64(number % 3)), 'Dynamic') AS k, number
    FROM numbers(20)
    LIMIT 2 OFFSET 1 BY k
);
SELECT count(), arraySort(groupArray(number)) FROM
(
    SELECT CAST(initializeAggregation('sumState', toUInt64(number % 3)), 'Variant(AggregateFunction(sum, UInt64), String)') AS k,
        number
    FROM numbers(20)
    LIMIT 2 OFFSET 1 BY k
);
SELECT count(), arraySort(groupArray(number)) FROM
(
    SELECT CAST(concat('{"a": ', toString(number % 3), '}'), 'JSON') AS k, number
    FROM numbers(20)
    LIMIT 2 OFFSET 1 BY k
);
