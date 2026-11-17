-- Input blocks are smaller than a merged block; the re-merge must keep the peak memory bounded.
SET max_threads = 1, max_block_size = 65409, max_bytes_before_remerge_sort = 20000000,
    remerge_sort_lowered_memory_bytes_ratio = 2, max_bytes_before_external_sort = 0,
    max_bytes_ratio_before_external_sort = 0, max_memory_usage = '60Mi';

SELECT count(), sum(cityHash64(c1, c2))
FROM
(
    SELECT c2, c1
    FROM
    (
        SELECT toFixedString(concat('AB', substring(hex(sipHash128(number)), 1, 32)), 34) AS c1,
            toDateTime('2026-01-01 00:00:00', 'UTC') + intDiv(number * 7, 10) % 20000000 AS c2
        FROM numbers(5000000)
        WHERE number % 5 < 2
    )
    ORDER BY c2, c1
    LIMIT 196227
);
