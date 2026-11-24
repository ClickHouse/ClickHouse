-- `LIMIT` is smaller than `max_block_size`; the re-merge must keep the peak memory bounded.
SET max_threads = 1, max_block_size = 8192, max_bytes_before_remerge_sort = 1000000,
    remerge_sort_lowered_memory_bytes_ratio = 2, max_bytes_before_external_sort = 0,
    max_bytes_ratio_before_external_sort = 0, max_memory_usage = '60Mi';

SELECT count(), sum(k)
FROM
(
    SELECT k, s
    FROM (SELECT sipHash64(number) AS k, toFixedString(toString(number), 72) AS s FROM numbers(5000000))
    ORDER BY k, s
    LIMIT 1000
);
