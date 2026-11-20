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

-- With equal re-merge and spill thresholds, a useful re-merge must run before the sort spills to disk.
SELECT sum(k) FROM (SELECT sipHash64(number) AS k FROM numbers(1000000) ORDER BY k LIMIT 8192)
SETTINGS max_block_size = 4096, max_bytes_before_remerge_sort = 1048576, max_bytes_before_external_sort = 1048576,
    use_top_k_dynamic_filtering = 0, log_comment = '05828_remerge_before_spill';

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ExternalSortWritePart'] FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND log_comment = '05828_remerge_before_spill' AND type = 'QueryFinish';
