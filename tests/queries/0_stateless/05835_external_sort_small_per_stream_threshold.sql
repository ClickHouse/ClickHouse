-- External ORDER BY with a `max_bytes_before_external_sort` that is small for the number of sorting streams.

SET max_threads = 4;
SET max_threads_min_free_memory_per_thread = 0;
SET max_block_size = 8192;
SET max_bytes_ratio_before_external_sort = 0;
SET prefer_external_sort_block_bytes = DEFAULT;
SET temporary_files_buffer_size = 1048576;
-- Otherwise the payload is computed after the sort, and only the key is sorted.
SET query_plan_execute_functions_after_sorting = 0;

-- A threshold below the number of streams still spills.
SELECT number FROM numbers_mt(100000) ORDER BY number DESC FORMAT Null
SETTINGS max_bytes_before_external_sort = 1, log_comment = '05835_tiny';

-- Each stream spills runs of the whole threshold, not of a quarter of it.
SELECT cityHash64(number) AS k, randomPrintableASCII(24) AS s FROM numbers_mt(2000000) ORDER BY k FORMAT Null
SETTINGS max_bytes_before_external_sort = 8388608, log_comment = '05835_narrow';

-- The merge of the temporary files takes less memory than the sort in memory.
SELECT cityHash64(number) AS k, randomPrintableASCII(1000) AS s FROM numbers_mt(300000) ORDER BY k FORMAT Null
SETTINGS max_bytes_before_external_sort = 0, log_comment = '05835_wide_in_memory';

SELECT cityHash64(number) AS k, randomPrintableASCII(1000) AS s FROM numbers_mt(300000) ORDER BY k FORMAT Null
SETTINGS max_bytes_before_external_sort = 16777216, log_comment = '05835_wide_spilled';

SYSTEM FLUSH LOGS query_log;

SELECT 'tiny threshold spills', min(ProfileEvents['ExternalSortWritePart']) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05835_tiny';

SELECT 'narrow file count', min(ProfileEvents['ExternalSortWritePart']) >= 1 AND max(ProfileEvents['ExternalSortWritePart']) <= 16
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05835_narrow';

SELECT 'wide spilled sort uses less memory',
    minIf(ProfileEvents['ExternalSortWritePart'], log_comment = '05835_wide_spilled') > 0
    AND maxIf(memory_usage, log_comment = '05835_wide_spilled') < minIf(memory_usage, log_comment = '05835_wide_in_memory')
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05835_wide_in_memory', '05835_wide_spilled');
