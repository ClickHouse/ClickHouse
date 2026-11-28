-- A `JSON` key writes the path of every value into its serialized form, while the byte size of the column
-- counts the path once: 65536 values under one 2048-byte path serialize into about 128 MiB of keys from a
-- column of 2 MiB. The spilling `LIMIT BY` estimates such keys from a sample of their serialized form before
-- it hashes a chunk whole, so it spills instead of growing its key arena past the memory limit.
SET max_threads = 1, max_block_size = 65536, max_untracked_memory = 0;
SET max_bytes_ratio_before_external_limit_by = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_json_keys;
-- A wide part names a file after the path, which is longer than a file name may be, so long names are hashed.
CREATE TABLE t_json_keys (k JSON, n UInt64) ENGINE = MergeTree ORDER BY n
SETTINGS index_granularity = 8192, replace_long_file_name_to_hash = 1, max_file_name_length = 127;
-- The cast writes the path for every value, so it runs in small blocks; the table stores the path once.
INSERT INTO t_json_keys
SELECT CAST(tuple(number), concat('Tuple(', repeat('p', 2048), ' UInt64)'))::JSON, number FROM numbers(65536)
SETTINGS max_block_size = 4096;

SELECT count(), uniqExact(n)
FROM (SELECT k, n FROM t_json_keys LIMIT 1 BY k)
SETTINGS max_memory_usage = 160000000, max_bytes_before_external_limit_by = 72000000,
    log_comment = '05484_external_limit_by_dynamic_key_growth';

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ExternalLimitByWritePart'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND log_comment = '05484_external_limit_by_dynamic_key_growth';

DROP TABLE t_json_keys;
