-- With `max_rows_to_group_by` under `group_by_overflow_mode = 'any'`, the admitted keys survive
-- external-aggregation spills in the hash table itself, and every flush writes only the states
-- accumulated since the previous one. Re-seeding the keys into the table instead would make
-- every flush rewrite every admitted key, so the total spill volume would grow quadratically
-- with the input (and here, with a flush forced after every block, reach hundreds of megabytes).

SET max_threads = 1, max_block_size = 65536, enable_parallel_replicas = 0;
SET max_rows_to_group_by = 100000000, group_by_overflow_mode = 'any';
SET group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 0;
SET max_bytes_ratio_before_external_group_by = 0, max_untracked_memory = 0;
SET max_bytes_before_external_group_by = 1;
SET enable_adaptive_aggregator = 0;

-- Every key has exactly 10 rows, spread across blocks, so the rows of a key keep landing after
-- its states were flushed: any row lost to a flush shows up immediately.
-- A lone `count()` runs with the counter inlined in the hash table.
SELECT min(c), max(c), count()
FROM (SELECT number % 200000 AS k, count() AS c FROM numbers(2000000) GROUP BY k)
SETTINGS log_comment = '05493_linear_spill_count';

-- `sum` runs with ordinary aggregate states.
SELECT min(c), max(c), min(s = 10 * k + 9000000), count()
FROM (SELECT number % 200000 AS k, count() AS c, sum(number) AS s FROM numbers(2000000) GROUP BY k)
SETTINGS log_comment = '05493_linear_spill_sum';

SYSTEM FLUSH LOGS query_log;

-- Both queries spilled repeatedly, kept the admitted keys through every flush, and the total
-- spill volume stayed linear in the input (~2M rows of a UInt64 key with small states; the
-- quadratic re-seeded volume exceeds 90 MiB).
SELECT
    ProfileEvents['ExternalAggregationWritePart'] > 1 AS spilled_many_parts,
    ProfileEvents['AggregationKeptKeysSpills'] > 1 AS kept_keys,
    ProfileEvents['ExternalAggregationUncompressedBytes'] < 64000000 AS linear_spill
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment IN ('05493_linear_spill_count', '05493_linear_spill_sum')
    AND type = 'QueryFinish'
    AND event_date >= yesterday()
ORDER BY log_comment;
