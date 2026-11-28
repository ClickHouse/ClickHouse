SET max_threads = 4;
SET max_threads_min_free_memory_per_thread = 0;
SET max_block_size = 65536;
SET group_by_two_level_threshold = 100000;
SET group_by_two_level_threshold_bytes = 50000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET optimize_aggregation_in_order = 0;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;
SET max_rows_to_group_by = 0;
SET enable_parallel_single_level_merge = 1;
SET enable_parallel_single_level_merge_by_index = 1;
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 16384;
-- Pin (randomized in CI): a lower byte bound freezes the tables, which are then not merged partition by partition.
SET adaptive_aggregator_freeze_threshold_bytes = 4194304;
SET enable_fixed_and_packed_string_keys_in_aggregation = 1;

-- Each lane feeds one thread, and every lane holds the same 1800 keys of a `UInt32` and a `String`, far fewer than
-- the freeze threshold. So every table stays single-level, and the tables merge partition by partition through their
-- cell indexes. The strings are empty, short enough to be inline in the key, or longer, so the merge reads both
-- sub-tables of the tables and keys whose strings stay in the arena of their table.
CREATE VIEW lanes AS
    SELECT number FROM numbers(0, 10000)
    UNION ALL SELECT number FROM numbers(10000, 10000)
    UNION ALL SELECT number FROM numbers(20000, 10000)
    UNION ALL SELECT number FROM numbers(30000, 10000);

CREATE VIEW keys AS
    SELECT
        toUInt32(number % 200) AS k,
        multiIf(number % 3 = 0, '', number % 3 = 1, toString(number % 7), repeat('x', 20 + number % 5)) AS s,
        number
    FROM lanes;

-- The map method `keys32_packed_string`, whose `uniqExact` states merge after the cells of each partition.
SELECT
    (SELECT sum(cityHash64(k, s, c, u)) FROM (SELECT k, s, count() AS c, uniqExact(number) AS u FROM keys GROUP BY k, s))
  = (SELECT sum(cityHash64(k, s, c, u)) FROM (SELECT k, s, count() AS c, uniqExact(number) AS u FROM keys GROUP BY k, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0, enable_adaptive_aggregator = 0)
SETTINGS log_comment = 'map';

-- Its key-only form `keys32_packed_string_void`, whose cells hold only the keys.
SELECT
    (SELECT (count(), sum(cityHash64(k, s))) FROM (SELECT k, s FROM keys GROUP BY k, s))
  = (SELECT (count(), sum(cityHash64(k, s))) FROM (SELECT k, s FROM keys GROUP BY k, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0, enable_adaptive_aggregator = 0)
SETTINGS log_comment = 'key-only';

-- The tables were indexed, and the merge read them through their indexes.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['AggregationIndexedTables'] > 0, ProfileEvents['AggregationPartitionsMergedByCellIndex'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
    AND log_comment IN ('map', 'key-only')
ORDER BY log_comment;
