-- The adaptive aggregator with the methods of fixed-width keys and one `String` key (`keys32_packed_string`,
-- `keys64_packed_string`, `keys128_packed_string`). A key missed by a frozen table is staged as its packed fixed-width
-- values followed by the bytes of its string, and the merge rebuilds the key from the record. Each comparison runs the
-- adaptive aggregator with the method on in one subquery and the ordinary aggregator with the serialized method in the
-- other, so the profile events of the query belong to the adaptive aggregator with the method on.

-- The free-memory cap of the thread count is off, so the queries keep their four streams on a loaded server.
SET max_threads = 4;
SET max_threads_min_free_memory_per_thread = 0;
SET max_block_size = 8192;
SET optimize_aggregation_in_order = 0;
SET collect_hash_table_stats_during_aggregation = 0;
SET group_by_two_level_threshold = 100000;
SET group_by_two_level_threshold_bytes = 50000000;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;
SET adaptive_aggregator_freeze_threshold = 64;
-- The profile of the test server limits the number of groups, and an aggregation with such a limit prunes no groups
-- of a top-K. The distributed-plan suite serializes every plan, which the top-K plan pass refuses.
SET max_rows_to_group_by = 0;
SET serialize_query_plan = 0;
SET log_queries = 1;
SET log_profile_events = 1;

DROP VIEW IF EXISTS v_05482;

-- Empty strings, strings of zero bytes, strings of up to 26 bytes, around both inline limits, and longer ones.
CREATE VIEW v_05482 AS
SELECT
    toUInt32(intDiv(number, 3) % 4000) AS a,
    toUInt8(number % 3) AS b,
    toUInt16(number % 5) AS c,
    intDiv(number, 3) % 4000 * 1000003 AS d,
    multiIf(
        number % 29 = 0, '',
        number % 29 = 1, repeat('\0', 1 + number % 3),
        number % 29 = 2, repeat('y', 40 + number % 50),
        substring(concat(hex(cityHash64(intDiv(number, 7))), hex(cityHash64(intDiv(number, 11)))), 1, number % 29 - 2)) AS s
FROM numbers_mt(120000);

-- Records with a count.
SELECT 'keys32, count',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, count() FROM v_05482 GROUP BY a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, enable_adaptive_aggregator = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, count() FROM v_05482 GROUP BY a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0, enable_adaptive_aggregator = 0)
SETTINGS log_comment = '05482_keys32_count';

-- Records of the key alone.
SELECT 'keys32, keys only',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s FROM v_05482 GROUP BY a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, enable_adaptive_aggregator = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s FROM v_05482 GROUP BY a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0, enable_adaptive_aggregator = 0)
SETTINGS log_comment = '05482_keys32_keys_only';

-- Records with arguments, a `String` among them.
SELECT 'keys128, arguments',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT d, b, s, count(), sum(c), max(s) FROM v_05482 GROUP BY d, b, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, enable_adaptive_aggregator = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT d, b, s, count(), sum(c), max(s) FROM v_05482 GROUP BY d, b, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0, enable_adaptive_aggregator = 0)
SETTINGS log_comment = '05482_keys128_arguments';

-- The staged records are written to disk, and the merge copies the keys it reads back.
SELECT 'keys32, written to disk',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, count(), sum(d) FROM v_05482 GROUP BY a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, enable_adaptive_aggregator = 1, max_bytes_before_external_group_by = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, count(), sum(d) FROM v_05482 GROUP BY a, s)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0, enable_adaptive_aggregator = 0)
SETTINGS log_comment = '05482_keys32_external';

-- Groups with equal counts can take each other's place, so only the counts are compared.
SELECT 'keys64, ORDER BY count() LIMIT',
    (SELECT arraySort(groupArray(cnt)) FROM (SELECT d, s, count() AS cnt FROM v_05482 GROUP BY d, s ORDER BY cnt DESC LIMIT 10)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, enable_adaptive_aggregator = 1)
    = (SELECT arraySort(groupArray(cnt)) FROM (SELECT d, s, count() AS cnt FROM v_05482 GROUP BY d, s ORDER BY cnt DESC LIMIT 10)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0, enable_adaptive_aggregator = 0)
SETTINGS log_comment = '05482_keys64_top_k';

-- The merge counts the groups first and builds the other states for the best groups only. `count(total)` keeps
-- `sum(d)` in the aggregation, without depending on which groups of equal counts are taken.
SELECT 'keys32, ORDER BY count() LIMIT, counted first',
    (SELECT arraySort(groupArray(cnt)), count(total) FROM (SELECT a, s, count() AS cnt, sum(d) AS total FROM v_05482 GROUP BY a, s ORDER BY cnt DESC LIMIT 10)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 1, enable_adaptive_aggregator = 1)
    = (SELECT arraySort(groupArray(cnt)), count(total) FROM (SELECT a, s, count() AS cnt, sum(d) AS total FROM v_05482 GROUP BY a, s ORDER BY cnt DESC LIMIT 10)
        SETTINGS enable_fixed_and_packed_string_keys_in_aggregation = 0, enable_adaptive_aggregator = 0)
SETTINGS log_comment = '05482_keys32_count_first';

SYSTEM FLUSH LOGS query_log;

SELECT 'frozen', log_comment, ProfileEvents['AdaptiveAggregationLocalFreezes'] > 0, ProfileEvents['AdaptiveAggregationStagedRecords'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish'
    AND log_comment IN ('05482_keys32_count', '05482_keys32_keys_only', '05482_keys128_arguments', '05482_keys32_external',
        '05482_keys64_top_k', '05482_keys32_count_first')
ORDER BY log_comment;

SELECT 'written to disk', log_comment, ProfileEvents['AdaptiveAggregationSpills'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish' AND log_comment = '05482_keys32_external';

SELECT 'pruned', log_comment, ProfileEvents['AdaptiveAggregationPrunedRecords'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish'
    AND log_comment IN ('05482_keys64_top_k', '05482_keys32_count_first')
ORDER BY log_comment;

SELECT 'counted first', log_comment, ProfileEvents['AdaptiveAggregationCountFirstUnits'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish' AND log_comment = '05482_keys32_count_first';

DROP VIEW v_05482;
