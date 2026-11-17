SET max_threads = 4;
SET max_threads_min_free_memory_per_thread = 0;
SET max_block_size = 65536;
SET group_by_two_level_threshold = 100000;
SET group_by_two_level_threshold_bytes = 50000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET optimize_aggregation_in_order = 0;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;
SET enable_parallel_single_level_merge = 1;
SET max_rows_to_group_by = 0;

-- Each lane feeds one producer, so every table holds all the keys of its lane whatever the scheduling.
CREATE VIEW lanes AS
    SELECT number FROM numbers(0, 200000)
    UNION ALL SELECT number FROM numbers(200000, 200000)
    UNION ALL SELECT number FROM numbers(400000, 200000)
    UNION ALL SELECT number FROM numbers(600000, 200000);

-- The string key `sk` of a number `k` fills every submap of the string table: 0 gives the empty key, and the
-- other numbers give keys of 1 to 34 bytes, some of which end in a zero byte. The `FixedString(16)` key `fk`
-- ends in a zero byte of padding for an odd `k`.
CREATE VIEW keys AS
    SELECT if(k = 0, '', concat(toString(k), repeat('s', k % 30), if(k % 11 = 0, '\0', ''))) AS sk,
        toFixedString(substring(concat(toString(k), repeat('f', 16)), 1, 16 - k % 2), 16) AS fk, number
    FROM (SELECT number % 8000 AS k, number FROM lanes);

-- No table freezes, so the partitions of the merge read the tables through their indexes. The states own
-- memory, so they stay in their source tables until the merge of their partition takes them.
SELECT 'string key, no freeze';
SELECT
    (SELECT sum(cityHash64(sk, u)) FROM (SELECT sk, uniqExact(intHash64(number) % 1000) AS u FROM keys GROUP BY sk))
  = (SELECT sum(cityHash64(sk, u)) FROM (SELECT sk, uniqExact(intHash64(number) % 1000) AS u FROM keys GROUP BY sk) SETTINGS enable_adaptive_aggregator = 0)
SETTINGS enable_adaptive_aggregator = 1, enable_packed_string_keys_in_aggregation = 0, log_comment = 'no_freeze';

-- Every table freezes at 1000 keys and stages the rows of the others, and the adaptive merge reads the frozen
-- tables through their indexes.
SELECT 'FixedString key, every table frozen';
SELECT
    (SELECT sum(cityHash64(fk, u)) FROM (SELECT fk, uniqExact(intHash64(number) % 1000) AS u FROM keys GROUP BY fk))
  = (SELECT sum(cityHash64(fk, u)) FROM (SELECT fk, uniqExact(intHash64(number) % 1000) AS u FROM keys GROUP BY fk) SETTINGS enable_adaptive_aggregator = 0)
SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 1000, log_comment = 'fixed_frozen';

-- The first lane holds distinct keys. The others repeat 10000 keys in a scattered order, 20 times each.
CREATE VIEW thawing_lanes AS
    SELECT if(k = 0, '', concat(toString(k), repeat('s', k % 30), if(k % 11 = 0, '\0', ''))) AS sk, n FROM
    (
        SELECT toUInt64(number) AS k, number AS n FROM numbers(0, 200000)
        UNION ALL SELECT intHash64(number) % 10000, number FROM numbers(200000, 200000)
        UNION ALL SELECT intHash64(number) % 10000, number FROM numbers(400000, 200000)
        UNION ALL SELECT intHash64(number) % 10000, number FROM numbers(600000, 200000)
    );

-- Every table freezes. The staged rows of the first lane do not repeat, so its table stays frozen, while the
-- other tables thaw on their repeating rows and grow past the two-level threshold. The adaptive merge reads
-- the frozen table through its index and the others through their buckets. The ordinary aggregation keeps
-- its tables single-level, so that only the adaptive tables convert.
SELECT 'string key, frozen table with thawed two-level tables';
SELECT
    (SELECT sum(cityHash64(sk, c, s)) FROM (SELECT sk, count() AS c, sum(n) AS s FROM thawing_lanes GROUP BY sk))
  = (SELECT sum(cityHash64(sk, c, s)) FROM (SELECT sk, count() AS c, sum(n) AS s FROM thawing_lanes GROUP BY sk)
     SETTINGS enable_adaptive_aggregator = 0, group_by_two_level_threshold = 1000000)
SETTINGS enable_adaptive_aggregator = 1, enable_packed_string_keys_in_aggregation = 0, adaptive_aggregator_freeze_threshold = 128,
    group_by_two_level_threshold = 1000, log_comment = 'thawed';

-- No table freezes, but the aggregation feeds a top-K, which the adaptive merge prunes through the indexes.
-- Without a freeze, the pruning needs heavy states and tables of at least 16384 cells and 256 per group that
-- the top keeps: each lane holds 6039 keys. Number `k` below 38 counts `4 * (2k + 1)` distinct values, number
-- 38 counts 224, and the other numbers count 4, so the top has no ties.
SELECT 'string key, no table frozen, top-K by uniqExact';
SELECT toUInt64OrZero(extract(sk, '^[0-9]+')) AS k, u FROM
(
    SELECT if(k = 0, '', concat(toString(k), repeat('s', k % 30), if(k % 11 = 0, '\0', ''))) AS sk, uniqExact(number) AS u
    FROM
    (
        SELECT if(number % 200000 < 1500, toUInt64(floor(sqrt(number % 200000))), 1000 + number % 200000) AS k, number
        FROM lanes WHERE number % 200000 < 7500
    )
    GROUP BY sk ORDER BY u DESC LIMIT 5
)
SETTINGS enable_adaptive_aggregator = 1, enable_packed_string_keys_in_aggregation = 0, log_comment = 'topk_uniq';

-- Every adaptive table is indexed. Only the thawed tables convert to two-level, at most once each, and only
-- the top-K prunes.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['AggregationIndexedTables'] > 0, ProfileEvents['AdaptiveAggregationLocalFreezes'] > 0,
    ProfileEvents['AggregationConvertedToTwoLevel'] > 0,
    ProfileEvents['AggregationConvertedToTwoLevel'] <= ProfileEvents['AdaptiveAggregationThaws'],
    ProfileEvents['AdaptiveAggregationPrunedUnits'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
    AND log_comment IN ('no_freeze', 'fixed_frozen', 'thawed', 'topk_uniq')
ORDER BY log_comment;
