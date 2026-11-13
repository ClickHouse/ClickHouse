-- The merges of an adaptive aggregation read the cells of a single-level table through its cell index, a partition of
-- the cells at a time, instead of converting the table to two-level or walking every table for every partition. Every
-- result must equal the aggregation with the adaptive aggregator off, and the counters show which tables were indexed.
-- Each comparison runs the adaptive aggregation in the first subquery and the ordinary one in the second, whose
-- `SETTINGS` override those of the query. The thresholds keep the ordinary tables single-level, and the statistics stay
-- off, so the layout does not depend on previous runs.

SET max_threads = 4;
SET max_threads_min_free_memory_per_thread = 0;
SET max_block_size = 65536;
SET group_by_two_level_threshold = 100000;
SET group_by_two_level_threshold_bytes = 50000000;
-- Pin the adaptive aggregator freeze thresholds to their defaults: the randomizer lowers them, so a
-- thread's local table freezes earlier and the indexed-table counters the test checks change.
SET adaptive_aggregator_freeze_threshold = 16384;
SET adaptive_aggregator_freeze_threshold_bytes = 4194304;
SET collect_hash_table_stats_during_aggregation = 0;
SET optimize_aggregation_in_order = 0;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;
SET enable_parallel_single_level_merge = 1;
SET max_rows_to_group_by = 0;
SET query_plan_aggregation_bucket_top_k = 1; -- The top-K pruning of the adaptive merge

-- Each lane feeds one producer, so every table holds all the keys of its lane whatever the scheduling.
CREATE VIEW lanes AS
    SELECT number FROM numbers(0, 200000)
    UNION ALL SELECT number FROM numbers(200000, 200000)
    UNION ALL SELECT number FROM numbers(400000, 200000)
    UNION ALL SELECT number FROM numbers(600000, 200000);

-- The first lane holds distinct keys. The others repeat 10000 keys in a scattered order, 20 times each.
CREATE VIEW thawing_lanes AS
    SELECT toUInt64(number) AS k, number AS n FROM numbers(0, 200000)
    UNION ALL SELECT intHash64(number) % 10000, number FROM numbers(200000, 200000)
    UNION ALL SELECT intHash64(number) % 10000, number FROM numbers(400000, 200000)
    UNION ALL SELECT intHash64(number) % 10000, number FROM numbers(600000, 200000);

-- No table freezes: each holds 8000 keys, and the partitions of the merge read them through the indexes.
SELECT 'cheap states, no freeze';
SELECT
    (SELECT sum(cityHash64(k, s)) FROM (SELECT toUInt64(number % 8000) AS k, sum(number) AS s FROM lanes GROUP BY k))
  = (SELECT sum(cityHash64(k, s)) FROM (SELECT toUInt64(number % 8000) AS k, sum(number) AS s FROM lanes GROUP BY k) SETTINGS enable_adaptive_aggregator = 0)
SETTINGS enable_adaptive_aggregator = 1, log_comment = 'cheap';

-- Every cell holds 25 strings of 100 bytes, about 4 KiB, so a worker emits a chunk per about 1000 rows of its 2000: the
-- next call of the worker goes on claiming partitions into a new table. The ordinary aggregation keeps its tables
-- single-level, so that only the adaptive tables could convert.
SELECT 'large states, no freeze, several chunks per worker';
SELECT
    (SELECT sum(cityHash64(k, a)) FROM (SELECT toUInt64(number % 8000) AS k, arraySort(groupArray(concat(repeat('x', 92), toString(number)))) AS a FROM lanes GROUP BY k))
  = (SELECT sum(cityHash64(k, a)) FROM (SELECT toUInt64(number % 8000) AS k, arraySort(groupArray(concat(repeat('x', 92), toString(number)))) AS a FROM lanes GROUP BY k)
     SETTINGS enable_adaptive_aggregator = 0, group_by_two_level_threshold_bytes = 0)
SETTINGS enable_adaptive_aggregator = 1, log_comment = 'chunks';

-- A single `Nullable(Int128)` key packs its null map into the key (`nullable_keys256`), so NULL is an
-- ordinary cell. The single-key Nullable methods keep NULL in a separate cell instead, and the adaptive
-- aggregator does not take them. No table freezes: each holds 8001 keys, NULL included, and the partitions of
-- the merge read them through the indexes.
SELECT 'single Nullable(Int128) key, no freeze';
SELECT
    (SELECT sum(cityHash64(isNull(k), ifNull(k, 0), s)) FROM (SELECT if(number % 7 = 0, NULL, toInt128(number % 8000)) AS k, sum(number) AS s FROM lanes GROUP BY k))
  = (SELECT sum(cityHash64(isNull(k), ifNull(k, 0), s)) FROM (SELECT if(number % 7 = 0, NULL, toInt128(number % 8000)) AS k, sum(number) AS s FROM lanes GROUP BY k) SETTINGS enable_adaptive_aggregator = 0)
SETTINGS enable_adaptive_aggregator = 1, log_comment = 'nullable_wide';

-- Every table freezes at 1000 keys and stages the rows of the other keys. The adaptive merge reads the cells of the
-- frozen tables through their indexes. The states own memory, so a table must not destroy the states that the merge
-- took from it.
SELECT 'every table frozen';
SELECT
    (SELECT sum(cityHash64(k, u)) FROM (SELECT toUInt64(number % 40000) AS k, uniqExact(intHash64(number) % 1000) AS u FROM lanes GROUP BY k))
  = (SELECT sum(cityHash64(k, u)) FROM (SELECT toUInt64(number % 40000) AS k, uniqExact(intHash64(number) % 1000) AS u FROM lanes GROUP BY k) SETTINGS enable_adaptive_aggregator = 0)
SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 1000, log_comment = 'frozen';

-- The first lane freezes on its 40000 keys, and the other three hold 500 keys and stay learning. The top-K pruning by
-- the count reads the buckets of the learning tables through their indexes. Key `k` below 500 counts `3 * (2k + 1)`
-- rows and the other keys count 5, so the top has no ties.
SELECT 'some tables frozen, top-K';
SELECT k, c FROM
(
    SELECT k, count() AS c FROM
    (
        SELECT toUInt64(1000 + number % 40000) AS k FROM numbers(0, 200000)
        UNION ALL SELECT toUInt64(floor(sqrt(number))) FROM numbers(0, 250000)
        UNION ALL SELECT toUInt64(floor(sqrt(number))) FROM numbers(0, 250000)
        UNION ALL SELECT toUInt64(floor(sqrt(number))) FROM numbers(0, 250000)
    )
    GROUP BY k ORDER BY c DESC LIMIT 5
)
SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 1000, log_comment = 'mixed';

-- Every table freezes. The staged rows of the first lane do not repeat, so its table stays frozen, while the other
-- tables thaw on their repeating rows and grow past the two-level threshold. The adaptive merge reads the frozen table
-- through its index and the others through their bucket tables, so the frozen table is not converted. The ordinary
-- aggregation keeps its tables single-level, so that only the adaptive tables convert.
SELECT 'frozen table with thawed two-level tables';
SELECT
    (SELECT sum(cityHash64(k, c, s)) FROM (SELECT k, count() AS c, sum(n) AS s FROM thawing_lanes GROUP BY k))
  = (SELECT sum(cityHash64(k, c, s)) FROM (SELECT k, count() AS c, sum(n) AS s FROM thawing_lanes GROUP BY k)
     SETTINGS enable_adaptive_aggregator = 0, group_by_two_level_threshold = 1000000)
SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 128, group_by_two_level_threshold = 1000, log_comment = 'thawed';

-- No table freezes, but the aggregation feeds a top-K, which only the adaptive merge prunes: it reads the tables through
-- their indexes and takes the buckets with the largest counts first. Without a frozen table, the pruning pays for the
-- buckets of the adaptive merge when it skips the merge of heavy states, and the tables hold at least 16384 cells and
-- 256 per group that the top keeps. Each lane holds 6039 keys: key `k` below 38 counts `4 * (2k + 1)` rows, distinct
-- ones for `uniqExact`, key 38 counts 224, and the other keys count 4, so the top has no ties.
SELECT 'no table frozen, top-K by uniqExact';
SELECT k, u FROM
(
    SELECT if(number % 200000 < 1500, toUInt64(floor(sqrt(number % 200000))), 1000 + number % 200000) AS k, uniqExact(number) AS u
    FROM lanes WHERE number % 200000 < 7500 GROUP BY k ORDER BY u DESC LIMIT 5
)
SETTINGS enable_adaptive_aggregator = 1, log_comment = 'topk_uniq';

-- An early `LIMIT` cancels the adaptive merge, and the source tables destroy the states they keep.
SELECT 'early limit';
SELECT k, uniqExact(number) FROM (SELECT toUInt64(number % 40000) AS k, number FROM lanes) GROUP BY k LIMIT 1 FORMAT Null
SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 1000;
SELECT 'ok';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['AggregationIndexedTables'] > 0, ProfileEvents['AdaptiveAggregationLocalFreezes'] > 0,
    ProfileEvents['AggregationConvertedToTwoLevel'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
    AND log_comment IN ('cheap', 'chunks', 'frozen', 'mixed', 'nullable_wide')
ORDER BY log_comment;

-- Only the thawed tables convert to two-level, each once, and the frozen one stays indexed.
SELECT log_comment, ProfileEvents['AggregationIndexedTables'] > 0, ProfileEvents['AdaptiveAggregationThaws'] > 0,
    ProfileEvents['AggregationConvertedToTwoLevel'] BETWEEN 1 AND ProfileEvents['AdaptiveAggregationThaws']
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND log_comment = 'thawed';

SELECT log_comment, ProfileEvents['AdaptiveAggregationLocalFreezes'] > 0, ProfileEvents['AdaptiveAggregationPrunedUnits'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND log_comment = 'topk_uniq';
