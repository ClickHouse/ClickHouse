-- The parallel single-level merge of states with destructors, such as the sets of `uniqExact`, the arrays of `groupArray`
-- or the values of `quantileExact`, takes one of two paths. The cells of the tables of an adaptive aggregation are
-- indexed, and the merge takes them partition by partition of the indexes, the large states of a group together on the
-- thread pool. The other tables are walked source by source, and the large states merge on the thread pool after the
-- last source. Every result must equal the serial merge of the whole tables on both paths. The thresholds keep the tables
-- single-level and in memory, the keys are few enough that no adaptive table freezes, and the statistics stay off, so
-- the layout does not depend on previous runs.

SET max_threads = 4;
SET max_threads_min_free_memory_per_thread = 0;
SET group_by_two_level_threshold = 100000;
SET group_by_two_level_threshold_bytes = 50000000;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;
SET collect_hash_table_stats_during_aggregation = 0;
SET optimize_aggregation_in_order = 0;

SELECT 'tables merged source by source';
SET enable_adaptive_aggregator = 0;

SELECT 'uniqExact';
SELECT
    (SELECT sum(cityHash64(k, u)) FROM (SELECT toUInt64(number % 1000) AS k, uniqExact(intHash64(number) % 300) AS u FROM numbers_mt(400000) GROUP BY k) SETTINGS enable_parallel_single_level_merge = 0)
  = (SELECT sum(cityHash64(k, u)) FROM (SELECT toUInt64(number % 1000) AS k, uniqExact(intHash64(number) % 300) AS u FROM numbers_mt(400000) GROUP BY k) SETTINGS enable_parallel_single_level_merge = 1);

SELECT 'groupArray, quantileExact and sum';
SELECT
    (SELECT sum(cityHash64(k, a, q, s)) FROM (SELECT toUInt64(number % 500) AS k, arraySort(groupArray(number)) AS a, quantileExact(0.5)(number % 1000) AS q, sum(number) AS s FROM numbers_mt(200000) GROUP BY k) SETTINGS enable_parallel_single_level_merge = 0)
  = (SELECT sum(cityHash64(k, a, q, s)) FROM (SELECT toUInt64(number % 500) AS k, arraySort(groupArray(number)) AS a, quantileExact(0.5)(number % 1000) AS q, sum(number) AS s FROM numbers_mt(200000) GROUP BY k) SETTINGS enable_parallel_single_level_merge = 1);

SELECT '-ForEach over uniqExact';
SELECT
    (SELECT sum(cityHash64(k, u)) FROM (SELECT toUInt64(number % 300) AS k, uniqExactForEach([number % 1000, number % 7]) AS u FROM numbers_mt(200000) GROUP BY k) SETTINGS enable_parallel_single_level_merge = 0)
  = (SELECT sum(cityHash64(k, u)) FROM (SELECT toUInt64(number % 300) AS k, uniqExactForEach([number % 1000, number % 7]) AS u FROM numbers_mt(200000) GROUP BY k) SETTINGS enable_parallel_single_level_merge = 1);

-- The string hash table dispatches the keys over sub-tables by their length, and the empty key has a slot of its own.
SELECT 'string keys, short, long and empty';
SELECT
    (SELECT sum(cityHash64(k, u)) FROM (SELECT if(number % 11 = 0, '', concat('k', toString(number % 700), if(number % 3 = 0, '_long_enough_to_leave_the_packed_sub_tables', ''))) AS k, uniqExact(number % 300) AS u FROM numbers_mt(200000) GROUP BY k) SETTINGS enable_parallel_single_level_merge = 0, enable_packed_string_keys_in_aggregation = 0)
  = (SELECT sum(cityHash64(k, u)) FROM (SELECT if(number % 11 = 0, '', concat('k', toString(number % 700), if(number % 3 = 0, '_long_enough_to_leave_the_packed_sub_tables', ''))) AS k, uniqExact(number % 300) AS u FROM numbers_mt(200000) GROUP BY k) SETTINGS enable_parallel_single_level_merge = 1, enable_packed_string_keys_in_aggregation = 0);

-- An early `LIMIT` cancels the partitions that are still merging, whose source tables then destroy the states they keep.
SELECT 'early limit';
SELECT k, uniqExact(number % 1000) FROM (SELECT toUInt64(number % 1000) AS k, number FROM numbers_mt(2000000)) GROUP BY k LIMIT 1 FORMAT Null
SETTINGS enable_parallel_single_level_merge = 1;
SELECT 'ok';

SELECT 'tables merged partition by partition through their indexes';
SET enable_adaptive_aggregator = 1;

SELECT 'uniqExact';
SELECT
    (SELECT sum(cityHash64(k, u)) FROM (SELECT toUInt64(number % 1000) AS k, uniqExact(intHash64(number) % 300) AS u FROM numbers_mt(400000) GROUP BY k) SETTINGS enable_parallel_single_level_merge = 0)
  = (SELECT sum(cityHash64(k, u)) FROM (SELECT toUInt64(number % 1000) AS k, uniqExact(intHash64(number) % 300) AS u FROM numbers_mt(400000) GROUP BY k) SETTINGS enable_parallel_single_level_merge = 1);

-- Key 0 holds most of the values, so its states merge on the thread pool, while the small groups of the other keys merge
-- on the worker of their partition.
SELECT 'uniqExact group large enough for the thread pool';
SELECT k, uniqExact(n) FROM (SELECT if(number < 200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(240000)) GROUP BY k ORDER BY k
SETTINGS enable_parallel_single_level_merge = 1;

SELECT 'groupArray, quantileExact and sum';
SELECT
    (SELECT sum(cityHash64(k, a, q, s)) FROM (SELECT toUInt64(number % 500) AS k, arraySort(groupArray(number)) AS a, quantileExact(0.5)(number % 1000) AS q, sum(number) AS s FROM numbers_mt(200000) GROUP BY k) SETTINGS enable_parallel_single_level_merge = 0)
  = (SELECT sum(cityHash64(k, a, q, s)) FROM (SELECT toUInt64(number % 500) AS k, arraySort(groupArray(number)) AS a, quantileExact(0.5)(number % 1000) AS q, sum(number) AS s FROM numbers_mt(200000) GROUP BY k) SETTINGS enable_parallel_single_level_merge = 1);

SELECT '-ForEach over uniqExact';
SELECT
    (SELECT sum(cityHash64(k, u)) FROM (SELECT toUInt64(number % 300) AS k, uniqExactForEach([number % 1000, number % 7]) AS u FROM numbers_mt(200000) GROUP BY k) SETTINGS enable_parallel_single_level_merge = 0)
  = (SELECT sum(cityHash64(k, u)) FROM (SELECT toUInt64(number % 300) AS k, uniqExactForEach([number % 1000, number % 7]) AS u FROM numbers_mt(200000) GROUP BY k) SETTINGS enable_parallel_single_level_merge = 1);

-- The string hash table keeps its keys in submaps by their length, and the empty key in a slot of its own.
-- Its tables, which never freeze here, are indexed like any other, so a partition reads its cells from every
-- submap of every index.
SELECT 'string keys, short, long and empty';
SELECT
    (SELECT sum(cityHash64(k, u)) FROM (SELECT if(number % 11 = 0, '', concat('k', toString(number % 700), if(number % 3 = 0, '_long_enough_to_leave_the_packed_sub_tables', ''))) AS k, uniqExact(number % 300) AS u FROM numbers_mt(200000) GROUP BY k) SETTINGS enable_parallel_single_level_merge = 0, enable_packed_string_keys_in_aggregation = 0)
  = (SELECT sum(cityHash64(k, u)) FROM (SELECT if(number % 11 = 0, '', concat('k', toString(number % 700), if(number % 3 = 0, '_long_enough_to_leave_the_packed_sub_tables', ''))) AS k, uniqExact(number % 300) AS u FROM numbers_mt(200000) GROUP BY k) SETTINGS enable_parallel_single_level_merge = 1, enable_packed_string_keys_in_aggregation = 0);

SELECT 'early limit';
SELECT k, uniqExact(number % 1000) FROM (SELECT toUInt64(number % 1000) AS k, number FROM numbers_mt(2000000)) GROUP BY k LIMIT 1 FORMAT Null
SETTINGS enable_parallel_single_level_merge = 1;
SELECT 'ok';
