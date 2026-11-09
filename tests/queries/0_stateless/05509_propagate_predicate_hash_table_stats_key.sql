-- a copy made after join reordering must not store the size of its filtered build side under the key of the unfiltered input
SET enable_parallel_replicas = 0;
SET join_algorithm = 'hash';
SET collect_hash_table_stats_during_joins = 1, use_hash_table_stats_for_join_reordering = 1;
SET query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_shard_by_pk_ranges = 0;

DROP TABLE IF EXISTS prop_stats_s;
DROP TABLE IF EXISTS prop_stats_d;

CREATE TABLE prop_stats_s (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '', index_granularity = 8192;
CREATE TABLE prop_stats_d (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v SETTINGS auto_statistics_types = '', index_granularity = 8192;

INSERT INTO prop_stats_s SELECT number, number FROM numbers(100000);
INSERT INTO prop_stats_d SELECT number % 100000, number FROM numbers(1000000);

-- builds a hash table from prop_stats_d with the copied `k < 100`: 1000 of its 1000000 rows
SELECT 'filtered build', count() FROM (SELECT k FROM prop_stats_s WHERE k < 100) AS s INNER JOIN prop_stats_d AS d ON s.k = d.k
SETTINGS query_plan_join_swap_table = 'false';

-- the reads in plan order: the first one is the probe side, the second one the build side
SELECT 'wider filter', arrayStringConcat(arrayFilter(x -> x != '', groupArray(extract(explain, 'ReadFromMergeTree \\(\\w+\\.(prop_stats_\\w)\\)'))), ' ')
FROM (
    EXPLAIN SELECT count() FROM (SELECT k FROM prop_stats_s WHERE k < 50000) AS s INNER JOIN prop_stats_d AS d ON s.k = d.k
    SETTINGS query_plan_join_swap_table = 'auto'
);

SELECT 'without the copy', arrayStringConcat(arrayFilter(x -> x != '', groupArray(extract(explain, 'ReadFromMergeTree \\(\\w+\\.(prop_stats_\\w)\\)'))), ' ')
FROM (
    EXPLAIN SELECT count() FROM (SELECT k FROM prop_stats_s WHERE k < 100) AS s INNER JOIN prop_stats_d AS d ON s.k = d.k
    SETTINGS query_plan_join_swap_table = 'auto', query_plan_propagate_predicate_across_join = 0
);

DROP TABLE prop_stats_s;
DROP TABLE prop_stats_d;
