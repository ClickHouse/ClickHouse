-- a copy the target index cannot use must not hide the target row estimate from join reordering
SET enable_parallel_replicas = 0;
SET use_hash_table_stats_for_join_reordering = 0;
SET query_plan_join_swap_table = 'auto', query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_shard_by_pk_ranges = 0;

DROP TABLE IF EXISTS prop_swap_s;
DROP TABLE IF EXISTS prop_swap_d;

CREATE TABLE prop_swap_s (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '', index_granularity = 8192;
CREATE TABLE prop_swap_d (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v SETTINGS auto_statistics_types = '', index_granularity = 8192;

INSERT INTO prop_swap_s SELECT number, number FROM numbers(100000);
-- every key of prop_swap_d passes the copied `k >= 99000`
INSERT INTO prop_swap_d SELECT number + 99000, number FROM numbers(1000000);

-- the reads in plan order: the first one is the probe side, the second one the build side
SELECT 'without the copy', arrayStringConcat(arrayFilter(x -> x != '', groupArray(extract(explain, 'ReadFromMergeTree \\(\\w+\\.(prop_swap_\\w)\\)'))), ' ')
FROM (
    EXPLAIN SELECT count() FROM (SELECT k FROM prop_swap_s WHERE k >= 99000) AS s INNER JOIN prop_swap_d AS d ON s.k = d.k
    SETTINGS query_plan_propagate_predicate_across_join = 0
);

SELECT 'with the copy', arrayStringConcat(arrayFilter(x -> x != '', groupArray(extract(explain, 'ReadFromMergeTree \\(\\w+\\.(prop_swap_\\w)\\)'))), ' ')
FROM (
    EXPLAIN SELECT count() FROM (SELECT k FROM prop_swap_s WHERE k >= 99000) AS s INNER JOIN prop_swap_d AS d ON s.k = d.k
    SETTINGS query_plan_propagate_predicate_across_join = 1
);

-- the copy is still made: the source filter and the copy
SELECT 'copied', countIf(explain LIKE '%ilter column:%k >= 99000%') >= 2
FROM (
    EXPLAIN actions = 1 SELECT count() FROM (SELECT k FROM prop_swap_s WHERE k >= 99000) AS s INNER JOIN prop_swap_d AS d ON s.k = d.k
    SETTINGS query_plan_propagate_predicate_across_join = 1
);

SELECT 'count', count() FROM (SELECT k FROM prop_swap_s WHERE k >= 99000) AS s INNER JOIN prop_swap_d AS d ON s.k = d.k
SETTINGS query_plan_propagate_predicate_across_join = 1;

DROP TABLE prop_swap_s;
DROP TABLE prop_swap_d;
