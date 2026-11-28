-- Disable force_primary_key_reverse_order: the reference pins the choice of merge joins over inputs sorted by an ascending key
SET force_primary_key_reverse_order = 0;

-- A chain of joins on the sorting key, planned in groups of `query_plan_optimize_join_order_limit` tables: the joins between
-- the groups are sort-merge joins of sorted inputs too. The statistics hint gives the sizes the plans are chosen for.

SET enable_analyzer = 1;
SET query_plan_optimize_join_order_algorithm = 'dpsub';
SET query_plan_join_order_use_interesting_orders = 1;
SET enable_join_transitive_predicates = 1;
SET optimize_read_in_order = 1;
SET max_rows_in_set_to_optimize_join = 0;
SET query_plan_join_swap_table = 'auto';
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET optimize_sorting_by_input_stream_properties = 1;
SET use_statistics = 0;
SET use_hash_table_stats_for_join_reordering = 0;
SET query_plan_join_shard_by_pk_ranges = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;

CREATE TABLE t1 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t3 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t4 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO t1 SELECT number, number FROM numbers(1000);
INSERT INTO t2 SELECT number, number FROM numbers(1000);
INSERT INTO t3 SELECT number, number FROM numbers(1000);
INSERT INTO t4 SELECT number, number FROM numbers(1000);

SET param__internal_join_table_stat_hints = '{
    "t1": { "cardinality": 50000000, "distinct_keys": { "k": 50000000 } },
    "t2": { "cardinality": 50000000, "distinct_keys": { "k": 50000000 } },
    "t3": { "cardinality": 50000000, "distinct_keys": { "k": 50000000 } },
    "t4": { "cardinality": 50000000, "distinct_keys": { "k": 50000000 } }
}';

SELECT '-- planned two tables at a time: merge joins only, every input of a merge join already sorted, every join sharded';
SELECT countIf(explain LIKE '%FullSortingMergeJoin%'), countIf(explain LIKE '%HashJoin%'), countIf(explain LIKE '%Sort % before JOIN%'),
    countIf(explain LIKE '%Prefix sort description:%'), countIf(explain LIKE '%Sharding:%')
FROM (EXPLAIN PLAN sorting = 1 SELECT count() FROM t1 JOIN t2 USING (k) JOIN t3 USING (k) JOIN t4 USING (k))
SETTINGS query_plan_optimize_join_order_limit = 2;

SELECT '-- without optimize_sorting_by_input_stream_properties a join result is sorted again, so the joins between the groups hash';
SELECT countIf(explain LIKE '%FullSortingMergeJoin%'), countIf(explain LIKE '%HashJoin%')
FROM (EXPLAIN PLAN SELECT count() FROM t1 JOIN t2 USING (k) JOIN t3 USING (k) JOIN t4 USING (k))
SETTINGS query_plan_optimize_join_order_limit = 2, optimize_sorting_by_input_stream_properties = 0;

SELECT '-- results';
SELECT count(), sum(t1.v + t2.v + t3.v + t4.v) FROM t1 JOIN t2 USING (k) JOIN t3 USING (k) JOIN t4 USING (k)
SETTINGS query_plan_optimize_join_order_limit = 2;
SELECT count(), sum(t1.v + t2.v + t3.v + t4.v) FROM t1 JOIN t2 USING (k) JOIN t3 USING (k) JOIN t4 USING (k)
SETTINGS query_plan_optimize_join_order_limit = 2, query_plan_join_order_use_interesting_orders = 0;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
