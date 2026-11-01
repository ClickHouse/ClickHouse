-- DPsub with `query_plan_join_order_use_interesting_orders` picks `full_sorting_merge` for joins of inputs sorted by the join key.

SET enable_analyzer = 1;
SET query_plan_optimize_join_order_algorithm = 'dpsub';
SET query_plan_join_order_use_interesting_orders = 1;
SET enable_join_transitive_predicates = 1;
SET optimize_read_in_order = 1;
SET max_rows_in_set_to_optimize_join = 0;
SET join_algorithm = 'hash';
SET query_plan_join_swap_table = 'auto';
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET optimize_sorting_by_input_stream_properties = 1;
SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
SET materialize_statistics_on_insert = 1;
SET use_statistics = 1;

DROP TABLE IF EXISTS sorted1;
DROP TABLE IF EXISTS sorted2;
DROP TABLE IF EXISTS sorted3;
DROP TABLE IF EXISTS sorted4;
DROP TABLE IF EXISTS unsorted1;
DROP TABLE IF EXISTS unsorted2;
DROP TABLE IF EXISTS tiny;
DROP TABLE IF EXISTS probe;
DROP TABLE IF EXISTS build_narrow;
DROP TABLE IF EXISTS build_wide;

CREATE TABLE sorted1 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE sorted2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE sorted3 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE sorted4 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE unsorted1 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE unsorted2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE tiny (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE probe (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE build_narrow (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE build_wide (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';

-- A merge join pays a fixed cost, so it wins only for inputs of a few million rows.
INSERT INTO sorted1 SELECT number, number FROM numbers(3000000);
INSERT INTO sorted2 SELECT number, number FROM numbers(3000000);
INSERT INTO sorted3 SELECT number, number FROM numbers(3000000);
INSERT INTO sorted4 SELECT number, number FROM numbers(3000000);
INSERT INTO unsorted1 SELECT number, number FROM numbers(3000000);
INSERT INTO unsorted2 SELECT number, number FROM numbers(3000000);
INSERT INTO tiny SELECT number * 1000, number FROM numbers(10);
-- Build keys in a quarter of the probe range, or spread over all of it.
INSERT INTO probe SELECT number, number FROM numbers(10000000);
INSERT INTO build_narrow SELECT number, number FROM numbers(2500000);
INSERT INTO build_wide SELECT number * 4, number FROM numbers(2500000);

SELECT '-- both inputs sorted by the key: merge join';
SELECT if(explain LIKE '%Algorithm:%', if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join'), replaceRegexpAll(explain, '^[│└├─ ]+', '')) FROM (EXPLAIN PLAN sorting = 1 SELECT sum(sorted1.v + sorted2.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k)
WHERE explain LIKE '%Algorithm:%' OR explain LIKE '%Read type:%'
ORDER BY 1;

SELECT '-- neither input sorted: hash join';
SELECT if(explain LIKE '%Algorithm:%', if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join'), replaceRegexpAll(explain, '^[│└├─ ]+', '')) FROM (EXPLAIN PLAN sorting = 1 SELECT sum(unsorted1.v + unsorted2.v) FROM unsorted1 JOIN unsorted2 ON unsorted1.k = unsorted2.k)
WHERE explain LIKE '%Algorithm:%';

SELECT '-- sorted inputs, but a tiny build side: hash join';
SELECT if(explain LIKE '%Algorithm:%', if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join'), replaceRegexpAll(explain, '^[│└├─ ]+', '')) FROM (EXPLAIN PLAN sorting = 1 SELECT sum(sorted1.v + tiny.v) FROM sorted1 JOIN tiny ON sorted1.k = tiny.k)
WHERE explain LIKE '%Algorithm:%';

SELECT '-- a chain on one key: both joins merge, the result of the first one is not sorted again';
SELECT if(explain LIKE '%Algorithm:%', if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join'), replaceRegexpAll(explain, '^[│└├─ ]+', '')) FROM (EXPLAIN PLAN sorting = 1
    SELECT sum(sorted1.v + sorted2.v + sorted3.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k JOIN sorted3 ON sorted2.k = sorted3.k)
WHERE explain LIKE '%Algorithm:%' OR explain LIKE '%Read type:%' OR explain LIKE '%Prefix sort description:%'
ORDER BY 1;

SELECT '-- a chain of four, in whatever tree shape: every input of every merge join is already sorted';
SELECT countIf(explain LIKE '%FullSortingMergeJoin%'), countIf(explain LIKE '%Sort % before JOIN%'), countIf(explain LIKE '%Prefix sort description:%')
FROM (EXPLAIN PLAN sorting = 1
    SELECT sum(sorted1.k + sorted2.k + sorted3.k + sorted4.k) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k
        JOIN sorted3 ON sorted2.k = sorted3.k JOIN sorted4 ON sorted3.k = sorted4.k);

SELECT '-- build keys in a narrow range: a hash join, whose runtime filter skips most of the sorted probe side';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN SELECT sum(probe.v + build_narrow.v) FROM probe JOIN build_narrow ON probe.k = build_narrow.k)
WHERE explain LIKE '%Algorithm:%';

SELECT '-- build keys spread over the probe range: the runtime filter skips nothing, a merge join';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN SELECT sum(probe.v + build_wide.v) FROM probe JOIN build_wide ON probe.k = build_wide.k)
WHERE explain LIKE '%Algorithm:%';

SELECT '-- without optimize_sorting_by_input_stream_properties a join result is sorted again, so the second join hashes';
SELECT if(explain LIKE '%Algorithm:%', if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join'), replaceRegexpAll(explain, '^[│└├─ ]+', '')) FROM (EXPLAIN PLAN sorting = 1
    SELECT sum(sorted1.v + sorted2.v + sorted3.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k JOIN sorted3 ON sorted2.k = sorted3.k)
WHERE explain LIKE '%Algorithm:%'
ORDER BY 1
SETTINGS optimize_sorting_by_input_stream_properties = 0;

SELECT '-- the setting is off: hash join';
SELECT if(explain LIKE '%Algorithm:%', if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join'), replaceRegexpAll(explain, '^[│└├─ ]+', '')) FROM (EXPLAIN PLAN sorting = 1 SELECT sum(sorted1.v + sorted2.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS query_plan_join_order_use_interesting_orders = 0;

SELECT '-- results';
SELECT count(), sum(sorted1.v + sorted2.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k;
SELECT count(), sum(sorted1.v + sorted2.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k
SETTINGS query_plan_join_order_use_interesting_orders = 0;
SELECT count(), sum(sorted1.v + sorted2.v + sorted3.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k JOIN sorted3 ON sorted2.k = sorted3.k;
SELECT count(), sum(sorted1.v + sorted2.v + sorted3.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k JOIN sorted3 ON sorted2.k = sorted3.k
SETTINGS query_plan_join_order_use_interesting_orders = 0;
SELECT count(), sum(sorted1.v + tiny.v) FROM sorted1 JOIN tiny ON sorted1.k = tiny.k;
SELECT count(), sum(probe.v + build_narrow.v) FROM probe JOIN build_narrow ON probe.k = build_narrow.k;
SELECT count(), sum(probe.v + build_wide.v) FROM probe JOIN build_wide ON probe.k = build_wide.k;
SELECT count(), sum(sorted1.v + sorted2.v + sorted3.v + sorted4.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k
    JOIN sorted3 ON sorted2.k = sorted3.k JOIN sorted4 ON sorted3.k = sorted4.k;
SELECT count(), sum(sorted1.v + sorted2.v + sorted3.v + sorted4.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k
    JOIN sorted3 ON sorted2.k = sorted3.k JOIN sorted4 ON sorted3.k = sorted4.k
SETTINGS query_plan_join_order_use_interesting_orders = 0;

DROP TABLE sorted1;
DROP TABLE sorted2;
DROP TABLE sorted3;
DROP TABLE sorted4;
DROP TABLE unsorted1;
DROP TABLE unsorted2;
DROP TABLE tiny;
DROP TABLE probe;
DROP TABLE build_narrow;
DROP TABLE build_wide;
