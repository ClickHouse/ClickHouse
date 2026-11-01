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

DROP TABLE IF EXISTS sorted1;
DROP TABLE IF EXISTS sorted2;
DROP TABLE IF EXISTS sorted3;
DROP TABLE IF EXISTS unsorted1;
DROP TABLE IF EXISTS unsorted2;
DROP TABLE IF EXISTS tiny;

CREATE TABLE sorted1 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE sorted2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE sorted3 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE unsorted1 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE unsorted2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE tiny (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';

INSERT INTO sorted1 SELECT number, number FROM numbers(100000);
INSERT INTO sorted2 SELECT number * 2, number FROM numbers(100000);
INSERT INTO sorted3 SELECT number * 3, number FROM numbers(100000);
INSERT INTO unsorted1 SELECT number, number FROM numbers(100000);
INSERT INTO unsorted2 SELECT number * 2, number FROM numbers(100000);
INSERT INTO tiny SELECT number * 1000, number FROM numbers(10);

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

DROP TABLE sorted1;
DROP TABLE sorted2;
DROP TABLE sorted3;
DROP TABLE unsorted1;
DROP TABLE unsorted2;
DROP TABLE tiny;
