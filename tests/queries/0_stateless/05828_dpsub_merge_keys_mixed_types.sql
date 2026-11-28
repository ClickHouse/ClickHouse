-- Disable force_primary_key_reverse_order: the reference pins the choice of a merge join over inputs sorted by an ascending key
SET force_primary_key_reverse_order = 0;

-- A sort-merge join chosen by DPsub must not use a class of columns of different types as a merge key:
-- equality of `Date` and `DateTime` in two time zones is not transitive.
-- https://github.com/ClickHouse/ClickHouse/issues/130142

SET enable_analyzer = 1;
SET query_plan_optimize_join_order_algorithm = 'dpsub';
SET query_plan_join_order_use_interesting_orders = 1;
SET enable_join_transitive_predicates = 1;
SET optimize_read_in_order = 1;
SET optimize_sorting_by_input_stream_properties = 1;
SET query_plan_join_shard_by_pk_ranges = 1;
SET max_rows_in_set_to_optimize_join = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET use_statistics = 0;
SET use_hash_table_stats_for_join_reordering = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_mixed_a;
DROP TABLE IF EXISTS t_mixed_b;
DROP TABLE IF EXISTS t_mixed_c;

CREATE TABLE t_mixed_a (k UInt64, x Date) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_mixed_b (k UInt64, y DateTime('UTC')) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_mixed_c (k UInt64, z DateTime('Asia/Tokyo')) ENGINE = MergeTree ORDER BY (k, z);

-- For k = 1, `x = y` and `y = z` hold, `x = z` does not: the `Date` is midnight in UTC, not in Tokyo.
-- For k = 2, `x = y` holds, `y = z` does not, so the row must not match.
INSERT INTO t_mixed_a VALUES (1, '2024-01-01'), (2, '2024-01-01');
INSERT INTO t_mixed_b VALUES (1, toDateTime('2024-01-01 00:00:00', 'UTC')), (2, toDateTime('2024-01-01 00:00:00', 'UTC'));
INSERT INTO t_mixed_c VALUES (1, toDateTime('2024-01-01 00:00:00', 'UTC')), (2, toDateTime('2024-01-01 00:00:01', 'UTC'));

-- Joins `t_mixed_a` with `t_mixed_c` first as a sort-merge join on `k`.
SET param__internal_join_table_stat_hints = '{"t_mixed_a": {"cardinality": 20000000, "distinct_keys": {"k": 20000000}}, "t_mixed_b": {"cardinality": 100000000, "distinct_keys": {"k": 100000000}}, "t_mixed_c": {"cardinality": 50000000, "distinct_keys": {"k": 50000000}}}';

SELECT t_mixed_a.k
FROM t_mixed_a
JOIN t_mixed_b ON (t_mixed_a.k = t_mixed_b.k AND t_mixed_a.x = t_mixed_b.y)
JOIN t_mixed_c ON (t_mixed_b.k = t_mixed_c.k AND t_mixed_b.y = t_mixed_c.z)
ORDER BY ALL;

SELECT t_mixed_a.k
FROM t_mixed_a
JOIN t_mixed_b ON (t_mixed_a.k = t_mixed_b.k AND t_mixed_a.x = t_mixed_b.y)
JOIN t_mixed_c ON (t_mixed_b.k = t_mixed_c.k AND t_mixed_b.y = t_mixed_c.z)
ORDER BY ALL
SETTINGS query_plan_join_order_use_interesting_orders = 0;

-- A written key of columns of different types still costs sorting the inputs ordered only by `k` within its groups:
-- the join chooses the same algorithm as on columns of one type.
DROP TABLE IF EXISTS t_cost_a;
DROP TABLE IF EXISTS t_cost_mixed;
DROP TABLE IF EXISTS t_cost_same;
CREATE TABLE t_cost_a (k UInt64, x Date) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_cost_mixed (k UInt64, y DateTime('UTC')) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_cost_same (k UInt64, y Date) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_cost_a VALUES (1, '2024-01-01');
INSERT INTO t_cost_mixed VALUES (1, toDateTime('2024-01-01 00:00:00', 'UTC'));
INSERT INTO t_cost_same VALUES (1, '2024-01-01');

SET param__internal_join_table_stat_hints = '{"t_cost_a": {"cardinality": 100000000, "distinct_keys": {"k": 1000, "x": 30000}}, "t_cost_mixed": {"cardinality": 100000000, "distinct_keys": {"k": 1000, "y": 30000}}, "t_cost_same": {"cardinality": 100000000, "distinct_keys": {"k": 1000, "y": 30000}}}';

SELECT 'mixed types', if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join')
FROM (EXPLAIN PLAN SELECT count() FROM t_cost_a JOIN t_cost_mixed ON t_cost_a.k = t_cost_mixed.k AND t_cost_a.x = t_cost_mixed.y)
WHERE explain LIKE '%Algorithm:%';
SELECT 'one type', if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join')
FROM (EXPLAIN PLAN SELECT count() FROM t_cost_a JOIN t_cost_same ON t_cost_a.k = t_cost_same.k AND t_cost_a.x = t_cost_same.y)
WHERE explain LIKE '%Algorithm:%';

-- With inputs sorted by both keys, a monotonic cast of a key keeps the order: `UInt32` to `UInt64` needs no sorting,
-- so the inputs merge as they are. `Date` to `DateTime` is not monotonic on the whole type, so a merge would sort them.
DROP TABLE IF EXISTS t_sorted_u32;
DROP TABLE IF EXISTS t_sorted_u64;
DROP TABLE IF EXISTS t_sorted_date;
DROP TABLE IF EXISTS t_sorted_datetime;
CREATE TABLE t_sorted_u32 (k UInt64, x UInt32) ENGINE = MergeTree ORDER BY (k, x);
CREATE TABLE t_sorted_u64 (k UInt64, y UInt64) ENGINE = MergeTree ORDER BY (k, y);
CREATE TABLE t_sorted_date (k UInt64, x Date) ENGINE = MergeTree ORDER BY (k, x);
CREATE TABLE t_sorted_datetime (k UInt64, y DateTime('UTC')) ENGINE = MergeTree ORDER BY (k, y);
INSERT INTO t_sorted_u32 SELECT intDiv(number, 10), number % 10 FROM numbers(100);
INSERT INTO t_sorted_u64 SELECT intDiv(number, 10), number % 10 FROM numbers(100);
INSERT INTO t_sorted_date SELECT intDiv(number, 10), toDate('2024-01-01') + number % 10 FROM numbers(100);
INSERT INTO t_sorted_datetime SELECT intDiv(number, 10), toDateTime(toDate('2024-01-01') + number % 10, 'UTC') FROM numbers(100);

SET param__internal_join_table_stat_hints = '{"t_sorted_u32": {"cardinality": 5000000, "distinct_keys": {"k": 500000, "x": 10}}, "t_sorted_u64": {"cardinality": 5000000, "distinct_keys": {"k": 500000, "y": 10}}, "t_sorted_date": {"cardinality": 5000000, "distinct_keys": {"k": 500000, "x": 10}}, "t_sorted_datetime": {"cardinality": 5000000, "distinct_keys": {"k": 500000, "y": 10}}}';

SELECT 'widening cast', if(explain LIKE '%Algorithm:%', if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join'), replaceRegexpAll(explain, '^[│└├─ ]+', ''))
FROM (EXPLAIN PLAN sorting = 1 SELECT count() FROM t_sorted_u32 JOIN t_sorted_u64 ON t_sorted_u32.k = t_sorted_u64.k AND t_sorted_u32.x = t_sorted_u64.y)
WHERE explain LIKE '%Algorithm:%' OR explain LIKE '%Prefix sort description:%';
SELECT 'Date to DateTime', if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join')
FROM (EXPLAIN PLAN SELECT count() FROM t_sorted_date JOIN t_sorted_datetime ON t_sorted_date.k = t_sorted_datetime.k AND t_sorted_date.x = t_sorted_datetime.y)
WHERE explain LIKE '%Algorithm:%';
SELECT count() FROM t_sorted_u32 JOIN t_sorted_u64 ON t_sorted_u32.k = t_sorted_u64.k AND t_sorted_u32.x = t_sorted_u64.y;

DROP TABLE t_mixed_a;
DROP TABLE t_mixed_b;
DROP TABLE t_mixed_c;
DROP TABLE t_cost_a;
DROP TABLE t_cost_mixed;
DROP TABLE t_cost_same;
DROP TABLE t_sorted_u32;
DROP TABLE t_sorted_u64;
DROP TABLE t_sorted_date;
DROP TABLE t_sorted_datetime;
