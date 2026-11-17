-- The exact selectivity estimate of `IN` turns every set element into a point range. `-0.` and `0.` are
-- distinct set elements but equal values, so they must count as one point: the estimate of the set must
-- equal the estimate of `IN (0.)`.

DROP TABLE IF EXISTS t_in_set_equal;
DROP TABLE IF EXISTS t_in_set_equal_dim;
DROP TABLE IF EXISTS t_in_set_equal_estimates;

CREATE TABLE t_in_set_equal (k UInt64, f Float64) ENGINE = MergeTree ORDER BY k
SETTINGS auto_statistics_types = 'basic, uniq_v2';

CREATE TABLE t_in_set_equal_dim (id UInt64) ENGINE = MergeTree ORDER BY id;

CREATE TABLE t_in_set_equal_estimates (name String, estimated_rows Nullable(UInt64)) ENGINE = Memory;

-- Statistics are materialized by a merge, so two parts and an OPTIMIZE are required.
INSERT INTO t_in_set_equal SELECT number, number % 100 FROM numbers(50000);
INSERT INTO t_in_set_equal SELECT number + 50000, number % 100 FROM numbers(50000);
INSERT INTO t_in_set_equal_dim SELECT number FROM numbers(100);
OPTIMIZE TABLE t_in_set_equal FINAL;

-- Each estimate is the row count the join reorder derived for `t_in_set_equal` under the given predicate.
INSERT INTO t_in_set_equal_estimates
SELECT 'negative_zero_and_zero', toUInt64OrNull(extract(explain, 't_in_set_equal\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1
      SELECT count() FROM t_in_set_equal INNER JOIN t_in_set_equal_dim ON t_in_set_equal_dim.id = t_in_set_equal.k
      WHERE t_in_set_equal.f IN (-0., 0.)
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0, min_marks_per_part_to_load_statistics = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

INSERT INTO t_in_set_equal_estimates
SELECT 'zero', toUInt64OrNull(extract(explain, 't_in_set_equal\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1
      SELECT count() FROM t_in_set_equal INNER JOIN t_in_set_equal_dim ON t_in_set_equal_dim.id = t_in_set_equal.k
      WHERE t_in_set_equal.f IN (0.)
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0, min_marks_per_part_to_load_statistics = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

INSERT INTO t_in_set_equal_estimates
SELECT 'zero_and_one', toUInt64OrNull(extract(explain, 't_in_set_equal\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1
      SELECT count() FROM t_in_set_equal INNER JOIN t_in_set_equal_dim ON t_in_set_equal_dim.id = t_in_set_equal.k
      WHERE t_in_set_equal.f IN (0., 1.)
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0, min_marks_per_part_to_load_statistics = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

SELECT 'every estimate was extracted', count() = 3 AND countIf(estimated_rows > 0) = 3
FROM t_in_set_equal_estimates;

SELECT '-0. and 0. are one point',
       (SELECT estimated_rows FROM t_in_set_equal_estimates WHERE name = 'negative_zero_and_zero')
     = (SELECT estimated_rows FROM t_in_set_equal_estimates WHERE name = 'zero');

-- A second distinct value does widen the estimate, so the estimates above come from the statistics.
SELECT 'a second value widens the estimate',
       (SELECT estimated_rows FROM t_in_set_equal_estimates WHERE name = 'zero_and_one')
     > (SELECT estimated_rows FROM t_in_set_equal_estimates WHERE name = 'zero');

SELECT 'rows', countIf(f IN (-0., 0.)), countIf(f IN (0.)) FROM t_in_set_equal;

DROP TABLE t_in_set_equal_estimates;
DROP TABLE t_in_set_equal;
DROP TABLE t_in_set_equal_dim;
